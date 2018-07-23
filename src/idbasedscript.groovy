/*
 * PutElasticSearch5IndexProcessor.groovy
 */

import java.nio.charset.*
import java.util.concurrent.atomic.*

import org.apache.commons.lang.*
import org.apache.nifi.annotation.lifecycle.*
import org.apache.nifi.components.PropertyDescriptor
import org.apache.nifi.components.ValidationContext
import org.apache.nifi.components.ValidationResult
import org.apache.nifi.flowfile.*
import org.apache.nifi.logging.*
import org.apache.nifi.processor.*
import org.apache.nifi.processor.exception.*
import org.apache.nifi.processor.io.*
import org.elasticsearch.*
import org.elasticsearch.action.bulk.*
import org.elasticsearch.client.*
import org.elasticsearch.client.transport.*
import org.elasticsearch.common.settings.*
import org.elasticsearch.common.transport.*
import org.elasticsearch.common.unit.TimeValue
import org.elasticsearch.common.xcontent.XContentType
import org.elasticsearch.script.Script
import org.elasticsearch.script.ScriptType
import org.elasticsearch.transport.client.*

import com.sangupta.murmur.*
import groovy.json.*

class PutElasticSearch5IndexProcessorScript implements Processor {
    static final Relationship REL_SUCCESS = new Relationship.Builder().name("success")
            .description("All FlowFiles that are written to Elasticsearch are routed to this relationship").build();

    static final Relationship REL_FAILURE = new Relationship.Builder().name("failure")
            .description("All FlowFiles that cannot be written to Elasticsearch are routed to this relationship").build();

    static final Relationship REL_RETRY = new Relationship.Builder().name("retry")
            .description("A FlowFile is routed to this relationship if the database cannot be updated but attempting the operation again may succeed")
            .build();

    private static final Set<Relationship> relationships;

    static {
        final Set<Relationship> _rels = new HashSet<>();
        _rels.add(REL_SUCCESS);
        _rels.add(REL_FAILURE);
        _rels.add(REL_RETRY);
        relationships = Collections.unmodifiableSet(_rels);
    }

    private ComponentLog logger;

    @Override
    public void initialize(ProcessorInitializationContext context) {
        logger = context.getLogger();
    }

    @Override
    Set<Relationship> getRelationships() {
        return relationships
    }

    protected final AtomicReference<Client> esClient = new AtomicReference<>();
    protected List<InetSocketAddress> esHosts;

    @Override
    void onTrigger(ProcessContext context, ProcessSessionFactory sessionFactory) throws ProcessException {
        synchronized (esClient) {
            if(esClient.get() == null) {
                createElasticsearchClient(context);
            }
        }

        final def session = sessionFactory.createSession()

        try {
            final FlowFile flowFile = session.get();
            if (flowFile == null) {
                return;
            }

            // /user/hive/warehouse/sys_search.db/mel_search_keyword
            final String path = flowFile.getAttribute('path');
            String[] pathDetail = StringUtils.split(path, "/=");

            // mel_search_keyword
            final String indexFromPath = StringUtils.join(pathDetail, '_', 4, pathDetail.length);
            final String index = StringUtils.defaultString(flowFile.getAttribute('index'), indexFromPath);
            final String docType = 'detail';
            final String indexOp = StringUtils.defaultString(flowFile.getAttribute('index-op'), 'index');
            final Charset charset = Charset.forName('UTF-8');

            logger.warn(index);

            try {
                final BulkRequestBuilder bulk = esClient.get().prepareBulk().setTimeout(TimeValue.timeValueMinutes(1));
                session.read(flowFile, { inputStream ->
                    final BufferedReader inReader = new BufferedReader(new InputStreamReader(inputStream, 'UTF-8'))
                    String line
                    while (line = inReader.readLine()) {
                        String id = null, script = null;
                        Map<String, Object> paramsMap = new HashMap<>();
                        def jsonObject = new JsonSlurper().parseText(line);

                        for (entry in jsonObject) {
                            if (entry.key.startsWith("_")) {
                                if (entry.key.equals('_id')) {
                                    id = entry.value;
                                } else if (entry.key.equals('_script')) {
                                    script = entry.value;
                                } else if (entry.key.equals('_script_params')) {
                                    String scriptParams = entry.value;
                                    String[] params = scriptParams.split(",");
                                    for (String p: params) {
                                        String[] keyValue = p.split(":");
                                        paramsMap.put(keyValue[0], keyValue[1]);
                                    }
                                } else {
                                    logger.warn("Unknwon document field starts with underscore, field name: " + entry.key + ", value: " + entry.value + ".");
                                }
                            }
                        }

                        jsonObject.entrySet().removeIf{e -> e.getKey().startsWith('_')}

                        assert jsonObject.containsKey('_id') == false

                        byte[] bytes = JsonOutput.toJson(jsonObject).getBytes(charset);

                        if (id == null) {
                            if (indexOp.equalsIgnoreCase("delete")) {
                                throw new ProcessException("delete operation must be set to id");
                            }

                            id = '' + Murmur2.hash64(bytes, bytes.length, 0x7f3a21eal)
                        }

                        if (indexOp.equalsIgnoreCase("index")) {
                            bulk.add(esClient.get().prepareIndex(index, docType, id).setSource(bytes, XContentType.JSON));
                        } else if (indexOp.equalsIgnoreCase("update")) {
                            bulk.add(esClient.get().prepareUpdate(index, docType, id).setDoc(bytes, XContentType.JSON));
                        } else if (indexOp.equalsIgnoreCase("upsert")) {
                            if ( script == null ) {
                                bulk.add(esClient.get().prepareUpdate(index, docType, id).setDoc(json.getBytes(charset)).setDocAsUpsert(true).setRetryOnConflict(3));
                            } else {
                                Script upsertScript = new Script(ScriptType.INLINE, "painless", script, paramsMap);
                                bulk.add(esClient.get().prepareUpdate(index, docType, id).setUpsert(bytes, XContentType.JSON).setScript(upsertScript).setRetryOnConflict(3));
                            }
                        } else if (indexOp.equalsIgnoreCase("delete")) {
                            bulk.add(esClient.get().prepareDelete(index, docType, id));
                        } else {
                            throw new ProcessException("Index operation: " + indexOp + " not supported.");
                        }
                    }
                } as InputStreamCallback)

                if (bulk.numberOfActions() > 0) {
                    final BulkResponse response = bulk.execute().actionGet(TimeValue.timeValueMinutes(1)); // 최대 2분 대기
                    if (response.hasFailures()) {
                        BulkItemResponse[] responses = response.getItems();
                        if (responses != null && responses.length > 0) {
                            for (int i = responses.length - 1; i >= 0; i--) {
                                final BulkItemResponse item = responses[i];
                                if (item.isFailed()) {
                                    logger.warn("Failed to insert into Elasticsearch due to {}, transferring to failure", [
                                            item.getFailure().getMessage()
                                    ]);
                                }
                            }
                        }
                        session.transfer(flowFile, REL_FAILURE);
                    } else {
                        session.transfer(flowFile, REL_SUCCESS);
                    }
                } else {
                    session.transfer(flowFile, REL_SUCCESS);
                }
            } catch (exceptionToFail) {
                logger.error("Failed to insert into Elasticsearch due to {}, transferring to failure",[
                        exceptionToFail.getLocalizedMessage()
                ], exceptionToFail);
                session.transfer(flowFile, REL_RETRY);
                context.yield();
            }

            session.commit()
        } catch (e) {
            logger.error("{} failed to process due to {}; rolling back session", [this, e]);
            session.rollback(true);
            throw new ProcessException(e)
        }
    }

    protected void createElasticsearchClient(ProcessContext context) throws ProcessException {
        if (esClient.get() != null) {
            return;
        }

        final String clusterName = context.getProperty("cluster_name").getValue();
        final String hosts = context.getProperty("hosts").getValue();

        if ( clusterName == null || hosts == null ) {
            throw new ProcessException("Property cluster_name and hosts must be set");
        }

        logger.debug("Creating ElasticSearch Client");
        try {
            Settings.Builder settingsBuilder = Settings.builder()
                    .put("path.home", ".")
                    .put("cluster.name", clusterName)
                    .put("client.transport.ping_timeout", "10s")
                    .put("client.transport.nodes_sampler_interval", "10s");

            esHosts = getEsHosts(hosts);
            Client transportClient = getTransportClient(settingsBuilder, esHosts);
            esClient.set(transportClient);
        } catch (Exception e) {
            logger.error("Failed to create Elasticsearch client due to {}", [e], e);
            throw new ProcessException(e);
        }
    }

    protected Client getTransportClient(Settings.Builder settingsBuilder, List<InetSocketAddress> esHosts) throws MalformedURLException {
        // Map of headers
        Map<String, String> headers = new HashMap<>();
        TransportClient transportClient = new PreBuiltTransportClient(settingsBuilder.build());

        if(esHosts != null) {
            for (final InetSocketAddress host : esHosts) {
                try {
                    transportClient.addTransportAddress(new InetSocketTransportAddress(host));
                } catch (IllegalArgumentException iae) {
                    logger.error("Could not add transport address {}", [host]);
                }
            }
        }

        Client client = transportClient.filterWithHeader(headers);
        return client;
    }

    @OnStopped
    public void closeClient() {
        Client client = esClient.get();
        if (client != null) {
            logger.info("Closing ElasticSearch Client");
            esClient.set(null);
            client.close();
        }
    }

    private List<InetSocketAddress> getEsHosts(String hosts) {
        if (hosts == null) {
            return null;
        }
        final List<String> esList = Arrays.asList(hosts.split(","));
        List<InetSocketAddress> esHosts = new ArrayList<>();

        for (String item : esList) {

            String[] addresses = item.split(":");
            final String hostName = addresses[0].trim();
            final int port = Integer.parseInt(addresses[1].trim());

            esHosts.add(new InetSocketAddress(hostName, port));
        }
        return esHosts;
    }

    @Override
    public Collection<ValidationResult> validate(ValidationContext context) {return null;}

    @Override
    public PropertyDescriptor getPropertyDescriptor(String name) {return null;}

    @Override
    public void onPropertyModified(PropertyDescriptor descriptor, String oldValue, String newValue) {}

    @Override
    public List<PropertyDescriptor> getPropertyDescriptors() {return null;}

    @Override
    public String getIdentifier() {return null;}

}

processor = new PutElasticSearch5IndexProcessorScript()