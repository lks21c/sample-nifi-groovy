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
import org.elasticsearch.transport.client.*

class PutElasticSearch5GroovyProcessorScript2 implements Processor {
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
            if (esClient.get() == null) {
                createElasticsearchClient(context);
            }
        }


        final FlowFile flowFile = session.get();
        if (flowFile == null) {
            return;
        }



        // /user/hive/warehouse/sys_matrix.db/mel_com_private_pv_realtime_member/log_date=2017071011
        final String path = flowFile.getAttribute('path');
        String[] pathDetail = StringUtils.split(path, "/=");

        // mel_com_private_pv_realtime_member_2017071011
        String index = "a";
        final String docType = 'info'
        final Charset charset = Charset.forName('UTF-8');

        try {
            final BulkRequestBuilder bulk = esClient.get().prepareBulk().setTimeout(TimeValue.timeValueMinutes(1));
            session.read(flowFile, { inputStream ->
                final BufferedReader inReader = new BufferedReader(new InputStreamReader(inputStream, 'UTF-8'))
                String line
                while (line = inReader.readLine()) {
                    byte[] bytes = line.getBytes(charset)
                    // String id = '' + Murmur2.hash64(bytes, bytes.length, 0x7f3a21eal)
                    bulk.add(esClient.get().prepareIndex(index, docType).setSource("{\"a\":\"1\"}".getBytes(), XContentType.JSON));
                }
            } as InputStreamCallback)

            if (bulk.numberOfActions() > 0) {
                final BulkResponse response = bulk.execute().actionGet(TimeValue.timeValueMinutes(1)); // �ִ� 2�� ���
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
                    logger.warn("REL_FAILURE");

                    session.transfer(flowFile, REL_FAILURE);
                } else {
                    logger.warn("REL_SUCCESS");
                    session.transfer(flowFile, REL_SUCCESS);
                }
            } else {
                session.transfer(flowFile, REL_SUCCESS);
            }
        } catch (exceptionToFail) {
            logger.error("Failed to insert into Elasticsearch due to {}, transferring to failure", [
                    exceptionToFail.getLocalizedMessage()
            ], exceptionToFail);
            session.transfer(flowFile, REL_RETRY);
            context.yield();
        }

        session.commit()
    }


    protected void createElasticsearchClient(ProcessContext context) throws ProcessException {
        if (esClient.get() != null) {
            return;
        }

        logger.debug("Creating ElasticSearch Client");
        try {
            Settings.Builder settingsBuilder = Settings.builder()
                    .put("path.home", ".")
                    .put("cluster.name", "alyes")
                    .put("client.transport.ping_timeout", "10s")
                    .put("client.transport.nodes_sampler_interval", "10s");

            esHosts = getEsHosts("localhost:9300");
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

        if (esHosts != null) {
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
    public Collection<ValidationResult> validate(ValidationContext context) { return null; }

    @Override
    public PropertyDescriptor getPropertyDescriptor(String name) { return null; }

    @Override
    public void onPropertyModified(PropertyDescriptor descriptor, String oldValue, String newValue) {}

    @Override
    public List<PropertyDescriptor> getPropertyDescriptors() { return null; }

    @Override
    public String getIdentifier() { return null; }

}

processor = new PutElasticSearch5GroovyProcessorScript2()