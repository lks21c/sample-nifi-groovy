import groovy.json.JsonOutput
import groovy.json.JsonSlurper
import org.apache.nifi.flowfile.FlowFile
import org.apache.nifi.processor.io.InputStreamCallback
import org.apache.nifi.processor.io.OutputStreamCallback

import java.nio.charset.StandardCharsets

import java.util.Random;

flowFile = session.get()
if (!flowFile) return

def body = StringBuilder.newInstance()

String[] server = ["http://mpalyes02:19100"
                   , "http://mpalyes03:19100"
                   , "http://mpalyes04:19100"
                   , "http://mpalyes05:19100"
                   , "http://mpalyes06:19100"
                   , "http://mpalyes07:19100"
                   , "http://mpalyes08:19100"
                   , "http://mpalyes09:19100"
                   , "http://mpalyes12:19100"
                   , "http://mpalyes13:19100"
                   , "http://mpalyes14:19100"]
Random generator = new Random();
int randomNum = generator.nextInt(server.length);

try {
    String indexName = flowFile.getAttribute("index_name");
    String idBasedOp = flowFile.getAttribute("id-based-op");
    String typeName = "info";

    if ("yes".equals(idBasedOp)) {
        typeName = "detail";
    }
    String indexOp = flowFile.getAttribute('index-op');
    if(indexOp == null || indexOp.length() == 0) {
        indexOp = "index;"
    }

    flowFile = session.putAttribute(flowFile, 'server_name', server[randomNum] + "/_bulk")
    session.read(flowFile, { inputStream ->
        inputStream.eachLine("UTF-8") { line, number ->
            def jsonObject = new JsonSlurper().parseText(line);
            if ("yes".equals(idBasedOp)) {
                String id = jsonObject.get("_id");
                body << "{ \"" + indexOp + "\" : { \"_index\" : \"" + indexName + "\", \"_type\" : \"" + typeName + "\", \"_id\" : \"" + id + "\" } }" + "\n"
            } else {
                body << "{ \"index\" : { \"_index\" : \"" + indexName + "\", \"_type\" : \"" + typeName + "\" } }" + "\n"
            }

            if (!indexOp.equalsIgnoreCase("delete")) {
                jsonObject.remove("_id")
                body << JsonOutput.toJson(jsonObject) + "\n"
            }
        }

        body = "" + body + ""

        FlowFile newFlowFile = session.create(flowFile)
        newFlowFile = session.write(newFlowFile, { outputStream ->
            outputStream.write(body.getBytes(StandardCharsets.UTF_8))
        } as OutputStreamCallback)

        session.transfer(newFlowFile, REL_SUCCESS)

    } as InputStreamCallback)
    session.remove((FlowFile) flowFile)
} catch (e) {
    log.error("Can't pare the matrix data.", e)
    session.transfer(flowFile, REL_FAILURE)
}