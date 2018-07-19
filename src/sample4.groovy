import org.apache.nifi.flowfile.FlowFile
import org.apache.nifi.processor.io.InputStreamCallback
import org.apache.nifi.processor.io.OutputStreamCallback

import java.nio.charset.StandardCharsets

import java.util.Random;

flowFile = session.get()
if (!flowFile) return

def body = StringBuilder.newInstance()

String[] server = ["http://mpalyes02:19100/_bulk"
                   , "http://mpalyes03:19100/_bulk"
                   , "http://mpalyes04:19100/_bulk"
                   , "http://mpalyes05:19100/_bulk"
                   , "http://mpalyes06:19100/_bulk"
                   , "http://mpalyes07:19100/_bulk"
                   , "http://mpalyes08:19100/_bulk"
                   , "http://mpalyes09:19100/_bulk"
                   , "http://mpalyes12:19100/_bulk"
                   , "http://mpalyes13:19100/_bulk"
                   , "http://mpalyes14:19100/_bulk"]
Random generator = new Random();
int randomNum = generator.nextInt(server.length);

try {
    String indexName = flowFile.getAttribute("index_name");
    flowFile = session.putAttribute(flowFile, 'server_name', server[randomNum])
    session.read(flowFile, { inputStream ->
        inputStream.eachLine("UTF-8") { line, number ->
            body << "{ \"index\" : { \"_index\" : \"" + indexName + "\", \"_type\" : \"info\" } }" + "\n"
            body << line + "\n"
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