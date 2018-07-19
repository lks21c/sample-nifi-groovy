import org.apache.nifi.flowfile.FlowFile
import org.apache.nifi.processor.io.InputStreamCallback
import org.apache.nifi.processor.io.OutputStreamCallback
import groovy.json.JsonOutput
import groovy.json.JsonSlurper
import groovy.json.StringEscapeUtils

import java.nio.charset.StandardCharsets

flowFile = session.get()
if (!flowFile) return

def jsonSlurper = new JsonSlurper()
def body = StringBuilder.newInstance()

try {
    session.read(flowFile, { inputStream ->
        inputStream.eachLine("UTF-8") { line, number ->

            obj = new HashMap<String,String>();

            obj.put("a", "1")

            body << JsonOutput.toJson(obj)
        }

        body = "{ \"index\" : { \"_index\" : \"a\", \"_type\" : \"info\" } }" + "\n" + body + "\n"

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