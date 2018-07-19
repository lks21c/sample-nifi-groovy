import org.apache.nifi.flowfile.FlowFile
import org.apache.nifi.processor.io.InputStreamCallback

import java.nio.charset.StandardCharsets


flowFile = session.get()
if (!flowFile) return


try {
    flowFile = session.putAttribute(flowFile, 'path', "/user/hive/warehouse/sys_matrix.db/mel_com_private_comm_code")
    flowFile = session.putAttribute(flowFile, 'path2', "/user/hive/warehouse/sys_matrix.db/mel_com_private_invalid_st_realtime/log_date=20180719")
    session.transfer(flowFile, REL_SUCCESS)
} catch (e) {
    log.error("Can't pare the matrix data.", e)
    session.transfer(flowFile, REL_FAILURE)
}