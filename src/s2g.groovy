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

            obj = jsonSlurper.parseText(line)

            obj.t_props.each { key, val ->
                if (key.contains("BASKET")) {
                    convertBasketToArray(obj, key)
                }
            }

            if (obj.get("t_id") != null) {
                obj.put("id", obj.get("t_id").toString())
                obj.remove("t_id")
            } else {
                obj.put("from", obj.get("t_from").toString())
                obj.remove("t_from")

                obj.put("to", obj.get("t_to").toString())
                obj.remove("t_to")

                obj.put("label", obj.get("t_label"))
                obj.remove("t_label")
            }

            obj.put("timestamp", obj.get("t_timestamp"))
            obj.remove("t_timestamp")


            obj.put("props", obj.get("t_props"))
            obj.remove("t_props")

            if (obj.get("props") != null) {
                /* 곡 메타 */
                convertToInteger(obj, "play_time")
                convertToInteger(obj, "svc_avail_flg")
                removeIfNull(obj, "song_name")
                removeIfNull(obj, "rep_song_id")
                removeIfNull(obj, "album_id")
                removeIfNull(obj, "album_name")
                removeIfNull(obj, "album_img_path")
                removeIfNull(obj, "issue_date")
                removeIfNull(obj, "order_issue_date")
                removeIfNull(obj, "reg_date")

                /* 아티스트 메타 */
                removeIfNull(obj, "act_type_name")
                removeIfNull(obj, "artist_img_path")
                removeIfNull(obj, "artist_name")

                /* 앨범 메타 */
                removeIfNull(obj, "album_img_path")
                removeIfNull(obj, "rep_song_name")

                /* 사용자 정보 메타 */
                removeIfNull(obj, "birthday")
                removeIfNull(obj, "gender")
                removeIfNull(obj, "prefe_artist")
                removeIfNull(obj, "prefe_gnr")
                removeIfNull(obj, "prefe_style")
                convertToInteger(obj, "main_use_dow")
                convertToInteger(obj, "main_use_tmz")
                convertToInteger(obj, "ad_age_band")

                if (obj.get("t_songs") == null) {
                    if ("true".equals(obj.props.get("top100_yn"))) {
                        obj.props.put("top100_yn", true)
                    } else if ("false".equals(obj.props.get("top100_yn"))) {
                        obj.props.put("top100_yn", false)
                    }
                } else {
                    if ("true".equals(obj.props.get("admin_open_yn"))) {
                        obj.props.put("admin_open_yn", true)
                    } else {
                        obj.props.put("admin_open_yn", false)
                    }
                    if ("true".equals(obj.props.get("admin_recm_yn"))) {
                        obj.props.put("admin_recm_yn", true)
                    } else {
                        obj.props.put("admin_recm_yn", false)
                    }
                    if ("true".equals(obj.props.get("del_posbl_yn"))) {
                        obj.props.put("del_posbl_yn", true)
                    } else {
                        obj.props.put("del_posbl_yn", false)
                    }
                    if ("true".equals(obj.props.get("del_yn"))) {
                        obj.props.put("del_yn", true)
                    } else {
                        obj.props.put("del_yn", false)
                    }
                    if ("true".equals(obj.props.get("djmag_reg_yn"))) {
                        obj.props.put("djmag_reg_yn", true)
                    } else {
                        obj.props.put("djmag_reg_yn", false)
                    }
                    if ("true".equals(obj.props.get("djmag_reg_yn"))) {
                        obj.props.put("djmag_reg_yn", true)
                    } else {
                        obj.props.put("djmag_reg_yn", false)
                    }
                    if ("true".equals(obj.props.get("open_yn"))) {
                        obj.props.put("open_yn", true)
                    } else {
                        obj.props.put("open_yn", false)
                    }

                    obj.props.put("songs", obj.get("t_songs"))
                    obj.remove("t_songs")
                }
            }

            if (number != 1) {
                body << ","
            }

            body << JsonOutput.toJson(obj)
        }

        body = "[" + body + "]"

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

def static convertBasketToArray(obj, key) {
    if (obj.t_props.get(key) != null && obj.t_props.get(key).length() > 0)
        obj.t_props.put(key, obj.t_props.get(key).tokenize("@#%@#%"))
}

def static convertToInteger(obj, key) {
    if (obj.props.get(key) == null) {
        obj.props.remove(key)
    } else {
        obj.props.put(key, obj.props.get(key).toInteger())
    }
}

def static removeIfNull(obj, key) {
    if (obj.props.get(key) == null) {
        obj.props.remove(key)
    }
}