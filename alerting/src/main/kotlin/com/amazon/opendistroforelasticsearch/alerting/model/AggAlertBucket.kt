package com.amazon.opendistroforelasticsearch.alerting.model

import org.elasticsearch.common.ParsingException
import org.elasticsearch.common.io.stream.StreamInput
import org.elasticsearch.common.io.stream.StreamOutput
import org.elasticsearch.common.io.stream.Writeable
import org.elasticsearch.common.xcontent.ToXContent
import org.elasticsearch.common.xcontent.XContentBuilder
import org.elasticsearch.common.xcontent.XContentParser
import org.elasticsearch.common.xcontent.XContentParserUtils
import java.io.IOException
import java.util.Locale

class AggAlertBucket(
    val parentBucketPath: String?,
    val bucketKey: String?,
    val bucket: Map<String, Any>?
): Writeable, ToXContent {

    @Throws(IOException::class)
    constructor(sin: StreamInput) : this(sin.readString(), sin.readString(), sin.readMap())

    override fun writeTo(out: StreamOutput) {
        out.writeString(parentBucketPath)
        out.writeString(bucketKey)
        out.writeMap(bucket)
    }

    override fun toXContent(builder: XContentBuilder, params: ToXContent.Params): XContentBuilder {
        builder.startObject(AGG_ALERT_CONFIG_NAME)
        builder.field("parentBucketPath", parentBucketPath)
        builder.field("bucketKey", bucketKey)
        builder.field("bucket", bucket)
        builder.endObject()
        return builder
    }

    companion object {
        const val AGG_ALERT_CONFIG_NAME = "aggAlertContent"
        const val PARENTS_BUCKET_PATH = "parentBucketPath"
        const val BUCKET_KEY = "bucketKey"
        const val BUCKET = "bucket"

        fun parse(xcp: XContentParser): AggAlertBucket {
            var parentBucketPath: String? = null
            var bucketKey: String? = null
            var bucket: MutableMap<String, Any>? = null
            XContentParserUtils.ensureExpectedToken(XContentParser.Token.START_OBJECT, xcp.currentToken(), xcp)

            if (AGG_ALERT_CONFIG_NAME != xcp.currentName()) {
                throw ParsingException(xcp.tokenLocation,
                    String.format(
                        Locale.ROOT, "Failed to parse object: expecting token with name [%s] but found [%s]",
                        AGG_ALERT_CONFIG_NAME, xcp.currentName())
                )
            }
            while (xcp.nextToken() != XContentParser.Token.END_OBJECT) {
                val fieldName = xcp.currentName()
                xcp.nextToken()
                when (fieldName) {
                    PARENTS_BUCKET_PATH -> parentBucketPath = xcp.text()
                    BUCKET_KEY -> bucketKey = xcp.text()
                    BUCKET -> bucket = xcp.map()
                }
            }
            return AggAlertBucket(parentBucketPath, bucketKey, bucket)
        }
    }
}
