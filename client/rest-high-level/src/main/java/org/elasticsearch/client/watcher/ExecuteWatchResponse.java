package org.elasticsearch.client.watcher;

import org.elasticsearch.common.ParseField;
import org.elasticsearch.common.bytes.BytesReference;
import org.elasticsearch.common.xcontent.ConstructingObjectParser;
import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.common.xcontent.XContentFactory;
import org.elasticsearch.common.xcontent.XContentParser;

import java.io.IOException;

public class ExecuteWatchResponse {

    public static final ParseField ID_FIELD = new ParseField("_id");
    public static final ParseField WATCH_FIELD = new ParseField("watch_record");

    private String recordId;
    private BytesReference contentSource;

    public ExecuteWatchResponse() {
    }

    public ExecuteWatchResponse(String recordId, BytesReference contentSource) {
        this.recordId = recordId;
        this.contentSource = contentSource;
    }

    /**
     * @return The id of the watch record holding the watch execution result.
     */
    public String getRecordId() {
        return recordId;
    }

    /**
     * @return The watch record source
     */
    public BytesReference getRecord() {
        return contentSource;
    }

    private static final ConstructingObjectParser<ExecuteWatchResponse, Void> PARSER
        = new ConstructingObjectParser<>("x_pack_execute_watch_response", false,
        (fields) -> new ExecuteWatchResponse((String)fields[0], (BytesReference) fields[1]));
    static {
        PARSER.declareString(ConstructingObjectParser.constructorArg(), ID_FIELD);
        PARSER.declareObject(ConstructingObjectParser.constructorArg(), (p, c) -> readBytesReference(p), WATCH_FIELD);
    }

    public static ExecuteWatchResponse fromXContent(XContentParser parser) throws IOException {
        return PARSER.parse(parser, null);
    }

    private static BytesReference readBytesReference(XContentParser parser) throws IOException {
        try (XContentBuilder builder = XContentFactory.jsonBuilder()) {
            builder.copyCurrentStructure(parser);
            return BytesReference.bytes(builder);
        }
    }

}
