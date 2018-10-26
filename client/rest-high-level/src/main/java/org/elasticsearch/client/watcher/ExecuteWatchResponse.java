package org.elasticsearch.client.watcher;

import org.elasticsearch.client.common.XContentSource;
import org.elasticsearch.common.ParseField;
import org.elasticsearch.common.bytes.BytesReference;
import org.elasticsearch.common.xcontent.ConstructingObjectParser;
import org.elasticsearch.common.xcontent.XContentParser;
import org.elasticsearch.common.xcontent.XContentType;

import java.io.IOException;

public class ExecuteWatchResponse {

    private String recordId;
    private XContentSource recordSource;

    public ExecuteWatchResponse() {
    }

    public ExecuteWatchResponse(String recordId, XContentSource source) {
        this.recordId = recordId;
        this.recordSource = source;
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
    public XContentSource getRecordSource() {
        return recordSource;
    }

    private static final ConstructingObjectParser<ExecuteWatchResponse, Void> PARSER
        = new ConstructingObjectParser<>("x_pack_execute_watch_response", true,
        (fields) -> new ExecuteWatchResponse((String)fields[0], (XContentSource)fields[1]));
    static {
        PARSER.declareString(ConstructingObjectParser.constructorArg(), new ParseField("_id"));
        PARSER.declareObject(ConstructingObjectParser.constructorArg(),
            (p, c) -> new XContentSource(p), new ParseField("watch_record"));
    }

    public static ExecuteWatchResponse fromXContent(XContentParser parser) throws IOException {
        return PARSER.parse(parser, null);
    }

}
