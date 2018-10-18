package org.elasticsearch.client.watcher;

import org.elasticsearch.common.bytes.BytesReference;
import org.elasticsearch.common.xcontent.XContentParser;
import org.elasticsearch.common.xcontent.XContentType;

import java.io.IOException;

public class ExecuteWatchResponse {

    private String recordId;
    private XContentSource recordSource;

    public ExecuteWatchResponse() {
    }

    public ExecuteWatchResponse(String recordId, BytesReference recordSource, XContentType contentType) {
        this.recordId = recordId;
        this.recordSource = new XContentSource(recordSource, contentType);
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

    public static ExecuteWatchResponse fromXContent(XContentParser parser) throws IOException {
        return null;
    }

}
