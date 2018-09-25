package org.elasticsearch.client.watcher;


import org.elasticsearch.common.bytes.BytesReference;
import org.elasticsearch.common.xcontent.*;
import org.elasticsearch.test.ESTestCase;

import java.io.IOException;

public class DeactivateWatchResponseTests extends ESTestCase {

    public void testBasicParsing() throws IOException {
        XContentType contentType = randomFrom(XContentType.values());
        int version = randomInt();
        ExecutionState executionState = randomFrom(ExecutionState.values());
        XContentBuilder builder = XContentFactory.contentBuilder(contentType).startObject()
            .startObject("status")
            .field("version", version)
            .field("execution_state", executionState)
            .endObject()
            .endObject();
        BytesReference bytes = BytesReference.bytes(builder);
        DeactivateWatchResponse response = parse(contentType, bytes);
        WatchStatus status = response.getStatus();
        assertNotNull(status);
        assertEquals(version, status.version());
        assertEquals(executionState, status.getExecutionState());
    }

    private DeactivateWatchResponse parse(XContentType contentType, BytesReference bytes) throws IOException {
        XContentParser parser = XContentFactory.xContent(contentType)
            .createParser(NamedXContentRegistry.EMPTY, null, bytes.streamInput());
        parser.nextToken();
        return DeactivateWatchResponse.fromXContent(parser);
    }
}
