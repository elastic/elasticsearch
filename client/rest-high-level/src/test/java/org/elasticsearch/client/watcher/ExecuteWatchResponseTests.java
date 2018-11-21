package org.elasticsearch.client.watcher;

import org.elasticsearch.client.common.XContentSource;
import org.elasticsearch.common.bytes.BytesReference;
import org.elasticsearch.common.xcontent.NamedXContentRegistry;
import org.elasticsearch.common.xcontent.ToXContent;
import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.common.xcontent.XContentFactory;
import org.elasticsearch.common.xcontent.XContentParser;
import org.elasticsearch.common.xcontent.XContentType;
import org.elasticsearch.test.ESTestCase;

import java.io.IOException;

import static org.elasticsearch.test.AbstractXContentTestCase.xContentTester;

public class ExecuteWatchResponseTests extends ESTestCase {

    public void testFromXContent() throws IOException {
        xContentTester(this::createParser, this::createTestInstance, this::toXContent,
            ExecuteWatchResponse::fromXContent).test();
    }

    private void toXContent(ExecuteWatchResponse response, XContentBuilder builder) throws IOException {
        builder.startObject();
        builder.field(ExecuteWatchResponse.ID_FIELD.getPreferredName(), response.getRecordId());
        builder.field(ExecuteWatchResponse.WATCH_FIELD.getPreferredName(),
            (b, params) -> b.value(response.getRecordSource().getAsMap()));
        builder.endObject();
    }

    protected ExecuteWatchResponse createTestInstance() {
        String id = "my_watch_0-2015-06-02T23:17:55.124Z";
        try {
            XContentBuilder builder = XContentFactory.jsonBuilder();
            builder.startObject();
            builder.startObject("watch_record");
            builder.field("watch_id", "my_watch");
            builder.field("node", "my_node");
            builder.startArray("messages");
            builder.endArray();
            builder.startObject("trigger_event");
            builder.field("type", "manual");
            builder.endObject();
            builder.field("state", "executed");
            builder.endObject();
            builder.endObject();
            BytesReference bytes = BytesReference.bytes(builder);
            XContentParser parser = XContentType.JSON.xContent()
                .createParser(NamedXContentRegistry.EMPTY, null, bytes.streamInput());

            return new ExecuteWatchResponse(id, new XContentSource(parser));
        }
        catch (IOException e) {
            throw new AssertionError(e);
        }
    }

}
