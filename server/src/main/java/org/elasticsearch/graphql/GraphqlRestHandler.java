package org.elasticsearch.graphql;

import org.elasticsearch.client.node.NodeClient;
import org.elasticsearch.common.bytes.BytesReference;
import org.elasticsearch.rest.*;

public class GraphqlRestHandler implements RestHandler {

    @Override
    public void handleRequest(RestRequest request, RestChannel channel, NodeClient client) throws Exception {
        channel.sendResponse(new RestResponse() {
            @Override
            public String contentType() {
                return "application/json";
            }

            @Override
            public BytesReference content() {
                try {
                    return BytesReference.bytes(channel.newBuilder()
                        .startObject()
                        .field("foo", "bar")
                        .endObject());
                } catch (Exception error) {
                    return null;
                }
            }

            @Override
            public RestStatus status() {
                return RestStatus.OK;
            }
        });
    }
}
