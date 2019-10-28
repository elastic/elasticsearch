package org.elasticsearch.client.indices;

import org.elasticsearch.client.AbstractResponseTestCase;
import org.elasticsearch.common.xcontent.XContentParser;
import org.elasticsearch.common.xcontent.XContentType;

import java.io.IOException;

public class ResizeResponseTests extends
    AbstractResponseTestCase<org.elasticsearch.action.admin.indices.shrink.ResizeResponse, ResizeResponse> {

    @Override
    protected org.elasticsearch.action.admin.indices.shrink.ResizeResponse createServerTestInstance(XContentType xContentType) {
        return new org.elasticsearch.action.admin.indices.shrink.ResizeResponse(randomBoolean(), randomBoolean(), randomAlphaOfLength(5));
    }

    @Override
    protected ResizeResponse doParseToClientInstance(XContentParser parser) throws IOException {
        return ResizeResponse.fromXContent(parser);
    }

    @Override
    protected void assertInstances(org.elasticsearch.action.admin.indices.shrink.ResizeResponse serverTestInstance, ResizeResponse clientInstance) {
        assertEquals(serverTestInstance.isAcknowledged(), clientInstance.isAcknowledged());
        assertEquals(serverTestInstance.isShardsAcknowledged(), clientInstance.isShardsAcknowledged());
        assertEquals(serverTestInstance.index(), clientInstance.index());
    }
}
