/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.client.indices;

import org.elasticsearch.client.AbstractResponseTestCase;
import org.elasticsearch.common.xcontent.XContentParser;
import org.elasticsearch.common.xcontent.XContentType;

import java.io.IOException;

public class ResizeResponseTests extends
    AbstractResponseTestCase<org.elasticsearch.action.admin.indices.shrink.ResizeResponse, ResizeResponse> {

    @Override
    protected org.elasticsearch.action.admin.indices.shrink.ResizeResponse createServerTestInstance(XContentType xContentType) {
        boolean acked = randomBoolean();
        return new org.elasticsearch.action.admin.indices.shrink.ResizeResponse(acked, acked, randomAlphaOfLength(5));
    }

    @Override
    protected ResizeResponse doParseToClientInstance(XContentParser parser) throws IOException {
        return ResizeResponse.fromXContent(parser);
    }

    @Override
    protected void assertInstances(org.elasticsearch.action.admin.indices.shrink.ResizeResponse serverTestInstance,
                                   ResizeResponse clientInstance) {
        assertEquals(serverTestInstance.isAcknowledged(), clientInstance.isAcknowledged());
        assertEquals(serverTestInstance.isShardsAcknowledged(), clientInstance.isShardsAcknowledged());
        assertEquals(serverTestInstance.index(), clientInstance.index());
    }
}
