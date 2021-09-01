/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.client.ccr;

import org.elasticsearch.client.AbstractResponseTestCase;
import org.elasticsearch.common.xcontent.XContentParser;
import org.elasticsearch.common.xcontent.XContentType;
import org.elasticsearch.xpack.core.ccr.action.PutFollowAction;

import java.io.IOException;

import static org.hamcrest.Matchers.is;

public class PutFollowResponseTests extends AbstractResponseTestCase<PutFollowAction.Response, PutFollowResponse> {

    @Override
    protected PutFollowAction.Response createServerTestInstance(XContentType xContentType) {
        return new PutFollowAction.Response(randomBoolean(), randomBoolean(), randomBoolean());
    }

    @Override
    protected PutFollowResponse doParseToClientInstance(XContentParser parser) throws IOException {
        return PutFollowResponse.fromXContent(parser);
    }

    @Override
    protected void assertInstances(PutFollowAction.Response serverTestInstance, PutFollowResponse clientInstance) {
        assertThat(serverTestInstance.isFollowIndexCreated(), is(clientInstance.isFollowIndexCreated()));
        assertThat(serverTestInstance.isFollowIndexShardsAcked(), is(clientInstance.isFollowIndexShardsAcked()));
        assertThat(serverTestInstance.isIndexFollowingStarted(), is(clientInstance.isIndexFollowingStarted()));
    }
}
