/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */
package org.elasticsearch.client.watcher.hlrc;

import org.elasticsearch.client.AbstractResponseTestCase;
import org.elasticsearch.client.watcher.PutWatchResponse;
import org.elasticsearch.common.xcontent.XContentParser;
import org.elasticsearch.common.xcontent.XContentType;

import java.io.IOException;

import static org.hamcrest.Matchers.equalTo;

public class PutWatchResponseTests extends AbstractResponseTestCase<
    org.elasticsearch.protocol.xpack.watcher.PutWatchResponse, PutWatchResponse> {

    @Override
    protected org.elasticsearch.protocol.xpack.watcher.PutWatchResponse createServerTestInstance(XContentType xContentType) {
        String id = randomAlphaOfLength(10);
        long seqNo = randomNonNegativeLong();
        long primaryTerm = randomLongBetween(1, 20);
        long version = randomLongBetween(1, 10);
        boolean created = randomBoolean();
        return new org.elasticsearch.protocol.xpack.watcher.PutWatchResponse(id, version, seqNo, primaryTerm, created);
    }

    @Override
    protected PutWatchResponse doParseToClientInstance(XContentParser parser) throws IOException {
        return PutWatchResponse.fromXContent(parser);
    }

    @Override
    protected void assertInstances(org.elasticsearch.protocol.xpack.watcher.PutWatchResponse serverTestInstance,
                                   PutWatchResponse clientInstance) {
        assertThat(clientInstance.getId(), equalTo(serverTestInstance.getId()));
        assertThat(clientInstance.getSeqNo(), equalTo(serverTestInstance.getSeqNo()));
        assertThat(clientInstance.getPrimaryTerm(), equalTo(serverTestInstance.getPrimaryTerm()));
        assertThat(clientInstance.getVersion(), equalTo(serverTestInstance.getVersion()));
        assertThat(clientInstance.isCreated(), equalTo(serverTestInstance.isCreated()));
    }
}
