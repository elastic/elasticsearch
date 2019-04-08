/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.client.watcher;

import org.elasticsearch.common.xcontent.XContentParser;
import org.elasticsearch.client.AbstractHlrcXContentTestCase;
import org.elasticsearch.test.ESTestCase;

import java.io.IOException;

public class PutWatchResponseHlrcTests extends
        AbstractHlrcXContentTestCase<org.elasticsearch.protocol.xpack.watcher.PutWatchResponse, org.elasticsearch.client.watcher.PutWatchResponse> {

    @Override
    protected org.elasticsearch.protocol.xpack.watcher.PutWatchResponse createTestInstance() {
        String id = ESTestCase.randomAlphaOfLength(10);
        long seqNo = ESTestCase.randomNonNegativeLong();
        long primaryTerm = ESTestCase.randomLongBetween(1, 20);
        long version = ESTestCase.randomLongBetween(1, 10);
        boolean created = ESTestCase.randomBoolean();
        return new org.elasticsearch.protocol.xpack.watcher.PutWatchResponse(id, version, seqNo, primaryTerm, created);
    }

    @Override
    protected org.elasticsearch.protocol.xpack.watcher.PutWatchResponse doParseInstance(XContentParser parser) throws IOException {
        return org.elasticsearch.protocol.xpack.watcher.PutWatchResponse.fromXContent(parser);
    }

    @Override
    public org.elasticsearch.client.watcher.PutWatchResponse doHlrcParseInstance(XContentParser parser) throws IOException {
        return org.elasticsearch.client.watcher.PutWatchResponse.fromXContent(parser);
    }

    @Override
    public org.elasticsearch.protocol.xpack.watcher.PutWatchResponse convertHlrcToInternal(org.elasticsearch.client.watcher.PutWatchResponse instance) {
        return new org.elasticsearch.protocol.xpack.watcher.PutWatchResponse(instance.getId(), instance.getVersion(), instance.getSeqNo(), instance.getPrimaryTerm(),
            instance.isCreated());
    }

    @Override
    protected boolean supportsUnknownFields() {
        return false;
    }
}
