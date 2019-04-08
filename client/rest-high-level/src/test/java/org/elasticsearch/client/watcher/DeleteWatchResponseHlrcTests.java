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

public class DeleteWatchResponseHlrcTests extends
        AbstractHlrcXContentTestCase<org.elasticsearch.protocol.xpack.watcher.DeleteWatchResponse, org.elasticsearch.client.watcher.DeleteWatchResponse> {

    @Override
    protected org.elasticsearch.protocol.xpack.watcher.DeleteWatchResponse createTestInstance() {
        String id = ESTestCase.randomAlphaOfLength(10);
        long version = ESTestCase.randomLongBetween(1, 10);
        boolean found = ESTestCase.randomBoolean();
        return new org.elasticsearch.protocol.xpack.watcher.DeleteWatchResponse(id, version, found);
    }

    @Override
    protected org.elasticsearch.protocol.xpack.watcher.DeleteWatchResponse doParseInstance(XContentParser parser) throws IOException {
        return org.elasticsearch.protocol.xpack.watcher.DeleteWatchResponse.fromXContent(parser);
    }

    @Override
    public org.elasticsearch.client.watcher.DeleteWatchResponse doHlrcParseInstance(XContentParser parser) throws IOException {
        return org.elasticsearch.client.watcher.DeleteWatchResponse.fromXContent(parser);
    }

    @Override
    public org.elasticsearch.protocol.xpack.watcher.DeleteWatchResponse convertHlrcToInternal(org.elasticsearch.client.watcher.DeleteWatchResponse instance) {
        return new org.elasticsearch.protocol.xpack.watcher.DeleteWatchResponse(instance.getId(), instance.getVersion(), instance.isFound());
    }

    @Override
    protected boolean supportsUnknownFields() {
        return false;
    }
}
