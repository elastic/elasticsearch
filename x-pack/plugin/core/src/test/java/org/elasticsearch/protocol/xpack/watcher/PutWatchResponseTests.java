/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.protocol.xpack.watcher;

import org.elasticsearch.common.xcontent.XContentParser;
import org.elasticsearch.protocol.AbstractHLRCXContentTestCase;

import java.io.IOException;

public class PutWatchResponseTests extends AbstractHLRCXContentTestCase<PutWatchResponse,
    org.elasticsearch.client.watcher.PutWatchResponse> {

    @Override
    protected PutWatchResponse createTestInstance() {
        String id = randomAlphaOfLength(10);
        long version = randomLongBetween(1, 10);
        boolean created = randomBoolean();
        return new PutWatchResponse(id, version, created);
    }

    @Override
    protected PutWatchResponse doParseInstance(XContentParser parser) throws IOException {
        return PutWatchResponse.fromXContent(parser);
    }

    @Override
    public org.elasticsearch.client.watcher.PutWatchResponse doHLRCParseInstance(XContentParser parser) throws IOException {
        return org.elasticsearch.client.watcher.PutWatchResponse.fromXContent(parser);
    }

    @Override
    public PutWatchResponse convert(org.elasticsearch.client.watcher.PutWatchResponse instance) {
        return new PutWatchResponse(instance.getId(), instance.getVersion(), instance.isCreated());
    }

    @Override
    protected boolean supportsUnknownFields() {
        return false;
    }
}
