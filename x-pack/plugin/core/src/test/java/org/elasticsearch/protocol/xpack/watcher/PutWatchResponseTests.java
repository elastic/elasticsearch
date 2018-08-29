/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.protocol.xpack.watcher;

import org.elasticsearch.common.xcontent.XContentParser;
import org.elasticsearch.test.AbstractXContentTestCase;

import java.io.IOException;

public class PutWatchResponseTests extends AbstractXContentTestCase<PutWatchResponse> {

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
    protected boolean supportsUnknownFields() {
        return false;
    }
}
