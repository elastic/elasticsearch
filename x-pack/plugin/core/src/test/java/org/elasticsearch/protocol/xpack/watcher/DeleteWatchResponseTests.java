/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.protocol.xpack.watcher;

import org.elasticsearch.common.xcontent.XContentParser;
import org.elasticsearch.test.AbstractXContentTestCase;

import java.io.IOException;

public class DeleteWatchResponseTests extends AbstractXContentTestCase<DeleteWatchResponse> {

    @Override
    protected DeleteWatchResponse createTestInstance() {
        String id = randomAlphaOfLength(10);
        long version = randomLongBetween(1, 10);
        boolean found = randomBoolean();
        return new DeleteWatchResponse(id, version, found);
    }

    @Override
    protected DeleteWatchResponse doParseInstance(XContentParser parser) throws IOException {
        return DeleteWatchResponse.fromXContent(parser);
    }

    @Override
    protected boolean supportsUnknownFields() {
        return false;
    }
}
