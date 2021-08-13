/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */
package org.elasticsearch.client.watcher;

import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.test.ESTestCase;

import java.io.IOException;

import static org.elasticsearch.test.AbstractXContentTestCase.xContentTester;

public class DeleteWatchResponseTests extends ESTestCase {

    public void testFromXContent() throws IOException {
        xContentTester(this::createParser,
            DeleteWatchResponseTests::createTestInstance,
            DeleteWatchResponseTests::toXContent,
            DeleteWatchResponse::fromXContent)
            .supportsUnknownFields(true)
            .assertToXContentEquivalence(false)
            .test();
    }

    private static XContentBuilder toXContent(DeleteWatchResponse response, XContentBuilder builder) throws IOException {
        return builder.startObject()
            .field("_id", response.getId())
            .field("_version", response.getVersion())
            .field("found", response.isFound())
            .endObject();
    }

    private static DeleteWatchResponse createTestInstance() {
        String id = randomAlphaOfLength(10);
        long version = randomLongBetween(1, 10);
        boolean found = randomBoolean();
        return new DeleteWatchResponse(id, version, found);
    }
}
