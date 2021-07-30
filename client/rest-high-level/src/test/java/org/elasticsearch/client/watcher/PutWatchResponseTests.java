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

public class PutWatchResponseTests extends ESTestCase {

    public void testFromXContent() throws IOException {
        xContentTester(this::createParser,
            PutWatchResponseTests::createTestInstance,
            PutWatchResponseTests::toXContent,
            PutWatchResponse::fromXContent)
            .supportsUnknownFields(true)
            .assertToXContentEquivalence(false)
            .test();
    }

    private static XContentBuilder toXContent(PutWatchResponse response, XContentBuilder builder) throws IOException {
        return builder.startObject()
            .field("_id", response.getId())
            .field("_version", response.getVersion())
            .field("_seq_no", response.getSeqNo())
            .field("_primary_term", response.getPrimaryTerm())
            .field("created", response.isCreated())
            .endObject();
    }

    private static PutWatchResponse createTestInstance() {
        String id = randomAlphaOfLength(10);
        long seqNo = randomNonNegativeLong();
        long primaryTerm = randomLongBetween(1, 200);
        long version = randomLongBetween(1, 10);
        boolean created = randomBoolean();
        return new PutWatchResponse(id, version, seqNo, primaryTerm, created);
    }
}
