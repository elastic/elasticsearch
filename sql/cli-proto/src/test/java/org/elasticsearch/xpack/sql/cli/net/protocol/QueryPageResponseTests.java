/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.xpack.sql.cli.net.protocol;

import org.elasticsearch.test.ESTestCase;

import java.io.IOException;

import static org.elasticsearch.xpack.sql.cli.net.protocol.CliRoundTripTestUtils.assertRoundTripCurrentVersion;
import static org.elasticsearch.xpack.sql.cli.net.protocol.QueryPageRequestTests.randomQueryPageRequest;

public class QueryPageResponseTests extends ESTestCase {
    static QueryPageResponse randomQueryPageResponse() {
        byte[] cursor = new byte[between(0, 5)];
        random().nextBytes(cursor);
        return new QueryPageResponse(randomNonNegativeLong(), cursor, randomAlphaOfLength(5));
    }

    public void testRoundTrip() throws IOException {
        assertRoundTripCurrentVersion(randomQueryPageRequest(), randomQueryPageResponse());
    }

    public void testToString() {
        assertEquals("QueryPageResponse<tookNanos=[123] cursor=[0103] data=[test]>",
                new QueryPageResponse(123, new byte[] {0x01, 0x03}, "test").toString());
    }
}
