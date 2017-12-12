/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.xpack.sql.jdbc.net.protocol;

import org.elasticsearch.test.ESTestCase;

import java.io.IOException;

import static org.elasticsearch.xpack.sql.jdbc.net.protocol.JdbcRoundTripTestUtils.assertRoundTripCurrentVersion;

public class QueryCloseResponseTests extends ESTestCase {
    static QueryCloseResponse randomQueryCloseResponse() {
        return new QueryCloseResponse(randomBoolean());
    }

    public void testRoundTrip() throws IOException {
        assertRoundTripCurrentVersion(QueryCloseRequestTests::randomQueryCloseRequest, randomQueryCloseResponse());
    }

    public void testToString() {
        assertEquals("QueryCloseResponse<true>", new QueryCloseResponse(true).toString());
    }
}
