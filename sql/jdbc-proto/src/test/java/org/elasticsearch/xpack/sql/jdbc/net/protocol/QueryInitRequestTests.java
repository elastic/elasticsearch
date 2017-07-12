/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.xpack.sql.jdbc.net.protocol;

import org.elasticsearch.test.ESTestCase;

import java.io.IOException;
import java.util.TimeZone;

import static org.elasticsearch.xpack.sql.jdbc.net.protocol.JdbcRoundTripTestUtils.assertRoundTripCurrentVersion;
import static org.elasticsearch.xpack.sql.jdbc.net.protocol.TimeoutInfoTests.randomTimeoutInfo;


public class QueryInitRequestTests extends ESTestCase {
    public static QueryInitRequest randomQueryInitRequest() {
        return new QueryInitRequest(between(0, Integer.MAX_VALUE), randomAlphaOfLength(5), randomTimeZone(random()), randomTimeoutInfo());
    }

    public void testRoundTrip() throws IOException {
        assertRoundTripCurrentVersion(randomQueryInitRequest());
    }

    public void testToString() {
        assertEquals("QueryInitRequest<query=[SELECT * FROM test.doc]>",
                new QueryInitRequest(10, "SELECT * FROM test.doc", TimeZone.getTimeZone("UTC"), new TimeoutInfo(1, 1, 1)).toString());
        assertEquals("QueryInitRequest<query=[SELECT * FROM test.doc] timeZone=[GMT-05:00]>",
                new QueryInitRequest(10, "SELECT * FROM test.doc", TimeZone.getTimeZone("GMT-5"), new TimeoutInfo(1, 1, 1)).toString());
    }
}
