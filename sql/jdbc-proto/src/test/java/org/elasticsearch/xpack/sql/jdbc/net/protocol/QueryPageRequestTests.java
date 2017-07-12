/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.xpack.sql.jdbc.net.protocol;

import org.elasticsearch.test.ESTestCase;

import java.io.IOException;

import static org.elasticsearch.xpack.sql.jdbc.net.protocol.JdbcRoundTripTestUtils.assertRoundTripCurrentVersion;
import static org.elasticsearch.xpack.sql.jdbc.net.protocol.TimeoutInfoTests.randomTimeoutInfo;

public class QueryPageRequestTests extends ESTestCase {
    public static QueryPageRequest randomQueryPageRequest(Page page) {
        return new QueryPageRequest(randomAlphaOfLength(5), randomTimeoutInfo(), page);
    }

    public void testRoundTrip() throws IOException {
        assertRoundTripCurrentVersion(randomQueryPageRequest(null));
    }

    public void testToString() {
        assertEquals("QueryPageRequest<test_id>", new QueryPageRequest("test_id", new TimeoutInfo(1, 1, 1), null).toString());
    }
}
