/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.xpack.sql.jdbc.net.protocol;

import org.elasticsearch.test.ESTestCase;

import java.io.IOException;

import static org.elasticsearch.xpack.sql.jdbc.net.protocol.JdbcRoundTripTestUtils.assertRoundTripCurrentVersion;

public class MetaTableRequestTests extends ESTestCase {
    public static MetaTableRequest randomMetaTableRequest() {
        return new MetaTableRequest(randomAlphaOfLength(10));
    }

    public void testRoundTrip() throws IOException {
        assertRoundTripCurrentVersion(randomMetaTableRequest());
    }

    public void testToString() {
        assertEquals("MetaTableRequest<test.do%>", new MetaTableRequest("test.do%").toString());
    }
}
