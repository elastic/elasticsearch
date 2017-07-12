/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.xpack.sql.jdbc.net.protocol;

import org.elasticsearch.test.ESTestCase;

import java.io.IOException;

import static org.elasticsearch.xpack.sql.jdbc.net.protocol.JdbcRoundTripTestUtils.assertRoundTripCurrentVersion;

public class MetaColumnRequestTests extends ESTestCase {
    public static MetaColumnRequest randomMetaColumnRequest() {
        return new MetaColumnRequest(randomAlphaOfLength(10), randomAlphaOfLength(10));
    }

    public void testRoundTrip() throws IOException {
        assertRoundTripCurrentVersion(randomMetaColumnRequest());
    }

    public void testToString() {
        assertEquals("MetaColumnRequest<table=[test.do%] column=[d%]>", new MetaColumnRequest("test.do%", "d%").toString());
    }
}
