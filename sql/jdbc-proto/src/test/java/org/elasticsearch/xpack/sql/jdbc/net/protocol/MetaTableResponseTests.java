/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.xpack.sql.jdbc.net.protocol;

import org.elasticsearch.test.ESTestCase;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

import static java.util.Collections.emptyList;
import static org.elasticsearch.xpack.sql.jdbc.net.protocol.JdbcRoundTripTestUtils.assertRoundTripCurrentVersion;

public class MetaTableResponseTests extends ESTestCase {
    static MetaTableResponse randomMetaTableResponse() {
        int size = between(0, 10);
        List<String> tables = new ArrayList<>(size);
        for (int i = 0; i < size; i++) {
            tables.add(randomAlphaOfLength(5));
        }
        return new MetaTableResponse(tables);
    }

    public void testRoundTrip() throws IOException {
        assertRoundTripCurrentVersion(MetaTableRequestTests::randomMetaTableRequest, randomMetaTableResponse());
    }

    public void testToString() {
        assertEquals("MetaTableResponse<>", new MetaTableResponse(emptyList()).toString());
        assertEquals("MetaTableResponse<a.doc, b.doc, c.doc>", new MetaTableResponse(Arrays.asList("a.doc", "b.doc", "c.doc")).toString());
    }
}
