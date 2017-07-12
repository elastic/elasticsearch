/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.xpack.sql.jdbc.net.protocol;

import org.elasticsearch.test.ESTestCase;

import java.io.IOException;
import java.sql.JDBCType;

import static org.elasticsearch.xpack.sql.test.RoundTripTestUtils.assertRoundTrip;

public class MetaColumnInfoTests extends ESTestCase {
    static MetaColumnInfo randomMetaColumnInfo() {
        return new MetaColumnInfo(randomAlphaOfLength(5), randomAlphaOfLength(5), randomFrom(JDBCType.values()),
                between(1, Integer.MAX_VALUE), between(1, Integer.MAX_VALUE));
    }

    public void testRoundTrip() throws IOException {
        assertRoundTrip(randomMetaColumnInfo(), MetaColumnInfo::write, MetaColumnInfo::new);
    }

    public void testToString() {
        assertEquals("test.doc.col<type=[VARCHAR] size=[100] position=[1]>",
                new MetaColumnInfo("test.doc", "col", JDBCType.VARCHAR, 100, 1).toString());
    }
}
