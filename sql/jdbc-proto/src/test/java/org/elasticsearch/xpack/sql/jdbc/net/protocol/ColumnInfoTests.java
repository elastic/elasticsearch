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

public class ColumnInfoTests extends ESTestCase {
    static ColumnInfo varcharInfo(String name) {
        return new ColumnInfo(name, JDBCType.VARCHAR, "", "", "", "");
    }

    static ColumnInfo intInfo(String name) {
        return new ColumnInfo(name, JDBCType.INTEGER, "", "", "", ""); 
    }

    static ColumnInfo doubleInfo(String name) {
        return new ColumnInfo(name, JDBCType.DOUBLE, "", "", "", ""); 
    }

    static Object randomValueFor(ColumnInfo info) {
        switch (info.type) {
        case VARCHAR: return randomAlphaOfLength(5);
        case INTEGER: return randomInt();
        case DOUBLE: return randomDouble();
        default:
            throw new IllegalArgumentException("Unsupported type [" + info.type + "]");
        }
    }

    static ColumnInfo randomColumnInfo() {
        return new ColumnInfo(randomAlphaOfLength(5), randomFrom(JDBCType.values()), randomAlphaOfLength(5), randomAlphaOfLength(5),
                randomAlphaOfLength(5), randomAlphaOfLength(5));
    }

    public void testRoundTrip() throws IOException {
        assertRoundTrip(randomColumnInfo(), ColumnInfo::write, ColumnInfo::new);
    }

    public void testToString() {
        assertEquals("test.doc.a<type=[VARCHAR] catalog=[as] schema=[ads] label=[lab]>",
                new ColumnInfo("a", JDBCType.VARCHAR, "test.doc", "as", "ads", "lab").toString());
        assertEquals("test.doc.a<type=[VARCHAR]>",
                new ColumnInfo("a", JDBCType.VARCHAR, "test.doc", "", "", "").toString());
        assertEquals("string<type=[VARCHAR]>", varcharInfo("string").toString());
        assertEquals("int<type=[INTEGER]>", intInfo("int").toString());
        assertEquals("d<type=[DOUBLE]>", doubleInfo("d").toString());
    }
}
