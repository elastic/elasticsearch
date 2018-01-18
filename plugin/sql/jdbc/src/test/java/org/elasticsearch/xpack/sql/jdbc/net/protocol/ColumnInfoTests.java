/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.xpack.sql.jdbc.net.protocol;

import org.elasticsearch.test.ESTestCase;

import java.sql.JDBCType;

public class ColumnInfoTests extends ESTestCase {
    static ColumnInfo varcharInfo(String name) {
        return new ColumnInfo(name, JDBCType.VARCHAR, "", "", "", "", 0);
    }

    static ColumnInfo intInfo(String name) {
        return new ColumnInfo(name, JDBCType.INTEGER, "", "", "", "", 11);
    }

    static ColumnInfo doubleInfo(String name) {
        return new ColumnInfo(name, JDBCType.DOUBLE, "", "", "", "", 25);
    }

    public void testToString() {
        assertEquals("test.doc.a<type=[VARCHAR] catalog=[as] schema=[ads] label=[lab]>",
                new ColumnInfo("a", JDBCType.VARCHAR, "test.doc", "as", "ads", "lab", 0).toString());
        assertEquals("test.doc.a<type=[VARCHAR]>",
                new ColumnInfo("a", JDBCType.VARCHAR, "test.doc", "", "", "", 0).toString());
        assertEquals("string<type=[VARCHAR]>", varcharInfo("string").toString());
        assertEquals("int<type=[INTEGER]>", intInfo("int").toString());
        assertEquals("d<type=[DOUBLE]>", doubleInfo("d").toString());
    }
}
