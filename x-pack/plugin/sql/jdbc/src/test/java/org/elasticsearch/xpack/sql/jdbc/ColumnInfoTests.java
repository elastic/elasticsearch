/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.xpack.sql.jdbc;

import org.elasticsearch.test.ESTestCase;

import static org.elasticsearch.xpack.sql.client.StringUtils.EMPTY;

public class ColumnInfoTests extends ESTestCase {
    static JdbcColumnInfo varcharInfo(String name) {
        return new JdbcColumnInfo(name, EsType.KEYWORD, EMPTY, EMPTY, EMPTY, EMPTY, 0);
    }

    static JdbcColumnInfo intInfo(String name) {
        return new JdbcColumnInfo(name, EsType.INTEGER, EMPTY, EMPTY, EMPTY, EMPTY, 11);
    }

    static JdbcColumnInfo doubleInfo(String name) {
        return new JdbcColumnInfo(name, EsType.DOUBLE, EMPTY, EMPTY, EMPTY, EMPTY, 25);
    }

    public void testToString() {
        assertEquals("test.doc.a<type=[KEYWORD] catalog=[as] schema=[ads] label=[lab]>",
                new JdbcColumnInfo("a", EsType.KEYWORD, "test.doc", "as", "ads", "lab", 0).toString());
        assertEquals("test.doc.a<type=[KEYWORD]>",
                new JdbcColumnInfo("a", EsType.KEYWORD, "test.doc", EMPTY, EMPTY, EMPTY, 0).toString());
        assertEquals("string<type=[KEYWORD]>", varcharInfo("string").toString());
        assertEquals("int<type=[INTEGER]>", intInfo("int").toString());
        assertEquals("d<type=[DOUBLE]>", doubleInfo("d").toString());
    }
}
