/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */
package org.elasticsearch.xpack.sql.jdbc;

import org.elasticsearch.test.ESTestCase;

import java.sql.SQLException;

public class SqlQueryParameterAnalyzerTests extends ESTestCase {

    public void testNoParameters() throws Exception {
        assertEquals(0, SqlQueryParameterAnalyzer.parametersCount("SELECT * FROM table"));
        assertEquals(0, SqlQueryParameterAnalyzer.parametersCount("SELECT * FROM 'table'"));
        assertEquals(0, SqlQueryParameterAnalyzer.parametersCount("SELECT * FROM \"table\""));
        assertEquals(0, SqlQueryParameterAnalyzer.parametersCount("SELECT * FROM \"table\" WHERE i = 0"));
        assertEquals(0, SqlQueryParameterAnalyzer.parametersCount("SELECT * FROM 'table' WHERE s = '?'"));
        assertEquals(0, SqlQueryParameterAnalyzer.parametersCount("SELECT * FROM 'table' WHERE s = 'foo''bar''?'"));
        assertEquals(0, SqlQueryParameterAnalyzer.parametersCount("SELECT * FROM `table` where b = 'fo\"o\\\"b{ar\\}?b\"az?}\\-?\"?\\?{'"));

    }

    public void testSingleParameter() throws Exception {
        assertEquals(1, SqlQueryParameterAnalyzer.parametersCount("SELECT * FROM 'table' WHERE s = '?' AND b = ?"));
        assertEquals(1, SqlQueryParameterAnalyzer.parametersCount("SELECT * FROM 'table' WHERE b = ? AND s = '?'"));
        assertEquals(1, SqlQueryParameterAnalyzer.parametersCount("""
            SELECT ?/10 /* multiline \s
             * query\s
             * more ? /* lines */ ? here\s
             */ FROM foo"""));
        assertEquals(1, SqlQueryParameterAnalyzer.parametersCount("SELECT ?"));

    }

    public void testMultipleParameters() throws Exception {
        assertEquals(4, SqlQueryParameterAnalyzer.parametersCount("SELECT ?, ?, ? , ?"));
        assertEquals(3, SqlQueryParameterAnalyzer.parametersCount("SELECT ?, ?, '?' , ?"));
        assertEquals(3, SqlQueryParameterAnalyzer.parametersCount("SELECT ?, ?\n, '?' , ?"));
        assertEquals(3, SqlQueryParameterAnalyzer.parametersCount("""
            SELECT ? - 10 -- first parameter with ????
            , ? -- second parameter with random " and '\s
            , ? -- last parameter without new line"""));
    }

    public void testUnclosedJdbcEscape() {
        SQLException exception = expectThrows(SQLException.class, () -> SqlQueryParameterAnalyzer.parametersCount("SELECT {foobar"));
        assertEquals("Jdbc escape sequences are not supported yet", exception.getMessage());
    }

    public void testUnclosedMultilineComment() {
        SQLException exception = expectThrows(SQLException.class, () -> SqlQueryParameterAnalyzer.parametersCount("SELECT /* * * * "));
        assertEquals("Cannot parse given sql; unclosed /* comment", exception.getMessage());
    }

    public void testUnclosedSingleQuoteString() {
        SQLException exception = expectThrows(SQLException.class, () -> SqlQueryParameterAnalyzer.parametersCount("SELECT ' '' '' "));
        assertEquals("Cannot parse given sql; unclosed string", exception.getMessage());
    }
}
