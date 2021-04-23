/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */
package org.elasticsearch.xpack.sql.jdbc;

import java.sql.SQLException;

/**
 * Simplistic parser that can find parameters and escape sequences in a query and counts the number of parameters
 * <p>
 * It is used by JDBC client to approximate the number of parameters that JDBC client should accept. The parser is simplistic
 * in a sense that it can accept ? in the places where it's not allowed by the server (for example as a index name), but that's
 * a reasonable compromise to avoid sending the prepared statement server to the server for extra validation.
 */
final class SqlQueryParameterAnalyzer {

    private SqlQueryParameterAnalyzer() {

    }

    /**
     * Returns number of parameters in the specified SQL query
     */
    public static int parametersCount(String sql) throws SQLException {

        int l = sql.length();
        int params = 0;
        for (int i = 0; i < l; i++) {
            char c = sql.charAt(i);

            switch (c) {
                case '{':
                    i = skipJdbcEscape(i, sql);
                    break;
                case '\'':
                    i = skipString(i, sql, c);
                    break;
                case '"':
                    i = skipString(i, sql, c);
                    break;
                case '?':
                    params ++;
                    break;
                case '-':
                    if (i + 1 < l && sql.charAt(i + 1) == '-') {
                        i = skipLineComment(i, sql);
                    }
                    break;
                case '/':
                    if (i + 1 < l && sql.charAt(i + 1) == '*') {
                        i = skipMultiLineComment(i, sql);
                    }
                    break;
            }
        }
        return params;
    }

    /**
     * Skips jdbc escape sequence starting at the current position i, returns the length of the sequence
     */
    private static int skipJdbcEscape(int i, String sql) throws SQLException {
        // TODO: JDBC escape syntax
        // https://db.apache.org/derby/docs/10.5/ref/rrefjdbc1020262.html
        throw new SQLException("Jdbc escape sequences are not supported yet");
    }


    /**
     * Skips a line comment starting at the current position i, returns the length of the comment
     */
    private static int skipLineComment(int i, String sql) {
        for (; i < sql.length(); i++) {
            char c = sql.charAt(i);
            if (c == '\n' || c == '\r') {
                return i;
            }
        }
        return i;
    }

    /**
     * Skips a multi-line comment starting at the current position i, returns the length of the comment
     */
    private static int skipMultiLineComment(int i, String sql) throws SQLException {
        int block = 0;

        for (; i < sql.length() - 1; i++) {
            char c = sql.charAt(i);
            if (c == '/' && sql.charAt(i + 1) == '*') {
                i++;
                block++;
            } else if (c == '*' && sql.charAt(i + 1) == '/') {
                i++;
                block--;
            }
            if (block == 0) {
                return i;
            }
        }
        throw new SQLException("Cannot parse given sql; unclosed /* comment");
    }

    /**
     * Skips a string starting at the current position i, returns the length of the string
     */
    private static int skipString(int i, String sql, char q) throws SQLException {
        for (i = i + 1; i < sql.length(); i++) {
            char c = sql.charAt(i);
            if (c == q) {
                // double quotes mean escaping
                if (i + 1 < sql.length() && sql.charAt(i + 1) == q) {
                    i++;
                } else {
                    return i;
                }
            }
        }
        throw new SQLException("Cannot parse given sql; unclosed string");
    }
}
