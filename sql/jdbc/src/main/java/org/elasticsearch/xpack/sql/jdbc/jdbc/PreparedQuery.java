/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.xpack.sql.jdbc.jdbc;

import java.sql.JDBCType;
import java.util.ArrayList;
import java.util.List;

class PreparedQuery {

    static class ParamInfo {
        JDBCType type;
        Object value;

        ParamInfo(Object value, JDBCType type) {
            this.value = value;
            this.type = type;
        }
    }

    private final List<String> fragments;
    final ParamInfo[] params;

    PreparedQuery(List<String> fragments) {
        this.fragments = fragments;
        this.params = new ParamInfo[fragments.size() - 1];
        clearParams();
    }

    ParamInfo getParam(int param) {
        if (param < 1 || param > params.length) {
            throw new JdbcException("Invalid parameter index %s", param);
        }
        return params[param - 1];
    }

    void setParam(int param, Object value, JDBCType type) {
        if (param < 1 || param > params.length) {
            throw new JdbcException("Invalid parameter index %s", param);
        }
        params[param - 1].value = value;
        params[param - 1].type = type;
    }

    int paramCount() {
        return params.length;
    }

    void clearParams() {
        for (int i = 0; i < params.length; i++) {
            params[i] = new ParamInfo(null, JDBCType.VARCHAR);
        }
    }

    String assemble() {
        StringBuilder sb = new StringBuilder();
        for (int i = 0; i < fragments.size(); i++) {
            sb.append(fragments.get(i));
            if (i < params.length) {
                sb.append(params[i]);
            }
        }

        return sb.toString();
    }

    @Override
    public String toString() {
        return assemble();
    }

    // Find the ? parameters for binding
    // Additionally, throw away all JDBC escaping
    static PreparedQuery prepare(String sql) {
        int l = sql.length();

        List<String> fragments = new ArrayList<>();
        StringBuilder current = new StringBuilder();

        for (int i = 0; i < l; i++) {
            char c = sql.charAt(i);

            switch (c) {
                // JDBC escape syntax
                // https://db.apache.org/derby/docs/10.5/ref/rrefjdbc1020262.html
                case '{':
                    jdbcEscape();
                    break;
                case '\'':
                    i = string(i, sql, current, c);
                    break;
                case '"':
                    i = string(i, sql, current, c);
                    break;
                case '?':
                    fragments.add(current.toString());
                    current.setLength(0);
                    i++;
                    break;
                case '-':
                    if (i + 1 < l && sql.charAt(i + 1) == '-') {
                        i = lineComment(i, sql, current);
                    }
                    else {
                        current.append(c);
                    }
                    break;
                case '/':
                    if (i + 1 < l && sql.charAt(i + 1) == '*') {
                        i = multiLineComment(i, sql, current);
                    }
                    else {
                        current.append(c);
                    }
                    break;

                default:
                    current.append(c);
                    break;
            }
        }
        
        fragments.add(current.toString());

        return new PreparedQuery(fragments);
    }

    private static void jdbcEscape() {
        throw new JdbcException("JDBC escaping not supported yet");
    }


    private static int lineComment(int i, String sql, StringBuilder current) {
        for (; i < sql.length(); i++) {
            char c = sql.charAt(i);
            if (c != '\n' && c != '\r') {
                current.append(c);
            }
            else {
                return i;
            }
        }
        return i;
    }

    private static int multiLineComment(int i, String sql, StringBuilder current) {
        int block = 1;

        for (; i < sql.length() - 1; i++) {
            char c = sql.charAt(i);
            if (c == '/' && sql.charAt(i + 1) == '*') {
                current.append(c);
                current.append(sql.charAt(++i));
                block++;
            }
            else if (c == '*' && sql.charAt(i + 1) == '/') {
                current.append(c);
                current.append(sql.charAt(++i));
                block--;
            }
            else {
                current.append(c);
            }
            if (block == 0) {
                return i;
            }
        }
        throw new JdbcException("Cannot parse given sql; unclosed /* comment");
    }

    private static int string(int i, String sql, StringBuilder current, char q) {
        current.append(sql.charAt(i++));
        for (; i < sql.length(); i++) {
            char c = sql.charAt(i);
            if (c == q) {
                current.append(c);
                // double quotes mean escaping
                if (sql.charAt(i + 1) == q) {
                    current.append(sql.charAt(++i));
                }
                else {
                    return i;
                }
            }
            else {
                current.append(c);
            }
        }
        throw new JdbcException("Cannot parse given sql; unclosed string");
    }

    static String escapeString(String s) {
        if (s == null) {
            return "NULL";
        }
        
        if (s.contains("'") ) {
            s = escapeString(s, '\'');
        }
        if (s.contains("\"")) {
            s = escapeString(s, '"');
        }
        
        // add quotes
        return "'" + s + "'";
    }

    private static String escapeString(String s, char sq) {
        StringBuilder sb = new StringBuilder();
        
        // escape individual single quotes
        for (int i = 0; i < s.length(); i++) {
            char c = s.charAt(i);

            // needs escaping
            if (c == sq) {
                // check if it's already escaped
                if (s.charAt(i + 1) == sq) {
                    i++;
                }
                sb.append(c);
                sb.append(c);
            }
        }
        
        return sb.toString();
    }
}