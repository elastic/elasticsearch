/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */
package org.elasticsearch.xpack.sql.qa.jdbc;

import org.apache.logging.log4j.Logger;
import org.elasticsearch.xpack.sql.proto.ColumnInfo;
import org.elasticsearch.xpack.sql.proto.formatter.SimpleFormatter;

import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.List;

import static org.elasticsearch.xpack.sql.proto.formatter.SimpleFormatter.FormatOption.CLI;

final class JdbcTestUtils {

    private JdbcTestUtils() {}

    private static final int MAX_WIDTH = 20;

    static final String SQL_TRACE = "org.elasticsearch.xpack.sql:TRACE";
    static final String JDBC_TIMEZONE = "timezone";

    static void logResultSetMetaData(ResultSet rs, Logger logger) throws SQLException {
        ResultSetMetaData metaData = rs.getMetaData();
        // header
        StringBuilder sb = new StringBuilder();
        StringBuilder column = new StringBuilder();

        int columns = metaData.getColumnCount();
        for (int i = 1; i <= columns; i++) {
            if (i > 1) {
                sb.append(" | ");
            }
            column.setLength(0);
            column.append(metaData.getColumnName(i));
            column.append("(");
            column.append(metaData.getColumnTypeName(i));
            column.append(")");

            sb.append(trimOrPad(column));
        }

        int l = sb.length();
        logger.info(sb.toString());
        sb.setLength(0);
        for (int i = 0; i < l; i++) {
            sb.append("-");
        }

        logger.info(sb.toString());
    }

    static void logResultSetData(ResultSet rs, Logger log) throws SQLException {
        ResultSetMetaData metaData = rs.getMetaData();

        int columns = metaData.getColumnCount();

        while (rs.next()) {
            log.info(rowAsString(rs, columns));
        }
    }

    static String resultSetCurrentData(ResultSet rs) throws SQLException {
        ResultSetMetaData metaData = rs.getMetaData();
        return rowAsString(rs, metaData.getColumnCount());
    }

    private static String rowAsString(ResultSet rs, int columns) throws SQLException {
        StringBuilder sb = new StringBuilder();
        StringBuilder column = new StringBuilder();
        for (int i = 1; i <= columns; i++) {
            column.setLength(0);
            if (i > 1) {
                sb.append(" | ");
            }
            sb.append(trimOrPad(column.append(rs.getString(i))));
        }
        return sb.toString();
    }

    private static StringBuilder trimOrPad(StringBuilder buffer) {
        if (buffer.length() > MAX_WIDTH) {
            buffer.setLength(MAX_WIDTH - 1);
            buffer.append("~");
        } else {
            for (int i = buffer.length(); i < MAX_WIDTH; i++) {
                buffer.append(" ");
            }
        }
        return buffer;
    }

    public static void logLikeCLI(ResultSet rs, Logger logger) throws SQLException {
        ResultSetMetaData metaData = rs.getMetaData();
        int columns = metaData.getColumnCount();

        List<ColumnInfo> cols = new ArrayList<>(columns);

        for (int i = 1; i <= columns; i++) {
            cols.add(
                new ColumnInfo(
                    metaData.getTableName(i),
                    metaData.getColumnName(i),
                    metaData.getColumnTypeName(i),
                    metaData.getColumnDisplaySize(i)
                )
            );
        }

        List<List<Object>> data = new ArrayList<>();

        while (rs.next()) {
            List<Object> entry = new ArrayList<>(columns);
            for (int i = 1; i <= columns; i++) {
                entry.add(rs.getObject(i));
            }
            data.add(entry);
        }

        SimpleFormatter formatter = new SimpleFormatter(cols, data, CLI);
        logger.info("\n" + formatter.formatWithHeader(cols, data));
    }
}
