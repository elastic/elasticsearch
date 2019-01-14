/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.xpack.sql.qa.jdbc;

import org.apache.logging.log4j.Logger;
import org.elasticsearch.xpack.sql.action.CliFormatter;
import org.elasticsearch.xpack.sql.proto.ColumnInfo;
import org.elasticsearch.xpack.sql.proto.StringUtils;

import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.SQLException;
import java.time.Instant;
import java.time.ZoneId;
import java.time.ZonedDateTime;
import java.util.ArrayList;
import java.util.List;

public abstract class JdbcTestUtils {

    public static final String SQL_TRACE = "org.elasticsearch.xpack.sql:TRACE";

    public static final String JDBC_TIMEZONE = "timezone";
    
    public static ZoneId UTC = ZoneId.of("Z");

    public static void logResultSetMetadata(ResultSet rs, Logger logger) throws SQLException {
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

    private static final int MAX_WIDTH = 20;

    public static void logResultSetData(ResultSet rs, Logger log) throws SQLException {
        ResultSetMetaData metaData = rs.getMetaData();
        StringBuilder sb = new StringBuilder();
        StringBuilder column = new StringBuilder();

        int columns = metaData.getColumnCount();

        while (rs.next()) {
            sb.setLength(0);
            for (int i = 1; i <= columns; i++) {
                column.setLength(0);
                if (i > 1) {
                    sb.append(" | ");
                }
                sb.append(trimOrPad(column.append(rs.getString(i))));
            }
            log.info(sb);
        }
    }

    public static String resultSetCurrentData(ResultSet rs) throws SQLException {
        ResultSetMetaData metaData = rs.getMetaData();
        StringBuilder column = new StringBuilder();

        int columns = metaData.getColumnCount();

        StringBuilder sb = new StringBuilder();
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
        }
        else {
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
            cols.add(new ColumnInfo(metaData.getTableName(i), metaData.getColumnName(i), metaData.getColumnTypeName(i),
                    metaData.getColumnType(i), metaData.getColumnDisplaySize(i)));
        }


        List<List<Object>> data = new ArrayList<>();

        while (rs.next()) {
            List<Object> entry = new ArrayList<>(columns);
            for (int i = 1; i <= columns; i++) {
                entry.add(rs.getObject(i));
            }
            data.add(entry);
        }

        CliFormatter formatter = new CliFormatter(cols, data);
        logger.info("\n" + formatter.formatWithHeader(cols, data));
    }
    
    public static String of(long millis) {
        return StringUtils.toString(ZonedDateTime.ofInstant(Instant.ofEpochMilli(millis), UTC));
    }
}