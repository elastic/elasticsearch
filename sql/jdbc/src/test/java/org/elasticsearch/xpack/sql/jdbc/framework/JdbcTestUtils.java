/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.xpack.sql.jdbc.framework;

import org.apache.logging.log4j.Logger;
import org.elasticsearch.common.logging.Loggers;
import org.elasticsearch.xpack.sql.util.CollectionUtils;

import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.SQLException;
import java.util.Map;
import java.util.Map.Entry;

public abstract class JdbcTestUtils {

    public static void sqlLogging() {
        String t = "TRACE";
        String d = "DEBUG";
        
        Map<String, String> of = CollectionUtils.of("org.elasticsearch.xpack.sql.parser", t, 
                "org.elasticsearch.xpack.sql.analysis.analyzer", t, 
                "org.elasticsearch.xpack.sql.optimizer", t, 
                "org.elasticsearch.xpack.sql.rule", t, 
                "org.elasticsearch.xpack.sql.planner", t,
                "org.elasticsearch.xpack.sql.execution.search", t);
        
        for (Entry<String, String> entry : of.entrySet()) {
            Loggers.setLevel(Loggers.getLogger(entry.getKey()), entry.getValue());
        }
    }

    public static void printResultSet(ResultSet set) throws Exception {
        Logger logger = Loggers.getLogger("org.elasticsearch.xpack.sql.test");
        Loggers.setLevel(logger, "INFO");

        ResultSetMetaData metaData = set.getMetaData();
        // header
        StringBuilder sb = new StringBuilder();

        int colSize = 15;
        for (int column = 1; column <= metaData.getColumnCount(); column++) {
            String colName = metaData.getColumnName(column);

            int size = colName.length();
            if (column > 1) {
                sb.append("|");
                size++;
            }
            sb.append(colName);
            for (int i = size; i < colSize; i++) {
                sb.append(" ");
            }
        }

        logger.info(sb.toString());
    }

    private static final int MAX_WIDTH = 20;

    public static void resultSetToLogger(Logger log, ResultSet rs) throws SQLException {
        ResultSetMetaData metaData = rs.getMetaData();
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
        sb.append("\n");
        for (int i = 0; i < l; i++) {
            sb.append("=");
        }
        log.info(sb);

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
}