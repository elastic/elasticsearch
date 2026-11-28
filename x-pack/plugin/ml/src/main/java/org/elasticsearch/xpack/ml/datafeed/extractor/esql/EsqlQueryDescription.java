/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.ml.datafeed.extractor.esql;

import org.elasticsearch.xpack.core.esql.action.ColumnInfo;
import org.elasticsearch.xpack.core.esql.action.EsqlResponse;
import org.elasticsearch.xpack.core.ml.job.messages.Messages;

import java.util.Collections;
import java.util.Iterator;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

/**
 * What {@code <query> | DESCRIBE} reports about an ES|QL query: the type of each output column, in output order, its origin (for
 * example {@code mapped}, {@code renamed} or {@code computed}), its source field, and its bucket interval and time zone. Only
 * columns that report a value appear in the maps after the first. Rows are read by column name, so extra columns are ignored.
 */
record EsqlQueryDescription(
    Map<String, String> columnTypes,
    Map<String, String> columnOrigins,
    Map<String, String> columnFields,
    Map<String, String> columnIntervals,
    Map<String, String> columnTimeZones
) {

    static EsqlQueryDescription from(EsqlResponse response) {
        List<? extends ColumnInfo> header = response.columns();
        int name = indexOf(header, "name");
        int type = indexOf(header, "type");
        int origin = optionalIndexOf(header, "origin");
        int field = optionalIndexOf(header, "field");
        int interval = optionalIndexOf(header, "interval");
        int timeZone = optionalIndexOf(header, "time_zone");
        Map<String, String> columnTypes = new LinkedHashMap<>();
        Map<String, String> columnOrigins = new LinkedHashMap<>();
        Map<String, String> columnFields = new LinkedHashMap<>();
        Map<String, String> columnIntervals = new LinkedHashMap<>();
        Map<String, String> columnTimeZones = new LinkedHashMap<>();
        for (Iterable<Object> row : response.rows()) {
            int position = 0;
            String columnName = null;
            String columnType = null;
            String columnOrigin = null;
            String columnField = null;
            String columnInterval = null;
            String columnTimeZone = null;
            for (Iterator<Object> it = row.iterator(); it.hasNext(); position++) {
                Object value = it.next();
                if (position == name) {
                    columnName = String.valueOf(value);
                } else if (position == type) {
                    columnType = String.valueOf(value);
                } else if (position == origin && value != null) {
                    columnOrigin = String.valueOf(value);
                } else if (position == field && value != null) {
                    columnField = String.valueOf(value);
                } else if (position == interval && value != null) {
                    columnInterval = String.valueOf(value);
                } else if (position == timeZone && value != null) {
                    columnTimeZone = String.valueOf(value);
                }
            }
            columnTypes.put(columnName, columnType);
            if (columnOrigin != null) {
                columnOrigins.put(columnName, columnOrigin);
            }
            if (columnField != null) {
                columnFields.put(columnName, columnField);
            }
            if (columnInterval != null) {
                columnIntervals.put(columnName, columnInterval);
            }
            if (columnTimeZone != null) {
                columnTimeZones.put(columnName, columnTimeZone);
            }
        }
        return new EsqlQueryDescription(
            Collections.unmodifiableMap(columnTypes),
            Collections.unmodifiableMap(columnOrigins),
            Collections.unmodifiableMap(columnFields),
            Collections.unmodifiableMap(columnIntervals),
            Collections.unmodifiableMap(columnTimeZones)
        );
    }

    private static int optionalIndexOf(List<? extends ColumnInfo> header, String name) {
        for (int i = 0; i < header.size(); i++) {
            if (name.equals(header.get(i).name())) {
                return i;
            }
        }
        return -1;
    }

    private static int indexOf(List<? extends ColumnInfo> header, String name) {
        int index = optionalIndexOf(header, name);
        if (index >= 0) {
            return index;
        }
        throw new IllegalArgumentException(Messages.getMessage(Messages.DATAFEED_ESQL_RESPONSE_MISSING_COLUMN, name));
    }
}
