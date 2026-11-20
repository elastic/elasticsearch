/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.ml.datafeed.extractor.esql;

import org.elasticsearch.core.Nullable;
import org.elasticsearch.xpack.core.esql.action.ColumnInfo;
import org.elasticsearch.xpack.core.esql.action.EsqlClusterInfo;
import org.elasticsearch.xpack.core.esql.action.EsqlResponse;
import org.elasticsearch.xpack.core.ml.job.messages.Messages;

import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;

/**
 * What {@code <query> | DESCRIBE} reports about an ES|QL query: one {@link Column} per output column, and the per-cluster metadata
 * of the execution. Rows are read by column name, so extra columns are ignored and the optional ones may be absent.
 */
record EsqlQueryDescription(List<Column> columns, List<EsqlClusterInfo> clusters) {

    /** One DESCRIBE row; everything but {@code name} and {@code type} is null when DESCRIBE does not report it. */
    record Column(
        String name,
        String type,
        @Nullable String role,
        @Nullable String field,
        @Nullable String origin,
        @Nullable String interval,
        @Nullable String timeZone
    ) {}

    static EsqlQueryDescription from(EsqlResponse response) {
        List<? extends ColumnInfo> header = response.columns();
        int name = indexOf(header, "name", true);
        int type = indexOf(header, "type", true);
        int role = indexOf(header, "role", false);
        int field = indexOf(header, "field", false);
        int origin = indexOf(header, "origin", false);
        int interval = indexOf(header, "interval", false);
        int timeZone = indexOf(header, "time_zone", false);
        List<Column> columns = new ArrayList<>();
        for (Iterable<Object> row : response.rows()) {
            List<String> values = new ArrayList<>();
            for (Iterator<Object> it = row.iterator(); it.hasNext();) {
                Object value = it.next();
                values.add(value == null ? null : value.toString());
            }
            columns.add(
                new Column(
                    valueAt(values, name),
                    valueAt(values, type),
                    valueAt(values, role),
                    valueAt(values, field),
                    valueAt(values, origin),
                    valueAt(values, interval),
                    valueAt(values, timeZone)
                )
            );
        }
        return new EsqlQueryDescription(List.copyOf(columns), List.copyOf(response.clusters()));
    }

    /** The output column called {@code name}, or {@code null}. */
    @Nullable
    Column column(String name) {
        for (Column column : columns) {
            if (name.equals(column.name())) {
                return column;
            }
        }
        return null;
    }

    private static int indexOf(List<? extends ColumnInfo> header, String name, boolean required) {
        for (int i = 0; i < header.size(); i++) {
            if (name.equals(header.get(i).name())) {
                return i;
            }
        }
        if (required) {
            throw new IllegalArgumentException(Messages.getMessage(Messages.DATAFEED_ESQL_RESPONSE_MISSING_COLUMN, name));
        }
        return -1;
    }

    @Nullable
    private static String valueAt(List<String> values, int index) {
        return index >= 0 && index < values.size() ? values.get(index) : null;
    }
}
