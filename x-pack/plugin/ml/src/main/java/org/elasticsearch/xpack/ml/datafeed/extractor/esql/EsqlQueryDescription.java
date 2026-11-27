/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.ml.datafeed.extractor.esql;

import org.elasticsearch.xpack.core.esql.action.ColumnInfo;
import org.elasticsearch.xpack.core.esql.action.EsqlClusterInfo;
import org.elasticsearch.xpack.core.esql.action.EsqlResponse;
import org.elasticsearch.xpack.core.ml.job.messages.Messages;

import java.util.Collections;
import java.util.Iterator;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

/**
 * What {@code <query> | DESCRIBE} reports about an ES|QL query: the type of each output column, in output order, and the per-cluster
 * metadata of the execution. Rows are read by column name, so extra columns are ignored.
 */
record EsqlQueryDescription(Map<String, String> columnTypes, List<EsqlClusterInfo> clusters) {

    static EsqlQueryDescription from(EsqlResponse response) {
        List<? extends ColumnInfo> header = response.columns();
        int name = indexOf(header, "name");
        int type = indexOf(header, "type");
        Map<String, String> columnTypes = new LinkedHashMap<>();
        for (Iterable<Object> row : response.rows()) {
            int position = 0;
            String columnName = null;
            String columnType = null;
            for (Iterator<Object> it = row.iterator(); it.hasNext(); position++) {
                Object value = it.next();
                if (position == name) {
                    columnName = String.valueOf(value);
                } else if (position == type) {
                    columnType = String.valueOf(value);
                }
            }
            columnTypes.put(columnName, columnType);
        }
        return new EsqlQueryDescription(Collections.unmodifiableMap(columnTypes), List.copyOf(response.clusters()));
    }

    private static int indexOf(List<? extends ColumnInfo> header, String name) {
        for (int i = 0; i < header.size(); i++) {
            if (name.equals(header.get(i).name())) {
                return i;
            }
        }
        throw new IllegalArgumentException(Messages.getMessage(Messages.DATAFEED_ESQL_RESPONSE_MISSING_COLUMN, name));
    }
}
