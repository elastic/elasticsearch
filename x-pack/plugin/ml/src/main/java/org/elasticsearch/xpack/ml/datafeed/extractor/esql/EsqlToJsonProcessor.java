/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.ml.datafeed.extractor.esql;

import org.elasticsearch.core.Releasable;
import org.elasticsearch.xcontent.XContentBuilder;
import org.elasticsearch.xcontent.json.JsonXContent;
import org.elasticsearch.xpack.core.esql.action.ColumnInfo;

import java.io.IOException;
import java.io.OutputStream;
import java.util.ArrayList;
import java.util.List;
import java.util.Objects;

class EsqlToJsonProcessor implements Releasable {

    private final List<? extends ColumnInfo> columns;
    private final boolean[] isDateColumn;
    private final OutputStream outputStream;
    private final XContentBuilder jsonBuilder;
    private long writtenDocumentCount;

    EsqlToJsonProcessor(List<? extends ColumnInfo> columns, OutputStream outputStream) throws IOException {
        this.columns = Objects.requireNonNull(columns);
        this.outputStream = Objects.requireNonNull(outputStream);
        this.isDateColumn = new boolean[columns.size()];
        for (int i = 0; i < columns.size(); i++) {
            isDateColumn[i] = EsqlDatafeedColumnValues.isDateColumnType(columns.get(i).outputType());
        }
        this.jsonBuilder = new XContentBuilder(JsonXContent.jsonXContent, outputStream);
    }

    void process(Iterable<Object> row) throws IOException {
        jsonBuilder.startObject();
        int columnIndex = 0;
        for (Object value : row) {
            if (value != null) {
                writeField(columns.get(columnIndex).name(), value, isDateColumn[columnIndex]);
            }
            columnIndex++;
        }
        jsonBuilder.endObject();
        jsonBuilder.flush();
        outputStream.write('\n');
        writtenDocumentCount++;
    }

    long getWrittenDocumentCount() {
        return writtenDocumentCount;
    }

    private void writeField(String name, Object value, boolean isDate) throws IOException {
        if (isDate) {
            if (value instanceof List<?> list) {
                List<Long> epochMillis = new ArrayList<>(list.size());
                for (Object element : list) {
                    epochMillis.add(EsqlDatafeedColumnValues.toEpochMillis(element, true));
                }
                jsonBuilder.field(name, epochMillis);
            } else {
                jsonBuilder.field(name, EsqlDatafeedColumnValues.toEpochMillis(value, true));
            }
        } else if (value instanceof List<?> list) {
            jsonBuilder.field(name, list);
        } else {
            jsonBuilder.field(name, value);
        }
    }

    @Override
    public void close() {
        jsonBuilder.close();
    }
}
