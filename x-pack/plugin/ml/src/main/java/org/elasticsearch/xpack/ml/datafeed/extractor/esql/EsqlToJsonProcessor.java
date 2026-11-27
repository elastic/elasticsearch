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
import org.elasticsearch.xpack.core.ml.job.messages.Messages;

import java.io.IOException;
import java.io.OutputStream;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.Objects;

/**
 * Writes ES|QL rows as newline-delimited JSON documents for autodetect. While it writes a row it checks the row's time against the
 * extraction window, so the rows are read once and never copied.
 */
class EsqlToJsonProcessor implements Releasable {

    /** The extraction window the time column of every row must fall in, {@code [start, end)}. */
    record TimeWindow(String jobId, String timeField, long start, long end) {}

    private final List<? extends ColumnInfo> columns;
    private final TimeWindow timeWindow;
    private final int timeColumnIndex;
    private final boolean[] isDateColumn;
    private final OutputStream outputStream;
    private final XContentBuilder jsonBuilder;
    private long writtenDocumentCount;

    EsqlToJsonProcessor(List<? extends ColumnInfo> columns, TimeWindow timeWindow, OutputStream outputStream) throws IOException {
        this.columns = Objects.requireNonNull(columns);
        this.timeWindow = Objects.requireNonNull(timeWindow);
        this.timeColumnIndex = indexOfColumn(columns, timeWindow.timeField());
        this.outputStream = Objects.requireNonNull(outputStream);
        this.isDateColumn = new boolean[columns.size()];
        for (int i = 0; i < columns.size(); i++) {
            isDateColumn[i] = EsqlDatafeedColumnValues.isDateColumnType(columns.get(i).outputType());
        }
        this.jsonBuilder = new XContentBuilder(JsonXContent.jsonXContent, outputStream);
    }

    /** Writes {@code row}, or fails without writing it when its time is null, multi-valued or outside the window. */
    void process(Iterable<Object> row) throws IOException {
        // Check before starting the object, so a failing row leaves no partial document behind.
        long time = checkedTime(valueAt(row, timeColumnIndex));
        jsonBuilder.startObject();
        int columnIndex = 0;
        for (Object value : row) {
            if (columnIndex == timeColumnIndex) {
                jsonBuilder.field(columns.get(columnIndex).name(), time);
            } else if (value != null) {
                writeField(columns.get(columnIndex).name(), value, isDateColumn[columnIndex]);
            }
            columnIndex++;
        }
        jsonBuilder.endObject();
        jsonBuilder.flush();
        outputStream.write('\n');
        writtenDocumentCount++;
    }

    private long checkedTime(Object value) {
        return EsqlDatafeedQueryValidator.validateEmittedTimeValueInSourceWindow(
            timeWindow.jobId(),
            timeWindow.timeField(),
            value,
            timeWindow.start(),
            timeWindow.end()
        );
    }

    /** The value at {@code columnIndex}, or null when the row is shorter. */
    private static Object valueAt(Iterable<Object> row, int columnIndex) {
        Iterator<Object> values = row.iterator();
        for (int index = 0; index < columnIndex && values.hasNext(); index++) {
            values.next();
        }
        return values.hasNext() ? values.next() : null;
    }

    private static int indexOfColumn(List<? extends ColumnInfo> columns, String columnName) {
        for (int index = 0; index < columns.size(); index++) {
            if (columnName.equals(columns.get(index).name())) {
                return index;
            }
        }
        throw new IllegalArgumentException(Messages.getMessage(Messages.DATAFEED_ESQL_RESPONSE_MISSING_COLUMN, columnName));
    }

    long getWrittenDocumentCount() {
        return writtenDocumentCount;
    }

    private void writeField(String name, Object value, boolean isDate) throws IOException {
        if (isDate) {
            if (value instanceof List<?> list) {
                List<Long> epochMillis = new ArrayList<>(list.size());
                for (Object element : list) {
                    epochMillis.add(EsqlDatafeedColumnValues.toEpochMillis(element));
                }
                jsonBuilder.field(name, epochMillis);
            } else {
                jsonBuilder.field(name, EsqlDatafeedColumnValues.toEpochMillis(value));
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
