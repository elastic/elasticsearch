/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.ml.datafeed.extractor.esql;

import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.xpack.core.esql.action.ColumnInfo;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.time.Instant;
import java.util.Arrays;
import java.util.Iterator;
import java.util.List;

import static org.hamcrest.Matchers.containsString;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.not;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

public class EsqlToJsonProcessorTests extends ESTestCase {

    private static final String TIME_FIELD = "ts";
    private static final String DATE = "date";
    private static final String DATE_NANOS = "date_nanos";
    private static final String LONG = "long";
    private static final String KEYWORD = "keyword";
    private static final EsqlToJsonProcessor.TimeWindow WINDOW = new EsqlToJsonProcessor.TimeWindow("job-1", TIME_FIELD, 0L, 10_000L);

    public void testStringAndLongFieldsShouldPassThrough() throws IOException {
        String ndjson = rowsToString(
            List.of(column(TIME_FIELD, DATE), column("host", KEYWORD), column("count", LONG)),
            List.of("1970-01-01T00:00:01.500Z", "web-1", 7L)
        );

        assertThat(ndjson, containsString("\"" + TIME_FIELD + "\":1500"));
        assertThat(ndjson, containsString("\"host\":\"web-1\""));
        assertThat(ndjson, containsString("\"count\":7"));
    }

    public void testDateColumnShouldConvertToEpochMillis() throws IOException {
        String isoDate = isoAtEpochMillis(500L);

        assertThat(rowsToString(List.of(column(TIME_FIELD, DATE)), List.of(isoDate)), equalTo("{\"" + TIME_FIELD + "\":500}"));
    }

    public void testDateNanosColumnShouldConvertToEpochMillis() throws IOException {
        String isoDate = "1970-01-01T00:00:00.500123456Z";
        long expectedMillis = Instant.parse(isoDate).toEpochMilli();

        assertThat(
            rowsToString(List.of(column(TIME_FIELD, DATE_NANOS)), List.of(isoDate)),
            equalTo("{\"" + TIME_FIELD + "\":" + expectedMillis + "}")
        );
    }

    public void testMultiValuedNonDateColumnShouldPassThrough() throws IOException {
        assertThat(
            rowsToString(List.of(column(TIME_FIELD, DATE), column("tags", KEYWORD)), List.of(isoAtEpochMillis(100L), List.of("a", "b"))),
            containsString("\"tags\":[\"a\",\"b\"]")
        );
    }

    public void testNullFieldShouldBeOmittedFromRow() throws IOException {
        String ndjson = rowsToString(
            List.of(column(TIME_FIELD, DATE), column("optional_field", KEYWORD)),
            Arrays.asList(isoAtEpochMillis(100L), null)
        );

        assertThat(ndjson, containsString("\"" + TIME_FIELD + "\":"));
        assertThat(ndjson, not(containsString("optional_field")));
    }

    public void testMultipleRowsShouldIncrementWrittenDocumentCount() throws IOException {
        ByteArrayOutputStream outputStream = new ByteArrayOutputStream();
        List<ColumnInfo> columns = List.of(column(TIME_FIELD, DATE));
        try (EsqlToJsonProcessor processor = new EsqlToJsonProcessor(columns, WINDOW, outputStream)) {
            process(processor, List.of(isoAtEpochMillis(100L)));
            process(processor, List.of(isoAtEpochMillis(200L)));
            assertThat(processor.getWrittenDocumentCount(), equalTo(2L));
        }
    }

    public void testTimeOutsideTheWindowShouldFailTheRowAfterEarlierRowsWereWritten() throws IOException {
        ByteArrayOutputStream outputStream = new ByteArrayOutputStream();
        List<ColumnInfo> columns = List.of(column("host", KEYWORD), column(TIME_FIELD, DATE));
        try (EsqlToJsonProcessor processor = new EsqlToJsonProcessor(columns, WINDOW, outputStream)) {
            process(processor, List.of("web-1", isoAtEpochMillis(100L)));
            IllegalArgumentException e = expectThrows(
                IllegalArgumentException.class,
                () -> process(processor, List.of("web-2", isoAtEpochMillis(10_000L)))
            );
            assertThat(e.getMessage(), containsString("job-1"));
            assertThat(processor.getWrittenDocumentCount(), equalTo(1L));
        }
    }

    public void testRowWithoutTimeValueShouldFail() throws IOException {
        List<ColumnInfo> columns = List.of(column("host", KEYWORD), column(TIME_FIELD, DATE));
        expectThrows(IllegalArgumentException.class, () -> rowsToString(columns, List.of("web-1")));
        expectThrows(IllegalArgumentException.class, () -> rowsToString(columns, Arrays.asList("web-1", null)));
    }

    public void testMissingTimeColumnShouldFailBeforeWriting() {
        IllegalArgumentException e = expectThrows(
            IllegalArgumentException.class,
            () -> new EsqlToJsonProcessor(List.of(column("host", KEYWORD)), WINDOW, new ByteArrayOutputStream())
        );
        assertThat(e.getMessage(), containsString(TIME_FIELD));
    }

    public void testRowShouldBeReadOnceWhenTimeColumnIsLast() throws IOException {
        List<ColumnInfo> columns = List.of(column("host", KEYWORD), column("count", LONG), column(TIME_FIELD, DATE));
        List<Object> values = List.of("web-1", 7L, isoAtEpochMillis(100L));
        int[] cellsRead = { 0 };
        Iterable<Object> countingRow = () -> {
            Iterator<Object> it = values.iterator();
            return new Iterator<>() {
                @Override
                public boolean hasNext() {
                    return it.hasNext();
                }

                @Override
                public Object next() {
                    cellsRead[0]++;
                    return it.next();
                }
            };
        };
        try (EsqlToJsonProcessor processor = new EsqlToJsonProcessor(columns, WINDOW, new ByteArrayOutputStream())) {
            processor.process(countingRow, values.get(processor.timeColumnIndex()));
        }
        assertThat(cellsRead[0], equalTo(values.size()));
    }

    /** Passes the time value the way the extractor does: the cell at the time column index, or null when the row is shorter. */
    private static void process(EsqlToJsonProcessor processor, List<Object> row) throws IOException {
        int index = processor.timeColumnIndex();
        processor.process(row, index < row.size() ? row.get(index) : null);
    }

    private static String rowsToString(List<ColumnInfo> columns, List<Object> rowValues) throws IOException {
        ByteArrayOutputStream outputStream = new ByteArrayOutputStream();
        try (EsqlToJsonProcessor processor = new EsqlToJsonProcessor(columns, WINDOW, outputStream)) {
            process(processor, rowValues);
        }
        return outputStream.toString(StandardCharsets.UTF_8).stripTrailing();
    }

    private static ColumnInfo column(String name, String outputType) {
        ColumnInfo col = mock(ColumnInfo.class);
        when(col.name()).thenReturn(name);
        when(col.outputType()).thenReturn(outputType);
        return col;
    }

    private static String isoAtEpochMillis(long millis) {
        return Instant.ofEpochMilli(millis).toString();
    }
}
