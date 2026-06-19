/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.datasource.csv;

import org.apache.lucene.util.BytesRef;
import org.elasticsearch.common.breaker.NoopCircuitBreaker;
import org.elasticsearch.common.util.BigArrays;
import org.elasticsearch.compute.data.BlockFactory;
import org.elasticsearch.compute.data.BytesRefBlock;
import org.elasticsearch.compute.data.Page;
import org.elasticsearch.compute.operator.CloseableIterator;
import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.xpack.esql.datasources.spi.FormatReadContext;
import org.elasticsearch.xpack.esql.datasources.spi.RecordSplitter;
import org.elasticsearch.xpack.esql.datasources.spi.StorageObject;
import org.elasticsearch.xpack.esql.datasources.spi.StoragePath;

import java.io.BufferedInputStream;
import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.nio.charset.StandardCharsets;
import java.time.Instant;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;

import static org.hamcrest.Matchers.instanceOf;

/**
 * End-to-end behavior of the three dialects through {@link CsvFormatReader#read}: {@code plain}
 * (every byte literal), {@code escaped} (ClickHouse/MySQL/Postgres backslash semantics), and
 * {@code quoted} (RFC 4180, unchanged). The plain/escaped cases are the cure for the original
 * field-leading-quote failure: under the quoted dialect a value starting with {@code "} opens a
 * quoted region tab-delimited data never closes, gluing rows until the record cap.
 */
public class CsvDialectReadTests extends ESTestCase {

    private BlockFactory blockFactory;

    @Override
    public void setUp() throws Exception {
        super.setUp();
        blockFactory = BlockFactory.builder(BigArrays.NON_RECYCLING_INSTANCE).breaker(new NoopCircuitBreaker("none")).build();
    }

    /**
     * The original bug, as data: a TSV value that STARTS with a literal {@code "}. Under
     * {@code plain} the quote is a byte like any other — every row arrives, none glue.
     */
    public void testPlainTsvFieldLeadingQuoteReadsExactRows() throws IOException {
        int rows = 50;
        StringBuilder tsv = new StringBuilder("id:keyword\tnote:keyword\n");
        for (int i = 0; i < rows; i++) {
            // every 7th note starts with a lone quote that never closes
            String note = i % 7 == 0 ? "\"unbalanced quote " + i : "plain note " + i;
            tsv.append("id").append(i).append('\t').append(note).append('\n');
        }
        List<List<String>> values = readAll(tsvReader(Map.of("dialect", "plain")), tsv.toString());
        assertEquals(rows, values.size());
        assertEquals("\"unbalanced quote 0", values.get(0).get(1));
        assertEquals("\"unbalanced quote 49", values.get(49).get(1));
    }

    /** Under {@code plain} nothing decodes: a backslash sequence and {@code \N} are literal data. */
    public void testPlainKeepsBackslashSequencesLiteral() throws IOException {
        String tsv = """
            path:keyword\tnote:keyword
            C:\\temp\t\\N
            """;
        List<List<String>> values = readAll(tsvReader(Map.of("dialect", "plain")), tsv);
        assertEquals(1, values.size());
        assertEquals("C:\\temp", values.get(0).get(0));
        assertEquals("\\N", values.get(0).get(1));
    }

    /** {@code escaped}: {@code \t}/{@code \n} decode to the control bytes, {@code \\} to one backslash. */
    public void testEscapedDecodesClickHouseSequences() throws IOException {
        String tsv = """
            a:keyword\tb:keyword\tc:keyword
            has\\ttab\tline1\\nline2\tC:\\\\temp
            """;
        List<List<String>> values = readAll(tsvReader(Map.of("dialect", "escaped")), tsv);
        assertEquals(1, values.size());
        assertEquals("has\ttab", values.get(0).get(0));
        assertEquals("line1\nline2", values.get(0).get(1));
        assertEquals("C:\\temp", values.get(0).get(2));
    }

    /** {@code escaped}: a whole-field {@code \N} is null — the DB-export null representation. */
    public void testEscapedDecodesBackslashNAsNull() throws IOException {
        String tsv = """
            a:keyword\tb:keyword
            \\N\tvalue
            """;
        List<List<String>> values = readAll(tsvReader(Map.of("dialect", "escaped")), tsv);
        assertEquals(1, values.size());
        assertNull(values.get(0).get(0));
        assertEquals("value", values.get(0).get(1));
    }

    /**
     * {@code escaped} + a custom {@code null_value}: the two null routes stay consistent — a field
     * equal to {@code null_value} nulls via the tokenizer before the decode runs, and a whole-field
     * {@code \N} nulls via the decode. Both must land on null in the same read.
     */
    public void testEscapedWithCustomNullValueKeepsBothNullRoutes() throws IOException {
        String tsv = """
            a:keyword\tb:keyword\tc:keyword
            NULL\t\\N\tvalue
            """;
        List<List<String>> values = readAll(tsvReader(Map.of("dialect", "escaped", "null_value", "NULL")), tsv);
        assertEquals(1, values.size());
        assertNull(values.get(0).get(0)); // null_value match — tokenizer route
        assertNull(values.get(0).get(1)); // \N — decode route
        assertEquals("value", values.get(0).get(2));
    }

    /** {@code escaped} is still a no-quote dialect: a field-leading {@code "} is data, rows never glue. */
    public void testEscapedFieldLeadingQuoteIsData() throws IOException {
        String tsv = """
            a:keyword\tb:keyword
            "starts with quote\tnormal
            second row\talso normal
            """;
        List<List<String>> values = readAll(tsvReader(Map.of("dialect", "escaped")), tsv);
        assertEquals(2, values.size());
        assertEquals("\"starts with quote", values.get(0).get(0));
    }

    /** The default CSV path is untouched: RFC-4180 wrapping and doubling still work. */
    public void testQuotedDialectUnchanged() throws IOException {
        String csv = "a:keyword,b:keyword\n\"has,comma\",\"he said \"\"hi\"\"\"\n";
        List<List<String>> values = readAll(csvReader(Map.of()), csv);
        assertEquals(1, values.size());
        assertEquals("has,comma", values.get(0).get(0));
        assertEquals("he said \"hi\"", values.get(0).get(1));
    }

    /** Splitter dispatch is decided once, by dialect: no-quote dialects take the terminator scan. */
    public void testSplitterDispatchByDialect() throws IOException {
        assertThat(tsvReader(Map.of("dialect", "plain")).recordSplitter(1024), instanceOf(NewlineRecordSplitter.class));
        assertThat(tsvReader(Map.of("dialect", "escaped")).recordSplitter(1024), instanceOf(NewlineRecordSplitter.class));
        // The .tsv baseline is plain, so an unconfigured TSV reader takes the terminator scan; quoting is opt-in.
        assertThat(tsvReader(Map.of()).recordSplitter(1024), instanceOf(NewlineRecordSplitter.class));
        assertThat(tsvReader(Map.of("dialect", "quoted")).recordSplitter(1024), instanceOf(CsvRecordSplitter.class));
        assertThat(csvReader(Map.of()).recordSplitter(1024), instanceOf(CsvRecordSplitter.class));
    }

    /** Bare brackets selects QUOTED (bracket cells carry quoted elements), even on the plain .tsv baseline. */
    public void testBareBracketsSelectsQuotedDialect() throws IOException {
        assertThat(tsvReader(Map.of("multi_value_syntax", "brackets")).recordSplitter(1024), instanceOf(CsvRecordSplitter.class));
    }

    /**
     * The original ClickBench failure shape under the DEFAULT {@code .tsv} configuration — no
     * dialect option supplied. A field-leading {@code "} is data; rows never glue and the count is
     * exact.
     */
    public void testDefaultTsvFieldLeadingQuoteReadsExactRows() throws IOException {
        StringBuilder tsv = new StringBuilder("id:keyword\tnote:keyword\n");
        for (int i = 0; i < 50; i++) {
            String note = i % 10 == 0 ? "\"starts with a quote" : "plain note " + i;
            tsv.append("id").append(i).append('\t').append(note).append('\n');
        }
        List<List<String>> values = readAll(tsvReader(Map.of()), tsv.toString());
        assertEquals(50, values.size());
        assertEquals("\"starts with a quote", values.get(0).get(1));
    }

    /**
     * Volume guard for the merge: after schema resolution the data rows flow through the Jackson
     * BULK iterator (not the per-record sampler), which builds its schema from {@code newCsvSchema()}.
     * If that schema is not dialect-aware ({@code withoutQuoteChar()} for plain), a field-leading
     * {@code "} opens a region Jackson scans across newlines and rows glue. 500 rows span multiple
     * read batches, so this exercises the bulk path repeatedly, not a single small sample.
     */
    public void testBulkPathPlainHandlesFieldLeadingQuoteAtVolume() throws IOException {
        int rows = 500;
        StringBuilder tsv = new StringBuilder("id:keyword\tnote:keyword\n");
        for (int i = 0; i < rows; i++) {
            String note = i % 50 == 0 ? "\"unbalanced quote " + i : "note " + i;
            tsv.append("id").append(i).append('\t').append(note).append('\n');
        }
        List<List<String>> values = readAll(tsvReader(Map.of("dialect", "plain")), tsv.toString());
        assertEquals(rows, values.size());
        assertEquals("\"unbalanced quote 0", values.get(0).get(1)); // first quoted row, literal
        assertEquals("\"unbalanced quote 450", values.get(450).get(1)); // a later batch, still literal
    }

    /**
     * Volume guard: the escaped dialect decodes on the BULK data path (the merge's only per-value
     * seam there), so {@code \t} un-escapes and a whole-field {@code \N} is null across many batches,
     * exactly as on the per-record path.
     */
    public void testBulkPathEscapedDecodesAtVolume() throws IOException {
        int rows = 500;
        StringBuilder tsv = new StringBuilder("id:keyword\tnote:keyword\tmaybe:keyword\n");
        for (int i = 0; i < rows; i++) {
            tsv.append("id").append(i).append("\thas\\ttab").append('\t').append(i % 2 == 0 ? "\\N" : "present").append('\n');
        }
        List<List<String>> values = readAll(tsvReader(Map.of("dialect", "escaped")), tsv.toString());
        assertEquals(rows, values.size());
        assertEquals("has\ttab", values.get(0).get(1)); // \t decoded on the bulk path
        assertEquals("has\ttab", values.get(499).get(1)); // still decoded in a later batch
        assertNull(values.get(0).get(2)); // whole-field \N -> null
        assertEquals("present", values.get(1).get(2));
    }

    /** The no-quote splitter never reports a too-large record for well-formed newline-terminated data. */
    public void testNewlineSplitterBoundaries() throws IOException {
        NewlineRecordSplitter splitter = new NewlineRecordSplitter(32);
        byte[] data = "row with a \" quote\nsecond\n".getBytes(StandardCharsets.UTF_8);
        assertEquals("row with a \" quote\n".length(), splitter.findNextRecordBoundary(new ByteArrayInputStream(data)));
        assertEquals(data.length - 1, splitter.findLastRecordBoundary(data, 0, data.length));

        byte[] oversized = ("x".repeat(40) + "\n").getBytes(StandardCharsets.UTF_8);
        assertEquals(RecordSplitter.RECORD_TOO_LARGE, splitter.findNextRecordBoundary(new ByteArrayInputStream(oversized)));
    }

    /**
     * A lone-CR boundary restores the peeked byte: reading the NEXT record through the SAME stream
     * starts at the first byte after the CR, so nothing is dropped. (The stream is passed as a
     * {@link BufferedInputStream} because the splitter only reuses an already-buffered stream —
     * a fresh wrapper per call would discard the wrapper's read-ahead between calls.)
     */
    public void testNewlineSplitterLoneCrRestoresPeekedByte() throws IOException {
        NewlineRecordSplitter splitter = new NewlineRecordSplitter(32);
        byte[] data = "a\rb\n".getBytes(StandardCharsets.UTF_8);
        InputStream stream = new BufferedInputStream(new ByteArrayInputStream(data));
        assertEquals(2L, splitter.findNextRecordBoundary(stream)); // "a\r"
        assertEquals(2L, splitter.findNextRecordBoundary(stream)); // "b\n" — the peeked 'b' was pushed back
    }

    // ---- harness ----

    private CsvFormatReader csvReader(Map<String, Object> config) {
        return configured(new CsvFormatReader(blockFactory, "csv", List.of(".csv")), config);
    }

    private CsvFormatReader tsvReader(Map<String, Object> config) {
        return configured(new CsvFormatReader(blockFactory, CsvFormatOptions.TSV, "tsv", List.of(".tsv")), config);
    }

    private static CsvFormatReader configured(CsvFormatReader reader, Map<String, Object> config) {
        return (CsvFormatReader) reader.withConfigTrackingConsumedKeys(config).value();
    }

    /** Reads every page and renders each value as a string ({@code null} stays null). */
    private static List<List<String>> readAll(CsvFormatReader reader, String content) throws IOException {
        StorageObject object = new InMemoryStorageObject(content.getBytes(StandardCharsets.UTF_8));
        List<List<String>> rows = new ArrayList<>();
        try (CloseableIterator<Page> pages = reader.read(object, FormatReadContext.of(null, 100))) {
            while (pages.hasNext()) {
                Page page = pages.next();
                try {
                    for (int p = 0; p < page.getPositionCount(); p++) {
                        List<String> row = new ArrayList<>(page.getBlockCount());
                        for (int b = 0; b < page.getBlockCount(); b++) {
                            BytesRefBlock block = page.getBlock(b);
                            if (block.isNull(p)) {
                                row.add(null);
                            } else {
                                row.add(block.getBytesRef(block.getFirstValueIndex(p), new BytesRef()).utf8ToString());
                            }
                        }
                        rows.add(row);
                    }
                } finally {
                    page.releaseBlocks();
                }
            }
        }
        return rows;
    }

    private record InMemoryStorageObject(byte[] bytes) implements StorageObject {
        @Override
        public InputStream newStream() {
            return new ByteArrayInputStream(bytes);
        }

        @Override
        public InputStream newStream(long position, long length) {
            return new ByteArrayInputStream(bytes, Math.toIntExact(position), Math.toIntExact(length));
        }

        @Override
        public long length() {
            return bytes.length;
        }

        @Override
        public Instant lastModified() {
            return Instant.EPOCH;
        }

        @Override
        public boolean exists() {
            return true;
        }

        @Override
        public StoragePath path() {
            return StoragePath.of("mem://csv-dialect-read-tests");
        }
    }
}
