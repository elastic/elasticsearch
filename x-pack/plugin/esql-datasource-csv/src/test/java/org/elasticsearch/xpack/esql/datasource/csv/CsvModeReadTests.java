/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.datasource.csv;

import org.apache.lucene.util.BytesRef;
import org.elasticsearch.common.breaker.NoopCircuitBreaker;
import org.elasticsearch.common.logging.HeaderWarning;
import org.elasticsearch.common.util.BigArrays;
import org.elasticsearch.compute.data.BlockFactory;
import org.elasticsearch.compute.data.BytesRefBlock;
import org.elasticsearch.compute.data.ElementType;
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
 * End-to-end behavior of the {@code mode} presets through {@link CsvFormatReader#read}: {@code plain}
 * (every byte literal), {@code escaped} (database-export backslash semantics), and
 * {@code quoted} (RFC 4180, unchanged) — plus the permissive {@code quote}/{@code escape} overrides
 * that reach the rest of the (quoting, escaping) grid. The plain/escaped cases are the cure for the
 * original field-leading-quote failure: with quoting on, a value starting with {@code "} opens a
 * quoted region tab-delimited data never closes, gluing rows until the record cap.
 */
public class CsvModeReadTests extends ESTestCase {

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
        List<List<String>> values = readAll(tsvReader(Map.of("mode", "plain")), tsv.toString());
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
        List<List<String>> values = readAll(tsvReader(Map.of("mode", "plain")), tsv);
        assertEquals(1, values.size());
        assertEquals("C:\\temp", values.get(0).get(0));
        assertEquals("\\N", values.get(0).get(1));
    }

    /** {@code escaped}: {@code \t}/{@code \n} decode to the control bytes, {@code \\} to one backslash. */
    public void testEscapedDecodesBackslashSequences() throws IOException {
        String tsv = """
            a:keyword\tb:keyword\tc:keyword
            has\\ttab\tline1\\nline2\tC:\\\\temp
            """;
        List<List<String>> values = readAll(tsvReader(Map.of("mode", "escaped")), tsv);
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
        List<List<String>> values = readAll(tsvReader(Map.of("mode", "escaped")), tsv);
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
        List<List<String>> values = readAll(tsvReader(Map.of("mode", "escaped", "null_value", "NULL")), tsv);
        assertEquals(1, values.size());
        assertNull(values.get(0).get(0)); // null_value match — tokenizer route
        assertNull(values.get(0).get(1)); // \N — decode route
        assertEquals("value", values.get(0).get(2));
    }

    /**
     * Documents the escaped-mode residual: {@code escaped} (quoting off, escaping on) keeps
     * {@code jacksonGrammarApplies()} true even under no-trim (the house grammar mirrors only the QUOTED /
     * PLAIN dialects, not the C-style decode), so escaped reads still tokenize with Jackson and inherit its
     * {@code SKIP_EMPTY_LINES} first-column leading-whitespace eating. This is a real no-trim gap for escaped
     * mode — but it is uniform across every escaped arm (per-record + bulk + inference all go through Jackson),
     * so there is no cross-path misbind. Pinned so a future widening of the house grammar to escaped mode (which
     * WOULD start preserving col-0 whitespace) is a deliberate, visible change rather than a silent drift.
     */
    public void testEscapedModeStillEatsColumnZeroLeadingWhitespaceUnderNoTrim() throws IOException {
        String tsv = "a:keyword\tb:keyword\n  x\t  y\n";
        List<List<String>> values = readAll(tsvReader(Map.of("mode", "escaped")), tsv);
        assertEquals(1, values.size());
        // Column 0's leading whitespace is eaten by Jackson's SKIP_EMPTY_LINES; a non-first column keeps its
        // padding, so the residual is column-0-specific (not general trimming).
        assertEquals("x", values.get(0).get(0));
        assertEquals("  y", values.get(0).get(1));
    }

    /** {@code escaped} is still a no-quote mode: a field-leading {@code "} is data, rows never glue. */
    public void testEscapedFieldLeadingQuoteIsData() throws IOException {
        String tsv = """
            a:keyword\tb:keyword
            "starts with quote\tnormal
            second row\talso normal
            """;
        List<List<String>> values = readAll(tsvReader(Map.of("mode", "escaped")), tsv);
        assertEquals(2, values.size());
        assertEquals("\"starts with quote", values.get(0).get(0));
    }

    /** The default CSV path is untouched: RFC-4180 wrapping and doubling still work. */
    public void testQuotedModeUnchanged() throws IOException {
        String csv = "a:keyword,b:keyword\n\"has,comma\",\"he said \"\"hi\"\"\"\n";
        List<List<String>> values = readAll(csvReader(Map.of()), csv);
        assertEquals(1, values.size());
        assertEquals("has,comma", values.get(0).get(0));
        assertEquals("he said \"hi\"", values.get(0).get(1));
    }

    /**
     * Splitter dispatch is decided once, from the (quoting, escaping) pair: only plain (no quoting, no
     * escaping) takes the strided terminator scan. Escaped keeps quoting off but escaping on, and a
     * backslash-escaped raw newline is in-field content, so it needs the escape-aware quote-state splitter.
     */
    public void testSplitterDispatchByQuoting() throws IOException {
        assertThat(tsvReader(Map.of("mode", "plain")).recordSplitter(1024), instanceOf(NewlineRecordSplitter.class));
        assertThat(tsvReader(Map.of("mode", "escaped")).recordSplitter(1024), instanceOf(CsvRecordSplitter.class));
        // The .tsv baseline is plain, so an unconfigured TSV reader takes the terminator scan; quoting is opt-in.
        assertThat(tsvReader(Map.of()).recordSplitter(1024), instanceOf(NewlineRecordSplitter.class));
        assertThat(tsvReader(Map.of("mode", "quoted")).recordSplitter(1024), instanceOf(CsvRecordSplitter.class));
        assertThat(csvReader(Map.of()).recordSplitter(1024), instanceOf(CsvRecordSplitter.class));
    }

    /** Bare brackets selects QUOTED (bracket cells carry quoted elements), even on the plain .tsv baseline. */
    public void testBareBracketsEnablesQuoting() throws IOException {
        assertThat(tsvReader(Map.of("multi_value_syntax", "brackets")).recordSplitter(1024), instanceOf(CsvRecordSplitter.class));
    }

    /**
     * Permissive override: {@code mode: plain, quote: "\""} turns quoting back on, so a quoted field
     * may carry the tab delimiter as data and the reader switches to the quote-state splitter.
     */
    public void testPlainQuoteOverrideEnablesQuoting() throws IOException {
        assertThat(tsvReader(Map.of("mode", "plain", "quote", "\"")).recordSplitter(1024), instanceOf(CsvRecordSplitter.class));
        String tsv = "a:keyword\tb:keyword\n\"x\ty\"\tz\n";
        List<List<String>> values = readAll(tsvReader(Map.of("mode", "plain", "quote", "\"")), tsv);
        assertEquals(1, values.size());
        assertEquals("x\ty", values.get(0).get(0)); // tab inside quotes is data, one field
        assertEquals("z", values.get(0).get(1));
    }

    /**
     * Permissive override: {@code escape: none} under quoted is pure RFC 4180 — quoting stays on but
     * the backslash is left as data, so a Windows path inside quotes survives intact instead of
     * losing the character after the backslash to Jackson's in-quote escape.
     */
    public void testQuotedEscapeNoneKeepsBackslashLiteral() throws IOException {
        String csv = "p:keyword\n\"C:\\Users\"\n";
        List<List<String>> values = readAll(csvReader(Map.of("escape", "none")), csv);
        assertEquals(1, values.size());
        assertEquals("C:\\Users", values.get(0).get(0));
    }

    /**
     * Permissive override: {@code mode: escaped, quote: "\""} turns quoting on, so the reader switches
     * to the quote-state splitter and a quoted field may carry the tab delimiter as data. (The C-style
     * value decode is off once quoting is on — {@link CsvFormatOptions#decodesEscapes()} is false — so
     * this is plain RFC-4180-with-escape parsing, not C-style escaped decoding.)
     */
    public void testEscapedQuoteOverrideEnablesQuoting() throws IOException {
        assertThat(tsvReader(Map.of("mode", "escaped", "quote", "\"")).recordSplitter(1024), instanceOf(CsvRecordSplitter.class));
        String tsv = "a:keyword\tb:keyword\n\"x\ty\"\tz\n";
        List<List<String>> values = readAll(tsvReader(Map.of("mode", "escaped", "quote", "\"")), tsv);
        assertEquals(1, values.size());
        assertEquals("x\ty", values.get(0).get(0)); // tab inside quotes is data — quoting is on
        assertEquals("z", values.get(0).get(1));
        drainWarnings(); // escaped+quote emits the expected decode-disabled config warning; clear it
    }

    /**
     * The original ClickBench failure shape under the DEFAULT {@code .tsv} configuration — no
     * mode option supplied. A field-leading {@code "} is data; rows never glue and the count is exact.
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
     * If that schema is not quoting-aware ({@code withoutQuoteChar()} for plain), a field-leading
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
        List<List<String>> values = readAll(tsvReader(Map.of("mode", "plain")), tsv.toString());
        assertEquals(rows, values.size());
        assertEquals("\"unbalanced quote 0", values.get(0).get(1)); // first quoted row, literal
        assertEquals("\"unbalanced quote 450", values.get(450).get(1)); // a later batch, still literal
    }

    /**
     * Volume guard: the escaped mode decodes on the BULK data path (the merge's only per-value seam
     * there), so {@code \t} un-escapes and a whole-field {@code \N} is null across many batches,
     * exactly as on the per-record path.
     */
    public void testBulkPathEscapedDecodesAtVolume() throws IOException {
        int rows = 500;
        StringBuilder tsv = new StringBuilder("id:keyword\tnote:keyword\tmaybe:keyword\n");
        for (int i = 0; i < rows; i++) {
            tsv.append("id").append(i).append("\thas\\ttab").append('\t').append(i % 2 == 0 ? "\\N" : "present").append('\n');
        }
        List<List<String>> values = readAll(tsvReader(Map.of("mode", "escaped")), tsv.toString());
        assertEquals(rows, values.size());
        assertEquals("has\ttab", values.get(0).get(1)); // \t decoded on the bulk path
        assertEquals("has\ttab", values.get(499).get(1)); // still decoded in a later batch
        assertNull(values.get(0).get(2)); // whole-field \N -> null
        assertEquals("present", values.get(1).get(2));
    }

    /**
     * Tripwire for the SCHEMA-INFERENCE path. Inference (here headerless → inferSchemaWithSyntheticNames)
     * samples rows through the per-record iterator and its mode schema. If that path were not
     * quoting-aware, a field-leading {@code "} in the sample would open a quoted region and the
     * sampling would glue rows — wrong count / mis-aligned columns. Under plain it stays data.
     */
    public void testPlainInferenceFieldLeadingQuoteDoesNotGlue() throws IOException {
        StringBuilder tsv = new StringBuilder();
        for (int i = 0; i < 30; i++) {
            String note = i % 5 == 0 ? "\"quote-start " + i : "note " + i;
            tsv.append("id").append(i).append('\t').append(note).append('\n');
        }
        List<List<String>> values = readAll(tsvReader(Map.of("mode", "plain", "header_row", false)), tsv.toString());
        assertEquals(30, values.size());
        assertEquals("\"quote-start 0", values.get(0).get(1));
    }

    /**
     * Tripwire for ESCAPED decoding ON THE INFERENCE PATH: a single column whose non-null samples
     * are integers and whose nulls are {@code \N} must infer as a NUMERIC type — which is only
     * possible if the sample {@code \N} was decoded to null before type inference. If the sample
     * path skipped decoding, the literal {@code \N} string forces the column to keyword (BYTES_REF),
     * and this assertion fails immediately.
     */
    public void testEscapedInferenceDecodesNullBeforeTypeInference() throws IOException {
        StringBuilder tsv = new StringBuilder();
        for (int i = 0; i < 30; i++) {
            tsv.append(i % 3 == 0 ? "\\N" : Integer.toString(i)).append('\n');
        }
        List<ElementType> types = readColumnTypes(tsvReader(Map.of("mode", "escaped", "header_row", false)), tsv.toString());
        assertEquals(1, types.size());
        assertNotEquals(
            "escaped \\N must decode to null in the sample so the column infers numeric, not keyword",
            ElementType.BYTES_REF,
            types.get(0)
        );
    }

    /**
     * Same tripwire through the HEADERED inference path (inferSchemaFromSample): a header with
     * no {@code :type} annotations forces type inference from the sample.
     */
    public void testEscapedHeaderedInferenceDecodesNull() throws IOException {
        StringBuilder tsv = new StringBuilder("id\tn\n");
        for (int i = 0; i < 30; i++) {
            tsv.append("id").append(i).append('\t').append(i % 3 == 0 ? "\\N" : Integer.toString(i)).append('\n');
        }
        List<ElementType> types = readColumnTypes(tsvReader(Map.of("mode", "escaped")), tsv.toString());
        assertEquals(2, types.size());
        assertNotEquals("the n column must infer numeric, proving \\N decoded in the sample", ElementType.BYTES_REF, types.get(1));
    }

    /**
     * Tripwire for the hint: a non-decoding mode ({@code plain} here) whose sample carries the
     * whole-field {@code \N} null marker emits a one-time response {@code Warning} header nudging
     * toward {@code mode: escaped}. The query author reads the response, so the channel is a Warning
     * header, not a DEBUG log they would never see. If the hint scan is dropped, no header is emitted
     * and this fails.
     */
    public void testPlainNullMarkerEmitsWarning() throws IOException {
        readAll(tsvReader(Map.of("mode", "plain", "header_row", false)), "id0\t\\N\nid1\tplain note\n");
        assertNullMarkerWarning(drainWarnings());
    }

    /**
     * Sharp-edge mitigation, config-time arm: {@code mode: escaped, quote: …} resolves to quoted,
     * which hands the escape char to Jackson and drops the C-style decode. The data scan can't catch
     * this (Jackson rewrites {@code \N} to {@code N} before the sample exists), so the resolver emits a
     * deterministic config-time response warning. Building the reader is enough to trigger it.
     */
    public void testEscapedPlusQuoteWarnsDecodeDisabled() {
        tsvReader(Map.of("mode", "escaped", "quote", "\""));
        List<String> warnings = drainWarnings();
        assertTrue(
            "expected a config-time decode-disabled warning, got: " + warnings,
            warnings.stream().anyMatch(w -> w.contains("disables the escaped-mode decode"))
        );
    }

    /**
     * The warning stays quiet when there is no whole-field {@code \N} and no contradictory override:
     * clean data (no false nudge), a literal Windows path {@code C:\temp} under plain (the path is NOT
     * a null marker — the key precision win over a broad backslash scan), and the {@code escaped} mode
     * with no quote (already decoding). No response warning of any kind should accumulate.
     */
    public void testNoWarningForCleanWindowsPathOrEscapedMode() throws IOException {
        readAll(tsvReader(Map.of("mode", "plain", "header_row", false)), "id0\tclean\nid1\talso clean\n");
        readAll(tsvReader(Map.of("mode", "plain", "header_row", false)), "id0\tC:\\temp\nid1\tC:\\Users\n");
        readAll(tsvReader(Map.of("mode", "escaped", "header_row", false)), "id0\t\\N\nid1\tvalue\n");
        assertTrue("no response warning expected", drainWarnings().isEmpty());
    }

    private static void assertNullMarkerWarning(List<String> warnings) {
        // Match on an escape-free slice of the message: HeaderWarning escapes backslashes and quotes in
        // the header value, so a literal "\N" / "\"mode\"" substring would not match the drained value.
        // Also assert the directed action and a location field are present.
        assertTrue(
            "expected an undecoded null-marker response warning, got: " + warnings,
            warnings.stream()
                .anyMatch(
                    w -> w.contains("null marker, but the current mode keeps it as literal text")
                        && w.contains("data row [")
                        && w.contains("Set ")
                )
        );
    }

    /** Drain and clear the response {@code Warning} headers accumulated on the test thread context. */
    private List<String> drainWarnings() {
        List<String> raw = threadContext.getResponseHeaders().getOrDefault("Warning", List.of());
        List<String> messages = raw.stream().map(s -> HeaderWarning.extractWarningValueFromWarningHeader(s, false)).toList();
        threadContext.stashContext();
        return messages;
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

    /** The inferred per-column {@link ElementType} from the first page (used to assert type inference). */
    private static List<ElementType> readColumnTypes(CsvFormatReader reader, String content) throws IOException {
        StorageObject object = new InMemoryStorageObject(content.getBytes(StandardCharsets.UTF_8));
        try (CloseableIterator<Page> pages = reader.read(object, FormatReadContext.of(null, 100))) {
            if (pages.hasNext() == false) {
                return List.of();
            }
            Page page = pages.next();
            try {
                List<ElementType> types = new ArrayList<>(page.getBlockCount());
                for (int b = 0; b < page.getBlockCount(); b++) {
                    types.add(page.getBlock(b).elementType());
                }
                return types;
            } finally {
                page.releaseBlocks();
            }
        }
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
            return StoragePath.of("mem://csv-mode-read-tests");
        }
    }
}
