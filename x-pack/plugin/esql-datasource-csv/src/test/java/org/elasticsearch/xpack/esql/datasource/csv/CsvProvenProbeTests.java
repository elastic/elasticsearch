/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.datasource.csv;

import org.elasticsearch.tasks.TaskCancelledException;
import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.xpack.esql.datasources.spi.RecordSplitter;
import org.elasticsearch.xpack.esql.datasources.spi.SegmentableFormatReader;

import java.io.BufferedInputStream;
import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.TreeSet;

/**
 * Differential-oracle tests for the proven-probe ({@link RecordSplitter#findProvenRecordBoundary}) and exact-walk
 * ({@link RecordSplitter#findRecordStartAtOrAfter}) macro-split primitives of {@link CsvRecordSplitter}.
 *
 * <p>The oracle is the trusted sequential scanner {@link RecordSplitter#findNextRecordBoundary} looped from the file
 * start: its prefix sums are the true record starts. The invariants asserted are:
 * <ul>
 *   <li>the probe, when it does not return {@code AMBIGUOUS}, always resolves to a <em>true</em> record start at or
 *       after the probe offset (never a byte inside a quoted field or escaped newline);</li>
 *   <li>the exact walk returns exactly the first true record start {@code >= minSkip};</li>
 *   <li>a boundary the probe proves is never earlier than the exact walk's first true start.</li>
 * </ul>
 */
public class CsvProvenProbeTests extends ESTestCase {

    public void testProvenCapabilityMatchesStridedComplement() {
        // Quoted and escaped modes are non-strided but proven-capable; bracket MVC is neither.
        assertTrue(splitter(quoted()).supportsProvenProbing());
        assertTrue(splitter(escaped()).supportsProvenProbing());
        assertFalse(splitter(brackets()).supportsProvenProbing());
    }

    public void testExactWalkMatchesOracleQuoted() throws IOException {
        // Quoted fields carry embedded raw newlines and doubled "" quotes; every offset must resolve correctly.
        String data = "id,name,note\n"
            + "1,\"embedded\nnewline\",ok\n"
            + "2,\"has \"\"escaped\"\" quotes\",ok\n"
            + "3,simple,value\n"
            + "4,\"multi\nline\nfield\",done\n";
        assertExactWalkMatchesOracle(quoted(), bytes(data));
    }

    public void testExactWalkMatchesOracleEscaped() throws IOException {
        // Backslash-escaped raw newlines are in-field content; a lone escape must not desync the walk.
        String data = "a\\\nb\nc,d\ne\\\\\nf\\\ng\\\nh\ni,j\n";
        assertExactWalkMatchesOracle(escaped(), bytes(data));
    }

    public void testExactWalkMatchesOracleCrLfAndLoneCr() throws IOException {
        String data = "a,b\r\nc,d\r\"e\rf\",g\r\nlast,row\r\n";
        assertExactWalkMatchesOracle(quoted(), bytes(data));
        // The lone-CR terminator (stepProbe's pendingCr resolution) is exercised by the probe too, not only the walk.
        assertProbeInvariants(quoted(), bytes(data));
    }

    public void testProbeNeverLandsInsideQuotedField() throws IOException {
        String data = "id,name,note\n"
            + "1,\"embedded\nnewline\",ok\n"
            + "2,\"has \"\"escaped\"\" quotes\",ok\n"
            + "3,simple,value\n"
            + "4,\"multi\nline\nfield\",done\n"
            + "5,tail,end\n";
        assertProbeInvariants(quoted(), bytes(data));
    }

    public void testProbeNeverLandsInsideEscapedNewline() throws IOException {
        String data = "a\\\nb\nc,d\ne\\\\\nf\\\ng\\\nh\ni,j\nk,l\n";
        assertProbeInvariants(escaped(), bytes(data));
    }

    public void testExactWalkMatchesOracleQuotedAndEscaped() throws IOException {
        // Default .csv enables quoting AND escaping: a quote opens a field, and a backslash escapes the next byte
        // (including a quote or a raw newline) inside or outside quotes. The lockstep probe and walk must handle both.
        String data = "id,name\n"
            + "1,\"quoted\nvalue\"\n"
            + "2,\"has \\\" escaped quote and \"\"doubled\"\"\"\n"
            + "3,unquoted\\,not-a-delim\n"
            + "4,\"trailing\\\nembedded\"\n";
        assertExactWalkMatchesOracle(both(), bytes(data));
        assertProbeInvariants(both(), bytes(data));
    }

    public void testProbeWithMultiByteUtf8() throws IOException {
        // Stride offsets loop over every byte, including UTF-8 continuation bytes of the multi-byte code points;
        // isCleanSymbol treats a continuation byte as clean content, so a mid-code-point start still resolves safely.
        String data = "id,city\n" + "1,\"München\nStraße\"\n" + "2,naïve café\n" + "3,\"日本\n語\",tail\n";
        assertProbeInvariants(quoted(), bytes(data));
        assertExactWalkMatchesOracle(quoted(), bytes(data));
    }

    public void testExactWalkAcrossBufferBoundary() throws IOException {
        // A single quoted record with embedded newlines longer than the internal 8 KiB BufferedInputStream buffer:
        // the walk must read one stream in a single pass without dropping read-ahead across the buffer refill.
        StringBuilder sb = new StringBuilder("id,blob\n1,\"");
        for (int i = 0; i < 4000; i++) {
            sb.append("ab\n");
        }
        sb.append("\"\n2,short\n3,\"c\nd\"\n");
        assertExactWalkMatchesOracle(quoted(), bytes(sb.toString()));
        assertProbeInvariants(quoted(), bytes(sb.toString()));
    }

    public void testOverMaxRecordBytesProbeAmbiguousWalkTooLarge() throws IOException {
        // A quoted record whose embedded-newline body exceeds maxRecordBytes: the probe must never certify a boundary
        // inside the oversized region (CONVERGENCE_WINDOW <= maxRecordBytes), and the exact walk must report the cap.
        int maxRecordBytes = 256;
        StringBuilder sb = new StringBuilder("h\n\"");
        for (int i = 0; i < 200; i++) {
            sb.append("x\n");
        }
        sb.append("\"\ntail\n");
        byte[] buf = bytes(sb.toString());
        RecordSplitter splitter = new CsvRecordSplitter(quoted(), maxRecordBytes);
        // The oversized record starts at offset 2 (after "h\n"); an offset inside its body cannot prove a boundary.
        for (long t = 3; t < 100; t++) {
            long probed = splitter.findProvenRecordBoundary(
                new BufferedInputStream(new ByteArrayInputStream(buf, (int) t, buf.length - (int) t))
            );
            assertEquals("probe must not certify a boundary inside the oversized record at t=" + t, RecordSplitter.AMBIGUOUS, probed);
        }
        // The exact walk from the file start, asked to skip past the oversized record, reports RECORD_TOO_LARGE.
        long walk = splitter.findRecordStartAtOrAfter(new ByteArrayInputStream(buf), 3L, () -> false);
        assertEquals(RecordSplitter.RECORD_TOO_LARGE, walk);
    }

    public void testExactWalkAbortsOnCancellation() {
        // A long quote-free stretch (no boundary before minSkip) so the walk runs past the cancel-check interval;
        // an always-cancelled supplier must abort promptly with TaskCancelledException rather than scan to EOF.
        byte[] buf = new byte[128 * 1024];
        Arrays.fill(buf, (byte) 'x');
        RecordSplitter splitter = splitter(quoted());
        expectThrows(
            TaskCancelledException.class,
            () -> splitter.findRecordStartAtOrAfter(new ByteArrayInputStream(buf), buf.length - 1L, () -> true)
        );
    }

    public void testSplitReconstructionMatchesWholeFile() throws IOException {
        for (CsvFormatOptions options : List.of(quoted(), escaped(), both())) {
            byte[] buf = bytes(randomCsv(options));
            assertSplitReconstruction(options, buf);
        }
    }

    public void testRandomizedDifferential() throws IOException {
        for (int iter = 0; iter < 20; iter++) {
            CsvFormatOptions options = randomFrom(quoted(), escaped(), both());
            byte[] buf = bytes(randomCsv(options));
            assertExactWalkMatchesOracle(options, buf);
            assertProbeInvariants(options, buf);
            assertSplitReconstruction(options, buf);
        }
    }

    /**
     * Caps how many byte offsets the per-offset oracle sweeps ({@link #assertExactWalkMatchesOracle},
     * {@link #assertProbeInvariants}) visit. The exact walk is O(minSkip) per call, so an every-offset sweep is
     * O(n^2); on the multi-KB randomized files that is minutes. Small hand-crafted buffers (shorter than the cap)
     * keep {@code step == 1}, i.e. the exhaustive sweep, so their coverage is unchanged; large randomized files
     * are sampled at {@value} evenly spaced offsets. The whole-file, every-record correctness of large files is
     * covered exhaustively (and cheaply, O(n)) by {@link #assertSplitReconstruction}.
     */
    private static final int MAX_SAMPLED_OFFSETS = 512;

    /** Even stride over {@code buf} so the per-offset oracles visit at most {@link #MAX_SAMPLED_OFFSETS} offsets. */
    private static long offsetStep(byte[] buf) {
        return Math.max(1, buf.length / MAX_SAMPLED_OFFSETS);
    }

    private void assertExactWalkMatchesOracle(CsvFormatOptions options, byte[] buf) throws IOException {
        RecordSplitter splitter = splitter(options);
        TreeSet<Long> trueStarts = trueRecordStarts(splitter, buf);
        long step = offsetStep(buf);
        for (long minSkip = 1; minSkip < buf.length; minSkip += step) {
            long expected = firstAtOrAfter(trueStarts, minSkip);
            long actual = splitter.findRecordStartAtOrAfter(new ByteArrayInputStream(buf), minSkip, () -> false);
            assertEquals("exact walk mismatch at minSkip=" + minSkip, expected, actual);
        }
    }

    private void assertProbeInvariants(CsvFormatOptions options, byte[] buf) throws IOException {
        RecordSplitter splitter = splitter(options);
        TreeSet<Long> trueStarts = trueRecordStarts(splitter, buf);
        long step = offsetStep(buf);
        for (long t = 1; t < buf.length; t += step) {
            long consumed = splitter.findProvenRecordBoundary(
                new BufferedInputStream(new ByteArrayInputStream(buf, (int) t, buf.length - (int) t))
            );
            if (consumed == RecordSplitter.AMBIGUOUS) {
                continue;
            }
            assertTrue("probe must consume at least one byte at t=" + t, consumed > 0);
            long boundary = t + consumed;
            assertTrue("probe boundary " + boundary + " must be a true record start (t=" + t + ")", trueStarts.contains(boundary));
            long firstTrue = firstAtOrAfter(trueStarts, t);
            assertTrue("probe boundary " + boundary + " must not precede the first true start " + firstTrue, boundary >= firstTrue);
        }
    }

    /**
     * True record starts: the file start (0) plus every prefix sum of {@link RecordSplitter#findNextRecordBoundary}
     * consumed lengths. When the file ends with a terminator the final prefix sum equals {@code buf.length}: that
     * post-terminator EOF position is included because both the exact walk and the probe legitimately resolve to it
     * (production discards a boundary {@code >= fileLength}, so it is harmless there). It is added only when the
     * trusted scanner actually reaches it via terminators, so a genuinely unterminated tail is still caught.
     */
    private static TreeSet<Long> trueRecordStarts(RecordSplitter splitter, byte[] buf) throws IOException {
        TreeSet<Long> starts = new TreeSet<>();
        starts.add(0L);
        long acc = 0;
        BufferedInputStream in = new BufferedInputStream(new ByteArrayInputStream(buf));
        long consumed;
        while ((consumed = splitter.findNextRecordBoundary(in)) >= 0) {
            acc += consumed;
            starts.add(acc);
        }
        return starts;
    }

    /** First true record start {@code >= minSkip}, or {@code -1} (the walk's EOF sentinel) when none exists. */
    private static long firstAtOrAfter(TreeSet<Long> trueStarts, long minSkip) {
        Long ceil = trueStarts.ceiling(minSkip);
        return ceil == null ? -1L : ceil;
    }

    private String randomCsv(CsvFormatOptions options) {
        StringBuilder sb = new StringBuilder("h1,h2,h3\n");
        int rows = randomIntBetween(10, 10000);
        for (int r = 0; r < rows; r++) {
            int cols = randomIntBetween(1, 4);
            for (int c = 0; c < cols; c++) {
                if (c > 0) {
                    sb.append(',');
                }
                sb.append(randomField(options));
            }
            // Mix all three terminators so the probe's lone-CR branch (\r not followed by \n) is covered, not only
            // the walk's.
            sb.append(randomFrom("\n", "\r\n", "\r"));
        }
        return sb.toString();
    }

    /** A random field drawn only from the constructs the enabled options actually recognize. */
    private String randomField(CsvFormatOptions options) {
        List<String> shapes = new ArrayList<>();
        shapes.add("plain" + randomInt(9));
        shapes.add("v" + randomInt(99));
        if (options.quoting()) {
            shapes.add("\"embedded\nnewline\"");
            shapes.add("\"doubled \"\"quote\"\"\"");
            shapes.add("\"comma,inside\"");
            // Field-leading whitespace before an opening quote: isCleanSymbol treats leading whitespace as non-clean,
            // so the probe must stay AMBIGUOUS here rather than mistaking the quote for a clean-symbol start.
            shapes.add(" \"leading ws\"");
        }
        if (options.escaping()) {
            shapes.add("a\\\nb"); // escaped newline (in-field)
            shapes.add("c\\\\d"); // escaped backslash
            shapes.add("e\\,f"); // escaped delimiter
        }
        if (options.quoting() && options.escaping()) {
            shapes.add("\"esc \\\" quote\""); // backslash-escaped quote inside a quoted field
        }
        return randomFrom(shapes);
    }

    /**
     * Builds a macro-split boundary list the way discovery does (probe at each stride, falling back to the exact walk on
     * {@code AMBIGUOUS}), then asserts the boundaries strictly increase, each is a true record start, and the splits
     * they induce ({@code [b_i, b_{i+1})}) partition the file's records exactly: scanning each split from its start with
     * {@link RecordSplitter#findNextRecordBoundary} and collecting the absolute record-start offsets must reproduce the
     * whole-file set of true record starts. Comparing the offset sets (not just their counts) catches a boundary that
     * shifts, drops, duplicates, or cuts a record even when the totals happen to coincide.
     */
    private void assertSplitReconstruction(CsvFormatOptions options, byte[] buf) throws IOException {
        RecordSplitter splitter = splitter(options);
        TreeSet<Long> trueStarts = trueRecordStarts(splitter, buf);
        long stride = Math.max(1, buf.length / 5);
        List<Long> boundaries = new ArrayList<>();
        boundaries.add(0L);
        long exactCursor = 0;
        long pos = stride;
        while (pos < buf.length) {
            long boundary;
            long probed = splitter.findProvenRecordBoundary(
                new BufferedInputStream(new ByteArrayInputStream(buf, (int) pos, buf.length - (int) pos))
            );
            if (probed >= 0) {
                boundary = pos + probed;
            } else {
                long start = splitter.findRecordStartAtOrAfter(
                    new ByteArrayInputStream(buf, (int) exactCursor, buf.length - (int) exactCursor),
                    pos - exactCursor,
                    () -> false
                );
                if (start < 0) {
                    break;
                }
                boundary = exactCursor + start;
            }
            if (boundary >= buf.length) {
                break;
            }
            assertTrue("macro-split boundary must be a true record start", trueStarts.contains(boundary));
            assertTrue("macro-split boundary must strictly increase", boundary > boundaries.get(boundaries.size() - 1));
            boundaries.add(boundary);
            exactCursor = boundary;
            pos = boundary + stride;
        }
        // trueStarts includes the post-final-terminator offset (== record-count starts, one per record plus the file
        // start); the reconstruction visits each record's start exactly once, so it should match trueStarts minus that
        // trailing sentinel.
        TreeSet<Long> expected = new TreeSet<>(trueStarts);
        expected.remove(trueStarts.last());
        TreeSet<Long> reconstructed = new TreeSet<>();
        for (int i = 0; i < boundaries.size(); i++) {
            long from = boundaries.get(i);
            long to = i + 1 < boundaries.size() ? boundaries.get(i + 1) : buf.length;
            collectRecordStarts(splitter, buf, (int) from, (int) to, reconstructed);
        }
        assertEquals("reconstructed record starts must equal the whole-file record starts", expected, reconstructed);
    }

    /**
     * Collects the absolute start offset of every record that terminates within {@code buf[from, to)} when scanned from
     * a known record start at {@code from}, adding each to {@code out}.
     */
    private static void collectRecordStarts(RecordSplitter splitter, byte[] buf, int from, int to, TreeSet<Long> out) throws IOException {
        int cursor = from;
        while (cursor < to) {
            out.add((long) cursor);
            long consumed = splitter.findNextRecordBoundary(new ByteArrayInputStream(buf, cursor, to - cursor));
            if (consumed < 0) {
                break;
            }
            cursor += Math.toIntExact(consumed);
        }
    }

    private static RecordSplitter splitter(CsvFormatOptions options) {
        return new CsvRecordSplitter(options, SegmentableFormatReader.DEFAULT_MAX_RECORD_BYTES);
    }

    private static byte[] bytes(String value) {
        return value.getBytes(StandardCharsets.UTF_8);
    }

    /** Comma-delimited, quoting on, escaping off. */
    private static CsvFormatOptions quoted() {
        return new CsvFormatOptions(
            ',',
            '"',
            '\\',
            "//",
            "",
            StandardCharsets.UTF_8,
            null,
            CsvFormatOptions.DEFAULT_MAX_FIELD_SIZE,
            CsvFormatOptions.MultiValueSyntax.NONE,
            true,
            CsvFormatOptions.DEFAULT_COLUMN_PREFIX,
            true,
            false,
            false
        );
    }

    /** Comma-delimited, quoting off, escaping on (mode=escaped). */
    private static CsvFormatOptions escaped() {
        return new CsvFormatOptions(
            ',',
            '"',
            '\\',
            "//",
            "",
            StandardCharsets.UTF_8,
            null,
            CsvFormatOptions.DEFAULT_MAX_FIELD_SIZE,
            CsvFormatOptions.MultiValueSyntax.NONE,
            true,
            CsvFormatOptions.DEFAULT_COLUMN_PREFIX,
            false,
            true,
            false
        );
    }

    /** Comma-delimited, quoting on AND escaping on (the default .csv grammar). */
    private static CsvFormatOptions both() {
        return new CsvFormatOptions(
            ',',
            '"',
            '\\',
            "//",
            "",
            StandardCharsets.UTF_8,
            null,
            CsvFormatOptions.DEFAULT_MAX_FIELD_SIZE,
            CsvFormatOptions.MultiValueSyntax.NONE,
            true,
            CsvFormatOptions.DEFAULT_COLUMN_PREFIX,
            true,
            true,
            false
        );
    }

    /** Bracket MVC: non-strided and not proven-capable. */
    private static CsvFormatOptions brackets() {
        return new CsvFormatOptions(
            ',',
            '"',
            '\\',
            "//",
            "",
            StandardCharsets.UTF_8,
            null,
            CsvFormatOptions.DEFAULT_MAX_FIELD_SIZE,
            CsvFormatOptions.MultiValueSyntax.BRACKETS,
            true,
            CsvFormatOptions.DEFAULT_COLUMN_PREFIX
        );
    }
}
