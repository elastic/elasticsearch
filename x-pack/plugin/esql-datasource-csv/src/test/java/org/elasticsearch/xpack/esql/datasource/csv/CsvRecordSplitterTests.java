/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.datasource.csv;

import org.elasticsearch.common.breaker.NoopCircuitBreaker;
import org.elasticsearch.common.util.BigArrays;
import org.elasticsearch.compute.data.BlockFactory;
import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.xpack.esql.datasources.spi.RecordSplitter;
import org.elasticsearch.xpack.esql.datasources.spi.SegmentableFormatReader;

import java.io.BufferedInputStream;
import java.io.BufferedReader;
import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.io.StringReader;
import java.nio.charset.StandardCharsets;
import java.util.Map;

public class CsvRecordSplitterTests extends ESTestCase {

    private BlockFactory blockFactory;

    @Override
    public void setUp() throws Exception {
        super.setUp();
        blockFactory = BlockFactory.builder(BigArrays.NON_RECYCLING_INSTANCE).breaker(new NoopCircuitBreaker("none")).build();
    }

    public void testTsvFindLastRecordBoundaryWithLiteralMidFieldQuotes() throws IOException {
        RecordSplitter splitter = splitter(quotedTsv());
        String data = "1\ta\"b\tc\n2\td\"e\"f\"g\th\n";
        byte[] buf = bytes(data);

        int boundary = splitter.findLastRecordBoundary(buf, buf.length);
        assertEquals("must find the terminating newline of the last complete record", buf.length - 1, boundary);
        assertEquals('\n', (char) buf[boundary]);
    }

    public void testTsvFindLastRecordBoundaryClickBenchShaped() throws IOException {
        RecordSplitter splitter = splitter(quotedTsv());
        String row = clickBenchShapedTsvRow();
        byte[] buf = bytes(row + row);

        int boundary = splitter.findLastRecordBoundary(buf, buf.length);
        assertEquals(buf.length - 1, boundary);
        assertEquals('\n', (char) buf[boundary]);
    }

    public void testTsvFindNextRecordBoundaryWithLiteralMidFieldQuotes() throws IOException {
        RecordSplitter splitter = splitter(quotedTsv());
        String row1 = "1\ta\"b\tc\n";
        byte[] buf = bytes(row1 + "2\td\te\n");

        long consumed = splitter.findNextRecordBoundary(new BufferedInputStream(new ByteArrayInputStream(buf)));
        assertEquals(row1.length(), consumed);
    }

    public void testTsvDoubledQuoteInQuotedFieldIsLiteral() throws IOException {
        RecordSplitter splitter = splitter(quotedTsv());
        String row1 = "\"a\"\"b\"\tc\n";
        byte[] buf = bytes(row1 + "d\te\n");

        assertEquals(row1.length(), splitter.findNextRecordBoundary(new BufferedInputStream(new ByteArrayInputStream(buf))));
        assertEquals(buf.length - 1, splitter.findLastRecordBoundary(buf, buf.length));
    }

    public void testBracketMvcNewlineDoesNotTerminateRecord() throws IOException {
        // CsvFormatOptions.DEFAULT now defaults to MultiValueSyntax.NONE, so a literal `[..\n..]` does
        // terminate at the first newline; construct an explicit BRACKETS options to test the bracket-aware path.
        RecordSplitter splitter = splitter(bracketsDefault());
        String row1 = "before,[line1\nline2\nline3],after\n";
        byte[] buf = bytes(row1 + "next\n");

        assertEquals(row1.length(), splitter.findNextRecordBoundary(new ByteArrayInputStream(buf)));
        assertEquals(buf.length - 1, splitter.findLastRecordBoundary(buf, buf.length));
    }

    public void testBracketMvcFindLastHonorsOffset() throws IOException {
        RecordSplitter splitter = splitter(bracketsDefault());
        byte[] prefix = bytes("xx");
        byte[] payload = bytes("a,[v1\nv2],b\nrow2,plain,c\n");
        byte[] buf = new byte[prefix.length + payload.length];
        System.arraycopy(prefix, 0, buf, 0, prefix.length);
        System.arraycopy(payload, 0, buf, prefix.length, payload.length);

        assertEquals(buf.length - 1, splitter.findLastRecordBoundary(buf, prefix.length, payload.length));
    }

    private static CsvFormatOptions bracketsDefault() {
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

    /** Tab-delimited, quoting on, escaping off - the quote-aware TSV shape these boundary tests target. */
    private static CsvFormatOptions quotedTsv() {
        return new CsvFormatOptions(
            '\t',
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

    /** No quoting, escaping on - the {@code mode=escaped} shape where a backslash carries the next byte verbatim. */
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

    public void testEscapedModeDisablesStridedProbing() {
        // mode=escaped has quoting off but escaping on: a backslash-escaped raw newline is in-field content,
        // so a mid-file probe could land right after a lone escape and misread the newline as a terminator.
        // The reader must therefore hand out the non-strided splitter, not the plain newline splitter.
        assertFalse(productionSplitter(escaped()).supportsStridedProbing());
    }

    public void testEscapedNewlineDoesNotTerminateRecord() throws IOException {
        RecordSplitter splitter = splitter(escaped());
        // x \ <NL> y <NL> z <NL> : the backslash escapes the first newline, so the first record is
        // "x\<NL>y" and the file holds two records, not three physical lines.
        byte[] buf = bytes("x\\\ny\nz\n");

        assertEquals("x\\\ny\n".length(), splitter.findNextRecordBoundary(new ByteArrayInputStream(buf)));
        assertEquals(buf.length - 1, splitter.findLastRecordBoundary(buf, buf.length));
    }

    public void testEscapedModeBoundaryCountAgreesWithReadRecord() throws IOException {
        CsvFormatOptions options = escaped();
        RecordSplitter splitter = splitter(options);
        // Rows whose fields carry backslash-escaped newlines, plain newlines, and escaped backslashes.
        String data = "a\\\nb\nc,d\ne\\\\\nf\\\ng\\\nh\n";
        byte[] buf = bytes(data);

        int parserRecords = 0;
        try (BufferedReader br = new BufferedReader(new StringReader(data))) {
            CsvLogicalRecordReader recordReader = new CsvLogicalRecordReader(
                br,
                options.quoteChar(),
                options.delimiter(),
                options.escapeChar(),
                SegmentableFormatReader.DEFAULT_MAX_RECORD_BYTES,
                options.encoding(),
                options.quoting(),
                options.escaping()
            );
            while (recordReader.readRecord(false) != null) {
                parserRecords++;
            }
        }

        int scannerRecords = 0;
        BufferedInputStream in = new BufferedInputStream(new ByteArrayInputStream(buf));
        while (splitter.findNextRecordBoundary(in) >= 0) {
            scannerRecords++;
        }

        assertEquals("record count mismatch for escaped data:\n" + data, parserRecords, scannerRecords);
        assertEquals(buf.length - 1, splitter.findLastRecordBoundary(buf, buf.length));
    }

    public void testMultiValueSyntaxNoneDoesNotTreatBracketsAsMvc() throws IOException {
        CsvFormatOptions options = new CsvFormatOptions(
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
            CsvFormatOptions.DEFAULT_COLUMN_PREFIX
        );
        RecordSplitter splitter = splitter(options);
        byte[] buf = bytes("before,[not\nmvc],after\nnext\n");

        assertEquals("before,[not\n".length(), splitter.findNextRecordBoundary(new ByteArrayInputStream(buf)));
    }

    public void testRecordBoundaryCountAgreesWithReadCsvRecord() throws IOException {
        for (boolean tsv : new boolean[] { true, false }) {
            CsvFormatOptions options = tsv ? CsvFormatOptions.TSV : CsvFormatOptions.DEFAULT;
            RecordSplitter splitter = splitter(options);
            char delim = options.delimiter();
            boolean bracketAware = options.multiValueSyntax() == CsvFormatOptions.MultiValueSyntax.BRACKETS && delim == ',';

            int rows = randomIntBetween(5, 50);
            StringBuilder sb = new StringBuilder();
            for (int r = 0; r < rows; r++) {
                int cols = randomIntBetween(1, 8);
                for (int c = 0; c < cols; c++) {
                    if (c > 0) {
                        sb.append(delim);
                    }
                    sb.append(randomFieldWithLiteralQuotes(delim));
                }
                sb.append('\n');
            }
            String data = sb.toString();
            byte[] buf = bytes(data);

            int parserRecords = 0;
            try (BufferedReader br = new BufferedReader(new StringReader(data))) {
                // The reader's quote/escape awareness must match the options the splitter reads, so the two
                // agree on where records end (TSV is now plain, DEFAULT is quoted).
                CsvLogicalRecordReader recordReader = new CsvLogicalRecordReader(
                    br,
                    options.quoteChar(),
                    delim,
                    options.escapeChar(),
                    SegmentableFormatReader.DEFAULT_MAX_RECORD_BYTES,
                    options.encoding(),
                    options.quoting(),
                    options.escaping()
                );
                while (recordReader.readRecord(bracketAware) != null) {
                    parserRecords++;
                }
            }

            int scannerRecords = 0;
            BufferedInputStream in = new BufferedInputStream(new ByteArrayInputStream(buf));
            while (splitter.findNextRecordBoundary(in) >= 0) {
                scannerRecords++;
            }

            assertEquals("record count mismatch (tsv=" + tsv + ") for data:\n" + data, parserRecords, scannerRecords);
            assertEquals(buf.length - 1, splitter.findLastRecordBoundary(buf, buf.length));
        }
    }

    public void testLoneCrBoundaryCountAgreesWithReadCsvRecord() throws IOException {
        CsvFormatOptions options = CsvFormatOptions.DEFAULT;
        RecordSplitter splitter = splitter(options);
        String data = "a,b\rc,d\r\"e\rf\",g\r";
        byte[] buf = bytes(data);

        int parserRecords = 0;
        try (BufferedReader br = new BufferedReader(new StringReader(data))) {
            CsvLogicalRecordReader recordReader = new CsvLogicalRecordReader(
                br,
                options.quoteChar(),
                options.delimiter(),
                SegmentableFormatReader.DEFAULT_MAX_RECORD_BYTES,
                options.encoding()
            );
            while (recordReader.readRecord(false) != null) {
                parserRecords++;
            }
        }

        int scannerRecords = 0;
        BufferedInputStream in = new BufferedInputStream(new ByteArrayInputStream(buf));
        while (splitter.findNextRecordBoundary(in) >= 0) {
            scannerRecords++;
        }

        assertEquals(parserRecords, scannerRecords);
        assertEquals(buf.length - 1, splitter.findLastRecordBoundary(buf, buf.length));
    }

    public void testQuotedReaderDisablesStridedProbing() {
        // Route through the production factory: with quoting on the reader hands out the quote-aware
        // splitter, whose state machine can carry a record across a raw newline. A mid-file probe is then
        // unsafe, so it reports it cannot be probed at arbitrary offsets and must be driven serially.
        assertFalse(productionSplitter(CsvFormatOptions.DEFAULT).supportsStridedProbing());
        assertFalse(productionSplitter(bracketsDefault()).supportsStridedProbing());
    }

    public void testPlainReaderKeepsStridedProbing() {
        // Route through the production factory: with quoting off (default .tsv is plain, and .csv with
        // mode=plain) every newline is an unambiguous boundary, so the reader hands out the plain newline
        // splitter that keeps the strided default enabling cross-node byte-range splitting.
        assertTrue(productionSplitter(CsvFormatOptions.TSV).supportsStridedProbing());
        assertTrue(plainModeCsvSplitter().supportsStridedProbing());
    }

    /**
     * Mirrors production: the reader, not the caller, picks the splitter from the quoting knob
     * (see {@link CsvFormatReader#recordSplitter(int)}). Tests of strided probing must go through here
     * rather than constructing a {@link CsvRecordSplitter} directly, since a plain file never gets one.
     */
    private RecordSplitter productionSplitter(CsvFormatOptions options) {
        return new CsvFormatReader(blockFactory).withOptions(options).recordSplitter();
    }

    /**
     * Plain-mode CSV via the real config path ({@code WITH {"mode":"plain"}}) rather than hand-building
     * every option field, so the test tracks whatever plain mode actually installs in production.
     */
    private RecordSplitter plainModeCsvSplitter() {
        SegmentableFormatReader reader = (SegmentableFormatReader) new CsvFormatReader(blockFactory).withConfig(Map.of("mode", "plain"));
        return reader.recordSplitter();
    }

    private static RecordSplitter splitter(CsvFormatOptions options) {
        return new CsvRecordSplitter(options, SegmentableFormatReader.DEFAULT_MAX_RECORD_BYTES);
    }

    private static byte[] bytes(String value) {
        return value.getBytes(StandardCharsets.UTF_8);
    }

    private static String clickBenchShapedTsvRow() {
        StringBuilder row = new StringBuilder();
        for (int c = 0; c < 105; c++) {
            if (c > 0) {
                row.append('\t');
            }
            switch (c % 7) {
                case 0 -> row.append(c);
                case 3 -> row.append("http://x/?q=\"a\"&p=").append(c);
                case 5 -> row.append("Title \"with\" quotes ").append(c);
                default -> row.append('v').append(c);
            }
        }
        row.append('\n');
        return row.toString();
    }

    private String randomFieldWithLiteralQuotes(char delim) {
        if (randomInt(4) == 0) {
            StringBuilder b = new StringBuilder("\"");
            int n = randomIntBetween(0, 6);
            for (int i = 0; i < n; i++) {
                int pick = randomInt(4);
                switch (pick) {
                    case 0 -> b.append(delim);
                    case 1 -> b.append('\n');
                    case 2 -> b.append("\"\"");
                    default -> b.append((char) ('a' + randomInt(25)));
                }
            }
            b.append('"');
            return b.toString();
        }
        StringBuilder b = new StringBuilder();
        b.append((char) ('a' + randomInt(25)));
        int n = randomIntBetween(0, 8);
        for (int i = 0; i < n; i++) {
            b.append(randomBoolean() ? '"' : (char) ('a' + randomInt(25)));
        }
        return b.toString();
    }
}
