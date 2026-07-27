/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.datasource.csv;

import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.xpack.esql.datasources.spi.RecordSplitter;

import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.nio.charset.StandardCharsets;

public class CsvRecordSplitterContractTests extends ESTestCase {

    public void testEmptyInputReturnsEof() throws IOException {
        for (Case testCase : cases(128)) {
            RecordSplitter splitter = testCase.newSplitter();
            assertEquals(testCase.name(), -1L, splitter.findNextRecordBoundary(new ByteArrayInputStream(new byte[0])));
            assertEquals(testCase.name(), -1, splitter.findLastRecordBoundary(new byte[0], 0, 0));
        }
    }

    public void testEofBeforeAnyTerminatorReturnsEof() throws IOException {
        for (Case testCase : cases(128)) {
            assertEquals(
                testCase.name(),
                -1L,
                testCase.newSplitter().findNextRecordBoundary(new ByteArrayInputStream(bytes("unterminated")))
            );
        }
    }

    public void testSingleLfTerminatedRecordConsumesThroughTerminator() throws IOException {
        for (Case testCase : cases(128)) {
            assertEquals(testCase.name(), 4L, testCase.newSplitter().findNextRecordBoundary(new ByteArrayInputStream(bytes("row\n"))));
        }
    }

    public void testEmbeddedTerminatorInRecord() throws IOException {
        Case quoted = quotedFieldsOnly(128);
        assertEquals(13L, quoted.newSplitter().findNextRecordBoundary(new ByteArrayInputStream(bytes("\"multi\nline\"\n"))));

        Case bracket = bracketMvc(128);
        assertEquals(13L, bracket.newSplitter().findNextRecordBoundary(new ByteArrayInputStream(bytes("[multi\nline]\n"))));
    }

    public void testRecordTooLargeSentinel() throws IOException {
        int maxRecordBytes = 16;
        for (Case testCase : cases(maxRecordBytes)) {
            assertEquals(
                testCase.name(),
                RecordSplitter.RECORD_TOO_LARGE,
                testCase.newSplitter().findNextRecordBoundary(new ByteArrayInputStream(repeatedByte('x', maxRecordBytes + 1)))
            );
        }
    }

    public void testFindLastRecordBoundaryMatchesForwardScan() throws IOException {
        for (Case testCase : cases(128)) {
            byte[] payload = bytes("first\nsecond\ntail");
            int expectedBoundary = driveForwardToLastBoundary(testCase.newSplitter(), payload);

            RecordSplitter splitter = testCase.newSplitter();
            assertEquals(testCase.name(), expectedBoundary, splitter.findLastRecordBoundary(payload, 0, payload.length));
            assertEquals(testCase.name(), expectedBoundary, splitter.findLastRecordBoundary(payload, payload.length));

            byte[] prefix = bytes("xx");
            byte[] prefixedPayload = concat(prefix, payload);
            assertEquals(
                testCase.name(),
                prefix.length + expectedBoundary,
                splitter.findLastRecordBoundary(prefixedPayload, prefix.length, payload.length)
            );
        }
    }

    private static Case[] cases(int maxRecordBytes) {
        return new Case[] { quotedFieldsOnly(maxRecordBytes), bracketMvc(maxRecordBytes), newlineNoQuote(maxRecordBytes) };
    }

    private static Case newlineNoQuote(int maxRecordBytes) {
        // The no-quote modes' splitter honors the same boundary contract as its quote-aware siblings.
        return new Case("newline-no-quote", () -> new NewlineRecordSplitter(maxRecordBytes));
    }

    private static Case quotedFieldsOnly(int maxRecordBytes) {
        // Quoting on, escaping off, no bracket MVC: the plain quote-aware path. TSV is now a plain (no
        // quote, no escape) preset, so it would exercise the strided newline scan instead - not this path.
        return new Case("quoted-fields-only", () -> new CsvRecordSplitter(quotedNoEscape(), maxRecordBytes));
    }

    private static CsvFormatOptions quotedNoEscape() {
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

    private static Case bracketMvc(int maxRecordBytes) {
        // CsvFormatOptions.DEFAULT now defaults to MultiValueSyntax.NONE; construct an explicit
        // BRACKETS-mode options so the bracket-MVC contract assertions actually exercise that path.
        return new Case("bracket-mvc", () -> new CsvRecordSplitter(bracketsDefault(), maxRecordBytes));
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

    private static int driveForwardToLastBoundary(RecordSplitter splitter, byte[] input) throws IOException {
        int cumulative = 0;
        int lastBoundary = -1;
        while (cumulative < input.length) {
            long consumed = splitter.findNextRecordBoundary(new ByteArrayInputStream(input, cumulative, input.length - cumulative));
            if (consumed < 0) {
                return lastBoundary;
            }
            assertTrue("splitter must make progress", consumed > 0);
            assertTrue("splitter consumed past the buffer", consumed <= input.length - cumulative);
            cumulative += Math.toIntExact(consumed);
            lastBoundary = cumulative - 1;
        }
        return lastBoundary;
    }

    private static byte[] bytes(String value) {
        return value.getBytes(StandardCharsets.UTF_8);
    }

    private static byte[] repeatedByte(char value, int count) {
        byte[] bytes = new byte[count];
        for (int i = 0; i < count; i++) {
            bytes[i] = (byte) value;
        }
        return bytes;
    }

    private static byte[] concat(byte[]... chunks) {
        int length = 0;
        for (byte[] chunk : chunks) {
            length += chunk.length;
        }
        byte[] result = new byte[length];
        int offset = 0;
        for (byte[] chunk : chunks) {
            System.arraycopy(chunk, 0, result, offset, chunk.length);
            offset += chunk.length;
        }
        return result;
    }

    private record Case(String name, SplitterSupplier supplier) {
        private RecordSplitter newSplitter() throws IOException {
            return supplier.get();
        }
    }

    @FunctionalInterface
    private interface SplitterSupplier {
        RecordSplitter get() throws IOException;
    }
}
