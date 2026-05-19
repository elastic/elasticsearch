/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.datasource.csv;

import org.elasticsearch.test.ESTestCase;

import java.nio.charset.StandardCharsets;

/**
 * Regression suite that runs against any {@link CsvBoundaryScanner.Impl}. Each scanner — the
 * prefix-XOR fast path and the per-byte scalar reference — has a one-line concrete subclass that
 * supplies the {@link #impl()} under test, so every scenario below executes on both
 * implementations.
 * <p>
 * The randomized cross-implementation equivalence check lives separately in
 * {@code CsvBoundaryScannerEquivalenceTests} — that one structurally needs both implementations
 * in scope and doesn't fit the parameterized pattern.
 */
public abstract class AbstractCsvBoundaryScannerTests extends ESTestCase {

    protected static final byte QUOTE = (byte) '"';

    /** The scanner under test. Each concrete subclass returns one of {@link CsvBoundaryScanner}'s impls. */
    protected abstract CsvBoundaryScanner.Impl impl();

    // -------- findLastRealTerminator regressions ----------------------------------------------

    public void testSimpleTwoLines() {
        byte[] data = "a,b\nc,d\n".getBytes(StandardCharsets.UTF_8);
        assertEquals(data.length - 1, impl().findLastRealTerminator(data, 0, data.length, QUOTE));
    }

    public void testSingleTrailingLineNoTerminator() {
        byte[] data = "a,b\nc,d".getBytes(StandardCharsets.UTF_8);
        assertEquals(3, impl().findLastRealTerminator(data, 0, data.length, QUOTE));
    }

    public void testEmptyBuffer() {
        assertEquals(-1, impl().findLastRealTerminator(new byte[0], 0, 0, QUOTE));
    }

    public void testAllInsideQuotedField() {
        // Open quote, body with newlines, never closed: open-tail rule → -1.
        byte[] data = "\"foo\nbar\nbaz".getBytes(StandardCharsets.UTF_8);
        assertEquals(-1, impl().findLastRealTerminator(data, 0, data.length, QUOTE));
    }

    public void testSkipsEmbeddedNewlineInQuotedField() {
        byte[] data = "a,\"foo\nbar\",c\nd,e,f\n".getBytes(StandardCharsets.UTF_8);
        assertEquals(data.length - 1, impl().findLastRealTerminator(data, 0, data.length, QUOTE));
    }

    public void testEmbeddedNewlineFollowedByUnterminatedTail() {
        byte[] data = "a,b\n\"foo\nbar".getBytes(StandardCharsets.UTF_8);
        assertEquals(3, impl().findLastRealTerminator(data, 0, data.length, QUOTE));
    }

    public void testCRLF() {
        byte[] data = "a,b\r\nc,d\r\n".getBytes(StandardCharsets.UTF_8);
        assertEquals(data.length - 1, impl().findLastRealTerminator(data, 0, data.length, QUOTE));
    }

    public void testDoubledQuoteEscape() {
        // "foo""bar" is one quoted field with a literal " in the middle; the trailing \n closes the
        // record.
        byte[] data = "\"foo\"\"bar\"\n".getBytes(StandardCharsets.UTF_8);
        assertEquals(data.length - 1, impl().findLastRealTerminator(data, 0, data.length, QUOTE));
    }

    public void testLengthSubsetOfBuffer() {
        byte[] data = "a,b\nc,d\nEXTRA".getBytes(StandardCharsets.UTF_8);
        assertEquals(3, impl().findLastRealTerminator(data, 0, 7, QUOTE));
    }

    public void testOffsetRespectedDoesNotReadBefore() {
        byte[] data = "PREFIX\na,b\nc,d\n".getBytes(StandardCharsets.UTF_8);
        // From offset 7 onwards. Last real terminator at relative offset 7 (= absolute 14).
        assertEquals(7, impl().findLastRealTerminator(data, 7, data.length - 7, QUOTE));
    }

    public void testCrossBlockBoundaryQuoteStaysOpen() {
        // 71 bytes: open quote, 68 a's, closing quote, \n. The quote run spans block 0 (64 bytes);
        // the in-quote carry must propagate to the tail where the \n at byte 70 is the terminator.
        StringBuilder sb = new StringBuilder("\"");
        for (int i = 0; i < 68; i++)
            sb.append('a');
        sb.append('"');
        sb.append('\n');
        byte[] data = sb.toString().getBytes(StandardCharsets.UTF_8);
        assertEquals(71, data.length);
        assertEquals(70, impl().findLastRealTerminator(data, 0, data.length, QUOTE));
    }

    public void testNewlineInsideQuoteSpanningBlockBoundary() {
        // \n at exact block boundary (byte 64) inside the still-open quote — NOT a real terminator.
        StringBuilder sb = new StringBuilder("\"");
        for (int i = 0; i < 63; i++)
            sb.append('a');
        sb.append('\n'); // byte 64
        for (int i = 0; i < 5; i++)
            sb.append('b');
        sb.append('"');
        sb.append('\n'); // the real terminator
        byte[] data = sb.toString().getBytes(StandardCharsets.UTF_8);
        assertEquals(data.length - 1, impl().findLastRealTerminator(data, 0, data.length, QUOTE));
    }

    public void testDoubledQuoteStraddlesBlockBoundary() {
        // First " at byte 63 (end of block 0), second " at byte 64 (start of block 1) — RFC 4180
        // escape pair across the block boundary. The run stays open; the \n at byte 70 is inside.
        StringBuilder sb = new StringBuilder("\"");
        for (int i = 0; i < 62; i++)
            sb.append('a'); // bytes 1..62
        sb.append('"'); // byte 63
        sb.append('"'); // byte 64
        sb.append('x');
        sb.append('y');
        sb.append('z');
        sb.append('w');
        sb.append('v');
        sb.append('\n'); // byte 70 — INSIDE the still-open run
        sb.append('"'); // byte 71 — closes the run
        sb.append('\n'); // byte 72 — the terminator
        byte[] data = sb.toString().getBytes(StandardCharsets.UTF_8);
        assertEquals(data.length - 1, impl().findLastRealTerminator(data, 0, data.length, QUOTE));
    }

    // -------- countRealTerminators regressions ------------------------------------------------

    public void testCountTwoRecords() {
        byte[] data = "a,b\nc,d\n".getBytes(StandardCharsets.UTF_8);
        long[] carryOut = new long[1];
        assertEquals(2L, impl().countRealTerminators(data, 0, data.length, QUOTE, 0L, carryOut));
        assertEquals(0L, carryOut[0]);
    }

    public void testCountIgnoresEmbeddedNewlines() {
        byte[] data = "a,\"foo\nbar\",c\nd,e,f\n".getBytes(StandardCharsets.UTF_8);
        assertEquals(2L, impl().countRealTerminators(data, 0, data.length, QUOTE, 0L, null));
    }

    public void testCountPropagatesCarryAcrossBuffers() {
        // First buffer opens a quote that doesn't close — carry-out must be -1L (inside-quote).
        byte[] first = "a,b,\"foo\n".getBytes(StandardCharsets.UTF_8);
        long[] mid = new long[1];
        assertEquals(0L, impl().countRealTerminators(first, 0, first.length, QUOTE, 0L, mid));
        assertEquals(-1L, mid[0]);

        // Second buffer: enters inside-quote, closing " toggles out, trailing \n is the terminator.
        byte[] second = "bar\nbaz\",g,h\n".getBytes(StandardCharsets.UTF_8);
        long[] end = new long[1];
        assertEquals(1L, impl().countRealTerminators(second, 0, second.length, QUOTE, mid[0], end));
        assertEquals(0L, end[0]);
    }

    public void testCountEmptyBufferPreservesCarry() {
        long[] carryOut = new long[1];
        assertEquals(0L, impl().countRealTerminators(new byte[0], 0, 0, QUOTE, -1L, carryOut));
        assertEquals(-1L, carryOut[0]);
    }
}
