/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.datasource.csv;

import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.xpack.esql.datasources.spi.RecordSplitter;
import org.elasticsearch.xpack.esql.datasources.spi.SegmentableFormatReader;

import java.io.BufferedInputStream;
import java.io.ByteArrayInputStream;
import java.io.FilterInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.nio.charset.StandardCharsets;

import static org.hamcrest.Matchers.greaterThan;
import static org.hamcrest.Matchers.greaterThanOrEqualTo;
import static org.hamcrest.Matchers.lessThan;

/**
 * What split discovery spends on a CSV that contains no quote character anywhere - the shape of a taxi or metrics
 * export - and what it spends on one that does.
 *
 * <p>The proven probe certifies a record start only where its out-of-quote and in-quote readings agree, and the
 * in-quote reading can only be retired by seeing a quote character. Data with none therefore never converges: every
 * probe returns {@code AMBIGUOUS} and the caller falls back to the exact walk, so the whole file is walked at
 * planning time before a single row is emitted. That is a property of the grammar and this suite pins it rather
 * than trying to fix it - a quote-free megabyte proves nothing, because a legal record may run to
 * {@code maxRecordBytes}.
 *
 * <p>What is negotiable is the cost per byte of that walk, and the second half of this suite watches the one part
 * of it that is visible from outside: whether a scanner asks the stream it was handed for one byte at a time.
 */
public class CsvQuoteFreeProbeCostTests extends ESTestCase {

    private static final int FILE_BYTES = 8 * 1024 * 1024;
    private static final long STRIDE = 1024 * 1024;

    /** Built once each: six cases read these, and each build is a fresh 8 MiB StringBuilder, String and array. */
    private static final byte[] QUOTE_FREE = buildCsv(false);
    private static final byte[] QUOTED = buildCsv(true);

    /** A quote-free CSV: every proven probe is AMBIGUOUS, whatever offset it starts at. */
    public void testQuoteFreeProbeNeverConverges() throws IOException {
        byte[] buf = QUOTE_FREE;
        RecordSplitter splitter = splitter();
        for (long pos = STRIDE; pos < buf.length; pos += STRIDE) {
            long probed = splitter.findProvenRecordBoundary(streamAt(buf, pos));
            assertEquals("probe at offset " + pos + " should not converge on quote-free data", RecordSplitter.AMBIGUOUS, probed);
        }
    }

    /** Control: the same data with one quoted field per row converges at every probe offset. */
    public void testQuotedProbeConverges() throws IOException {
        byte[] buf = QUOTED;
        RecordSplitter splitter = splitter();
        for (long pos = STRIDE; pos < buf.length; pos += STRIDE) {
            long probed = splitter.findProvenRecordBoundary(streamAt(buf, pos));
            assertTrue("probe at offset " + pos + " should converge when quotes are present, got " + probed, probed >= 0);
        }
    }

    /**
     * Drives the same loop {@code RecordBoundaryProbe.provenBoundaries} runs and counts the bytes it pulls.
     * Quote-free data pays the whole file; quoted data pays a small constant per stride.
     */
    public void testSplitDiscoveryReadsWholeFileWhenQuoteFree() throws IOException {
        long quoteFree = bytesReadDuringSplitDiscovery(QUOTE_FREE);
        long quoted = bytesReadDuringSplitDiscovery(QUOTED);
        logger.info("split discovery read: quote-free={} bytes, quoted={} bytes, file={} bytes", quoteFree, quoted, FILE_BYTES);
        assertThat("quote-free split discovery should read at least the whole file", quoteFree, greaterThanOrEqualTo((long) FILE_BYTES));
        assertThat("quoted split discovery should read a small fraction of the file", quoted, lessThan((long) FILE_BYTES / 10));
    }

    /**
     * The exact walk reads a whole span rather than one record, so what it charges per byte sets the cost of
     * planning a query over quote-free CSV. It must take that span a block at a time.
     *
     * <p>Two things are asserted, because a single-byte counter alone is not enough. The counter catches a scanner
     * that asks a caller-supplied {@link BufferedInputStream} for one byte at a time. It does not catch one that
     * wraps a {@link BufferedInputStream} of its own and reads per byte from that, because the delegate then sees
     * the same block-sized calls in the same number - so the call stack is checked as well, and a
     * {@link BufferedInputStream} anywhere beneath the read is a failure.
     *
     * <p>What remains uncovered is a locking wrapper that is not a {@link BufferedInputStream} - a hand-rolled
     * {@code synchronized} stream would pass both assertions. Only a benchmark covers that.
     */
    public void testExactWalkTakesItsSpanBlockAtATime() throws IOException {
        byte[] buf = QUOTE_FREE;
        int[] singleByteReads = new int[1];
        InputStream in = countingSingleByteReads(new ByteArrayInputStream(buf), singleByteReads);
        long start = splitter().findRecordStartAtOrAfter(in, buf.length - 64L, () -> false);
        assertThat("the walk should reach a record start near the end of the file", start, greaterThan(0L));
        assertEquals("the exact walk must not pull its span one read() at a time", 0, singleByteReads[0]);

        long viaStack = splitter().findRecordStartAtOrAfter(refusingBufferedInputStream(buf), buf.length - 64L, () -> false);
        assertEquals("the same boundary, read without a BufferedInputStream in the stack", start, viaStack);
    }

    /** The same for the probe, which reads up to its convergence window at every offset of a file. */
    public void testProbeTakesItsWindowBlockAtATime() throws IOException {
        byte[] buf = QUOTE_FREE;
        int[] singleByteReads = new int[1];
        InputStream in = countingSingleByteReads(new ByteArrayInputStream(buf), singleByteReads);
        assertEquals(RecordSplitter.AMBIGUOUS, splitter().findProvenRecordBoundary(in));
        assertEquals("the probe must not pull its window one read() at a time", 0, singleByteReads[0]);
        assertEquals(RecordSplitter.AMBIGUOUS, splitter().findProvenRecordBoundary(refusingBufferedInputStream(buf)));
    }

    /** The provenBoundaries loop, byte-counted. Approximates it: neither minSegment break is reproduced. */
    private long bytesReadDuringSplitDiscovery(byte[] buf) throws IOException {
        RecordSplitter splitter = splitter();
        long[] readCounter = new long[1];
        long exactCursor = 0L;
        long pos = STRIDE;
        while (pos < buf.length) {
            long boundary;
            long probed = splitter.findProvenRecordBoundary(counting(streamAt(buf, pos), readCounter));
            if (probed >= 0) {
                boundary = pos + probed;
            } else {
                long start = splitter.findRecordStartAtOrAfter(
                    counting(streamAt(buf, exactCursor), readCounter),
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
            exactCursor = boundary;
            pos = boundary + STRIDE;
        }
        return readCounter[0];
    }

    /** Taxi-shaped rows: numeric/date columns, plus one quoted free-text column when {@code withQuotes}. */
    private static byte[] buildCsv(boolean withQuotes) {
        StringBuilder sb = new StringBuilder(FILE_BYTES + 1024);
        sb.append("vendor_id,pickup_datetime,passenger_count,trip_distance,fare_amount,total_amount");
        if (withQuotes) {
            sb.append(",store_and_fwd");
        }
        sb.append('\n');
        int row = 0;
        while (sb.length() < FILE_BYTES) {
            sb.append(row % 3 + 1).append(",2024-06-14 02:31:07,").append(row % 6 + 1).append(',');
            sb.append(row % 97).append('.').append(row % 100).append(',');
            sb.append(row % 53).append(".25,").append(row % 53 + 4).append(".75");
            if (withQuotes) {
                sb.append(",\"N\"");
            }
            sb.append('\n');
            row++;
        }
        return sb.toString().getBytes(StandardCharsets.UTF_8);
    }

    private static InputStream streamAt(byte[] buf, long pos) {
        return new ByteArrayInputStream(buf, Math.toIntExact(pos), buf.length - Math.toIntExact(pos));
    }

    private static InputStream counting(InputStream delegate, long[] counter) {
        return new FilterInputStream(delegate) {
            @Override
            public int read() throws IOException {
                int b = super.read();
                if (b != -1) {
                    counter[0]++;
                }
                return b;
            }

            @Override
            public int read(byte[] b, int off, int len) throws IOException {
                int n = super.read(b, off, len);
                if (n > 0) {
                    counter[0] += n;
                }
                return n;
            }
        };
    }

    /**
     * A stream that refuses to be read through a {@link BufferedInputStream}. A scanner that wraps one privately
     * and reads per byte from it sends this delegate the same block-sized calls a block cursor does, so the count
     * cannot tell them apart - but the wrapper is on the stack when the call arrives, and that can.
     */
    private static InputStream refusingBufferedInputStream(byte[] buf) {
        return new FilterInputStream(new ByteArrayInputStream(buf)) {
            @Override
            public int read(byte[] b, int off, int len) throws IOException {
                boolean buffered = StackWalker.getInstance(StackWalker.Option.RETAIN_CLASS_REFERENCE)
                    .walk(frames -> frames.anyMatch(f -> f.getDeclaringClass() == BufferedInputStream.class));
                assertFalse("the read reached this stream through a BufferedInputStream", buffered);
                return super.read(b, off, len);
            }
        };
    }

    /** A {@link BufferedInputStream} that records how many times a caller asked it for a single byte. */
    private static InputStream countingSingleByteReads(InputStream delegate, int[] counter) {
        return new BufferedInputStream(delegate) {
            @Override
            public int read() throws IOException {
                counter[0]++;
                return super.read();
            }
        };
    }

    private static RecordSplitter splitter() {
        return new CsvRecordSplitter(CsvTestOptions.csvDefaults(), SegmentableFormatReader.DEFAULT_MAX_RECORD_BYTES);
    }

}
