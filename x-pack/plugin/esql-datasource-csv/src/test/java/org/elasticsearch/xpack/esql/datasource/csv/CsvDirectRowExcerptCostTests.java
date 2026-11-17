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
import org.elasticsearch.compute.data.Page;
import org.elasticsearch.compute.operator.CloseableIterator;
import org.elasticsearch.monitor.jvm.SunThreadInfo;
import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.xpack.esql.datasources.spi.AbstractTestStorageObject;
import org.elasticsearch.xpack.esql.datasources.spi.ErrorPolicy;
import org.elasticsearch.xpack.esql.datasources.spi.ExternalSourceMetrics;
import org.elasticsearch.xpack.esql.datasources.spi.FormatReadContext;
import org.elasticsearch.xpack.esql.datasources.spi.StorageIdentity;
import org.elasticsearch.xpack.esql.datasources.spi.StorageObject;
import org.elasticsearch.xpack.esql.datasources.spi.StoragePath;
import org.junit.Before;

import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.nio.charset.StandardCharsets;
import java.time.Instant;
import java.util.List;

import static org.hamcrest.Matchers.greaterThan;
import static org.hamcrest.Matchers.lessThan;

/**
 * What the direct-to-block CSV walk spends on a row that errors, under each lenient error mode.
 *
 * <p>The direct walkers convert a record straight from its character range into typed block builders, with no
 * {@link String} per cell. A row-level error message is the one thing on that path that would materialize the
 * whole record, and only {@code fail_fast} ever reads one: it goes into the thrown message. Under
 * {@code skip_row} the row is dropped with a warning carrying the message alone, and under {@code null_field}
 * the failure is reported per column and the row is kept, so neither has a reader for a record excerpt. A file
 * whose every row errors must therefore allocate no more than one whose rows are all clean.
 *
 * <p>Each case reads two files of the same shape and length that differ in one cell's content, so the
 * difference between the two reads is what the error path itself costs. Records are deliberately large —
 * {@link #PAD_CHARS} of payload in an unprojected column — because the per-row costs that remain (the failing
 * cell's own value, the held message, the warning) are independent of record size, and only a record far bigger
 * than those makes an excerpt the dominant term. {@link #testFailFastDoesMaterializeTheRecord} is the control: it
 * reads a file of the same length whose last row errors, so a counter that saw nothing could not pass it.
 *
 * <p>Allocation is read from {@link SunThreadInfo}, the JDK counter {@code HotThreads} reports with; a JVM that
 * does not offer it skips every case here.
 *
 * <p>What this cannot pin: {@code withDirectBlockEnabled(true)} is a request, and no reader status reports which
 * walk served a read. If the reader ever declined the direct path for this schema, projection or mode, these
 * cases would measure the batch path and still pass.
 */
public class CsvDirectRowExcerptCostTests extends ESTestCase {

    /** Rows per file. Kept small because the signal comes from record SIZE rather than from row count. */
    private static final int ROWS = 200;
    /** Characters of clean, unprojected payload per record — what an excerpt would copy. */
    private static final int PAD_CHARS = 64 * 1024;
    /** Bytes of record text across the whole file, which is what an excerpt per row would cost. */
    private static final long RECORD_TEXT_BYTES = (long) ROWS * PAD_CHARS;

    private BlockFactory blockFactory;

    @Before
    public void initBlockFactory() {
        blockFactory = BlockFactory.builder(BigArrays.NON_RECYCLING_INSTANCE).breaker(NoopCircuitBreaker.INSTANCE).build();
    }

    /** Lenient reads emit response-header warnings by design; drain them as the sibling tests do. */
    private void drainWarnings() {
        threadContext.stashContext();
    }

    /** Under {@code null_field} the row is kept and the failure reported per column, so nothing quotes the record. */
    public void testNullFieldRowDoesNotMaterializeTheRecord() throws IOException {
        assertExcerptNotBuilt(ErrorPolicy.Mode.NULL_FIELD, ROWS);
    }

    /** Under {@code skip_row} the row is dropped with a warning carrying the message alone, which quotes nothing. */
    public void testSkipRowRejectionDoesNotMaterializeTheRecord() throws IOException {
        assertExcerptNotBuilt(ErrorPolicy.Mode.SKIP_ROW, 0);
    }

    /**
     * The width rejection is the other error exit of {@code finishDirectRow}, and it is reached by a different
     * fault: the row's shape rather than a cell's content. Reverting that exit alone would be invisible to the
     * two cases above, which error on a cell.
     */
    public void testTooWideRowDoesNotMaterializeTheRecord() throws IOException {
        assumeAllocationCounterAvailable();
        warmUp();

        // Both lenient modes drop a too-wide row -- it is structural, so null_field does not null-fill it.
        for (ErrorPolicy.Mode mode : List.of(ErrorPolicy.Mode.SKIP_ROW, ErrorPolicy.Mode.NULL_FIELD)) {
            long clean = allocatedDuringRead(utf8(buildCsv("1")), mode, ROWS);
            long tooWide = allocatedDuringRead(utf8(tooWideRows()), mode, 0);

            logger.info("{} width rejection allocated: clean={} wide={} delta={}", mode, clean, tooWide, tooWide - clean);
            assertThat(
                "a row rejected for its width under " + mode + " must not pay for an excerpt nobody reads",
                tooWide - clean,
                lessThan(RECORD_TEXT_BYTES / 4)
            );
        }
    }

    /**
     * The control for the two cases above, and the reason their comparisons mean anything: under
     * {@code fail_fast} an excerpt IS read, so one is built, and the counter must see it.
     *
     * <p>The two reads must differ only in the excerpt, so they share a row count — the bad cell is in the LAST
     * row of an identically sized file — they do their UTF-8 encoding outside the measured window, and they run
     * after a warm-up that has already grown the record buffer past its initial 256 characters.
     */
    public void testFailFastDoesMaterializeTheRecord() throws IOException {
        assumeAllocationCounterAvailable();
        warmUp();

        long clean = allocatedDuringRead(utf8(buildCsv("1")), ErrorPolicy.Mode.FAIL_FAST, ROWS);
        long failing = allocatedDuringFailingRead(utf8(badLastRow()));

        logger.info("fail_fast allocated {} bytes against a clean read's {}", failing, clean);
        assertThat(
            "fail_fast quotes the record it rejects, so one record-sized copy must be visible",
            failing - clean,
            greaterThan((long) PAD_CHARS / 2)
        );
    }

    private void assertExcerptNotBuilt(ErrorPolicy.Mode mode, int expectedRowsFromBadFile) throws IOException {
        assumeAllocationCounterAvailable();
        warmUp();

        long clean = allocatedDuringRead(utf8(buildCsv("1")), mode, ROWS);
        long erroring = allocatedDuringRead(utf8(buildCsv("notanumber")), mode, expectedRowsFromBadFile);

        logger.info(
            "{} read allocated: clean={} bytes, erroring={} bytes, delta={} bytes, record text={} bytes",
            mode,
            clean,
            erroring,
            erroring - clean,
            RECORD_TEXT_BYTES
        );
        assertThat(
            "a row reported under " + mode + " must not pay for a record excerpt nobody reads",
            erroring - clean,
            lessThan(RECORD_TEXT_BYTES / 4)
        );
    }

    private void assumeAllocationCounterAvailable() {
        logger.info(
            "per-thread allocation counter: supported={} enabled={}",
            SunThreadInfo.INSTANCE.isThreadAllocatedMemorySupported(),
            SunThreadInfo.INSTANCE.isThreadAllocatedMemoryEnabled()
        );
        assumeTrue(
            "JVM does not report per-thread allocation",
            SunThreadInfo.INSTANCE.isThreadAllocatedMemorySupported() && SunThreadInfo.INSTANCE.isThreadAllocatedMemoryEnabled()
        );
    }

    /** Loads the read path, the conversion path and both error paths before anything is measured. */
    private void warmUp() throws IOException {
        read(utf8(buildCsv("1", 4, 64)), ErrorPolicy.Mode.NULL_FIELD, 4);
        read(utf8(buildCsv("notanumber", 4, 64)), ErrorPolicy.Mode.NULL_FIELD, 4);
        read(utf8(buildCsv("notanumber", 4, 64)), ErrorPolicy.Mode.SKIP_ROW, 0);
        expectThrows(Exception.class, () -> read(utf8(buildCsv("notanumber", 4, 64)), ErrorPolicy.Mode.FAIL_FAST, 0));
        drainWarnings();
    }

    private long allocatedDuringRead(byte[] content, ErrorPolicy.Mode mode, int expectedRows) throws IOException {
        long id = Thread.currentThread().threadId();
        long before = SunThreadInfo.INSTANCE.getThreadAllocatedBytes(id);
        read(content, mode, expectedRows);
        long after = SunThreadInfo.INSTANCE.getThreadAllocatedBytes(id);
        drainWarnings();
        return after - before;
    }

    private long allocatedDuringFailingRead(byte[] content) {
        long id = Thread.currentThread().threadId();
        long before = SunThreadInfo.INSTANCE.getThreadAllocatedBytes(id);
        expectThrows(Exception.class, () -> read(content, ErrorPolicy.Mode.FAIL_FAST, 0));
        long after = SunThreadInfo.INSTANCE.getThreadAllocatedBytes(id);
        drainWarnings();
        return after - before;
    }

    /**
     * Reads with the direct-to-block walk, projecting only the column that fails to convert: the padded column is
     * never materialized, so what an erroring row costs over a clean one is the excerpt and the error bookkeeping.
     */
    private void read(byte[] content, ErrorPolicy.Mode mode, int expectedRows) throws IOException {
        CsvFormatReader reader = new CsvFormatReader(blockFactory, "csv", List.of(".csv")).withDirectBlockEnabled(true);
        StorageObject object = new InMemoryStorageObject(content);
        FormatReadContext ctx = FormatReadContext.builder()
            .projectedColumns(List.of("a"))
            .batchSize(1024)
            .errorPolicy(new ErrorPolicy(mode, 100_000, 0.0, false))
            .build();
        int rows = 0;
        try (CloseableIterator<Page> pages = reader.read(object, ctx)) {
            while (pages.hasNext()) {
                Page page = pages.next();
                rows += page.getPositionCount();
                page.releaseBlocks();
            }
        }
        assertEquals("rows surviving " + mode, expectedRows, rows);
    }

    private static byte[] utf8(String s) {
        return s.getBytes(StandardCharsets.UTF_8);
    }

    /** Every row one field wider than the header, so each is rejected for its shape rather than a cell. */
    private static String tooWideRows() {
        String pad = "x".repeat(PAD_CHARS - 2);
        StringBuilder sb = new StringBuilder(ROWS * (PAD_CHARS + 32));
        sb.append("a:long,b:keyword\n");
        for (int i = 0; i < ROWS; i++) {
            sb.append("1,").append(pad).append(",surplus\n");
        }
        return sb.toString();
    }

    /** The same file as {@code buildCsv("1")} but for its final row's first cell, so the walk is the same length. */
    private static String badLastRow() {
        String pad = "x".repeat(PAD_CHARS);
        StringBuilder sb = new StringBuilder(ROWS * (PAD_CHARS + 32));
        sb.append("a:long,b:keyword\n");
        for (int i = 0; i < ROWS; i++) {
            sb.append(i == ROWS - 1 ? "notanumber" : "1").append(',').append(pad).append('\n');
        }
        return sb.toString();
    }

    private static String buildCsv(String firstColumn) {
        return buildCsv(firstColumn, ROWS, PAD_CHARS);
    }

    private static String buildCsv(String firstColumn, int rows, int padChars) {
        String pad = "x".repeat(padChars);
        StringBuilder sb = new StringBuilder(rows * (padChars + 32));
        sb.append("a:long,b:keyword\n");
        for (int i = 0; i < rows; i++) {
            sb.append(firstColumn).append(',').append(pad).append('\n');
        }
        return sb.toString();
    }

    private record InMemoryStorageObject(byte[] bytes) implements StorageObject {
        @Override
        public void attachMetrics(ExternalSourceMetrics metrics, String scheme) {}

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
            return StoragePath.of("mem://host/csv-direct-row-excerpt-cost-tests");
        }

        @Override
        public StorageIdentity storageIdentity() {
            return AbstractTestStorageObject.NOOP;
        }
    }
}
