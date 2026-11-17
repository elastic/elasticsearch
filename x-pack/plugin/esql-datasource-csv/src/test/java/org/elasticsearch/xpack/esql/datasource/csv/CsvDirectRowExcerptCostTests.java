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
import org.junit.After;
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
 * <p>Each case reads two files of identical size and shape that differ in one column's content, so the
 * difference between the two reads is what the error path itself costs. Records are deliberately large —
 * {@link #PAD_CHARS} of payload in an unprojected column — because the per-row costs the fix does not remove
 * (the failing cell's own value, the held error message, the warning) are independent of record size, and only
 * a record far bigger than those makes the excerpt the dominant term. The control case asserts the measurement
 * can see an excerpt at all, so a JVM reporting nothing cannot pass the others vacuously.
 *
 * <p>Allocation is read from {@link SunThreadInfo}, the JDK counter {@code HotThreads} reports with; a JVM that
 * does not offer it skips every case here.
 */
public class CsvDirectRowExcerptCostTests extends ESTestCase {

    /** Rows per file. Deliberately small: the signal comes from record SIZE, so the walk stays below C2. */
    private static final int ROWS = 200;
    /** Characters of clean, unprojected payload per record — what an excerpt would copy. */
    private static final int PAD_CHARS = 64 * 1024;
    /** Bytes of record text across the whole file, which is what an excerpt per row would cost. */
    private static final long RECORD_TEXT_BYTES = (long) ROWS * PAD_CHARS;

    private static final String CLEAN = buildCsv("1");
    private static final String ONE_BAD_COLUMN = buildCsv("notanumber");

    private BlockFactory blockFactory;

    @Before
    public void initBlockFactory() {
        blockFactory = BlockFactory.builder(BigArrays.NON_RECYCLING_INSTANCE).breaker(NoopCircuitBreaker.INSTANCE).build();
    }

    /** Lenient reads emit response-header warnings; drop them so the parent {@code ensureNoWarnings} passes. */
    @After
    public void clearWarningHeaders() {
        if (threadContext != null) {
            threadContext.stashContext();
        }
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
     * Control for the two cases above: {@code fail_fast} puts the record into the message it throws, so here the
     * excerpt IS read and one is built. It proves the measurement can see a single record-sized copy, which is
     * what makes a small difference in the other two cases evidence rather than a counter reading zero.
     */
    public void testFailFastDoesMaterializeTheRecord() throws IOException {
        assumeAllocationCounterAvailable();
        warmUp();

        long clean = allocatedDuringRead(CLEAN, ErrorPolicy.Mode.NULL_FIELD, ROWS);
        long failFast = allocatedDuringFailingRead(ONE_BAD_COLUMN);

        logger.info("fail_fast read allocated {} bytes against a clean read's {}", failFast, clean);
        assertThat(
            "fail_fast quotes the record in the message it throws, so one record-sized copy must be visible",
            failFast - clean,
            greaterThan((long) PAD_CHARS)
        );
    }

    private void assertExcerptNotBuilt(ErrorPolicy.Mode mode, int expectedRowsFromBadFile) throws IOException {
        assumeAllocationCounterAvailable();
        warmUp();

        long clean = allocatedDuringRead(CLEAN, mode, ROWS);
        long erroring = allocatedDuringRead(ONE_BAD_COLUMN, mode, expectedRowsFromBadFile);

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
        assumeTrue(
            "JVM does not report per-thread allocation",
            SunThreadInfo.INSTANCE.isThreadAllocatedMemorySupported() && SunThreadInfo.INSTANCE.isThreadAllocatedMemoryEnabled()
        );
    }

    /** Loads the read path, the conversion path and both error paths before anything is measured. */
    private void warmUp() throws IOException {
        read(buildCsv("1", 4, 64), ErrorPolicy.Mode.NULL_FIELD, 4);
        read(buildCsv("notanumber", 4, 64), ErrorPolicy.Mode.NULL_FIELD, 4);
        read(buildCsv("notanumber", 4, 64), ErrorPolicy.Mode.SKIP_ROW, 0);
        expectThrows(Exception.class, () -> read(buildCsv("notanumber", 4, 64), ErrorPolicy.Mode.FAIL_FAST, 0));
    }

    private long allocatedDuringRead(String content, ErrorPolicy.Mode mode, int expectedRows) throws IOException {
        long id = Thread.currentThread().threadId();
        long before = SunThreadInfo.INSTANCE.getThreadAllocatedBytes(id);
        read(content, mode, expectedRows);
        return SunThreadInfo.INSTANCE.getThreadAllocatedBytes(id) - before;
    }

    private long allocatedDuringFailingRead(String content) {
        long id = Thread.currentThread().threadId();
        long before = SunThreadInfo.INSTANCE.getThreadAllocatedBytes(id);
        expectThrows(Exception.class, () -> read(content, ErrorPolicy.Mode.FAIL_FAST, 0));
        return SunThreadInfo.INSTANCE.getThreadAllocatedBytes(id) - before;
    }

    /**
     * Reads with the direct-to-block walk, projecting only the column that fails to convert: the padded column is
     * never materialized, so what an erroring row costs over a clean one is the excerpt and the error bookkeeping.
     */
    private void read(String content, ErrorPolicy.Mode mode, int expectedRows) throws IOException {
        CsvFormatReader reader = new CsvFormatReader(blockFactory, "csv", List.of(".csv")).withDirectBlockEnabled(true);
        StorageObject object = new InMemoryStorageObject(content.getBytes(StandardCharsets.UTF_8));
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
