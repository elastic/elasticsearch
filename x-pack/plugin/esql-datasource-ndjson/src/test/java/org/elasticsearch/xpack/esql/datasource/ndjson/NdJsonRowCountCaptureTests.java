/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.datasource.ndjson;

import org.elasticsearch.common.breaker.NoopCircuitBreaker;
import org.elasticsearch.common.util.BigArrays;
import org.elasticsearch.compute.data.BlockFactory;
import org.elasticsearch.compute.data.Page;
import org.elasticsearch.compute.operator.CloseableIterator;
import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.xpack.esql.datasources.cache.ExternalRowCountCache;
import org.elasticsearch.xpack.esql.datasources.spi.ErrorPolicy;
import org.elasticsearch.xpack.esql.datasources.spi.FormatReadContext;
import org.elasticsearch.xpack.esql.datasources.spi.StorageObject;
import org.elasticsearch.xpack.esql.datasources.spi.StoragePath;
import org.junit.After;

import java.io.ByteArrayInputStream;
import java.io.InputStream;
import java.nio.charset.StandardCharsets;
import java.time.Instant;
import java.util.OptionalLong;
import java.util.UUID;

/** NDJSON counterpart of {@code CsvRowCountCaptureTests}: the cache-write side on close. */
public class NdJsonRowCountCaptureTests extends ESTestCase {

    private BlockFactory blockFactory;

    @Override
    public void setUp() throws Exception {
        super.setUp();
        blockFactory = BlockFactory.builder(BigArrays.NON_RECYCLING_INSTANCE).breaker(new NoopCircuitBreaker("none")).build();
        ExternalRowCountCache.clearForTests();
    }

    @Override
    public void tearDown() throws Exception {
        ExternalRowCountCache.clearForTests();
        super.tearDown();
    }

    /**
     * SKIP_ROW paths emit response-header warnings via {@code HeaderWarning}. Drop the accumulated
     * thread context here so the inherited {@code ensureNoWarnings} post-check sees an empty list.
     */
    @After
    public void clearWarningHeaders() {
        if (threadContext != null) {
            threadContext.stashContext();
        }
    }

    public void testWholeFileCleanDrainPopulatesCache() throws Exception {
        StorageObject o = obj("{\"a\":1}\n{\"a\":2}\n{\"a\":3}\n");
        try (
            CloseableIterator<Page> it = new NdJsonFormatReader(null, blockFactory).read(
                o,
                FormatReadContext.builder().batchSize(10).build()
            )
        ) {
            drain(it);
        }
        OptionalLong cached = ExternalRowCountCache.lookup(o);
        assertTrue("clean whole-file drain must populate cache", cached.isPresent());
        assertEquals(3L, cached.getAsLong());
    }

    public void testCloseWithoutFullDrainDoesNotPopulateCache() throws Exception {
        StorageObject o = obj("{\"a\":1}\n{\"a\":2}\n{\"a\":3}\n");
        try (
            CloseableIterator<Page> it = new NdJsonFormatReader(null, blockFactory).read(
                o,
                FormatReadContext.builder().batchSize(10).build()
            )
        ) {
            if (it.hasNext()) {
                it.next().releaseBlocks();
            }
        }
        assertTrue("close-before-EOF must not populate cache", ExternalRowCountCache.lookup(o).isEmpty());
    }

    public void testNonFirstSplitDoesNotPopulateCache() throws Exception {
        StorageObject o = obj("{\"a\":1}\n{\"a\":2}\n{\"a\":3}\n");
        FormatReadContext ctx = FormatReadContext.builder().batchSize(10).firstSplit(false).lastSplit(true).build();
        try (CloseableIterator<Page> it = new NdJsonFormatReader(null, blockFactory).read(o, ctx)) {
            drain(it);
        }
        assertTrue("non-first split read must not populate cache", ExternalRowCountCache.lookup(o).isEmpty());
    }

    public void testNonLastSplitDoesNotPopulateCache() throws Exception {
        StorageObject o = obj("{\"a\":1}\n{\"a\":2}\n{\"a\":3}\n");
        FormatReadContext ctx = FormatReadContext.builder().batchSize(10).firstSplit(true).lastSplit(false).build();
        try (CloseableIterator<Page> it = new NdJsonFormatReader(null, blockFactory).read(o, ctx)) {
            drain(it);
        }
        assertTrue("non-last split read must not populate cache", ExternalRowCountCache.lookup(o).isEmpty());
    }

    public void testRecordAlignedDoesNotPopulateCache() throws Exception {
        StorageObject o = obj("{\"a\":1}\n{\"a\":2}\n{\"a\":3}\n");
        FormatReadContext ctx = FormatReadContext.builder().batchSize(10).recordAligned(true).build();
        try (CloseableIterator<Page> it = new NdJsonFormatReader(null, blockFactory).read(o, ctx)) {
            drain(it);
        }
        assertTrue("record-aligned (parallel-sliced) read must not populate cache", ExternalRowCountCache.lookup(o).isEmpty());
    }

    /**
     * Defense-in-depth: under {@code SKIP_ROW}, an NDJSON file with one malformed line drains
     * naturally but {@code errorCount > 0}, so the data-driven gate suppresses the cache write.
     * Mirrors the CSV reader's counterpart.
     */
    public void testSkipRowWithErrorsDoesNotPopulateCache() throws Exception {
        // logErrors=false keeps warning-header emission out of the test runner's expected set.
        ErrorPolicy skipRowQuiet = new ErrorPolicy(ErrorPolicy.Mode.SKIP_ROW, 10, 1.0, false);
        StorageObject o = obj("{\"a\":1}\nnot-a-json-object\n{\"a\":3}\n");
        FormatReadContext ctx = FormatReadContext.builder().batchSize(10).errorPolicy(skipRowQuiet).build();
        try (CloseableIterator<Page> it = new NdJsonFormatReader(null, blockFactory).read(o, ctx)) {
            drain(it);
            assertTrue("SKIP_ROW with malformed line must have observed at least one error", it.errorsObserved() > 0);
        }
        assertTrue("SKIP_ROW with errors must not populate cache (count is policy-dependent)", ExternalRowCountCache.lookup(o).isEmpty());
    }

    /**
     * Defense-in-depth: a {@code LIMIT}-cut iteration sets {@code endOfFile} but not the natural-EOF
     * flag (the decoder still has bytes), so the cache write is suppressed.
     */
    public void testRowLimitTruncatedReadDoesNotPopulateCache() throws Exception {
        StorageObject o = obj("{\"a\":1}\n{\"a\":2}\n{\"a\":3}\n{\"a\":4}\n{\"a\":5}\n");
        // rowLimit smaller than the file's row count forces a truncated read.
        FormatReadContext ctx = FormatReadContext.builder().batchSize(2).rowLimit(2).build();
        try (CloseableIterator<Page> it = new NdJsonFormatReader(null, blockFactory).read(o, ctx)) {
            drain(it);
        }
        assertTrue("rowLimit-truncated read must not populate cache", ExternalRowCountCache.lookup(o).isEmpty());
    }

    private static void drain(CloseableIterator<Page> it) {
        while (it.hasNext()) {
            it.next().releaseBlocks();
        }
    }

    private StorageObject obj(String ndjson) {
        byte[] bytes = ndjson.getBytes(StandardCharsets.UTF_8);
        String uniquePath = "memory://" + UUID.randomUUID() + ".ndjson";
        Instant fixedMtime = Instant.now();
        return new StorageObject() {
            @Override
            public InputStream newStream() {
                return new ByteArrayInputStream(bytes);
            }

            @Override
            public InputStream newStream(long position, long length) {
                throw new UnsupportedOperationException("Range reads not needed");
            }

            @Override
            public long length() {
                return bytes.length;
            }

            @Override
            public Instant lastModified() {
                return fixedMtime;
            }

            @Override
            public boolean exists() {
                return true;
            }

            @Override
            public StoragePath path() {
                return StoragePath.of(uniquePath);
            }
        };
    }
}
