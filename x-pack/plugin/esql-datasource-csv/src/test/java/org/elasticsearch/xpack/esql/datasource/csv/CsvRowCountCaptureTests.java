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

/**
 * Verifies that {@code CsvFormatReader.read(...)} populates {@link ExternalRowCountCache} on
 * iterator close when (and only when) the iterator was constructed for a whole-file read, drained
 * naturally to EOF, and observed zero parse errors. Lookup side is covered by
 * {@code CsvRowCountMetadataLookupTests}.
 */
public class CsvRowCountCaptureTests extends ESTestCase {

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
     * thread context here so the inherited {@code ensureNoWarnings} post-check sees an empty list;
     * tests don't care about the warning contents, only the cache effect.
     */
    @After
    public void clearWarningHeaders() {
        if (threadContext != null) {
            threadContext.stashContext();
        }
    }

    public void testWholeFileCleanDrainPopulatesCache() throws Exception {
        StorageObject o = obj("id:integer,n:integer\n1,10\n2,20\n3,30\n4,40\n");
        try (CloseableIterator<Page> it = new CsvFormatReader(blockFactory).read(o, FormatReadContext.builder().batchSize(10).build())) {
            drain(it);
        }
        OptionalLong cached = ExternalRowCountCache.lookup(o);
        assertTrue("clean whole-file drain must populate cache", cached.isPresent());
        assertEquals(4L, cached.getAsLong());
    }

    public void testCloseWithoutFullDrainDoesNotPopulateCache() throws Exception {
        StorageObject o = obj("id:integer,n:integer\n1,10\n2,20\n3,30\n");
        try (CloseableIterator<Page> it = new CsvFormatReader(blockFactory).read(o, FormatReadContext.builder().batchSize(10).build())) {
            if (it.hasNext()) {
                it.next().releaseBlocks();
            }
            // Close without reaching natural EOF — must not cache.
        }
        assertTrue("close-before-EOF must not populate cache", ExternalRowCountCache.lookup(o).isEmpty());
    }

    public void testNonFirstSplitDoesNotPopulateCache() throws Exception {
        StorageObject o = obj("id:integer,n:integer\n1,10\n2,20\n3,30\n");
        FormatReadContext ctx = FormatReadContext.builder().batchSize(10).firstSplit(false).lastSplit(true).build();
        try (CloseableIterator<Page> it = new CsvFormatReader(blockFactory).read(o, ctx)) {
            drain(it);
        }
        assertTrue("non-first split read must not populate cache", ExternalRowCountCache.lookup(o).isEmpty());
    }

    public void testNonLastSplitDoesNotPopulateCache() throws Exception {
        StorageObject o = obj("id:integer,n:integer\n1,10\n2,20\n3,30\n");
        FormatReadContext ctx = FormatReadContext.builder().batchSize(10).firstSplit(true).lastSplit(false).build();
        try (CloseableIterator<Page> it = new CsvFormatReader(blockFactory).read(o, ctx)) {
            drain(it);
        }
        assertTrue("non-last split read must not populate cache", ExternalRowCountCache.lookup(o).isEmpty());
    }

    public void testRecordAlignedDoesNotPopulateCache() throws Exception {
        StorageObject o = obj("id:integer,n:integer\n1,10\n2,20\n3,30\n");
        FormatReadContext ctx = FormatReadContext.builder().batchSize(10).recordAligned(true).build();
        try (CloseableIterator<Page> it = new CsvFormatReader(blockFactory).read(o, ctx)) {
            drain(it);
        }
        assertTrue("record-aligned (parallel-sliced) read must not populate cache", ExternalRowCountCache.lookup(o).isEmpty());
    }

    /**
     * Defense-in-depth: under {@code SKIP_ROW}, a CSV with at least one malformed row drains
     * naturally to EOF but {@code errorCount > 0}, so the data-driven gate suppresses the cache
     * write. Caching the post-skip count would mix policy-dependent values into the per-file
     * cache and break the warm-path contract.
     */
    public void testSkipRowWithErrorsDoesNotPopulateCache() throws Exception {
        // Middle row is unparseable as integer; SKIP_ROW drops it and continues.
        // logErrors=false to keep warning-header emission out of the test-runner's expectation set.
        ErrorPolicy skipRowQuiet = new ErrorPolicy(ErrorPolicy.Mode.SKIP_ROW, 10, 1.0, false);
        StorageObject o = obj("id:integer,n:integer\n1,10\nnot-an-integer,20\n3,30\n");
        FormatReadContext ctx = FormatReadContext.builder().batchSize(10).errorPolicy(skipRowQuiet).build();
        try (CloseableIterator<Page> it = new CsvFormatReader(blockFactory).read(o, ctx)) {
            drain(it);
        }
        // Cache stayed empty is the real invariant — the gate suppressed the write because the
        // iterator's internal errorCount was non-zero after draining a row that SKIP_ROW dropped.
        assertTrue("SKIP_ROW with errors must not populate cache (count is policy-dependent)", ExternalRowCountCache.lookup(o).isEmpty());
    }

    private static void drain(CloseableIterator<Page> it) {
        while (it.hasNext()) {
            it.next().releaseBlocks();
        }
    }

    private StorageObject obj(String csvContent) {
        byte[] bytes = csvContent.getBytes(StandardCharsets.UTF_8);
        String uniquePath = "memory://" + UUID.randomUUID() + ".csv";
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
