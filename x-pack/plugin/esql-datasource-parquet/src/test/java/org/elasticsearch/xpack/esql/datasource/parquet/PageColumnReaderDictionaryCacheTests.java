/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.datasource.parquet;

import org.apache.lucene.util.BytesRef;
import org.apache.parquet.column.ParquetProperties;
import org.apache.parquet.conf.PlainParquetConfiguration;
import org.apache.parquet.example.data.Group;
import org.apache.parquet.example.data.simple.SimpleGroupFactory;
import org.apache.parquet.hadoop.ParquetWriter;
import org.apache.parquet.hadoop.example.ExampleParquetWriter;
import org.apache.parquet.hadoop.metadata.CompressionCodecName;
import org.apache.parquet.io.OutputFile;
import org.apache.parquet.io.PositionOutputStream;
import org.apache.parquet.schema.LogicalTypeAnnotation;
import org.apache.parquet.schema.MessageType;
import org.apache.parquet.schema.Types;
import org.elasticsearch.common.breaker.CircuitBreaker;
import org.elasticsearch.common.breaker.CircuitBreakingException;
import org.elasticsearch.common.unit.ByteSizeValue;
import org.elasticsearch.common.util.BigArrays;
import org.elasticsearch.common.util.BytesRefArray;
import org.elasticsearch.common.util.MockBigArrays;
import org.elasticsearch.common.util.PageCacheRecycler;
import org.elasticsearch.compute.data.BlockFactory;
import org.elasticsearch.compute.data.BytesRefBlock;
import org.elasticsearch.compute.data.BytesRefVector;
import org.elasticsearch.compute.data.IntVector;
import org.elasticsearch.compute.data.LocalCircuitBreaker;
import org.elasticsearch.compute.data.OrdinalBytesRefBlock;
import org.elasticsearch.compute.data.Page;
import org.elasticsearch.compute.operator.CloseableIterator;
import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.xpack.esql.datasources.spi.AbstractTestStorageObject;
import org.elasticsearch.xpack.esql.datasources.spi.ExternalSourceMetrics;
import org.elasticsearch.xpack.esql.datasources.spi.FormatReadContext;
import org.elasticsearch.xpack.esql.datasources.spi.StorageIdentity;
import org.elasticsearch.xpack.esql.datasources.spi.StorageObject;
import org.elasticsearch.xpack.esql.datasources.spi.StoragePath;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.time.Instant;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Random;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.IntFunction;

import static org.apache.parquet.schema.PrimitiveType.PrimitiveTypeName.BINARY;

/**
 * Verifies that {@link PageColumnReader} wraps the chunk dictionary in one thread-safe
 * {@link BytesRefVector} and shares that object across ordinal batches via {@code incRef},
 * while still being safe to produce on one thread and close on another.
 *
 * <p>Sharing is verified by identity ({@code assertSame} on live {@code getDictionaryVector()})
 * and by the breaker: with {@link MockBigArrays#withCircuitBreaking()} the underlying
 * {@code BigArrays} bytes flow through the breaker. If the dictionary were deep-copied per
 * batch, peak breaker usage while all batches are alive would scale with the batch count;
 * with sharing it stays bounded near a single dictionary's footprint.
 */
public class PageColumnReaderDictionaryCacheTests extends ESTestCase {

    /**
     * Many small batches over a single dictionary-encoded chunk share one underlying
     * {@code BytesRefVector}. We hold every emitted page alive simultaneously, then assert that
     * peak breaker usage is far below what a per-batch deep-copy would have produced. Pages are
     * then closed in random order to exercise the atomic ref-count.
     */
    public void testDictionaryArrayCachedAcrossBatches() throws IOException {
        int numRows = 5_000;
        int batchSize = 256;
        int dictSize = 64;
        IntFunction<String> valueFor = row -> "v_" + (row % dictSize);

        BigArrays bigArrays = new MockBigArrays(PageCacheRecycler.NON_RECYCLING_INSTANCE, ByteSizeValue.ofMb(256)).withCircuitBreaking();
        CircuitBreaker breaker = bigArrays.breakerService().getBreaker(CircuitBreaker.REQUEST);
        BlockFactory blockFactory = BlockFactory.builder(bigArrays).breaker(breaker).build();

        byte[] data = writeStringFile(numRows, false, valueFor);

        List<Page> pages = new ArrayList<>();
        long usedAfterFirstPage = -1;
        long peak = 0;
        try (CloseableIterator<Page> iter = openIterator(blockFactory, data, batchSize)) {
            while (iter.hasNext()) {
                pages.add(iter.next());
                long used = breaker.getUsed();
                peak = Math.max(peak, used);
                if (usedAfterFirstPage < 0) {
                    usedAfterFirstPage = used;
                }
            }
        }
        try {
            assertTrue("expected many batches per chunk: got " + pages.size(), pages.size() >= 20);

            int totalRows = 0;
            int ordinalPagesSeen = 0;
            for (Page page : pages) {
                BytesRefBlock block = page.getBlock(0);
                OrdinalBytesRefBlock ordinal = block.asOrdinals();
                if (ordinal != null) {
                    ordinalPagesSeen++;
                }
                BytesRef scratch = new BytesRef();
                for (int p = 0; p < page.getPositionCount(); p++) {
                    String expected = valueFor.apply(totalRows);
                    assertEquals("row " + totalRows, new BytesRef(expected), block.getBytesRef(p, scratch));
                    totalRows++;
                }
            }
            assertEquals(numRows, totalRows);
            // 64 distinct values in a 256-row batch cannot collapse to a constant block.
            assertEquals("every page should be ordinal for this layout", pages.size(), ordinalPagesSeen);

            // Sharing-by-breaker check, expressed as the average bytes added per additional
            // page rather than a multiplicative ratio. The first-page mark captures the full
            // chunk footprint (shared dict vector + ordinals + page bookkeeping). With sharing:
            // each subsequent page adds only ordinals + page overhead — observed locally at
            // well under 1 KB / page for a 64-entry dict, far below the first-page mark since
            // the dict vector dominates that baseline.
            // Without sharing (the regression we want to detect): every page deep-copies the
            // dict, so per-page growth approximates the *entire* first-page mark.
            // Bounding per-page growth at 1/3 of the first-page mark cleanly separates the two
            // regimes: shared sits around 0.15x; per-batch deep copy would land near 1.0x.
            // A per-batch shallow wrap of the same array is tens of bytes and would not trip
            // this bound; testCachedDictVectorIdentityAcrossBatches catches that via assertSame.
            long extraPages = pages.size() - 1;
            long perPageGrowth = extraPages > 0 ? (peak - usedAfterFirstPage) / extraPages : 0;
            long perPageCeiling = usedAfterFirstPage / 3;
            logger.debug(
                "dict cache test breaker stats: usedAfterFirstPage={}, peak={}, pages={}, perPageGrowth={}, perPageCeiling={}",
                usedAfterFirstPage,
                peak,
                pages.size(),
                perPageGrowth,
                perPageCeiling
            );
            assertTrue(
                "average per-page breaker growth ["
                    + perPageGrowth
                    + " bytes] should stay below 1/3 of first-page footprint ["
                    + perPageCeiling
                    + " bytes] across "
                    + pages.size()
                    + " pages — a per-batch deep copy of the dictionary would push this near 1.0x",
                perPageGrowth < perPageCeiling
            );
        } finally {
            List<Page> shuffled = new ArrayList<>(pages);
            Collections.shuffle(shuffled, new Random(randomLong()));
            for (Page page : shuffled) {
                page.releaseBlocks();
            }
        }
        assertEquals("breaker must return to zero after random-order release", 0, breaker.getUsed());
    }

    /**
     * Reads a multi-row-group file with a tracking breaker wired through {@link MockBigArrays}.
     * The breaker must return to zero — a leaked cached {@code BytesRefArray} would manifest as
     * a positive remainder because its underlying {@code BigArrays} bytes are never released.
     */
    public void testCachedDictionaryArrayReleasedOnChunkBoundary() throws IOException {
        int numRows = 6_000;
        int batchSize = 256;
        int dictSize = 50;
        IntFunction<String> valueFor = row -> "term_" + (row % dictSize);
        // Small row group → multiple chunks → exercises the close-and-rebuild path between
        // chunks where the previous reader's cached array must be released.
        byte[] data = writeStringFile(numRows, false, valueFor, /* rowGroupSize */ 8 * 1024L);

        BigArrays bigArrays = new MockBigArrays(PageCacheRecycler.NON_RECYCLING_INSTANCE, ByteSizeValue.ofMb(256)).withCircuitBreaking();
        CircuitBreaker breaker = bigArrays.breakerService().getBreaker(CircuitBreaker.REQUEST);
        BlockFactory blockFactory = BlockFactory.builder(bigArrays).breaker(breaker).build();

        int totalRows = 0;
        try (CloseableIterator<Page> iter = openIterator(blockFactory, data, batchSize)) {
            while (iter.hasNext()) {
                Page page = iter.next();
                totalRows += page.getPositionCount();
                page.releaseBlocks();
            }
        }
        assertEquals(numRows, totalRows);
        assertEquals("breaker must return to zero — a leaked cached dict vector would show here", 0, breaker.getUsed());
    }

    /**
     * One large row group, many batches: every live ordinal page shares the same dictionary vector.
     * This layout (64 values, 256-row batches) never collapses to a constant block.
     */
    public void testCachedDictVectorIdentityAcrossBatches() throws IOException {
        int numRows = 5_000;
        int batchSize = 256;
        int dictSize = 64;
        IntFunction<String> valueFor = row -> "v_" + (row % dictSize);
        byte[] data = writeStringFile(numRows, false, valueFor, 8 * 1024 * 1024L);

        BigArrays bigArrays = new MockBigArrays(PageCacheRecycler.NON_RECYCLING_INSTANCE, ByteSizeValue.ofMb(256)).withCircuitBreaking();
        CircuitBreaker breaker = bigArrays.breakerService().getBreaker(CircuitBreaker.REQUEST);
        BlockFactory blockFactory = BlockFactory.builder(bigArrays).breaker(breaker).build();

        List<Page> pages = new ArrayList<>();
        try (CloseableIterator<Page> iter = openIterator(blockFactory, data, batchSize)) {
            while (iter.hasNext()) {
                pages.add(iter.next());
            }
        }
        try {
            assertTrue("expected many batches: got " + pages.size(), pages.size() >= 20);
            BytesRefVector shared = null;
            int ordinalPages = 0;
            for (Page page : pages) {
                OrdinalBytesRefBlock ordinal = ordinals(page);
                if (ordinal == null) {
                    continue;
                }
                BytesRefVector dict = ordinal.getDictionaryVector();
                if (shared == null) {
                    shared = dict;
                } else {
                    assertSame("ordinal pages from one row group must share one dict vector", shared, dict);
                }
                ordinalPages++;
            }
            assertNotNull("expected at least one ordinal page", shared);
            // Same 64-value / 256-row layout as the breaker test: no batch is constant.
            assertEquals("every page should be ordinal for this layout", pages.size(), ordinalPages);
            assertFalse(shared.isReleased());
        } finally {
            for (Page page : pages) {
                page.releaseBlocks();
            }
        }
        assertEquals(0, breaker.getUsed());
    }

    /**
     * Sync path passes a driver-local child factory. Dict wrap charges the root; after the
     * operator hops each page (T1), closing the child breaker while pages are live must not
     * release the vector. Ordinals stay charged to the child until T1 — hopping first is
     * required, otherwise {@link LocalCircuitBreaker#close} returns the reserve and later
     * ordinal {@code decRef}s never reach the parent.
     */
    public void testDictVectorChargedToRootWhenReadThroughChildFactory() throws IOException {
        int numRows = 2_000;
        int batchSize = 256;
        int dictSize = 64;
        IntFunction<String> valueFor = row -> "v_" + (row % dictSize);
        byte[] data = writeStringFile(numRows, false, valueFor, 8 * 1024 * 1024L);

        BigArrays bigArrays = new MockBigArrays(PageCacheRecycler.NON_RECYCLING_INSTANCE, ByteSizeValue.ofMb(256)).withCircuitBreaking();
        CircuitBreaker rootBreaker = bigArrays.breakerService().getBreaker(CircuitBreaker.REQUEST);
        BlockFactory root = BlockFactory.builder(bigArrays).breaker(rootBreaker).build();
        LocalCircuitBreaker local = new LocalCircuitBreaker(rootBreaker, 8 * 1024, 512 * 1024);
        BlockFactory child = root.newChildFactory(local);

        List<Page> pages = new ArrayList<>();
        BytesRefVector dict = null;
        boolean pagesReleased = false;
        try {
            try (CloseableIterator<Page> iter = openIterator(child, data, batchSize)) {
                int toRead = 4;
                while (pages.size() < toRead && iter.hasNext()) {
                    pages.add(iter.next());
                }
                assertEquals(toRead, pages.size());
                assertTrue("reader hold must stay live", iter.hasNext());
            }
            for (Page page : pages) {
                page.allowPassingToDifferentDriver();
                OrdinalBytesRefBlock ordinal = ordinals(page);
                if (ordinal == null) {
                    continue;
                }
                if (dict == null) {
                    dict = ordinal.getDictionaryVector();
                } else {
                    assertSame(dict, ordinal.getDictionaryVector());
                }
            }
            assertNotNull(dict);
            assertSame("wrap-on-parent plus T1 leave the dict on the root factory", root, dict.blockFactory());
            assertFalse(dict.isReleased());
            local.close();
            local = null;
            assertFalse("pages still hold the vector after child breaker close", dict.isReleased());
            closePagesShuffled(pages);
            pagesReleased = true;
            assertTrue(dict.isReleased());
        } finally {
            if (local != null) {
                local.close();
            }
            if (pagesReleased == false) {
                for (Page page : pages) {
                    page.releaseBlocks();
                }
            }
        }
        assertEquals(0, rootBreaker.getUsed());
    }

    public void testDictVectorLifetimeReaderThenPages() throws IOException {
        assertDictVectorLifetime(true);
    }

    public void testDictVectorLifetimePagesThenReader() throws IOException {
        assertDictVectorLifetime(false);
    }

    /**
     * Capture {@code getDictionaryVector()} from a live ordinal page, then close the reader and
     * pages in the requested order. The vector is released only after the last holder closes.
     */
    private void assertDictVectorLifetime(boolean readerFirst) throws IOException {
        int numRows = 2_000;
        int batchSize = 256;
        int dictSize = 64;
        IntFunction<String> valueFor = row -> "v_" + (row % dictSize);
        byte[] data = writeStringFile(numRows, false, valueFor, 8 * 1024 * 1024L);

        BigArrays bigArrays = new MockBigArrays(PageCacheRecycler.NON_RECYCLING_INSTANCE, ByteSizeValue.ofMb(256)).withCircuitBreaking();
        CircuitBreaker breaker = bigArrays.breakerService().getBreaker(CircuitBreaker.REQUEST);
        BlockFactory blockFactory = BlockFactory.builder(bigArrays).breaker(breaker).build();

        CloseableIterator<Page> iter = openIterator(blockFactory, data, batchSize);
        List<Page> pages = new ArrayList<>();
        BytesRefVector captured = null;
        boolean pagesReleased = false;
        try {
            // Stop before EOF: draining hasNext() releases the reader via releaseHeldIo, which
            // would drop the creation ref before we can test pages-first vs reader-first.
            int toRead = 4;
            while (pages.size() < toRead && iter.hasNext()) {
                Page page = iter.next();
                pages.add(page);
                if (captured == null) {
                    OrdinalBytesRefBlock ordinal = ordinals(page);
                    if (ordinal != null) {
                        captured = ordinal.getDictionaryVector();
                    }
                }
            }
            assertEquals("need several live pages plus unread rows so the reader stays open", toRead, pages.size());
            assertTrue("iterator must still have rows so the reader hold is live", iter.hasNext());
            assertNotNull("expected a live ordinal page to capture the dict vector from", captured);
            assertFalse(captured.isReleased());

            if (readerFirst) {
                iter.close();
                iter = null;
                assertFalse("pages still hold the vector after reader close", captured.isReleased());
                closePagesShuffled(pages);
                pagesReleased = true;
                assertTrue("vector released after last page", captured.isReleased());
            } else {
                closePagesShuffled(pages);
                pagesReleased = true;
                assertFalse("reader still holds the vector after pages close", captured.isReleased());
                iter.close();
                iter = null;
                assertTrue("vector released after reader close", captured.isReleased());
            }
        } finally {
            if (iter != null) {
                iter.close();
            }
            if (pagesReleased == false) {
                for (Page page : pages) {
                    page.releaseBlocks();
                }
            }
        }
        assertEquals(0, breaker.getUsed());
    }

    /**
     * Producer hops each page then keeps calling {@code next()} while a consumer thread
     * hops again (T3) and {@code releaseBlocks()} concurrently. Iterator stays open until
     * produce finishes. Queue capacity > 1 keeps several pages live so hops overlap.
     */
    public void testCrossDriverCloseDuringDecode() throws Exception {
        int iterations = 200;
        int numRows = 1_500;
        int batchSize = 64;
        int dictSize = 32;
        IntFunction<String> valueFor = row -> "v_" + (row % dictSize);
        byte[] data = writeStringFile(numRows, false, valueFor);

        BigArrays bigArrays = new MockBigArrays(PageCacheRecycler.NON_RECYCLING_INSTANCE, ByteSizeValue.ofMb(256)).withCircuitBreaking();
        CircuitBreaker breaker = bigArrays.breakerService().getBreaker(CircuitBreaker.REQUEST);
        BlockFactory blockFactory = BlockFactory.builder(bigArrays).breaker(breaker).build();

        ExecutorService consumer = Executors.newSingleThreadExecutor();
        AtomicReference<Throwable> consumerFailure = new AtomicReference<>();
        try {
            for (int i = 0; i < iterations; i++) {
                BlockingQueue<Page> live = new LinkedBlockingQueue<>(8);
                AtomicBoolean producing = new AtomicBoolean(true);
                CountDownLatch started = new CountDownLatch(1);
                CountDownLatch consumed = new CountDownLatch(1);
                Random jitter = new Random(randomLong());
                consumer.execute(() -> {
                    started.countDown();
                    try {
                        while (producing.get() || live.isEmpty() == false) {
                            Page page = live.poll(10, TimeUnit.MILLISECONDS);
                            if (page == null) {
                                continue;
                            }
                            try {
                                Thread.sleep(0, jitter.nextInt(200_000));
                            } catch (InterruptedException ie) {
                                Thread.currentThread().interrupt();
                                page.releaseBlocks();
                                break;
                            }
                            if (jitter.nextBoolean()) {
                                page.allowPassingToDifferentDriver();
                            }
                            page.releaseBlocks();
                        }
                    } catch (Throwable t) {
                        consumerFailure.compareAndSet(null, t);
                    } finally {
                        producing.set(false);
                        drainAndRelease(live);
                        consumed.countDown();
                    }
                });
                if (started.await(30, TimeUnit.SECONDS) == false) {
                    fail("consumer did not start on iteration " + i);
                }
                int rowsRead = 0;
                try {
                    try (CloseableIterator<Page> iter = openIterator(blockFactory, data, batchSize)) {
                        while (iter.hasNext()) {
                            Page page = iter.next();
                            rowsRead += page.getPositionCount();
                            page.allowPassingToDifferentDriver();
                            if (live.offer(page, 30, TimeUnit.SECONDS) == false) {
                                page.releaseBlocks();
                                fail("consumer did not take pages on iteration " + i);
                            }
                        }
                    } finally {
                        producing.set(false);
                    }
                    assertEquals(numRows, rowsRead);
                    if (consumed.await(30, TimeUnit.SECONDS) == false) {
                        fail("consumer did not finish in time on iteration " + i);
                    }
                    if (consumerFailure.get() != null) {
                        throw new AssertionError("consumer thread failed on iteration " + i, consumerFailure.get());
                    }
                } finally {
                    drainAndRelease(live);
                }
            }
        } finally {
            consumer.shutdownNow();
            assertTrue("consumer did not terminate", consumer.awaitTermination(10, TimeUnit.SECONDS));
        }
        assertEquals("breaker must return to zero across all overlapping produce/consume iterations", 0, breaker.getUsed());
    }

    public void testFailureDuringFirstDictVectorCreateCleansUp() throws Exception {
        assertFailureInjectionCleansUp(0, null);
    }

    public void testFailureDuringOrdinalsBuildCleansUp() throws Exception {
        // Trip the first int-vector allocation after the dict vector exists (second batch's
        // ordinals). Extra int-vectors on the first page, before the wrap, do not count.
        assertFailureInjectionCleansUp(null, 0);
    }

    /**
     * A breaker trip aborts the iterator. Close the iterator plus any pages already emitted;
     * do not reopen the same {@code PageColumnReader}.
     */
    private void assertFailureInjectionCleansUp(Integer tripBytesRefAt, Integer tripIntAt) throws Exception {
        int numRows = 2_000;
        int batchSize = 64;
        int dictSize = 32;
        IntFunction<String> valueFor = row -> "v_" + (row % dictSize);
        byte[] data = writeStringFile(numRows, false, valueFor, 8 * 1024 * 1024L);

        BigArrays bigArrays = new MockBigArrays(PageCacheRecycler.NON_RECYCLING_INSTANCE, ByteSizeValue.ofMb(256)).withCircuitBreaking();
        CircuitBreaker breaker = bigArrays.breakerService().getBreaker(CircuitBreaker.REQUEST);
        BlockFactory blockFactory = new TripBlockFactory(breaker, bigArrays, tripBytesRefAt, tripIntAt);

        List<Page> pages = new ArrayList<>();
        boolean failed = false;
        CloseableIterator<Page> iter = null;
        try {
            iter = openIterator(blockFactory, data, batchSize);
            while (iter.hasNext()) {
                pages.add(iter.next());
            }
        } catch (Exception e) {
            failed = true;
            assertCircuitBreaking(e);
        } finally {
            try {
                if (iter != null) {
                    iter.close();
                }
            } finally {
                for (Page page : pages) {
                    page.releaseBlocks();
                }
            }
        }
        assertTrue("expected next() to fail under injected breaker trip", failed);
        if (tripBytesRefAt != null) {
            assertTrue("first dict-vector trip should emit no pages", pages.isEmpty());
        } else {
            assertEquals("ordinals trip on the batch after dict wrap should leave exactly one page", 1, pages.size());
        }
        assertEquals(0, breaker.getUsed());
    }

    /**
     * Same dictionary content in every row group: vectors are {@code assertSame} within a group
     * and {@code assertNotSame} across groups.
     */
    public void testCachedDictVectorDistinctAcrossRowGroups() throws IOException {
        int numRows = 3_000;
        int batchSize = 256;
        int dictSize = 50;
        int rowsPerGroup = 512;
        IntFunction<String> valueFor = row -> "term_" + (row % dictSize);
        byte[] data = writeStringFile(numRows, false, valueFor, 64 * 1024 * 1024L, rowsPerGroup);

        BigArrays bigArrays = new MockBigArrays(PageCacheRecycler.NON_RECYCLING_INSTANCE, ByteSizeValue.ofMb(256)).withCircuitBreaking();
        CircuitBreaker breaker = bigArrays.breakerService().getBreaker(CircuitBreaker.REQUEST);
        BlockFactory blockFactory = BlockFactory.builder(bigArrays).breaker(breaker).build();

        List<Page> pages = new ArrayList<>();
        try (CloseableIterator<Page> iter = openIterator(blockFactory, data, batchSize)) {
            while (iter.hasNext()) {
                pages.add(iter.next());
            }
        }
        try {
            BytesRefVector prevGroupDict = null;
            BytesRefVector groupDict = null;
            int lastGroup = -1;
            int groups = 0;
            int sameWithinGroup = 0;
            int ordinalPages = 0;
            int rowsSeen = 0;
            for (Page page : pages) {
                int startRow = rowsSeen;
                int endRow = rowsSeen + page.getPositionCount();
                int startGroup = startRow / rowsPerGroup;
                int endGroup = (endRow - 1) / rowsPerGroup;
                assertEquals("page must not span writer row-group boundaries", startGroup, endGroup);
                rowsSeen = endRow;
                OrdinalBytesRefBlock ordinal = ordinals(page);
                if (ordinal == null) {
                    continue;
                }
                BytesRefVector dict = ordinal.getDictionaryVector();
                if (startGroup != lastGroup) {
                    if (prevGroupDict != null) {
                        assertNotSame("equal content still yields a new dict vector per row group", prevGroupDict, dict);
                    }
                    prevGroupDict = dict;
                    groupDict = dict;
                    lastGroup = startGroup;
                    groups++;
                } else {
                    assertSame("ordinal pages from the same row group share one dict vector", groupDict, dict);
                    sameWithinGroup++;
                }
                ordinalPages++;
            }
            assertTrue("expected ordinal pages, got " + ordinalPages, ordinalPages >= 2);
            assertTrue("expected multiple row groups, got " + groups, groups >= 2);
            assertTrue("expected intra-group assertSame to run, got " + sameWithinGroup, sameWithinGroup >= 1);
        } finally {
            for (Page page : pages) {
                page.releaseBlocks();
            }
        }
        assertEquals(0, breaker.getUsed());
    }

    // --- helpers ---

    private static OrdinalBytesRefBlock ordinals(Page page) {
        BytesRefBlock block = page.getBlock(0);
        return block.asOrdinals();
    }

    private void closePagesShuffled(List<Page> pages) {
        List<Page> shuffled = new ArrayList<>(pages);
        Collections.shuffle(shuffled, new Random(randomLong()));
        for (Page page : shuffled) {
            page.releaseBlocks();
        }
    }

    private static void drainAndRelease(BlockingQueue<Page> live) {
        Page page;
        while ((page = live.poll()) != null) {
            page.releaseBlocks();
        }
    }

    private static void assertCircuitBreaking(Throwable e) {
        for (Throwable t = e; t != null; t = t.getCause()) {
            if (t instanceof CircuitBreakingException) {
                return;
            }
        }
        throw new AssertionError("expected CircuitBreakingException, got " + e, e);
    }

    /**
     * Test {@link BlockFactory} that trips a {@link CircuitBreakingException} on a chosen
     * allocation. {@code tripBytesRefAt}/{@code tripIntAt} are 0-based counters of
     * {@link BlockFactory#newBytesRefArrayVector} / {@link BlockFactory#newIntArrayVector}
     * calls; {@code null} means never trip that path.
     */
    private static final class TripBlockFactory extends BlockFactory {
        private final AtomicInteger bytesRefVectors = new AtomicInteger();
        private final AtomicInteger intVectors = new AtomicInteger();
        private final Integer tripBytesRefAt;
        private final Integer tripIntAt;

        TripBlockFactory(CircuitBreaker breaker, BigArrays bigArrays, Integer tripBytesRefAt, Integer tripIntAt) {
            super(breaker, bigArrays);
            this.tripBytesRefAt = tripBytesRefAt;
            this.tripIntAt = tripIntAt;
        }

        @Override
        public BytesRefVector newBytesRefArrayVector(BytesRefArray values, int positionCount) {
            maybeTrip(bytesRefVectors, tripBytesRefAt, "dict vector");
            return super.newBytesRefArrayVector(values, positionCount);
        }

        @Override
        public IntVector newIntArrayVector(int[] values, int positionCount, long preAdjustedBytes) {
            // Ordinals are built before the dict wrap. Count only int-vectors after the dict
            // exists so extra first-page int allocations cannot steal tripIntAt.
            if (tripIntAt != null && bytesRefVectors.get() > 0) {
                maybeTrip(intVectors, tripIntAt, "ordinals");
            }
            return super.newIntArrayVector(values, positionCount, preAdjustedBytes);
        }

        private static void maybeTrip(AtomicInteger counter, Integer tripAt, String label) {
            int n = counter.getAndIncrement();
            if (tripAt != null && n == tripAt) {
                throw new CircuitBreakingException("trip " + label, CircuitBreaker.Durability.TRANSIENT);
            }
        }
    }

    private CloseableIterator<Page> openIterator(BlockFactory blockFactory, byte[] data, int batchSize) throws IOException {
        StorageObject so = storageObject(data);
        // Force optimizedReader=true so we exercise the PageColumnReader / OptimizedParquetColumnIterator paths.
        return new ParquetFormatReader(blockFactory, true).read(so, FormatReadContext.of(List.of("name"), batchSize));
    }

    private byte[] writeStringFile(int numRows, boolean optional, IntFunction<String> valueFor) throws IOException {
        return writeStringFile(numRows, optional, valueFor, 64 * 1024L, Integer.MAX_VALUE);
    }

    private byte[] writeStringFile(int numRows, boolean optional, IntFunction<String> valueFor, long rowGroupSize) throws IOException {
        return writeStringFile(numRows, optional, valueFor, rowGroupSize, Integer.MAX_VALUE);
    }

    private byte[] writeStringFile(
        int numRows,
        boolean optional,
        IntFunction<String> valueFor,
        long rowGroupSize,
        int rowGroupRowCountLimit
    ) throws IOException {
        Types.MessageTypeBuilder schemaBuilder = Types.buildMessage();
        var col = optional ? Types.optional(BINARY) : Types.required(BINARY);
        col.as(LogicalTypeAnnotation.stringType());
        schemaBuilder.addField(col.named("name"));
        MessageType schema = schemaBuilder.named("dict_cache_test");

        ByteArrayOutputStream out = new ByteArrayOutputStream();
        SimpleGroupFactory factory = new SimpleGroupFactory(schema);
        PlainParquetConfiguration conf = new PlainParquetConfiguration();
        conf.set("parquet.enable.dictionary", "true");
        try (
            ParquetWriter<Group> writer = ExampleParquetWriter.builder(outputFile(out))
                .withConf(conf)
                .withCodecFactory(new PlainCompressionCodecFactory())
                .withType(schema)
                .withWriterVersion(ParquetProperties.WriterVersion.PARQUET_1_0)
                .withCompressionCodec(CompressionCodecName.UNCOMPRESSED)
                .withDictionaryEncoding(true)
                .withDictionaryPageSize(64 * 1024)
                .withRowGroupSize(rowGroupSize)
                .withRowGroupRowCountLimit(rowGroupRowCountLimit)
                .withPageSize(4 * 1024)
                .build()
        ) {
            for (int i = 0; i < numRows; i++) {
                Group g = factory.newGroup();
                String v = valueFor.apply(i);
                if (v != null) {
                    g.append("name", v);
                }
                writer.write(g);
            }
        }
        return out.toByteArray();
    }

    @SuppressWarnings("unused")
    private static StorageObject storageObject(byte[] data) {
        return new StorageObject() {
            @Override
            public void attachMetrics(ExternalSourceMetrics metrics, String scheme) {}

            @Override
            public String version() {
                return "v1";
            }

            @Override
            public StorageIdentity storageIdentity() {
                return AbstractTestStorageObject.NOOP;
            }

            @Override
            public InputStream newStream() {
                return new ByteArrayInputStream(data);
            }

            @Override
            public InputStream newStream(long position, long length) {
                return new ByteArrayInputStream(data, (int) position, (int) Math.min(length, data.length - position));
            }

            @Override
            public long length() {
                return data.length;
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
                return StoragePath.of("memory://dict-cache-test.parquet");
            }
        };
    }

    private static OutputFile outputFile(ByteArrayOutputStream out) {
        return new OutputFile() {
            @Override
            public PositionOutputStream create(long blockSizeHint) {
                return new PositionOutputStream() {
                    private long position = 0;

                    @Override
                    public long getPos() {
                        return position;
                    }

                    @Override
                    public void write(int b) throws IOException {
                        out.write(b);
                        position++;
                    }

                    @Override
                    public void write(byte[] b, int off, int len) throws IOException {
                        out.write(b, off, len);
                        position += len;
                    }

                    @Override
                    public void close() throws IOException {
                        out.close();
                    }
                };
            }

            @Override
            public PositionOutputStream createOrOverwrite(long blockSizeHint) {
                return create(blockSizeHint);
            }

            @Override
            public boolean supportsBlockSize() {
                return false;
            }

            @Override
            public long defaultBlockSize() {
                return 0;
            }

            @Override
            public String getPath() {
                return "memory://dict-cache-test.parquet";
            }
        };
    }
}
