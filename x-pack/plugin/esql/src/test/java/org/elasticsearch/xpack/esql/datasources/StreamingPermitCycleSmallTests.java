/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.datasources;

import org.elasticsearch.common.breaker.NoopCircuitBreaker;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.util.BigArrays;
import org.elasticsearch.compute.data.BlockFactory;
import org.elasticsearch.compute.data.Page;
import org.elasticsearch.compute.operator.CloseableIterator;
import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.xpack.esql.core.expression.Attribute;
import org.elasticsearch.xpack.esql.core.expression.ReferenceAttribute;
import org.elasticsearch.xpack.esql.core.tree.Source;
import org.elasticsearch.xpack.esql.core.type.DataType;
import org.elasticsearch.xpack.esql.datasource.gzip.GzipDecompressionCodec;
import org.elasticsearch.xpack.esql.datasource.ndjson.NdJsonFormatReader;
import org.elasticsearch.xpack.esql.datasources.spi.AbstractTestStorageObject;
import org.elasticsearch.xpack.esql.datasources.spi.ErrorPolicy;
import org.elasticsearch.xpack.esql.datasources.spi.SegmentableFormatReader;
import org.elasticsearch.xpack.esql.datasources.spi.StorageObject;
import org.elasticsearch.xpack.esql.datasources.spi.StoragePath;
import org.elasticsearch.xpack.esql.datasources.spi.StripeColumnScope;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.nio.charset.StandardCharsets;
import java.time.Instant;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.zip.GZIPOutputStream;

/**
 * Real-thread regression for the gzip/text permit×pool deadlock: opening the stream on the I/O
 * open-thread acquired a storage permit, then queued the segmentator behind remaining opens on the
 * same FIFO. Waiters occupied the pool; holders could not run. Lazy open after admission holds a
 * permit only on a thread that can finish the stream.
 */
public class StreamingPermitCycleSmallTests extends ESTestCase {

    private static final BlockFactory BLOCK_FACTORY = BlockFactory.builder(BigArrays.NON_RECYCLING_INSTANCE)
        .breaker(NoopCircuitBreaker.INSTANCE)
        .build();
    private static final List<Attribute> SCHEMA = List.of(new ReferenceAttribute(Source.EMPTY, "a", DataType.INTEGER));
    private static final int POOL_SIZE = 2;
    private static final int PERMITS = 2;
    private static final int FILE_COUNT = 8;
    private static final int ROWS_PER_FILE = 4;
    private static final long LIMITER_TIMEOUT_MS = 200L;

    public void testEightGzipStreamsCompleteUnderTinyLimiterBudget() throws Exception {
        byte[] gzipped = gzipCompress(("{\"a\":1}\n".repeat(ROWS_PER_FILE)).getBytes(StandardCharsets.UTF_8));
        ConcurrencyLimiter limiter = new ConcurrencyLimiter(
            "s3",
            new ExternalSourceSettings.BlobStoreConcurrency(PERMITS, false),
            LIMITER_TIMEOUT_MS
        );
        QueryConcurrencyBudget budget = new QueryConcurrencyBudget(PERMITS, LIMITER_TIMEOUT_MS, null);
        StreamingSegmentatorAdmission admission = new StreamingSegmentatorAdmission(POOL_SIZE - 1);
        ExecutorService ioPool = Executors.newFixedThreadPool(POOL_SIZE);
        ExecutorService drainPool = Executors.newFixedThreadPool(FILE_COUNT);
        CountDownLatch done = new CountDownLatch(FILE_COUNT);
        List<Throwable> failures = new CopyOnWriteArrayList<>();
        AtomicInteger totalRows = new AtomicInteger();
        long startNanos = System.nanoTime();
        try {
            for (int i = 0; i < FILE_COUNT; i++) {
                final int idx = i;
                ioPool.execute(() -> {
                    try {
                        StorageObject wrapped = productionWrap(bytesObject(gzipped, "s3://bucket/f" + idx + ".ndjson.gz"), budget, limiter);
                        DecompressingStorageObject decompressing = new DecompressingStorageObject(wrapped, new GzipDecompressionCodec());
                        CloseableIterator<Page> iterator = parallelRead(decompressing, ioPool, admission);
                        drainPool.execute(() -> {
                            try {
                                totalRows.addAndGet(drain(iterator));
                            } catch (Throwable t) {
                                failures.add(t);
                            } finally {
                                done.countDown();
                            }
                        });
                    } catch (Throwable t) {
                        failures.add(t);
                        done.countDown();
                    }
                });
            }
            assertTrue(
                "all iterators must complete in < 3s (permit×pool cycle parks until limiter timeout)",
                done.await(3, TimeUnit.SECONDS)
            );
            assertTrue("drain failures: " + failures, failures.isEmpty());
            // Consumers can finish before the outer admission wrapper releases its slot in finally.
            // Join the submitted work before checking ownership, within the original completion bound.
            ioPool.shutdown();
            drainPool.shutdown();
            assertTrue(
                "I/O tasks must finish within the 3s completion bound",
                ioPool.awaitTermination(Math.max(0L, TimeUnit.SECONDS.toNanos(3) - (System.nanoTime() - startNanos)), TimeUnit.NANOSECONDS)
            );
            assertTrue(
                "consumers must finish within the 3s completion bound",
                drainPool.awaitTermination(
                    Math.max(0L, TimeUnit.SECONDS.toNanos(3) - (System.nanoTime() - startNanos)),
                    TimeUnit.NANOSECONDS
                )
            );
            assertEquals(FILE_COUNT * ROWS_PER_FILE, totalRows.get());
            assertEquals(PERMITS, limiter.availablePermits());
            assertEquals(0, budget.inFlight());
            assertEquals(0, admission.running());
            assertEquals(0, admission.pending());
        } finally {
            ioPool.shutdownNow();
            drainPool.shutdownNow();
            ioPool.awaitTermination(3, TimeUnit.SECONDS);
            drainPool.awaitTermination(3, TimeUnit.SECONDS);
        }
        assertTrue(
            "cycle-free completion must be well under the 3s bound",
            TimeUnit.NANOSECONDS.toMillis(System.nanoTime() - startNanos) < 3_000
        );
    }

    public void testCloseOfNeverPromotedIteratorIsPromptAndHoldsNoPermit() throws Exception {
        byte[] gzipped = gzipCompress("{\"a\":1}\n".getBytes(StandardCharsets.UTF_8));
        ConcurrencyLimiter limiter = new ConcurrencyLimiter(
            "s3",
            new ExternalSourceSettings.BlobStoreConcurrency(PERMITS, false),
            LIMITER_TIMEOUT_MS
        );
        QueryConcurrencyBudget budget = new QueryConcurrencyBudget(PERMITS, LIMITER_TIMEOUT_MS, null);
        StreamingSegmentatorAdmission admission = new StreamingSegmentatorAdmission(1);
        ExecutorService ioPool = Executors.newFixedThreadPool(POOL_SIZE);
        CountDownLatch firstAcquired = new CountDownLatch(1);
        CountDownLatch holdFirst = new CountDownLatch(1);
        List<CloseableIterator<Page>> iterators = new ArrayList<>();
        try {
            StorageObject blockingRaw = new AbstractTestStorageObject() {
                @Override
                public InputStream newStream() throws IOException {
                    firstAcquired.countDown();
                    try {
                        holdFirst.await();
                    } catch (InterruptedException e) {
                        Thread.currentThread().interrupt();
                        throw new IOException("interrupted holding first stream", e);
                    }
                    return new ByteArrayInputStream(gzipped);
                }

                @Override
                public InputStream newStream(long position, long length) {
                    throw new UnsupportedOperationException();
                }

                @Override
                public long length() {
                    return gzipped.length;
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
                    return StoragePath.of("s3://bucket/blocking.ndjson.gz");
                }
            };
            StorageObject blocking = productionWrap(blockingRaw, budget, limiter);
            DecompressingStorageObject blockingDecomp = new DecompressingStorageObject(blocking, new GzipDecompressionCodec());
            iterators.add(parallelRead(blockingDecomp, ioPool, admission));
            assertTrue(firstAcquired.await(5, TimeUnit.SECONDS));
            assertEquals(1, limiter.availablePermits());
            assertEquals(1, budget.inFlight());

            for (int i = 1; i < FILE_COUNT; i++) {
                StorageObject wrapped = productionWrap(bytesObject(gzipped, "s3://bucket/p" + i + ".ndjson.gz"), budget, limiter);
                DecompressingStorageObject decomp = new DecompressingStorageObject(wrapped, new GzipDecompressionCodec());
                iterators.add(parallelRead(decomp, ioPool, admission));
            }
            assertEquals(FILE_COUNT - 1, admission.pending());

            for (int i = 1; i < iterators.size(); i++) {
                long startNanos = System.nanoTime();
                iterators.get(i).close();
                assertTrue(
                    "close of a never-promoted iterator must return in < 1s",
                    TimeUnit.NANOSECONDS.toMillis(System.nanoTime() - startNanos) < 1_000
                );
            }
            assertEquals(0, admission.pending());
            assertEquals("cancelled pending work must hold no permit", 1, limiter.availablePermits());
            assertEquals(1, budget.inFlight());

            holdFirst.countDown();
            iterators.get(0).close();
            assertBusy(() -> {
                assertEquals(PERMITS, limiter.availablePermits());
                assertEquals(0, budget.inFlight());
                assertEquals(0, admission.running());
            });
        } finally {
            holdFirst.countDown();
            for (CloseableIterator<Page> it : iterators) {
                try {
                    it.close();
                } catch (IOException ignored) {}
            }
            ioPool.shutdownNow();
        }
    }

    private static CloseableIterator<Page> parallelRead(
        DecompressingStorageObject decompressing,
        ExecutorService ioPool,
        StreamingSegmentatorAdmission admission
    ) throws IOException {
        NdJsonFormatReader reader = new NdJsonFormatReader(Settings.EMPTY, BLOCK_FACTORY, SCHEMA);
        return StreamingParallelParsingCoordinator.parallelRead(
            reader,
            decompressing::newStream,
            decompressing,
            List.of("a"),
            50,
            4,
            ioPool,
            ErrorPolicy.STRICT,
            SCHEMA,
            0L,
            SegmentableFormatReader.DEFAULT_MAX_RECORD_BYTES,
            null,
            -1L,
            StripeColumnScope.PROJECTED,
            StreamingParallelParsingCoordinator.WarningSinks.NONE,
            admission,
            NoopCircuitBreaker.INSTANCE,
            ExternalReadCounters.NOOP,
            null
        );
    }

    private static int drain(CloseableIterator<Page> iterator) throws IOException {
        int rows = 0;
        try (iterator) {
            while (iterator.hasNext()) {
                Page page = iterator.next();
                rows += page.getPositionCount();
                page.releaseBlocks();
            }
        }
        return rows;
    }

    private static StorageObject productionWrap(StorageObject raw, QueryConcurrencyBudget budget, ConcurrencyLimiter limiter) {
        return new QueryBudgetedStorageObject(
            new RetryableStorageObject(new ConcurrencyLimitedStorageObject(raw, limiter), RetryPolicy.NONE),
            budget
        );
    }

    private static StorageObject bytesObject(byte[] data, String path) {
        return new AbstractTestStorageObject() {
            @Override
            public InputStream newStream() {
                return new ByteArrayInputStream(data);
            }

            @Override
            public InputStream newStream(long position, long length) {
                throw new UnsupportedOperationException();
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
                return StoragePath.of(path);
            }
        };
    }

    private static byte[] gzipCompress(byte[] uncompressed) throws IOException {
        ByteArrayOutputStream bos = new ByteArrayOutputStream();
        try (GZIPOutputStream gz = new GZIPOutputStream(bos)) {
            gz.write(uncompressed);
        }
        return bos.toByteArray();
    }
}
