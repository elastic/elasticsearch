/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.parquet.repository;

import org.elasticsearch.blobcache.BlobCacheMetrics;
import org.elasticsearch.blobcache.shared.SharedBlobCacheService;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.env.Environment;
import org.elasticsearch.env.NodeEnvironment;
import org.elasticsearch.env.TestEnvironment;
import org.elasticsearch.node.Node;
import org.elasticsearch.telemetry.metric.MeterRegistry;
import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.threadpool.TestThreadPool;
import org.elasticsearch.threadpool.ThreadPool;
import org.elasticsearch.xpack.esql.datasources.spi.AbstractTestStorageObject;
import org.elasticsearch.xpack.esql.datasources.spi.StorageObject;
import org.elasticsearch.xpack.esql.datasources.spi.StoragePath;
import org.junit.After;
import org.junit.Before;

import java.io.IOException;
import java.io.InputStream;
import java.nio.ByteBuffer;
import java.time.Instant;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;

import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.greaterThan;

/**
 * Tests {@link ParquetBlobCache} and {@link CachedStorageObject} on a real cache file in the data path of a node environment, with a
 * fake file that counts the requests that reach it, so that the tests can tell a read from the cache from a read of the file.
 */
public class ParquetBlobCacheTests extends ESTestCase {

    private static final int REGION_BYTES = 1 << 20;

    private ThreadPool threadPool;
    private NodeEnvironment environment;
    private ParquetBlobCache cache;

    @Before
    public void createCache() throws IOException {
        Settings settings = Settings.builder()
            .put(Node.NODE_NAME_SETTING.getKey(), "node")
            .put("path.home", createTempDir())
            .putList(Environment.PATH_DATA_SETTING.getKey(), createTempDir().toString())
            .put(ParquetBlobCache.CACHE_SIZE.getKey(), "16mb")
            .put(ParquetBlobCache.CACHE_REGION_SIZE.getKey(), REGION_BYTES + "b")
            .build();
        threadPool = new TestThreadPool(getTestName(), ParquetBlobCache.fetchExecutorBuilder(settings));
        environment = new NodeEnvironment(settings, TestEnvironment.newEnvironment(settings));
        cache = new ParquetBlobCache(environment, settings, threadPool, new BlobCacheMetrics(MeterRegistry.NOOP, threadPool));
    }

    @After
    public void closeCache() {
        cache.close();
        environment.close();
        ThreadPool.terminate(threadPool, 10, TimeUnit.SECONDS);
    }

    /** A file in memory that counts the requests that reach it. */
    private static final class CountingFile extends AbstractTestStorageObject {
        private final String name;
        private final byte[] bytes;
        final AtomicInteger requests = new AtomicInteger();

        CountingFile(String name, byte[] bytes) {
            this.name = name;
            this.bytes = bytes;
        }

        @Override
        public InputStream newStream() {
            return newStream(0, READ_TO_END);
        }

        @Override
        public InputStream newStream(long position, long length) {
            requests.incrementAndGet();
            long end = length == READ_TO_END ? bytes.length : Math.min(bytes.length, position + length);
            return new java.io.ByteArrayInputStream(bytes, (int) position, (int) (end - position));
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
            return StoragePath.of("memory://" + name);
        }
    }

    public void testIsEnabledOnlyWithASize() {
        assertFalse(ParquetBlobCache.isEnabled(Settings.EMPTY));
        assertTrue(ParquetBlobCache.isEnabled(Settings.builder().put(ParquetBlobCache.CACHE_SIZE.getKey(), "1mb").build()));
    }

    public void testRangesAreReadFromTheCacheTheSecondTime() throws IOException {
        byte[] bytes = randomByteArrayOfLength(3_500_000);
        CountingFile file = new CountingFile("ranges", bytes);
        CachedStorageObject cached = new CachedStorageObject(file, cache);
        List<long[]> ranges = new ArrayList<>();
        for (int i = 0; i < 20; i++) {
            // not aligned to a page on purpose, and long enough to cross the border of a region
            long position = randomLongBetween(0, bytes.length - 1);
            long length = randomLongBetween(1, Math.min(2_000_000, bytes.length - position));
            ranges.add(new long[] { position, length });
        }
        for (long[] range : ranges) {
            assertRange(bytes, cached, range[0], (int) range[1]);
        }
        int requestsAfterFirst = file.requests.get();
        long writtenAfterFirst = cache.stats().writeBytes();
        assertThat("the first reads fetch from the file", requestsAfterFirst, greaterThan(0));
        assertThat("the first reads fill the cache", writtenAfterFirst, greaterThan(0L));
        for (long[] range : ranges) {
            assertRange(bytes, cached, range[0], (int) range[1]);
        }
        assertThat("the second reads do not reach the file", file.requests.get(), equalTo(requestsAfterFirst));
        assertThat("the second reads do not write the cache", cache.stats().writeBytes(), equalTo(writtenAfterFirst));
    }

    private static void assertRange(byte[] bytes, CachedStorageObject cached, long position, int length) throws IOException {
        ByteBuffer heap = ByteBuffer.allocate(length);
        assertThat(cached.readBytes(position, heap), equalTo(length));
        assertArrayEquals(Arrays.copyOfRange(bytes, (int) position, (int) position + length), heap.array());
    }

    public void testAReadIntoADirectBufferAndAReadPastTheEnd() throws IOException {
        byte[] bytes = randomByteArrayOfLength(randomIntBetween(10_000, 200_000));
        CachedStorageObject cached = new CachedStorageObject(new CountingFile("direct", bytes), cache);
        ByteBuffer direct = ByteBuffer.allocateDirect(5_000);
        int position = randomIntBetween(0, bytes.length - 5_000);
        assertThat(cached.readBytes(position, direct), equalTo(5_000));
        byte[] read = new byte[5_000];
        direct.flip().get(read);
        assertArrayEquals(Arrays.copyOfRange(bytes, position, position + 5_000), read);
        // a buffer that is longer than what is left gets what is left, and a read at the end gets -1
        ByteBuffer tail = ByteBuffer.allocate(10_000);
        assertThat(cached.readBytes(bytes.length - 100, tail), equalTo(100));
        assertThat(cached.readBytes(bytes.length, ByteBuffer.allocate(10)), equalTo(-1));
        assertThat(cached.readBytes(position, ByteBuffer.allocate(0)), equalTo(0));
    }

    public void testAStreamReturnsTheBytesOfItsRange() throws IOException {
        byte[] bytes = randomByteArrayOfLength(3_000_000);
        CountingFile file = new CountingFile("stream", bytes);
        CachedStorageObject cached = new CachedStorageObject(file, cache);
        for (int i = 0; i < 10; i++) {
            long position = randomLongBetween(0, bytes.length - 1);
            long length = randomBoolean() ? StorageObject.READ_TO_END : randomLongBetween(0, bytes.length - position + 1000);
            long end = length == StorageObject.READ_TO_END ? bytes.length : Math.min(bytes.length, position + length);
            try (InputStream stream = cached.newStream(position, length)) {
                assertArrayEquals(Arrays.copyOfRange(bytes, (int) position, (int) end), stream.readAllBytes());
            }
        }
        try (InputStream stream = cached.newStream()) {
            assertArrayEquals(bytes, stream.readAllBytes());
        }
        try (InputStream stream = cached.newStream(bytes.length, 10)) {
            assertThat(stream.read(), equalTo(-1));
        }
        expectThrows(IOException.class, () -> cached.newStream(-1, 10));
        expectThrows(IOException.class, () -> cached.newStream(bytes.length + 1, 10));
    }

    public void testTheMetadataIsThatOfTheWrappedFile() throws IOException {
        CountingFile file = new CountingFile("metadata", randomByteArrayOfLength(1_000));
        CachedStorageObject cached = new CachedStorageObject(file, cache);
        assertThat(cached.length(), equalTo(1_000L));
        assertThat(cached.path(), equalTo(file.path()));
        assertThat(cached.version(), equalTo(file.version()));
        assertTrue(cached.exists());
        assertThat(cached.lastModified(), equalTo(Instant.EPOCH));
    }

    public void testTwoFilesWithTheSamePathAndDifferentLengthsDoNotShareBytes() throws IOException {
        byte[] first = randomByteArrayOfLength(100_000);
        byte[] second = randomByteArrayOfLength(100_001);
        // the same path, as when a file is replaced: the length is part of the key, so the new file never reads the old bytes
        CachedStorageObject one = new CachedStorageObject(new CountingFile("same", first), cache);
        CachedStorageObject other = new CachedStorageObject(new CountingFile("same", second), cache);
        assertRange(first, one, 10, 90_000);
        assertRange(second, other, 10, 90_000);
    }

    public void testConcurrentReadersGetTheirBytes() throws Exception {
        byte[] bytes = randomByteArrayOfLength(4_000_000);
        CachedStorageObject cached = new CachedStorageObject(new CountingFile("concurrent", bytes), cache);
        AtomicReference<Throwable> failure = new AtomicReference<>();
        Thread[] threads = new Thread[8];
        for (int t = 0; t < threads.length; t++) {
            long seed = randomLong();
            threads[t] = new Thread(() -> {
                java.util.Random random = new java.util.Random(seed);
                try {
                    for (int i = 0; i < 30; i++) {
                        long position = random.nextInt(bytes.length - 1);
                        int length = 1 + random.nextInt((int) Math.min(1_500_000, bytes.length - position));
                        assertRange(bytes, cached, position, length);
                    }
                } catch (Throwable e) {
                    failure.compareAndSet(null, e);
                }
            });
            threads[t].start();
        }
        for (Thread thread : threads) {
            thread.join();
        }
        if (failure.get() != null) {
            throw new AssertionError("a reader got wrong bytes or failed", failure.get());
        }
    }

    public void testTheCacheNeverHoldsMoreThanItsSize() throws IOException {
        // more bytes than the 16 MB of the cache, so regions are evicted and the reads must still return the right bytes
        byte[] bytes = randomByteArrayOfLength(24_000_000);
        CachedStorageObject cached = new CachedStorageObject(new CountingFile("big", bytes), cache);
        for (int i = 0; i < 12; i++) {
            long position = randomLongBetween(0, bytes.length - 1_000_000);
            assertRange(bytes, cached, position, 1_000_000);
        }
        SharedBlobCacheService.Stats stats = cache.stats();
        assertThat(stats.writeBytes(), greaterThan(0L));
    }
}
