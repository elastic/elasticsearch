/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.stateless.cache.reader;

import org.apache.lucene.store.IOContext;
import org.elasticsearch.blobcache.BlobCacheMetrics;
import org.elasticsearch.index.store.PluggableDirectoryMetricsHolder;
import org.elasticsearch.index.store.StoreMetrics;
import org.elasticsearch.index.store.ThreadLocalDirectoryMetricHolder;
import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.xpack.stateless.cache.StatelessSharedBlobCacheService;
import org.junit.Before;

import java.io.IOException;
import java.lang.foreign.MemorySegment;
import java.nio.ByteBuffer;

import static org.elasticsearch.xpack.stateless.commits.BlobLocationTestUtils.createBlobFileRanges;
import static org.hamcrest.Matchers.equalTo;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyInt;
import static org.mockito.ArgumentMatchers.anyLong;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

/**
 * Tests that a reader accounts the bytes it reads from the cache, whether it hit or had to fetch them.
 */
public class CacheFileReaderStoreMetricsTests extends ESTestCase {

    private static final int REGION_SIZE = 16 * 1024 * 1024;
    private static final long FILE_LENGTH = 4096;

    private StatelessSharedBlobCacheService.CacheFile cacheFile;
    private ThreadLocalDirectoryMetricHolder<StoreMetrics> holder;

    @Before
    @SuppressWarnings("unchecked")
    public void createReaderDependencies() {
        cacheFile = mock(StatelessSharedBlobCacheService.CacheFile.class);
        holder = new ThreadLocalDirectoryMetricHolder<>(StoreMetrics::new);
    }

    private long bytesRead() {
        return holder.instance().getBytesRead();
    }

    private CacheFileReader newReader(PluggableDirectoryMetricsHolder<StoreMetrics> storeMetrics) {
        var reader = new CacheFileReader(
            cacheFile,
            mock(CacheBlobReader.class),
            createBlobFileRanges(1L, 0L, 0, FILE_LENGTH),
            BlobCacheMetrics.NOOP,
            System::currentTimeMillis,
            REGION_SIZE,
            IOContext.DEFAULT,
            true,
            true
        );
        reader.accountBytesReadTo(storeMetrics);
        return reader;
    }

    public void testFastPathReadAccountsWhatItRead() throws IOException {
        when(cacheFile.tryRead(any(), anyLong())).thenReturn(true);
        when(cacheFile.tryRead(any(), anyLong(), anyInt())).thenReturn(true);

        CacheFileReader reader = newReader(holder.singleThreaded());
        assertTrue(reader.tryRead(ByteBuffer.allocate(1024), 0L));

        assertThat(bytesRead(), equalTo(1024L));
    }

    public void testFastPathAccountsNothingWhenItDoesNotRead() throws IOException {
        when(cacheFile.tryRead(any(), anyLong())).thenReturn(false);
        when(cacheFile.tryRead(any(), anyLong(), anyInt())).thenReturn(false);

        CacheFileReader reader = newReader(holder.singleThreaded());
        assertFalse(reader.tryRead(ByteBuffer.allocate(1024), 0L));

        assertThat("the caller falls back to the slow path, which accounts instead", bytesRead(), equalTo(0L));
    }

    public void testDirectMemoryAccessIsAccounted() throws IOException {
        when(cacheFile.withMemorySegmentSlice(anyLong(), anyInt(), any())).thenReturn(true);
        when(cacheFile.withMemorySegmentSlice(anyLong(), anyInt(), any(), anyInt())).thenReturn(true);

        CacheFileReader reader = newReader(holder.singleThreaded());
        assertTrue(reader.withMemorySegmentSlice(0L, 512, segment -> {}));

        assertThat(bytesRead(), equalTo(512L));
    }

    public void testBulkDirectMemoryAccessAccountsEverySlice() throws IOException {
        when(cacheFile.withSliceAddresses(any(), anyInt(), anyInt(), any(), any())).thenReturn(true);
        when(cacheFile.withSliceAddresses(any(), anyInt(), anyInt(), any(), any(), anyInt())).thenReturn(true);

        CacheFileReader reader = newReader(holder.singleThreaded());
        assertTrue(reader.withSliceAddresses(new long[] { 0L, 64L, 128L }, 64, 3, MemorySegment.NULL, segment -> {}));

        assertThat(bytesRead(), equalTo(3L * 64));
    }

    public void testCopiesAccountToTheSameHolder() throws IOException {
        when(cacheFile.copy()).thenReturn(cacheFile);
        when(cacheFile.tryRead(any(), anyLong())).thenReturn(true);
        when(cacheFile.tryRead(any(), anyLong(), anyInt())).thenReturn(true);

        CacheFileReader reader = newReader(holder.singleThreaded());
        assertTrue(reader.copy().tryRead(ByteBuffer.allocate(1024), 0L));

        assertThat("clones and slices of an input reuse the reader's holder", bytesRead(), equalTo(1024L));
    }

    public void testCopiesAccountToTheThreadThatReads() throws Exception {
        when(cacheFile.copy()).thenReturn(cacheFile);
        when(cacheFile.tryRead(any(), anyLong())).thenReturn(true);
        when(cacheFile.tryRead(any(), anyLong(), anyInt())).thenReturn(true);

        CacheFileReader reader = newReader(holder.singleThreaded());
        assertTrue(reader.tryRead(ByteBuffer.allocate(1024), 0L));
        assertThat(bytesRead(), equalTo(1024L));

        // a copy is what an input hands out from clone() and slice(), and may be read from another thread
        CacheFileReader copy = reader.copy();
        Thread otherThread = new Thread(() -> {
            try {
                assertThat(bytesRead(), equalTo(0L));
                assertTrue(copy.tryRead(ByteBuffer.allocate(512), 0L));
                assertThat(bytesRead(), equalTo(512L));
            } catch (IOException e) {
                fail("IOException thrown in other thread: " + e.getMessage());
            }
        });
        otherThread.start();
        otherThread.join();

        assertThat("the reads of the other thread are accounted to it", bytesRead(), equalTo(1024L));
    }

    public void testAccountsNothingWithoutAHolder() throws IOException {
        when(cacheFile.tryRead(any(), anyLong())).thenReturn(true);
        when(cacheFile.tryRead(any(), anyLong(), anyInt())).thenReturn(true);

        CacheFileReader reader = newReader(StoreMetrics.NOOP_HOLDER);
        assertTrue(reader.tryRead(ByteBuffer.allocate(1024), 0L));

        assertThat(bytesRead(), equalTo(0L));
    }
}
