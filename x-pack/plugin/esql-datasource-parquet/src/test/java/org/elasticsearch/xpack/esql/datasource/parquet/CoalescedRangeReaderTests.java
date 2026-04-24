/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.datasource.parquet;

import org.elasticsearch.action.ActionListener;
import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.xpack.esql.datasource.parquet.CoalescedRangeReader.ByteRange;
import org.elasticsearch.xpack.esql.datasource.parquet.CoalescedRangeReader.MergedRange;
import org.elasticsearch.xpack.esql.datasources.spi.StorageObject;
import org.elasticsearch.xpack.esql.datasources.spi.StoragePath;

import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.nio.ByteBuffer;
import java.time.Instant;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.Executor;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;

public class CoalescedRangeReaderTests extends ESTestCase {

    public void testMergeAdjacentRanges() {
        List<ByteRange> ranges = List.of(new ByteRange(0, 100), new ByteRange(100, 200), new ByteRange(300, 50));
        // gap=0 means adjacent ranges (gap==0) are merged; all three are contiguous
        List<MergedRange> merged = CoalescedRangeReader.mergeRanges(ranges, 0);
        assertEquals(1, merged.size());
        assertEquals(0, merged.get(0).offset());
        assertEquals(350, merged.get(0).length());
        assertEquals(3, merged.get(0).constituents().size());
    }

    public void testMergeOverlappingRanges() {
        List<ByteRange> ranges = List.of(new ByteRange(0, 150), new ByteRange(100, 200));
        List<MergedRange> merged = CoalescedRangeReader.mergeRanges(ranges, 0);
        assertEquals(1, merged.size());
        assertEquals(0, merged.get(0).offset());
        assertEquals(300, merged.get(0).length());
    }

    public void testMergeWithGapBelowThreshold() {
        List<ByteRange> ranges = List.of(new ByteRange(0, 100), new ByteRange(200, 100));
        List<MergedRange> merged = CoalescedRangeReader.mergeRanges(ranges, 200);
        assertEquals(1, merged.size());
        assertEquals(0, merged.get(0).offset());
        assertEquals(300, merged.get(0).length());
    }

    public void testMergeWithGapAboveThreshold() {
        List<ByteRange> ranges = List.of(new ByteRange(0, 100), new ByteRange(200, 100));
        List<MergedRange> merged = CoalescedRangeReader.mergeRanges(ranges, 50);
        assertEquals(2, merged.size());
    }

    public void testMergeSingleRange() {
        List<ByteRange> ranges = List.of(new ByteRange(500, 200));
        List<MergedRange> merged = CoalescedRangeReader.mergeRanges(ranges, 1024);
        assertEquals(1, merged.size());
        assertEquals(500, merged.get(0).offset());
        assertEquals(200, merged.get(0).length());
    }

    public void testMergeUnsortedRanges() {
        List<ByteRange> ranges = List.of(new ByteRange(300, 50), new ByteRange(0, 100), new ByteRange(100, 200));
        // All three are contiguous [0,100) [100,300) [300,350) -> one merged range
        List<MergedRange> merged = CoalescedRangeReader.mergeRanges(ranges, 0);
        assertEquals(1, merged.size());
        assertEquals(0, merged.get(0).offset());
        assertEquals(350, merged.get(0).length());
    }

    public void testReadCoalescedParallelDispatch() throws Exception {
        byte[] data = new byte[1024];
        for (int i = 0; i < data.length; i++) {
            data[i] = (byte) (i & 0xFF);
        }

        AtomicInteger asyncCallCount = new AtomicInteger();
        StorageObject storageObject = new StorageObject() {
            @Override
            public InputStream newStream() {
                return new ByteArrayInputStream(data);
            }

            @Override
            public InputStream newStream(long position, long length) {
                return new ByteArrayInputStream(data, (int) position, (int) length);
            }

            @Override
            public long length() {
                return data.length;
            }

            @Override
            public Instant lastModified() {
                return Instant.now();
            }

            @Override
            public boolean exists() {
                return true;
            }

            @Override
            public StoragePath path() {
                return StoragePath.of("memory://test.parquet");
            }

            @Override
            public void readBytesAsync(long position, long length, Executor executor, ActionListener<ByteBuffer> listener) {
                asyncCallCount.incrementAndGet();
                StorageObject.super.readBytesAsync(position, length, executor, listener);
            }
        };

        // [0,100) and [100,300) are adjacent; [500,600) has gap=200 which exceeds maxCoalesceGap=50
        List<ByteRange> ranges = List.of(new ByteRange(0, 100), new ByteRange(100, 200), new ByteRange(500, 100));

        CountDownLatch latch = new CountDownLatch(1);
        AtomicReference<Map<ByteRange, ByteBuffer>> resultRef = new AtomicReference<>();
        AtomicReference<Exception> failureRef = new AtomicReference<>();

        CoalescedRangeReader.readCoalesced(storageObject, ranges, 50, Runnable::run, new ActionListener<>() {
            @Override
            public void onResponse(Map<ByteRange, ByteBuffer> result) {
                resultRef.set(result);
                latch.countDown();
            }

            @Override
            public void onFailure(Exception e) {
                failureRef.set(e);
                latch.countDown();
            }
        });

        assertTrue(latch.await(5, TimeUnit.SECONDS));
        assertNull(failureRef.get());

        Map<ByteRange, ByteBuffer> results = resultRef.get();
        assertNotNull(results);
        assertEquals(3, results.size());

        // Adjacent ranges [0,100) and [100,300) should be merged into one async call
        // Range [500,600) is separate -> 2 async calls total
        assertEquals(2, asyncCallCount.get());

        ByteBuffer buf0 = results.get(new ByteRange(0, 100));
        assertNotNull(buf0);
        assertEquals(100, buf0.remaining());
        assertEquals((byte) 0, buf0.get(0));

        ByteBuffer buf1 = results.get(new ByteRange(100, 200));
        assertNotNull(buf1);
        assertEquals(200, buf1.remaining());
        assertEquals((byte) 100, buf1.get(0));

        ByteBuffer buf2 = results.get(new ByteRange(500, 100));
        assertNotNull(buf2);
        assertEquals(100, buf2.remaining());
        assertEquals((byte) (500 & 0xFF), buf2.get(0));
    }

    public void testReadCoalescedEmptyRanges() throws Exception {
        CountDownLatch latch = new CountDownLatch(1);
        AtomicReference<Map<ByteRange, ByteBuffer>> resultRef = new AtomicReference<>();

        // null StorageObject is safe here: the empty-ranges path returns before any I/O
        CoalescedRangeReader.readCoalesced(null, List.of(), 0, Runnable::run, new ActionListener<>() {
            @Override
            public void onResponse(Map<ByteRange, ByteBuffer> result) {
                resultRef.set(result);
                latch.countDown();
            }

            @Override
            public void onFailure(Exception e) {
                latch.countDown();
            }
        });

        assertTrue(latch.await(5, TimeUnit.SECONDS));
        assertNotNull(resultRef.get());
        assertTrue(resultRef.get().isEmpty());
    }

    public void testReadCoalescedFailure() throws Exception {
        StorageObject failingObject = new StorageObject() {
            @Override
            public InputStream newStream() throws IOException {
                throw new IOException("test failure");
            }

            @Override
            public InputStream newStream(long position, long length) throws IOException {
                throw new IOException("test failure");
            }

            @Override
            public long length() {
                return 1000;
            }

            @Override
            public Instant lastModified() {
                return Instant.now();
            }

            @Override
            public boolean exists() {
                return true;
            }

            @Override
            public StoragePath path() {
                return StoragePath.of("memory://fail.parquet");
            }
        };

        CountDownLatch latch = new CountDownLatch(1);
        AtomicReference<Exception> failureRef = new AtomicReference<>();

        CoalescedRangeReader.readCoalesced(failingObject, List.of(new ByteRange(0, 100)), 0, Runnable::run, new ActionListener<>() {
            @Override
            public void onResponse(Map<ByteRange, ByteBuffer> result) {
                latch.countDown();
            }

            @Override
            public void onFailure(Exception e) {
                failureRef.set(e);
                latch.countDown();
            }
        });

        assertTrue(latch.await(5, TimeUnit.SECONDS));
        assertNotNull(failureRef.get());
        assertThat(failureRef.get().getMessage(), org.hamcrest.Matchers.containsString("test failure"));
    }
}
