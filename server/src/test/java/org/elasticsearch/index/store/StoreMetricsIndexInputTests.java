/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.index.store;

import org.apache.lucene.store.IndexInput;
import org.apache.lucene.store.RandomAccessInput;
import org.elasticsearch.core.CheckedConsumer;
import org.elasticsearch.core.DirectAccessInput;
import org.elasticsearch.test.ESTestCase;
import org.hamcrest.Matchers;

import java.io.IOException;
import java.lang.foreign.MemorySegment;

import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyInt;
import static org.mockito.ArgumentMatchers.anyLong;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;
import static org.mockito.Mockito.withSettings;

public class StoreMetricsIndexInputTests extends ESTestCase {

    public void testReadByteUpdatesMetrics() throws Exception {
        PluggableDirectoryMetricsHolder<StoreMetrics> metricHolder = new ThreadLocalDirectoryMetricHolder<>(StoreMetrics::new);
        IndexInput indexInput = StoreMetricsIndexInput.create("test", mock(IndexInput.class), metricHolder);

        assertEquals(0, metricHolder.instance().getBytesRead());
        indexInput.readByte();
        assertEquals(1, metricHolder.instance().getBytesRead());
        indexInput.readByte();
        assertEquals(2, metricHolder.instance().getBytesRead());
        indexInput.readBytes(new byte[1024], 0, 1024);
        assertEquals(1026, metricHolder.instance().getBytesRead());
    }

    public void testCopyMetricBeforeUsageCopyDoesNotChange() throws IOException {
        PluggableDirectoryMetricsHolder<StoreMetrics> metricHolder = new ThreadLocalDirectoryMetricHolder<>(StoreMetrics::new);
        var snapshot = metricHolder.instance().copy();
        IndexInput indexInput = StoreMetricsIndexInput.create("test", mock(IndexInput.class), metricHolder);

        assertEquals(0, metricHolder.instance().getBytesRead());
        assertEquals(0, snapshot.getBytesRead());
        indexInput.readBytes(new byte[1024], 0, 1024);
        assertEquals(1024, metricHolder.instance().getBytesRead());
        assertEquals(0, snapshot.getBytesRead());
    }

    public void testEachInputInstanceAttributedToFirstUseThread() throws Exception {
        // Models the normal search path: each task has its own input instance (from clone/slice).
        // With sticky cachedMetrics, per-task attribution is still correct because cachedMetrics on
        // each instance is set on its first-use thread.
        PluggableDirectoryMetricsHolder<StoreMetrics> metricHolder = new ThreadLocalDirectoryMetricHolder<>(StoreMetrics::new);
        IndexInput input1 = StoreMetricsIndexInput.create("test1", mock(IndexInput.class), metricHolder);
        IndexInput input2 = StoreMetricsIndexInput.create("test2", mock(IndexInput.class), metricHolder);

        input1.readBytes(new byte[100], 0, 100);
        assertEquals(100, metricHolder.instance().getBytesRead());

        Thread other = new Thread(() -> {
            try {
                assertEquals(0, metricHolder.instance().getBytesRead());
                input2.readBytes(new byte[200], 0, 200);
                assertEquals(200, metricHolder.instance().getBytesRead());
            } catch (IOException e) {
                fail("IOException in other thread: " + e.getMessage());
            }
        });
        other.start();
        other.join();

        assertEquals(100, metricHolder.instance().getBytesRead());
    }

    public void testHandoffAttributionIsStickyToFirstThread() throws Exception {
        // Documents the intentional contract tightening: attribution is sticky to the first-use
        // thread, not the current thread on every read. An input that is first-read on thread A
        // and then handed off to thread B accumulates all bytes in thread A's StoreMetrics.
        // Re-checking Thread.currentThread() on every read to restore rebinding would erase the
        // performance win.
        PluggableDirectoryMetricsHolder<StoreMetrics> metricHolder = new ThreadLocalDirectoryMetricHolder<>(StoreMetrics::new);
        IndexInput indexInput = StoreMetricsIndexInput.create("test", mock(IndexInput.class), metricHolder);

        // First read on main thread — sets cachedMetrics to main thread's StoreMetrics.
        indexInput.readByte();
        assertEquals(1, metricHolder.instance().getBytesRead());

        Thread other = new Thread(() -> {
            try {
                // cachedMetrics already set; bytes go to main thread's StoreMetrics.
                indexInput.readBytes(new byte[512], 0, 512);
                assertEquals(0, metricHolder.instance().getBytesRead());
            } catch (IOException e) {
                fail("IOException in other thread: " + e.getMessage());
            }
        });
        other.start();
        other.join();

        assertEquals(513, metricHolder.instance().getBytesRead());
    }

    public void testSliceMetrics() throws IOException {
        PluggableDirectoryMetricsHolder<StoreMetrics> metricHolder = new ThreadLocalDirectoryMetricHolder<>(StoreMetrics::new);
        IndexInput mockIndexInput = mock(IndexInput.class);
        when(mockIndexInput.clone()).thenReturn(mockIndexInput);
        when(mockIndexInput.slice(anyString(), anyLong(), anyLong())).thenReturn(mockIndexInput);
        IndexInput indexInput = StoreMetricsIndexInput.create("test", mockIndexInput, metricHolder);

        try {
            IndexInput sliceInput = indexInput.slice("slice", 0, 100);
            assertNotNull(sliceInput);
            assertTrue(sliceInput instanceof StoreMetricsIndexInput);
            StoreMetricsIndexInput storeMetricSlice = (StoreMetricsIndexInput) sliceInput;

            assertEquals(0, metricHolder.instance().getBytesRead());
            storeMetricSlice.readByte();
            assertEquals(1, metricHolder.instance().getBytesRead());
            storeMetricSlice.readBytes(new byte[256], 0, 256);
            assertEquals(257, metricHolder.instance().getBytesRead());
        } catch (IOException e) {
            fail("IOException thrown during slice metrics test: " + e.getMessage());
        }
    }

    public void testRandomAccessInputReadPrimitiveTypes() throws IOException {
        PluggableDirectoryMetricsHolder<StoreMetrics> metricHolder = new ThreadLocalDirectoryMetricHolder<>(StoreMetrics::new);
        IndexInput mockIndexInput = mock(IndexInput.class);
        RandomAccessInput mockRandomAccessInput = mock(RandomAccessInput.class);
        when(mockIndexInput.randomAccessSlice(anyLong(), anyLong())).thenReturn(mockRandomAccessInput);
        IndexInput indexInput = StoreMetricsIndexInput.create("test", mockIndexInput, metricHolder);

        RandomAccessInput randomAccessInput = indexInput.randomAccessSlice(0, 1000);

        assertEquals(0, metricHolder.instance().getBytesRead());
        randomAccessInput.readByte(0);
        assertEquals(1, metricHolder.instance().getBytesRead());
        randomAccessInput.readShort(0);
        assertEquals(3, metricHolder.instance().getBytesRead());
        randomAccessInput.readInt(0);
        assertEquals(7, metricHolder.instance().getBytesRead());
        randomAccessInput.readLong(0);
        assertEquals(15, metricHolder.instance().getBytesRead());
    }

    public void testEachRandomAccessInputAttributedToFirstUseThread() throws Exception {
        // Models the DocValues path on MMapDirectory: each getNumeric() call gets its own
        // randomAccessSlice() wrapper (a RandomAccessIndexInput), used on its own thread.
        // Per-task attribution is correct because cachedMetrics on each slice is set on first use.
        PluggableDirectoryMetricsHolder<StoreMetrics> metricHolder = new ThreadLocalDirectoryMetricHolder<>(StoreMetrics::new);
        IndexInput mockIndexInput = mock(IndexInput.class);
        // Slice implements both IndexInput and RandomAccessInput — takes the RandomAccessIndexInput path.
        IndexInput mockSlice = mock(IndexInput.class, withSettings().extraInterfaces(RandomAccessInput.class));
        when(mockIndexInput.randomAccessSlice(anyLong(), anyLong())).thenReturn((RandomAccessInput) mockSlice);
        IndexInput indexInput = StoreMetricsIndexInput.create("test", mockIndexInput, metricHolder);

        RandomAccessInput slice1 = indexInput.randomAccessSlice(0, 1000);
        slice1.readByte(0);
        assertEquals(1, metricHolder.instance().getBytesRead());

        RandomAccessInput slice2 = indexInput.randomAccessSlice(0, 1000);
        Thread other = new Thread(() -> {
            try {
                assertEquals(0, metricHolder.instance().getBytesRead());
                slice2.readLong(0);
                assertEquals(8, metricHolder.instance().getBytesRead());
            } catch (IOException e) {
                fail("IOException in other thread: " + e.getMessage());
            }
        });
        other.start();
        other.join();

        assertEquals(1, metricHolder.instance().getBytesRead());
    }

    public void testRandomAccessHandoffAttributionIsSticky() throws Exception {
        // Sticky-attribution counterpart for RandomAccessIndexInput: a slice first-read on thread A
        // accumulates all subsequent reads (including from thread B) into thread A's StoreMetrics.
        PluggableDirectoryMetricsHolder<StoreMetrics> metricHolder = new ThreadLocalDirectoryMetricHolder<>(StoreMetrics::new);
        IndexInput mockIndexInput = mock(IndexInput.class);
        // Slice implements both IndexInput and RandomAccessInput — takes the RandomAccessIndexInput path.
        IndexInput mockSlice = mock(IndexInput.class, withSettings().extraInterfaces(RandomAccessInput.class));
        when(mockIndexInput.randomAccessSlice(anyLong(), anyLong())).thenReturn((RandomAccessInput) mockSlice);
        IndexInput indexInput = StoreMetricsIndexInput.create("test", mockIndexInput, metricHolder);

        RandomAccessInput randomAccessInput = indexInput.randomAccessSlice(0, 1000);

        // First read on main thread — sets cachedMetrics to main thread's StoreMetrics.
        randomAccessInput.readByte(0);
        assertEquals(1, metricHolder.instance().getBytesRead());

        Thread other = new Thread(() -> {
            try {
                randomAccessInput.readLong(0);
                assertEquals(0, metricHolder.instance().getBytesRead());
            } catch (IOException e) {
                fail("IOException in other thread: " + e.getMessage());
            }
        });
        other.start();
        other.join();

        assertEquals(9, metricHolder.instance().getBytesRead());
    }

    public void testRandomAccessIndexInputReadBytes() throws IOException {
        PluggableDirectoryMetricsHolder<StoreMetrics> metricHolder = new ThreadLocalDirectoryMetricHolder<>(StoreMetrics::new);
        IndexInput mockIndexInput = mock(IndexInput.class, withSettings().extraInterfaces(RandomAccessInput.class));
        IndexInput indexInput = StoreMetricsIndexInput.create("test", mockIndexInput, metricHolder);

        assertThat(indexInput, Matchers.instanceOf(RandomAccessInput.class));
        RandomAccessInput randomAccessInput = (RandomAccessInput) indexInput;

        int length = randomIntBetween(1, 128);
        byte[] result = new byte[length];
        randomAccessInput.readBytes(10, result, 0, length);

        verify((RandomAccessInput) mockIndexInput).readBytes(10, result, 0, length);
        verify((RandomAccessInput) mockIndexInput, never()).readByte(anyLong());
        assertEquals(length, metricHolder.instance().getBytesRead());
    }

    public void testMetricsRandomAccessInputReadBytes() throws IOException {
        PluggableDirectoryMetricsHolder<StoreMetrics> metricHolder = new ThreadLocalDirectoryMetricHolder<>(StoreMetrics::new);
        IndexInput mockIndexInput = mock(IndexInput.class);
        RandomAccessInput mockRandomAccessInput = mock(RandomAccessInput.class);
        when(mockIndexInput.randomAccessSlice(anyLong(), anyLong())).thenReturn(mockRandomAccessInput);
        IndexInput indexInput = StoreMetricsIndexInput.create("test", mockIndexInput, metricHolder);

        RandomAccessInput randomAccessInput = indexInput.randomAccessSlice(0, 1000);

        int length = randomIntBetween(1, 128);
        byte[] result = new byte[length];
        randomAccessInput.readBytes(10, result, 0, length);

        verify(mockRandomAccessInput).readBytes(10, result, 0, length);
        verify(mockRandomAccessInput, never()).readByte(anyLong());
        assertEquals(length, metricHolder.instance().getBytesRead());
    }

    public void testCreate() {
        PluggableDirectoryMetricsHolder<StoreMetrics> metricHolder = new ThreadLocalDirectoryMetricHolder<>(StoreMetrics::new);
        IndexInput mockIndexInput = mock(IndexInput.class);
        IndexInput decorated = StoreMetricsIndexInput.create("test", mockIndexInput, metricHolder);
        assertThat(decorated, Matchers.not(Matchers.instanceOf(RandomAccessInput.class)));

        IndexInput mockRandomInput = mock(IndexInput.class, withSettings().extraInterfaces(RandomAccessInput.class));
        IndexInput decoratedRandom = StoreMetricsIndexInput.create("test", mockRandomInput, metricHolder);
        assertThat(decoratedRandom, Matchers.instanceOf(RandomAccessInput.class));
    }

    // Verifies that withMemorySegmentSlice delegates to the wrapped input when it implements DirectAccessInput.
    @SuppressWarnings("unchecked")
    public void testWithByteBufferSliceDelegatesToDAI() throws IOException {
        PluggableDirectoryMetricsHolder<StoreMetrics> metricHolder = new ThreadLocalDirectoryMetricHolder<>(StoreMetrics::new);
        IndexInput mockInput = mock(IndexInput.class, withSettings().extraInterfaces(DirectAccessInput.class));
        when(((DirectAccessInput) mockInput).withMemorySegmentSlice(anyLong(), anyLong(), any())).thenReturn(true);

        IndexInput decorated = StoreMetricsIndexInput.create("test", mockInput, metricHolder);
        assertThat(decorated, Matchers.instanceOf(DirectAccessInput.class));

        CheckedConsumer<MemorySegment, IOException> action = ms -> {};
        assertTrue(((DirectAccessInput) decorated).withMemorySegmentSlice(42L, 128L, action));
        verify((DirectAccessInput) mockInput).withMemorySegmentSlice(eq(42L), eq(128L), eq(action));
    }

    // Verifies that withMemorySegmentSlice returns false when the wrapped input does not implement DirectAccessInput.
    public void testWithByteBufferSliceReturnsFalseWhenInnerIsNotDAI() throws IOException {
        PluggableDirectoryMetricsHolder<StoreMetrics> metricHolder = new ThreadLocalDirectoryMetricHolder<>(StoreMetrics::new);
        IndexInput mockInput = mock(IndexInput.class);
        IndexInput decorated = StoreMetricsIndexInput.create("test", mockInput, metricHolder);

        assertThat(decorated, Matchers.instanceOf(DirectAccessInput.class));
        assertFalse(((DirectAccessInput) decorated).withMemorySegmentSlice(0L, 10L, ms -> fail("action should not be called")));
    }

    // Verifies that the bulk withMemorySegmentSlices delegates to the wrapped input when it implements DirectAccessInput.
    @SuppressWarnings("unchecked")
    public void testWithByteBufferSlicesDelegatesToDAI() throws IOException {
        PluggableDirectoryMetricsHolder<StoreMetrics> metricHolder = new ThreadLocalDirectoryMetricHolder<>(StoreMetrics::new);
        IndexInput mockInput = mock(IndexInput.class, withSettings().extraInterfaces(DirectAccessInput.class));
        when(((DirectAccessInput) mockInput).withMemorySegmentSlices(any(), anyInt(), anyInt(), any())).thenReturn(true);

        IndexInput decorated = StoreMetricsIndexInput.create("test", mockInput, metricHolder);
        CheckedConsumer<MemorySegment[], IOException> action = mss -> {};
        long[] offsets = { 0L, 100L, 200L };
        assertTrue(((DirectAccessInput) decorated).withMemorySegmentSlices(offsets, 64, 3, action));
        verify((DirectAccessInput) mockInput).withMemorySegmentSlices(eq(offsets), eq(64), eq(3), eq(action));
    }

    // Verifies that the bulk withMemorySegmentSlices returns false when the wrapped input does not implement DirectAccessInput.
    public void testWithByteBufferSlicesReturnsFalseWhenInnerIsNotDAI() throws IOException {
        PluggableDirectoryMetricsHolder<StoreMetrics> metricHolder = new ThreadLocalDirectoryMetricHolder<>(StoreMetrics::new);
        IndexInput mockInput = mock(IndexInput.class);
        IndexInput decorated = StoreMetricsIndexInput.create("test", mockInput, metricHolder);

        assertFalse(
            ((DirectAccessInput) decorated).withMemorySegmentSlices(new long[] { 0L }, 10, 1, mss -> fail("action should not be called"))
        );
    }

    public void testCachedMetricsUsedOnSubsequentReads() throws Exception {
        // Core regression guard: instance() must be called exactly once (lazy init on first read),
        // not on every read. If addBytesRead reverts to calling metricHolder.instance() directly,
        // instanceCallCount will exceed 1 and this test fails.
        ThreadLocalDirectoryMetricHolder<StoreMetrics> base = new ThreadLocalDirectoryMetricHolder<>(StoreMetrics::new);
        int[] instanceCallCount = { 0 };
        PluggableDirectoryMetricsHolder<StoreMetrics> metricHolder = new PluggableDirectoryMetricsHolder<>() {
            @Override
            public StoreMetrics instance() {
                instanceCallCount[0]++;
                return base.instance();
            }

            @Override
            public PluggableDirectoryMetricsHolder<StoreMetrics> singleThreaded() {
                return base.singleThreaded();
            }
        };
        IndexInput indexInput = StoreMetricsIndexInput.create("test", mock(IndexInput.class), metricHolder);

        for (int i = 0; i < 100; i++) {
            indexInput.readByte();
        }

        assertEquals(1, instanceCallCount[0]);
        assertEquals(100, base.instance().getBytesRead());
    }

    public void testSmallDocValuesReadsAreCountedImmediately() throws IOException {
        // Bytes from a RandomAccessIndexInput (the DocValues path on MMapDirectory) must be counted
        // on every read with no minimum volume. Previously, sub-threshold reads from wrappers that
        // were abandoned without close() were permanently lost.
        PluggableDirectoryMetricsHolder<StoreMetrics> metricHolder = new ThreadLocalDirectoryMetricHolder<>(StoreMetrics::new);
        IndexInput mockIndexInput = mock(IndexInput.class, withSettings().extraInterfaces(RandomAccessInput.class));
        IndexInput indexInput = StoreMetricsIndexInput.create("test", mockIndexInput, metricHolder);

        assertThat(indexInput, Matchers.instanceOf(RandomAccessInput.class));
        RandomAccessInput randomAccessInput = (RandomAccessInput) indexInput;

        // Single readLong without closing — models an abandoned per-acquisition DocValues wrapper.
        randomAccessInput.readLong(0);

        assertEquals(8, metricHolder.instance().getBytesRead());
    }
}
