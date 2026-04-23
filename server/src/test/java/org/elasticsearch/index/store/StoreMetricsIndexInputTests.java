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
import java.nio.ByteBuffer;

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

    public void testThreadIsolationOnMetrics() throws Exception {
        PluggableDirectoryMetricsHolder<StoreMetrics> metricHolder = new ThreadLocalDirectoryMetricHolder<>(StoreMetrics::new);
        IndexInput indexInput = StoreMetricsIndexInput.create("test", mock(IndexInput.class), metricHolder);

        assertEquals(0, metricHolder.instance().getBytesRead());
        indexInput.readByte();
        assertEquals(1, metricHolder.instance().getBytesRead());

        Thread otherThread = new Thread(() -> {
            try {
                assertEquals(0, metricHolder.instance().getBytesRead());
                indexInput.readBytes(new byte[512], 0, 512);
                assertEquals(512, metricHolder.instance().getBytesRead());
            } catch (IOException e) {
                fail("IOException thrown in other thread: " + e.getMessage());
            }
        });

        otherThread.start();
        otherThread.join();

        // Back in the original thread, metrics should be unchanged
        assertEquals(1, metricHolder.instance().getBytesRead());
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

    public void testRandomAccessInputReadyThreadIsolation() throws Exception {
        PluggableDirectoryMetricsHolder<StoreMetrics> metricHolder = new ThreadLocalDirectoryMetricHolder<>(StoreMetrics::new);
        IndexInput mockIndexInput = mock(IndexInput.class);
        RandomAccessInput mockRandomAccessInput = mock(RandomAccessInput.class);
        when(mockIndexInput.randomAccessSlice(anyLong(), anyLong())).thenReturn(mockRandomAccessInput);
        IndexInput indexInput = StoreMetricsIndexInput.create("test", mockIndexInput, metricHolder);

        RandomAccessInput randomAccessInput = indexInput.randomAccessSlice(0, 1000);

        assertEquals(0, metricHolder.instance().getBytesRead());
        randomAccessInput.readByte(0);
        assertEquals(1, metricHolder.instance().getBytesRead());

        Thread otherThread = new Thread(() -> {
            try {
                assertEquals(0, metricHolder.instance().getBytesRead());
                randomAccessInput.readLong(0);
                assertEquals(8, metricHolder.instance().getBytesRead());
            } catch (IOException e) {
                fail("IOException thrown in other thread: " + e.getMessage());
            }
        });

        otherThread.start();
        otherThread.join();

        // Back in the original thread, metrics should be unchanged
        assertEquals(1, metricHolder.instance().getBytesRead());
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

    // Verifies that withByteBufferSlice delegates to the wrapped input when it implements DirectAccessInput.
    @SuppressWarnings("unchecked")
    public void testWithByteBufferSliceDelegatesToDAI() throws IOException {
        PluggableDirectoryMetricsHolder<StoreMetrics> metricHolder = new ThreadLocalDirectoryMetricHolder<>(StoreMetrics::new);
        IndexInput mockInput = mock(IndexInput.class, withSettings().extraInterfaces(DirectAccessInput.class));
        when(((DirectAccessInput) mockInput).withByteBufferSlice(anyLong(), anyLong(), any())).thenReturn(true);

        IndexInput decorated = StoreMetricsIndexInput.create("test", mockInput, metricHolder);
        assertThat(decorated, Matchers.instanceOf(DirectAccessInput.class));

        CheckedConsumer<ByteBuffer, IOException> action = bb -> {};
        assertTrue(((DirectAccessInput) decorated).withByteBufferSlice(42L, 128L, action));
        verify((DirectAccessInput) mockInput).withByteBufferSlice(eq(42L), eq(128L), eq(action));
    }

    // Verifies that withByteBufferSlice returns false when the wrapped input does not implement DirectAccessInput.
    public void testWithByteBufferSliceReturnsFalseWhenInnerIsNotDAI() throws IOException {
        PluggableDirectoryMetricsHolder<StoreMetrics> metricHolder = new ThreadLocalDirectoryMetricHolder<>(StoreMetrics::new);
        IndexInput mockInput = mock(IndexInput.class);
        IndexInput decorated = StoreMetricsIndexInput.create("test", mockInput, metricHolder);

        assertThat(decorated, Matchers.instanceOf(DirectAccessInput.class));
        assertFalse(((DirectAccessInput) decorated).withByteBufferSlice(0L, 10L, bb -> fail("action should not be called")));
    }

    // Verifies that the bulk withByteBufferSlices delegates to the wrapped input when it implements DirectAccessInput.
    @SuppressWarnings("unchecked")
    public void testWithByteBufferSlicesDelegatesToDAI() throws IOException {
        PluggableDirectoryMetricsHolder<StoreMetrics> metricHolder = new ThreadLocalDirectoryMetricHolder<>(StoreMetrics::new);
        IndexInput mockInput = mock(IndexInput.class, withSettings().extraInterfaces(DirectAccessInput.class));
        when(((DirectAccessInput) mockInput).withByteBufferSlices(any(), anyInt(), anyInt(), any())).thenReturn(true);

        IndexInput decorated = StoreMetricsIndexInput.create("test", mockInput, metricHolder);
        CheckedConsumer<ByteBuffer[], IOException> action = bbs -> {};
        long[] offsets = { 0L, 100L, 200L };
        assertTrue(((DirectAccessInput) decorated).withByteBufferSlices(offsets, 64, 3, action));
        verify((DirectAccessInput) mockInput).withByteBufferSlices(eq(offsets), eq(64), eq(3), eq(action));
    }

    // Verifies that the bulk withByteBufferSlices returns false when the wrapped input does not implement DirectAccessInput.
    public void testWithByteBufferSlicesReturnsFalseWhenInnerIsNotDAI() throws IOException {
        PluggableDirectoryMetricsHolder<StoreMetrics> metricHolder = new ThreadLocalDirectoryMetricHolder<>(StoreMetrics::new);
        IndexInput mockInput = mock(IndexInput.class);
        IndexInput decorated = StoreMetricsIndexInput.create("test", mockInput, metricHolder);

        assertFalse(
            ((DirectAccessInput) decorated).withByteBufferSlices(new long[] { 0L }, 10, 1, bbs -> fail("action should not be called"))
        );
    }
}
