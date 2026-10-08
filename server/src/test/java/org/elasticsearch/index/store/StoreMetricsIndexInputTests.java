/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.index.store;

import org.apache.lucene.store.Directory;
import org.apache.lucene.store.IOContext;
import org.apache.lucene.store.IndexInput;
import org.apache.lucene.store.IndexOutput;
import org.apache.lucene.store.MMapDirectory;
import org.apache.lucene.store.RandomAccessInput;
import org.elasticsearch.core.CheckedConsumer;
import org.elasticsearch.core.DirectAccessInput;
import org.elasticsearch.lucene.store.IndexInputUtils;
import org.elasticsearch.test.ESTestCase;
import org.hamcrest.Matchers;

import java.io.IOException;
import java.lang.foreign.Arena;
import java.lang.foreign.MemorySegment;
import java.lang.foreign.ValueLayout;
import java.util.Arrays;

import static java.lang.foreign.ValueLayout.ADDRESS;
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

    public void testCreateLeavesSelfAccountingInputsUnwrapped() {
        PluggableDirectoryMetricsHolder<StoreMetrics> metricHolder = new ThreadLocalDirectoryMetricHolder<>(StoreMetrics::new);
        IndexInput selfAccounting = mock(IndexInput.class, withSettings().extraInterfaces(SelfAccountingIndexInput.class));

        IndexInput created = StoreMetricsIndexInput.create("test", selfAccounting, metricHolder);

        assertThat(created, Matchers.sameInstance(selfAccounting));
        verify((SelfAccountingIndexInput) selfAccounting).accountBytesReadTo(metricHolder);
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

    // Verifies that withMemorySegmentSlice returns false when the wrapped input implements neither DirectAccessInput nor
    // MemorySegmentAccessInput.
    public void testWithByteBufferSliceReturnsFalseWhenInnerIsNotDAI() throws IOException {
        PluggableDirectoryMetricsHolder<StoreMetrics> metricHolder = new ThreadLocalDirectoryMetricHolder<>(StoreMetrics::new);
        IndexInput mockInput = mock(IndexInput.class);
        IndexInput decorated = StoreMetricsIndexInput.create("test", mockInput, metricHolder);

        assertThat(decorated, Matchers.instanceOf(DirectAccessInput.class));
        assertFalse(((DirectAccessInput) decorated).withMemorySegmentSlice(0L, 10L, ms -> fail("action should not be called")));
    }

    // Verifies that the bulk withSliceAddresses delegates to the wrapped input when it implements DirectAccessInput.
    @SuppressWarnings("unchecked")
    public void testWithSliceAddressesDelegatesToDAI() throws IOException {
        PluggableDirectoryMetricsHolder<StoreMetrics> metricHolder = new ThreadLocalDirectoryMetricHolder<>(StoreMetrics::new);
        IndexInput mockInput = mock(IndexInput.class, withSettings().extraInterfaces(DirectAccessInput.class));
        when(((DirectAccessInput) mockInput).withSliceAddresses(any(), anyInt(), anyInt(), any(), any())).thenReturn(true);

        IndexInput decorated = StoreMetricsIndexInput.create("test", mockInput, metricHolder);
        CheckedConsumer<MemorySegment, IOException> action = addrs -> {};
        long[] offsets = { 0L, 100L, 200L };
        MemorySegment addrsOut = MemorySegment.ofArray(new long[3]);
        assertTrue(((DirectAccessInput) decorated).withSliceAddresses(offsets, 64, 3, addrsOut, action));
        verify((DirectAccessInput) mockInput).withSliceAddresses(eq(offsets), eq(64), eq(3), eq(addrsOut), eq(action));
    }

    // Verifies that the bulk withSliceAddresses returns false when the wrapped input does not implement DirectAccessInput.
    public void testWithSliceAddressesReturnsFalseWhenInnerIsNotDAI() throws IOException {
        PluggableDirectoryMetricsHolder<StoreMetrics> metricHolder = new ThreadLocalDirectoryMetricHolder<>(StoreMetrics::new);
        IndexInput mockInput = mock(IndexInput.class);
        IndexInput decorated = StoreMetricsIndexInput.create("test", mockInput, metricHolder);

        MemorySegment addrsOut = MemorySegment.ofArray(new long[1]);
        assertFalse(
            ((DirectAccessInput) decorated).withSliceAddresses(
                new long[] { 0L },
                10,
                1,
                addrsOut,
                addrs -> fail("action should not be called")
            )
        );
    }

    // Verifies that an mmap'd file wrapped for store metrics still hands out zero-copy segment slices. Readers such as the
    // ColumNAR zstd chunk codec go through IndexInputUtils#withSlice, which only sees the wrapper, and copy the bytes to the
    // heap when no slice is offered.
    public void testWithSliceOnMMapInputDoesNotCopy() throws IOException {
        byte[] data = randomByteArrayOfLength(randomIntBetween(64, 4096));
        try (Directory dir = new MMapDirectory(createTempDir())) {
            writeFile(dir, "data", data);
            PluggableDirectoryMetricsHolder<StoreMetrics> metricHolder = new ThreadLocalDirectoryMetricHolder<>(StoreMetrics::new);
            try (IndexInput in = StoreMetricsIndexInput.create("data", dir.openInput("data", IOContext.DEFAULT), metricHolder)) {
                assertThat(in, Matchers.instanceOf(StoreMetricsIndexInput.class));
                int offset = randomIntBetween(0, data.length / 2);
                int length = randomIntBetween(1, data.length - offset);
                in.seek(offset);

                byte[] read = IndexInputUtils.withSlice(in, length, len -> {
                    throw new AssertionError("copied to a heap buffer instead of using a segment slice");
                }, segment -> segment.toArray(ValueLayout.JAVA_BYTE));

                assertArrayEquals(Arrays.copyOfRange(data, offset, offset + length), read);
                assertEquals(offset + length, in.getFilePointer());
                assertEquals(length, metricHolder.instance().getBytesRead());
            }
        }
    }

    // Verifies that slice offsets are relative to a sliced input, not to the underlying file.
    public void testWithSliceOnSlicedMMapInputDoesNotCopy() throws IOException {
        byte[] data = randomByteArrayOfLength(randomIntBetween(128, 4096));
        try (Directory dir = new MMapDirectory(createTempDir())) {
            writeFile(dir, "data", data);
            PluggableDirectoryMetricsHolder<StoreMetrics> metricHolder = new ThreadLocalDirectoryMetricHolder<>(StoreMetrics::new);
            try (IndexInput file = StoreMetricsIndexInput.create("data", dir.openInput("data", IOContext.DEFAULT), metricHolder)) {
                int sliceOffset = randomIntBetween(1, data.length / 2);
                int sliceLength = randomIntBetween(2, data.length - sliceOffset);
                IndexInput in = file.slice("slice", sliceOffset, sliceLength);
                int offset = randomIntBetween(0, sliceLength / 2);
                int length = randomIntBetween(1, sliceLength - offset);
                in.seek(offset);

                byte[] read = IndexInputUtils.withSlice(in, length, len -> {
                    throw new AssertionError("copied to a heap buffer instead of using a segment slice");
                }, segment -> segment.toArray(ValueLayout.JAVA_BYTE));

                int start = sliceOffset + offset;
                assertArrayEquals(Arrays.copyOfRange(data, start, start + length), read);
                assertEquals(offset + length, in.getFilePointer());
                assertEquals(length, metricHolder.instance().getBytesRead());
            }
        }
    }

    // Verifies that an mmap'd file wrapped for store metrics resolves native addresses for the bulk withSliceAddresses path,
    // and that offsets are relative to a sliced input. Vector scorers normally unwrap the input first, but the wrapper
    // should not report "unavailable" for an input that can serve the request.
    public void testWithSliceAddressesOnMMapInputResolvesAddresses() throws IOException {
        byte[] data = randomByteArrayOfLength(512);
        try (Directory dir = new MMapDirectory(createTempDir())) {
            writeFile(dir, "data", data);
            PluggableDirectoryMetricsHolder<StoreMetrics> metricHolder = new ThreadLocalDirectoryMetricHolder<>(StoreMetrics::new);
            try (
                IndexInput file = StoreMetricsIndexInput.create("data", dir.openInput("data", IOContext.DEFAULT), metricHolder);
                Arena arena = Arena.ofConfined()
            ) {
                int sliceOffset = randomIntBetween(0, 64);
                IndexInput in = randomBoolean() ? file : file.slice("slice", sliceOffset, data.length - sliceOffset);
                int base = in == file ? 0 : sliceOffset;
                long[] offsets = { 0L, 100L, 200L };
                int length = 32;
                MemorySegment addrs = arena.allocate(3 * ADDRESS.byteSize(), ADDRESS.byteAlignment());

                boolean resolved = ((DirectAccessInput) in).withSliceAddresses(offsets, length, 3, addrs, resolvedAddrs -> {
                    for (int i = 0; i < offsets.length; i++) {
                        MemorySegment range = resolvedAddrs.getAtIndex(ADDRESS, i).reinterpret(length);
                        byte[] expected = Arrays.copyOfRange(data, base + (int) offsets[i], base + (int) offsets[i] + length);
                        assertArrayEquals(expected, range.toArray(ValueLayout.JAVA_BYTE));
                    }
                });

                assertTrue(resolved);
            }
        }
    }

    // Verifies that when the mmap cannot offer a contiguous segment (the range crosses a mmap chunk boundary) the wrapper
    // reports that, and that the copying fallback still reads the right bytes and counts them exactly once.
    public void testWithSliceOnMMapInputFallsBackToCopyWhenRangeCrossesChunks() throws IOException {
        byte[] data = randomByteArrayOfLength(256);
        // 32 byte mmap chunks, so a 64 byte range starting at 16 spans several of them.
        try (Directory dir = new MMapDirectory(createTempDir(), 32)) {
            writeFile(dir, "data", data);
            PluggableDirectoryMetricsHolder<StoreMetrics> metricHolder = new ThreadLocalDirectoryMetricHolder<>(StoreMetrics::new);
            try (IndexInput in = StoreMetricsIndexInput.create("data", dir.openInput("data", IOContext.DEFAULT), metricHolder)) {
                assertFalse(((DirectAccessInput) in).withMemorySegmentSlice(16, 64, segment -> fail("action should not be called")));
                assertEquals(0, metricHolder.instance().getBytesRead());

                in.seek(16);
                boolean[] copied = new boolean[1];
                byte[] read = IndexInputUtils.withSlice(in, 64, len -> {
                    copied[0] = true;
                    return new byte[len];
                }, segment -> segment.toArray(ValueLayout.JAVA_BYTE));

                assertTrue(copied[0]);
                assertArrayEquals(Arrays.copyOfRange(data, 16, 16 + 64), read);
                assertEquals(64, metricHolder.instance().getBytesRead());
            }
        }
    }

    private static void writeFile(Directory dir, String name, byte[] data) throws IOException {
        try (IndexOutput out = dir.createOutput(name, IOContext.DEFAULT)) {
            out.writeBytes(data, data.length);
        }
    }
}
