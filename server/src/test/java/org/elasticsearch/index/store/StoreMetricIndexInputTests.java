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
import org.elasticsearch.test.ESTestCase;

import java.io.IOException;

import static org.mockito.ArgumentMatchers.anyLong;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

public class StoreMetricIndexInputTests extends ESTestCase {

    public void testReadByteUpdatesMetrics() throws Exception {
        MetricHolder<StoreMetrics> metricHolder = new ThreadLocalMetricHolder<>(StoreMetrics::new);
        StoreMetricIndexInput indexInput = new StoreMetricIndexInput("test", mock(IndexInput.class), metricHolder);

        assertEquals(0, metricHolder.instance().getBytesRead());
        indexInput.readByte();
        assertEquals(1, metricHolder.instance().getBytesRead());
        indexInput.readByte();
        assertEquals(2, metricHolder.instance().getBytesRead());
        indexInput.readBytes(new byte[1024], 0, 1024);
        assertEquals(1026, metricHolder.instance().getBytesRead());
    }

    public void testSnapshotMetricBeforeUsageSnapshotDoesNotChange() throws IOException {
        MetricHolder<StoreMetrics> metricHolder = new ThreadLocalMetricHolder<>(StoreMetrics::new);
        var snapshot = metricHolder.instance().snapshot();
        StoreMetricIndexInput indexInput = new StoreMetricIndexInput("test", mock(IndexInput.class), metricHolder);

        assertEquals(0, metricHolder.instance().getBytesRead());
        assertEquals(0, snapshot.getBytesRead());
        indexInput.readBytes(new byte[1024], 0, 1024);
        assertEquals(1024, metricHolder.instance().getBytesRead());
        assertEquals(0, snapshot.getBytesRead());
    }

    public void testThreadIsolationOnMetrics() throws Exception {
        MetricHolder<StoreMetrics> metricHolder = new ThreadLocalMetricHolder<>(StoreMetrics::new);
        StoreMetricIndexInput indexInput = new StoreMetricIndexInput("test", mock(IndexInput.class), metricHolder);

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
        MetricHolder<StoreMetrics> metricHolder = new ThreadLocalMetricHolder<>(StoreMetrics::new);
        IndexInput mockIndexInput = mock(IndexInput.class);
        when(mockIndexInput.clone()).thenReturn(mockIndexInput);
        when(mockIndexInput.slice(anyString(), anyLong(), anyLong())).thenReturn(mockIndexInput);
        StoreMetricIndexInput indexInput = new StoreMetricIndexInput("test", mockIndexInput, metricHolder);

        try {
            IndexInput sliceInput = indexInput.slice("slice", 0, 100);
            assertNotNull(sliceInput);
            assertTrue(sliceInput instanceof StoreMetricIndexInput);
            StoreMetricIndexInput storeMetricSlice = (StoreMetricIndexInput) sliceInput;

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
        MetricHolder<StoreMetrics> metricHolder = new ThreadLocalMetricHolder<>(StoreMetrics::new);
        IndexInput mockIndexInput = mock(IndexInput.class);
        RandomAccessInput mockRandomAccessInput = mock(RandomAccessInput.class);
        when(mockIndexInput.randomAccessSlice(anyLong(), anyLong())).thenReturn(mockRandomAccessInput);
        StoreMetricIndexInput indexInput = new StoreMetricIndexInput("test", mockIndexInput, metricHolder);

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
        MetricHolder<StoreMetrics> metricHolder = new ThreadLocalMetricHolder<>(StoreMetrics::new);
        IndexInput mockIndexInput = mock(IndexInput.class);
        RandomAccessInput mockRandomAccessInput = mock(RandomAccessInput.class);
        when(mockIndexInput.randomAccessSlice(anyLong(), anyLong())).thenReturn(mockRandomAccessInput);
        StoreMetricIndexInput indexInput = new StoreMetricIndexInput("test", mockIndexInput, metricHolder);

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
}
