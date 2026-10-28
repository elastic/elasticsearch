/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.datasource.parquet;

import org.apache.parquet.ParquetReadOptions;
import org.apache.parquet.column.page.PageReadStore;
import org.apache.parquet.conf.PlainParquetConfiguration;
import org.apache.parquet.example.data.Group;
import org.apache.parquet.example.data.simple.SimpleGroupFactory;
import org.apache.parquet.example.data.simple.convert.GroupRecordConverter;
import org.apache.parquet.hadoop.ParquetFileReader;
import org.apache.parquet.hadoop.ParquetWriter;
import org.apache.parquet.hadoop.example.ExampleParquetWriter;
import org.apache.parquet.hadoop.metadata.CompressionCodecName;
import org.apache.parquet.io.ColumnIOFactory;
import org.apache.parquet.io.MessageColumnIO;
import org.apache.parquet.io.OutputFile;
import org.apache.parquet.io.PositionOutputStream;
import org.apache.parquet.io.RecordReader;
import org.apache.parquet.io.SeekableInputStream;
import org.apache.parquet.schema.MessageType;
import org.apache.parquet.schema.PrimitiveType;
import org.apache.parquet.schema.Types;
import org.elasticsearch.action.ActionListener;
import org.elasticsearch.common.breaker.CircuitBreaker;
import org.elasticsearch.common.breaker.CircuitBreakingException;
import org.elasticsearch.common.breaker.NoopCircuitBreaker;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.unit.ByteSizeValue;
import org.elasticsearch.common.util.BigArrays;
import org.elasticsearch.common.util.LimitedBreaker;
import org.elasticsearch.common.util.MockBigArrays;
import org.elasticsearch.common.util.PageCacheRecycler;
import org.elasticsearch.common.util.concurrent.EsRejectedExecutionException;
import org.elasticsearch.compute.data.LocalCircuitBreaker;
import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.xpack.esql.core.QlIllegalArgumentException;
import org.elasticsearch.xpack.esql.datasources.DrainSimulatingStorageObject;
import org.elasticsearch.xpack.esql.datasources.NodeByteBudgetService;
import org.elasticsearch.xpack.esql.datasources.cache.ExternalSourceCacheSettings;
import org.elasticsearch.xpack.esql.datasources.cache.FooterByteCache;
import org.elasticsearch.xpack.esql.datasources.spi.AbstractTestStorageObject;
import org.elasticsearch.xpack.esql.datasources.spi.ExternalSourceMetrics;
import org.elasticsearch.xpack.esql.datasources.spi.HeapFootprint;
import org.elasticsearch.xpack.esql.datasources.spi.NodeByteBudget;
import org.elasticsearch.xpack.esql.datasources.spi.RowGroupIo;
import org.elasticsearch.xpack.esql.datasources.spi.StorageIdentity;
import org.elasticsearch.xpack.esql.datasources.spi.StorageIoAffinity;
import org.elasticsearch.xpack.esql.datasources.spi.StorageObject;
import org.elasticsearch.xpack.esql.datasources.spi.StoragePath;
import org.hamcrest.Matchers;
import org.junit.Before;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.FilterInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.nio.ByteBuffer;
import java.time.Instant;
import java.util.ArrayList;
import java.util.List;
import java.util.Random;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;

public class ParquetStorageObjectAdapterTests extends ESTestCase {

    /**
     * Footer byte cache handed to every adapter this test constructs. In production the owning
     * format reader supplies its instance; a fresh per-test-class cache gives the same sharing
     * within a test and automatic isolation between tests.
     */
    private final FooterByteCache footerByteCache = FooterByteCache.fromSettings(Settings.EMPTY);

    private CircuitBreaker breaker;

    @Before
    public void initBreaker() {
        breaker = NoopCircuitBreaker.INSTANCE;
    }

    public void testNullStorageObjectThrowsException() {
        QlIllegalArgumentException e = expectThrows(
            QlIllegalArgumentException.class,
            () -> new ParquetStorageObjectAdapter(null, footerByteCache, breaker)
        );
        assertEquals("storageObject cannot be null", e.getMessage());
    }

    public void testGetLength() throws IOException {
        byte[] data = new byte[1024];
        randomBytes(data);
        StorageObject storageObject = createStorageObject(data);

        ParquetStorageObjectAdapter adapter = new ParquetStorageObjectAdapter(storageObject, footerByteCache, breaker);

        assertEquals(1024, adapter.getLength());
    }

    public void testNewStreamReturnsSeekableInputStream() throws IOException {
        byte[] data = new byte[100];
        randomBytes(data);
        StorageObject storageObject = createStorageObject(data);

        ParquetStorageObjectAdapter adapter = new ParquetStorageObjectAdapter(storageObject, footerByteCache, breaker);

        try (SeekableInputStream stream = adapter.newStream()) {
            assertNotNull(stream);
            assertEquals(0, stream.getPos());
        }
    }

    public void testWindowChargesAndReleasesWatermarkBesideRequest() throws IOException {
        byte[] data = new byte[2048];
        randomBytes(data);
        ParquetIoWatermark watermark = new ParquetIoWatermark(64 * 1024 * 1024);
        CircuitBreaker limited = new LimitedBreaker("test", ByteSizeValue.ofMb(16));
        ParquetStorageObjectAdapter adapter = new ParquetStorageObjectAdapter(
            createRangeReadStorageObject(data),
            footerByteCache,
            limited,
            watermark
        );
        assertEquals(0, watermark.used());
        try (SeekableInputStream stream = adapter.newStream()) {
            assertEquals(0, watermark.used());
            assertEquals(0, limited.getUsed());
            byte[] buf = new byte[64];
            stream.readFully(buf);
            assertEquals(HeapFootprint.byteArrayBytes(data.length), watermark.used());
            assertEquals(HeapFootprint.byteArrayBytes(data.length), limited.getUsed());
            stream.readFully(buf);
            assertEquals(HeapFootprint.byteArrayBytes(data.length), watermark.used());
            assertEquals(HeapFootprint.byteArrayBytes(data.length), limited.getUsed());
        }
        assertEquals(0, watermark.used());
        assertEquals(0, limited.getUsed());
    }

    /**
     * Open-time window forceAdd does not open {@link StorageIoAffinity}. Binding the iterator
     * lease afterwards must still trip wait-with-untracked; TLS attribution would miss this floor.
     */
    public void testWindowAllocWithoutAffinityThenAdmitAsyncTrips() throws Exception {
        assertNull(StorageIoAffinity.current());
        byte[] data = new byte[2048];
        randomBytes(data);
        NodeByteBudgetService budget = new NodeByteBudgetService(100);
        ParquetIoWatermark watermark = new ParquetIoWatermark(budget);
        CircuitBreaker limited = new LimitedBreaker("test", ByteSizeValue.ofMb(16));
        ParquetStorageObjectAdapter adapter = new ParquetStorageObjectAdapter(
            createRangeReadStorageObject(data),
            footerByteCache,
            limited,
            watermark
        );
        CountDownLatch granted = new CountDownLatch(1);
        AtomicReference<NodeByteBudget.Hold> owner = new AtomicReference<>();
        budget.admitAsync(150, new RowGroupIo(), () -> false, Runnable::run).addListener(ActionListener.wrap(h -> {
            owner.set(h);
            granted.countDown();
        }, e -> { throw new AssertionError(e); }));
        assertTrue(granted.await(5, TimeUnit.SECONDS));
        try (SeekableInputStream stream = adapter.newStream()) {
            byte[] buf = new byte[64];
            stream.readFully(buf);
            assertNull(StorageIoAffinity.current());
            RowGroupIo lease = new RowGroupIo();
            adapter.bindRowGroup(lease);
            assertThat(lease.untracked(), Matchers.greaterThan(0L));
            CountDownLatch failed = new CountDownLatch(1);
            AtomicReference<Exception> error = new AtomicReference<>();
            budget.admitAsync(5, lease, () -> false, Runnable::run).addListener(ActionListener.wrap(hold -> {
                hold.close();
                fail("waiting lease with untracked window bytes must not grant");
            }, e -> {
                error.set(e);
                failed.countDown();
            }));
            assertTrue(failed.await(5, TimeUnit.SECONDS));
            assertTrue(error.get().getMessage(), error.get().getMessage().contains("untracked bytes"));
        } finally {
            owner.get().close();
            budget.clearOwner(owner.get().lease());
        }
    }

    public void testReleaseIdleWindowsRefundsOnceAndReallocatesOnRead() throws IOException {
        byte[] data = new byte[ParquetStorageObjectAdapter.DEFAULT_WINDOW_SIZE + 1024];
        randomBytes(data);
        LimitedBreaker limited = new LimitedBreaker("test", ByteSizeValue.ofMb(16));
        ParquetIoWatermark watermark = new ParquetIoWatermark(64 * 1024 * 1024);
        ParquetStorageObjectAdapter adapter = new ParquetStorageObjectAdapter(
            createRangeReadStorageObject(data),
            footerByteCache,
            limited,
            watermark
        );
        long windowCharge = HeapFootprint.byteArrayBytes(ParquetStorageObjectAdapter.DEFAULT_WINDOW_SIZE);
        SeekableInputStream stream = adapter.newStream();
        try {
            assertEquals(1, adapter.trackedStreamCount());
            byte[] first = new byte[64];
            stream.readFully(first);
            assertEquals(windowCharge, limited.getUsed());
            assertEquals(windowCharge, watermark.used());
            for (int i = 0; i < first.length; i++) {
                assertEquals(data[i], first[i]);
            }

            adapter.releaseIdleWindows();
            assertEquals(0, limited.getUsed());
            assertEquals(0, watermark.used());
            assertEquals(1, adapter.trackedStreamCount());

            adapter.releaseIdleWindows();
            assertEquals(0, limited.getUsed());
            assertEquals(0, watermark.used());

            stream.seek(4096);
            byte[] afterSeek = new byte[32];
            stream.readFully(afterSeek);
            assertEquals(windowCharge, limited.getUsed());
            assertEquals(windowCharge, watermark.used());
            for (int i = 0; i < afterSeek.length; i++) {
                assertEquals(data[4096 + i], afterSeek[i]);
            }

            adapter.releaseIdleWindows();
            assertEquals(0, limited.getUsed());
            assertEquals(0, watermark.used());

            byte[] second = new byte[64];
            stream.readFully(second);
            assertEquals(windowCharge, limited.getUsed());
            assertEquals(windowCharge, watermark.used());
            for (int i = 0; i < second.length; i++) {
                assertEquals("stale window after release at " + i, data[4096 + 32 + i], second[i]);
            }
        } finally {
            stream.close();
        }
        assertEquals(0, limited.getUsed());
        assertEquals(0, watermark.used());
        assertEquals(0, adapter.trackedStreamCount());
    }

    public void testReleaseIdleWindowsNoopWhenNeverAllocatedAndAfterClose() throws IOException {
        byte[] data = new byte[2048];
        randomBytes(data);
        LimitedBreaker limited = new LimitedBreaker("test", ByteSizeValue.ofMb(16));
        ParquetIoWatermark watermark = new ParquetIoWatermark(64 * 1024 * 1024);
        ParquetStorageObjectAdapter adapter = new ParquetStorageObjectAdapter(
            createRangeReadStorageObject(data),
            footerByteCache,
            limited,
            watermark
        );
        try (SeekableInputStream stream = adapter.newStream()) {
            assertEquals(1, adapter.trackedStreamCount());
            adapter.releaseIdleWindows();
            assertEquals(0, limited.getUsed());
            assertEquals(0, watermark.used());
            assertEquals(1, adapter.trackedStreamCount());
        }
        assertEquals(0, adapter.trackedStreamCount());
        adapter.releaseIdleWindows();
        assertEquals(0, limited.getUsed());
        assertEquals(0, watermark.used());
    }

    public void testCloseAfterReleaseIdleWindowsDoesNotDoubleRefund() throws IOException {
        byte[] data = new byte[ParquetStorageObjectAdapter.DEFAULT_WINDOW_SIZE + 1024];
        randomBytes(data);
        LimitedBreaker limited = new LimitedBreaker("test", ByteSizeValue.ofMb(16));
        ParquetIoWatermark watermark = new ParquetIoWatermark(64 * 1024 * 1024);
        ParquetStorageObjectAdapter adapter = new ParquetStorageObjectAdapter(
            createRangeReadStorageObject(data),
            footerByteCache,
            limited,
            watermark
        );
        try (SeekableInputStream stream = adapter.newStream()) {
            stream.readFully(new byte[8]);
            adapter.releaseIdleWindows();
            assertEquals(0, limited.getUsed());
            assertEquals(0, watermark.used());
        }
        assertEquals(0, limited.getUsed());
        assertEquals(0, watermark.used());
        assertEquals(0, adapter.trackedStreamCount());
    }

    public void testReleaseIdleWindowsReallocCircuitBreaksWithoutWatermarkCharge() throws IOException {
        byte[] data = new byte[ParquetStorageObjectAdapter.DEFAULT_WINDOW_SIZE + 1024];
        randomBytes(data);
        long windowCharge = HeapFootprint.byteArrayBytes(ParquetStorageObjectAdapter.DEFAULT_WINDOW_SIZE);
        LimitedBreaker limited = new LimitedBreaker("test", ByteSizeValue.ofBytes(windowCharge + 1024));
        ParquetIoWatermark watermark = new ParquetIoWatermark(64 * 1024 * 1024);
        ParquetStorageObjectAdapter adapter = new ParquetStorageObjectAdapter(
            createRangeReadStorageObject(data),
            footerByteCache,
            limited,
            watermark
        );
        SeekableInputStream stream = adapter.newStream();
        try {
            stream.readFully(new byte[8]);
            assertEquals(windowCharge, limited.getUsed());
            assertEquals(windowCharge, watermark.used());
            adapter.releaseIdleWindows();
            assertEquals(0, limited.getUsed());
            assertEquals(0, watermark.used());
            limited.addEstimateBytesAndMaybeBreak(windowCharge, "filler");
            expectThrows(CircuitBreakingException.class, () -> stream.readFully(new byte[8]));
            assertEquals(windowCharge, limited.getUsed());
            assertEquals(0, watermark.used());
        } finally {
            stream.close();
        }
        assertEquals(windowCharge, limited.getUsed());
        assertEquals(0, watermark.used());
        assertEquals(0, adapter.trackedStreamCount());
    }

    public void testReleaseIdleWindowsRefundsTwoOpenStreams() throws IOException {
        byte[] data = new byte[ParquetStorageObjectAdapter.DEFAULT_WINDOW_SIZE + 1024];
        randomBytes(data);
        long windowCharge = HeapFootprint.byteArrayBytes(ParquetStorageObjectAdapter.DEFAULT_WINDOW_SIZE);
        LimitedBreaker limited = new LimitedBreaker("test", ByteSizeValue.ofMb(16));
        ParquetIoWatermark watermark = new ParquetIoWatermark(64 * 1024 * 1024);
        ParquetStorageObjectAdapter adapter = new ParquetStorageObjectAdapter(
            createRangeReadStorageObject(data),
            footerByteCache,
            limited,
            watermark
        );
        SeekableInputStream first = adapter.newStream();
        SeekableInputStream second = adapter.newStream();
        try {
            first.readFully(new byte[8]);
            second.readFully(new byte[8]);
            assertEquals(2, adapter.trackedStreamCount());
            assertEquals(2 * windowCharge, limited.getUsed());
            assertEquals(2 * windowCharge, watermark.used());
            adapter.releaseIdleWindows();
            assertEquals(0, limited.getUsed());
            assertEquals(0, watermark.used());
            first.close();
            first = null;
            assertEquals(1, adapter.trackedStreamCount());
            adapter.releaseIdleWindows();
            assertEquals(0, limited.getUsed());
            assertEquals(0, watermark.used());
        } finally {
            if (first != null) {
                first.close();
            }
            second.close();
        }
        assertEquals(0, limited.getUsed());
        assertEquals(0, watermark.used());
        assertEquals(0, adapter.trackedStreamCount());
    }

    /**
     * Incomplete window fill (cancel / exception mid-GET) must {@code abortStream} the range
     * rather than {@code close()}, which on S3 would drain the remaining 4–16 MiB.
     */
    public void testIncompleteWindowFillAbortsRatherThanDrains() throws IOException {
        byte[] data = new byte[ParquetStorageObjectAdapter.DEFAULT_WINDOW_SIZE + 1024];
        randomBytes(data);
        DrainSimulatingStorageObject.Tracking tracking = new DrainSimulatingStorageObject.Tracking();
        StorageObject raw = DrainSimulatingStorageObject.create(data, tracking);
        StorageObject failing = incompleteWindowStorage(raw);

        ParquetStorageObjectAdapter adapter = new ParquetStorageObjectAdapter(failing, footerByteCache, breaker);
        try (SeekableInputStream stream = adapter.newStream()) {
            expectThrows(IOException.class, () -> stream.readFully(new byte[1024]));
        }

        assertTrue("incomplete window fill must abort the range GET", tracking.aborted.get());
        assertThat(
            "incomplete fill must not drain the window; consumed " + tracking.bytesConsumed.get() + " of " + data.length,
            tracking.bytesConsumed.get(),
            Matchers.lessThan((long) data.length / 2)
        );
    }

    /**
     * A fully-read window must still {@code close()} the range GET. Abort-after-success would
     * handshake every 4–16 MiB parquet window. Remainder is 0, so close-time abort-on-large-leftover
     * must also stay off.
     */
    public void testCompleteWindowFillClosesRatherThanAborts() throws IOException {
        byte[] data = new byte[ParquetStorageObjectAdapter.DEFAULT_WINDOW_SIZE + 1024];
        randomBytes(data);
        DrainSimulatingStorageObject.Tracking tracking = new DrainSimulatingStorageObject.Tracking();
        StorageObject storage = DrainSimulatingStorageObject.create(data, tracking);

        ParquetStorageObjectAdapter adapter = new ParquetStorageObjectAdapter(storage, footerByteCache, breaker);
        try (SeekableInputStream stream = adapter.newStream()) {
            byte[] buf = new byte[1024];
            stream.readFully(buf);
        }

        assertFalse("completed window must close(), not abortStream", tracking.aborted.get());
        assertTrue("completed window must close the range GET", tracking.closed.get());
    }

    private static StorageObject incompleteWindowStorage(StorageObject raw) {
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
            public InputStream newStream() throws IOException {
                return throwAfterPrefix(raw.newStream());
            }

            @Override
            public InputStream newStream(long position, long length) throws IOException {
                return throwAfterPrefix(raw.newStream(position, length));
            }

            @Override
            public void abortStream(InputStream stream) throws IOException {
                raw.abortStream(stream);
            }

            @Override
            public long length() throws IOException {
                return raw.length();
            }

            @Override
            public Instant lastModified() throws IOException {
                return raw.lastModified();
            }

            @Override
            public boolean exists() throws IOException {
                return raw.exists();
            }

            @Override
            public StoragePath path() {
                return raw.path();
            }
        };
    }

    private static InputStream throwAfterPrefix(InputStream inner) {
        return new FilterInputStream(inner) {
            int consumed;

            @Override
            public int read() throws IOException {
                if (consumed >= 8192) {
                    throw new IOException("simulated incomplete window");
                }
                int b = super.read();
                if (b >= 0) {
                    consumed++;
                }
                return b;
            }

            @Override
            public int read(byte[] buf, int off, int len) throws IOException {
                if (consumed >= 8192) {
                    throw new IOException("simulated incomplete window");
                }
                int n = super.read(buf, off, Math.min(len, 8192 - consumed));
                if (n > 0) {
                    consumed += n;
                }
                return n;
            }
        };
    }

    /**
     * The first read charges the parent breaker. parquet-mr may open the stream on a generic
     * thread while the driver holds {@link LocalCircuitBreaker#assertBeginRunLoop()}.
     */
    public void testNewStreamFromOtherThreadChargesParentBreaker() throws Exception {
        assumeTrue("requires assertions enabled (-ea) to detect the I/O-thread race", assertionsEnabled());

        BigArrays bigArrays = new MockBigArrays(PageCacheRecycler.NON_RECYCLING_INSTANCE, ByteSizeValue.ofMb(16)).withCircuitBreaking();
        CircuitBreaker parent = bigArrays.breakerService().getBreaker(CircuitBreaker.REQUEST);
        LocalCircuitBreaker local = new LocalCircuitBreaker(parent, 0, 0);

        Thread setup = new Thread(() -> assertTrue(local.assertBeginRunLoop()), "setup-pin-driver-breaker");
        setup.start();
        setup.join();

        try {
            byte[] data = new byte[100];
            randomBytes(data);
            ParquetStorageObjectAdapter adapter = new ParquetStorageObjectAdapter(
                createRangeReadStorageObject(data),
                footerByteCache,
                local
            );
            try (SeekableInputStream stream = adapter.newStream()) {
                assertEquals(0, parent.getUsed());
                stream.readFully(new byte[data.length]);
                assertEquals(HeapFootprint.byteArrayBytes(data.length), parent.getUsed());
            }
            assertEquals(0, parent.getUsed());
        } finally {
            assertTrue(local.assertEndRunLoop());
            local.close();
            assertEquals(0, parent.getUsed());
        }
    }

    public void testTinyDefaultCtorChargesFileLength() throws IOException {
        byte[] data = new byte[100];
        randomBytes(data);
        LimitedBreaker limited = new LimitedBreaker("test", ByteSizeValue.ofMb(16));
        ParquetStorageObjectAdapter adapter = new ParquetStorageObjectAdapter(createRangeReadStorageObject(data), footerByteCache, limited);
        try (SeekableInputStream stream = adapter.newStream()) {
            assertEquals(0, limited.getUsed());
            stream.readFully(new byte[data.length]);
            assertEquals(HeapFootprint.byteArrayBytes(data.length), limited.getUsed());
        }
        assertEquals(0, limited.getUsed());
    }

    public void testTinyForRangeChargesFileLength() throws IOException {
        byte[] data = new byte[100];
        randomBytes(data);
        LimitedBreaker limited = new LimitedBreaker("test", ByteSizeValue.ofMb(16));
        ParquetStorageObjectAdapter adapter = ParquetStorageObjectAdapter.forRange(
            createRangeReadStorageObject(data),
            1024L,
            footerByteCache,
            limited
        );
        try (SeekableInputStream stream = adapter.newStream()) {
            assertEquals(0, limited.getUsed());
            byte[] buf = new byte[data.length];
            stream.readFully(buf);
            assertEquals(HeapFootprint.byteArrayBytes(data.length), limited.getUsed());
            assertArrayEquals(data, buf);
        }
        assertEquals(0, limited.getUsed());
    }

    public void testLargeFileChargesDefaultWindowSize() throws IOException {
        int size = ParquetStorageObjectAdapter.DEFAULT_WINDOW_SIZE + 123;
        byte[] data = new byte[size];
        randomBytes(data);
        LimitedBreaker limited = new LimitedBreaker("test", ByteSizeValue.ofMb(16));
        ParquetStorageObjectAdapter adapter = new ParquetStorageObjectAdapter(createRangeReadStorageObject(data), footerByteCache, limited);
        try (SeekableInputStream stream = adapter.newStream()) {
            assertEquals(0, limited.getUsed());
            assertEquals(data[0] & 0xFF, stream.read());
            assertEquals(HeapFootprint.byteArrayBytes(ParquetStorageObjectAdapter.DEFAULT_WINDOW_SIZE), limited.getUsed());
        }
        assertEquals(0, limited.getUsed());
    }

    public void testOpenSeekToEndAndCloseDoesNotCharge() throws IOException {
        byte[] data = new byte[256];
        randomBytes(data);
        LimitedBreaker limited = new LimitedBreaker("test", ByteSizeValue.ofMb(16));
        ParquetIoWatermark watermark = new ParquetIoWatermark(64 * 1024 * 1024);
        ParquetStorageObjectAdapter adapter = new ParquetStorageObjectAdapter(
            createRangeReadStorageObject(data),
            footerByteCache,
            limited,
            watermark
        );
        try (SeekableInputStream stream = adapter.newStream()) {
            assertEquals(0, stream.getPos());
            stream.seek(data.length);
            assertEquals(-1, stream.read());
            assertEquals(0, limited.getUsed());
            assertEquals(0, watermark.used());
        }
        assertEquals(0, limited.getUsed());
        assertEquals(0, watermark.used());
    }

    public void testFirstReadChargesOnceAcrossAWindowSlide() throws IOException {
        int size = ParquetStorageObjectAdapter.DEFAULT_WINDOW_SIZE + 123;
        byte[] data = new byte[size];
        randomBytes(data);
        LimitedBreaker limited = new LimitedBreaker("test", ByteSizeValue.ofMb(16));
        ParquetIoWatermark watermark = new ParquetIoWatermark(64 * 1024 * 1024);
        ParquetStorageObjectAdapter adapter = new ParquetStorageObjectAdapter(
            createRangeReadStorageObject(data),
            footerByteCache,
            limited,
            watermark
        );
        try (SeekableInputStream stream = adapter.newStream()) {
            assertEquals(0, limited.getUsed());
            assertEquals(data[0] & 0xFF, stream.read());
            assertEquals(HeapFootprint.byteArrayBytes(ParquetStorageObjectAdapter.DEFAULT_WINDOW_SIZE), limited.getUsed());
            assertEquals(HeapFootprint.byteArrayBytes(ParquetStorageObjectAdapter.DEFAULT_WINDOW_SIZE), watermark.used());
            stream.readFully(new byte[ParquetStorageObjectAdapter.DEFAULT_WINDOW_SIZE + 63]);
            assertEquals(HeapFootprint.byteArrayBytes(ParquetStorageObjectAdapter.DEFAULT_WINDOW_SIZE), limited.getUsed());
            assertEquals(HeapFootprint.byteArrayBytes(ParquetStorageObjectAdapter.DEFAULT_WINDOW_SIZE), watermark.used());
        }
        assertEquals(0, limited.getUsed());
        assertEquals(0, watermark.used());
    }

    public void testPreWarmedHitChargesWindowWithoutARangeGet() throws IOException {
        byte[] data = new byte[1024];
        randomBytes(data);
        AtomicInteger rangeReadCount = new AtomicInteger();
        StorageObject storage = createCountingRangeReadStorageObject(data, rangeReadCount);
        LimitedBreaker limited = new LimitedBreaker("test", ByteSizeValue.ofMb(16));
        ParquetIoWatermark watermark = new ParquetIoWatermark(64 * 1024 * 1024);
        ParquetStorageObjectAdapter adapter = new ParquetStorageObjectAdapter(storage, footerByteCache, limited, watermark);

        java.util.NavigableMap<Long, ColumnChunkPrefetcher.PrefetchedChunk> chunks = new java.util.TreeMap<>();
        ByteBuffer warm = ByteBuffer.wrap(data, 200, 200).slice();
        chunks.put(200L, new ColumnChunkPrefetcher.PrefetchedChunk(200L, 200L, warm));
        adapter.installPreWarmedChunks(chunks);

        try (SeekableInputStream stream = adapter.newStream()) {
            stream.seek(200);
            stream.readFully(new byte[200]);
            assertEquals(0, rangeReadCount.get());
            assertEquals(HeapFootprint.byteArrayBytes(data.length), limited.getUsed());
            assertEquals(HeapFootprint.byteArrayBytes(data.length), watermark.used());
        }
        assertEquals(0, limited.getUsed());
        assertEquals(0, watermark.used());
    }

    public void testTailCacheHitChargesOnTheCopy() throws IOException {
        byte[] data = new byte[ParquetStorageObjectAdapter.DEFAULT_WINDOW_SIZE + 1024];
        randomBytes(data);
        AtomicInteger rangeReadCount = new AtomicInteger();
        StorageObject storage = createCountingRangeReadStorageObject(data, rangeReadCount);
        LimitedBreaker limited = new LimitedBreaker("test", ByteSizeValue.ofMb(16));
        ParquetIoWatermark watermark = new ParquetIoWatermark(64 * 1024 * 1024);
        ParquetStorageObjectAdapter adapter = new ParquetStorageObjectAdapter(storage, footerByteCache, limited, watermark);
        long tailStart = data.length - 1024;
        byte[] tail = new byte[1024];

        try (SeekableInputStream stream = adapter.newStream()) {
            stream.seek(tailStart);
            stream.readFully(tail);
            assertEquals(1, rangeReadCount.get());
            assertEquals(HeapFootprint.byteArrayBytes(ParquetStorageObjectAdapter.DEFAULT_WINDOW_SIZE), limited.getUsed());
            assertEquals(HeapFootprint.byteArrayBytes(ParquetStorageObjectAdapter.DEFAULT_WINDOW_SIZE), watermark.used());
        }
        assertEquals(0, limited.getUsed());
        assertEquals(0, watermark.used());

        try (SeekableInputStream stream = adapter.newStream()) {
            stream.seek(tailStart);
            stream.readFully(new byte[1024]);
            assertEquals(1, rangeReadCount.get());
            assertEquals(HeapFootprint.byteArrayBytes(ParquetStorageObjectAdapter.DEFAULT_WINDOW_SIZE), limited.getUsed());
            assertEquals(HeapFootprint.byteArrayBytes(ParquetStorageObjectAdapter.DEFAULT_WINDOW_SIZE), watermark.used());
        }
        assertEquals(0, limited.getUsed());
        assertEquals(0, watermark.used());
    }

    public void testIncompleteFillKeepsTheChargeUntilClose() throws IOException {
        byte[] data = new byte[ParquetStorageObjectAdapter.DEFAULT_WINDOW_SIZE + 1024];
        randomBytes(data);
        DrainSimulatingStorageObject.Tracking tracking = new DrainSimulatingStorageObject.Tracking();
        StorageObject failing = incompleteWindowStorage(DrainSimulatingStorageObject.create(data, tracking));
        LimitedBreaker limited = new LimitedBreaker("test", ByteSizeValue.ofMb(16));
        ParquetIoWatermark watermark = new ParquetIoWatermark(64 * 1024 * 1024);
        ParquetStorageObjectAdapter adapter = new ParquetStorageObjectAdapter(failing, footerByteCache, limited, watermark);
        try (SeekableInputStream stream = adapter.newStream()) {
            expectThrows(IOException.class, () -> stream.readFully(new byte[1024]));
            assertEquals(HeapFootprint.byteArrayBytes(ParquetStorageObjectAdapter.DEFAULT_WINDOW_SIZE), limited.getUsed());
            assertEquals(HeapFootprint.byteArrayBytes(ParquetStorageObjectAdapter.DEFAULT_WINDOW_SIZE), watermark.used());
        }
        assertEquals(0, limited.getUsed());
        assertEquals(0, watermark.used());
    }

    public void testChargeTripDoesNotTouchTheWatermark() throws IOException {
        byte[] data = new byte[100];
        randomBytes(data);
        LimitedBreaker limited = new LimitedBreaker("test", ByteSizeValue.ofBytes(50));
        ParquetIoWatermark watermark = new ParquetIoWatermark(64 * 1024 * 1024);
        ParquetStorageObjectAdapter adapter = new ParquetStorageObjectAdapter(
            createRangeReadStorageObject(data),
            footerByteCache,
            limited,
            watermark
        );
        try (SeekableInputStream stream = adapter.newStream()) {
            expectThrows(CircuitBreakingException.class, stream::read);
            assertEquals(0, watermark.used());
            assertEquals(0, limited.getUsed());
        }
        assertEquals(0, watermark.used());
        assertEquals(0, limited.getUsed());
    }

    /**
     * A breaker trip on a cold footer tail must happen in {@code getOrAllocateWindow} before
     * {@code getOrLoad} / {@code newStream}. {@link #testChargeTripDoesNotTouchTheWatermark}
     * is a whole-file fill ({@code isTailRead} false) and does not count GETs, so a GET-then-charge
     * reorder would still throw with watermark 0.
     */
    public void testColdTailChargeTripDoesNotIssueARangeGet() throws IOException {
        byte[] data = new byte[ParquetStorageObjectAdapter.DEFAULT_WINDOW_SIZE + 1024];
        randomBytes(data);
        AtomicInteger rangeReadCount = new AtomicInteger();
        StorageObject storage = createCountingRangeReadStorageObject(data, rangeReadCount);
        LimitedBreaker limited = new LimitedBreaker("test", ByteSizeValue.ofBytes(ParquetStorageObjectAdapter.DEFAULT_WINDOW_SIZE - 1L));
        ParquetIoWatermark watermark = new ParquetIoWatermark(64 * 1024 * 1024);
        ParquetStorageObjectAdapter adapter = new ParquetStorageObjectAdapter(storage, footerByteCache, limited, watermark);
        long tailStart = data.length - 1024;

        try (SeekableInputStream stream = adapter.newStream()) {
            expectThrows(CircuitBreakingException.class, () -> {
                stream.seek(tailStart);
                stream.readFully(new byte[1024]);
            });
            assertEquals(0, rangeReadCount.get());
            assertEquals(0, watermark.used());
            assertEquals(0, limited.getUsed());
        }
        assertEquals(0, rangeReadCount.get());
        assertEquals(0, watermark.used());
        assertEquals(0, limited.getUsed());
    }

    public void testSeekableInputStreamRead() throws IOException {
        byte[] data = new byte[] { 1, 2, 3, 4, 5, 6, 7, 8, 9, 10 };
        StorageObject storageObject = createRangeReadStorageObject(data);

        ParquetStorageObjectAdapter adapter = new ParquetStorageObjectAdapter(storageObject, footerByteCache, breaker);

        try (SeekableInputStream stream = adapter.newStream()) {
            assertEquals(1, stream.read());
            assertEquals(1, stream.getPos());
            assertEquals(2, stream.read());
            assertEquals(2, stream.getPos());
        }
    }

    public void testSeekableInputStreamReadArray() throws IOException {
        byte[] data = new byte[] { 1, 2, 3, 4, 5, 6, 7, 8, 9, 10 };
        StorageObject storageObject = createRangeReadStorageObject(data);

        ParquetStorageObjectAdapter adapter = new ParquetStorageObjectAdapter(storageObject, footerByteCache, breaker);

        try (SeekableInputStream stream = adapter.newStream()) {
            byte[] buffer = new byte[5];
            int bytesRead = stream.read(buffer);
            assertEquals(5, bytesRead);
            assertEquals(5, stream.getPos());
            assertArrayEquals(new byte[] { 1, 2, 3, 4, 5 }, buffer);
        }
    }

    public void testSeekableInputStreamSeekForward() throws IOException {
        byte[] data = new byte[] { 1, 2, 3, 4, 5, 6, 7, 8, 9, 10 };
        StorageObject storageObject = createRangeReadStorageObject(data);

        ParquetStorageObjectAdapter adapter = new ParquetStorageObjectAdapter(storageObject, footerByteCache, breaker);

        try (SeekableInputStream stream = adapter.newStream()) {
            stream.seek(5);
            assertEquals(5, stream.getPos());
            assertEquals(6, stream.read());
            assertEquals(6, stream.getPos());
        }
    }

    public void testSeekableInputStreamSeekBackward() throws IOException {
        byte[] data = new byte[] { 1, 2, 3, 4, 5, 6, 7, 8, 9, 10 };
        StorageObject storageObject = createRangeReadStorageObject(data);

        ParquetStorageObjectAdapter adapter = new ParquetStorageObjectAdapter(storageObject, footerByteCache, breaker);

        try (SeekableInputStream stream = adapter.newStream()) {
            // Read some bytes to advance position
            stream.read();
            stream.read();
            stream.read();
            assertEquals(3, stream.getPos());

            // Seek backward
            stream.seek(1);
            assertEquals(1, stream.getPos());
            assertEquals(2, stream.read());
        }
    }

    public void testSeekableInputStreamSeekToNegativePositionThrows() throws IOException {
        byte[] data = new byte[100];
        StorageObject storageObject = createStorageObject(data);

        ParquetStorageObjectAdapter adapter = new ParquetStorageObjectAdapter(storageObject, footerByteCache, breaker);

        try (SeekableInputStream stream = adapter.newStream()) {
            IOException e = expectThrows(IOException.class, () -> stream.seek(-1));
            assertTrue(e.getMessage().contains("Cannot seek to negative position"));
        }
    }

    public void testSeekableInputStreamSeekBeyondEndThrows() throws IOException {
        byte[] data = new byte[100];
        StorageObject storageObject = createStorageObject(data);

        ParquetStorageObjectAdapter adapter = new ParquetStorageObjectAdapter(storageObject, footerByteCache, breaker);

        try (SeekableInputStream stream = adapter.newStream()) {
            IOException e = expectThrows(IOException.class, () -> stream.seek(200));
            assertTrue(e.getMessage().contains("Cannot seek beyond end of file"));
        }
    }

    public void testSeekableInputStreamReadFully() throws IOException {
        byte[] data = new byte[] { 1, 2, 3, 4, 5, 6, 7, 8, 9, 10 };
        StorageObject storageObject = createRangeReadStorageObject(data);

        ParquetStorageObjectAdapter adapter = new ParquetStorageObjectAdapter(storageObject, footerByteCache, breaker);

        try (SeekableInputStream stream = adapter.newStream()) {
            byte[] buffer = new byte[5];
            stream.readFully(buffer);
            assertArrayEquals(new byte[] { 1, 2, 3, 4, 5 }, buffer);
            assertEquals(5, stream.getPos());
        }
    }

    public void testSeekableInputStreamReadFullyWithOffset() throws IOException {
        byte[] data = new byte[] { 1, 2, 3, 4, 5, 6, 7, 8, 9, 10 };
        StorageObject storageObject = createRangeReadStorageObject(data);

        ParquetStorageObjectAdapter adapter = new ParquetStorageObjectAdapter(storageObject, footerByteCache, breaker);

        try (SeekableInputStream stream = adapter.newStream()) {
            byte[] buffer = new byte[10];
            stream.readFully(buffer, 2, 5);
            assertArrayEquals(new byte[] { 0, 0, 1, 2, 3, 4, 5, 0, 0, 0 }, buffer);
            assertEquals(5, stream.getPos());
        }
    }

    public void testSeekableInputStreamReadByteBuffer() throws IOException {
        byte[] data = new byte[] { 1, 2, 3, 4, 5, 6, 7, 8, 9, 10 };
        StorageObject storageObject = createRangeReadStorageObject(data);

        ParquetStorageObjectAdapter adapter = new ParquetStorageObjectAdapter(storageObject, footerByteCache, breaker);

        try (SeekableInputStream stream = adapter.newStream()) {
            ByteBuffer buffer = ByteBuffer.allocate(5);
            int bytesRead = stream.read(buffer);
            assertEquals(5, bytesRead);
            buffer.flip();
            assertEquals(1, buffer.get());
            assertEquals(2, buffer.get());
        }
    }

    public void testSeekableInputStreamReadFullyByteBuffer() throws IOException {
        byte[] data = new byte[] { 1, 2, 3, 4, 5, 6, 7, 8, 9, 10 };
        StorageObject storageObject = createRangeReadStorageObject(data);

        ParquetStorageObjectAdapter adapter = new ParquetStorageObjectAdapter(storageObject, footerByteCache, breaker);

        try (SeekableInputStream stream = adapter.newStream()) {
            ByteBuffer buffer = ByteBuffer.allocate(5);
            stream.readFully(buffer);
            buffer.flip();
            assertEquals(1, buffer.get());
            assertEquals(2, buffer.get());
            assertEquals(3, buffer.get());
            assertEquals(4, buffer.get());
            assertEquals(5, buffer.get());
        }
    }

    public void testReadByteBufferWithNonZeroPosition() throws IOException {
        byte[] data = new byte[] { 1, 2, 3, 4, 5, 6, 7, 8, 9, 10 };
        StorageObject storageObject = createRangeReadStorageObject(data);

        ParquetStorageObjectAdapter adapter = new ParquetStorageObjectAdapter(storageObject, footerByteCache, breaker);

        try (SeekableInputStream stream = adapter.newStream()) {
            ByteBuffer buffer = ByteBuffer.allocate(10);
            buffer.position(3);
            int bytesRead = stream.read(buffer);
            assertEquals(7, bytesRead);
            assertEquals(10, buffer.position());
            buffer.flip();
            buffer.position(3);
            assertEquals(1, buffer.get());
            assertEquals(2, buffer.get());
            assertEquals(3, buffer.get());
        }
    }

    public void testReadFullyByteBufferWithNonZeroPosition() throws IOException {
        byte[] data = new byte[] { 1, 2, 3, 4, 5, 6, 7, 8, 9, 10 };
        StorageObject storageObject = createRangeReadStorageObject(data);

        ParquetStorageObjectAdapter adapter = new ParquetStorageObjectAdapter(storageObject, footerByteCache, breaker);

        try (SeekableInputStream stream = adapter.newStream()) {
            ByteBuffer buffer = ByteBuffer.allocate(8);
            buffer.position(3);
            stream.readFully(buffer);
            assertEquals(8, buffer.position());
            buffer.flip();
            buffer.position(3);
            assertEquals(1, buffer.get());
            assertEquals(2, buffer.get());
            assertEquals(3, buffer.get());
            assertEquals(4, buffer.get());
            assertEquals(5, buffer.get());
        }
    }

    public void testReadDirectByteBuffer() throws IOException {
        byte[] data = new byte[] { 1, 2, 3, 4, 5, 6, 7, 8, 9, 10 };
        StorageObject storageObject = createRangeReadStorageObject(data);

        ParquetStorageObjectAdapter adapter = new ParquetStorageObjectAdapter(storageObject, footerByteCache, breaker);

        try (SeekableInputStream stream = adapter.newStream()) {
            ByteBuffer buffer = ByteBuffer.allocateDirect(5);
            assertFalse(buffer.hasArray());
            int bytesRead = stream.read(buffer);
            assertEquals(5, bytesRead);
            buffer.flip();
            assertEquals(1, buffer.get());
            assertEquals(2, buffer.get());
            assertEquals(3, buffer.get());
        }
    }

    public void testReadFullyDirectByteBuffer() throws IOException {
        byte[] data = new byte[] { 1, 2, 3, 4, 5, 6, 7, 8, 9, 10 };
        StorageObject storageObject = createRangeReadStorageObject(data);

        ParquetStorageObjectAdapter adapter = new ParquetStorageObjectAdapter(storageObject, footerByteCache, breaker);

        try (SeekableInputStream stream = adapter.newStream()) {
            ByteBuffer buffer = ByteBuffer.allocateDirect(5);
            assertFalse(buffer.hasArray());
            stream.readFully(buffer);
            buffer.flip();
            assertEquals(1, buffer.get());
            assertEquals(2, buffer.get());
            assertEquals(3, buffer.get());
            assertEquals(4, buffer.get());
            assertEquals(5, buffer.get());
        }
    }

    public void testByteBufferReadThenByteArrayRead() throws IOException {
        byte[] data = new byte[] { 1, 2, 3, 4, 5, 6, 7, 8, 9, 10 };
        StorageObject storageObject = createRangeReadStorageObject(data);

        ParquetStorageObjectAdapter adapter = new ParquetStorageObjectAdapter(storageObject, footerByteCache, breaker);

        try (SeekableInputStream stream = adapter.newStream()) {
            ByteBuffer buf = ByteBuffer.allocate(3);
            stream.readFully(buf);
            buf.flip();
            assertEquals(1, buf.get());
            assertEquals(2, buf.get());
            assertEquals(3, buf.get());
            assertEquals(3, stream.getPos());

            // Byte-array read continues from same stream position
            byte[] arr = new byte[3];
            stream.readFully(arr);
            assertArrayEquals(new byte[] { 4, 5, 6 }, arr);
            assertEquals(6, stream.getPos());
        }
    }

    public void testSeekThenByteBufferRead() throws IOException {
        byte[] data = new byte[] { 1, 2, 3, 4, 5, 6, 7, 8, 9, 10 };
        StorageObject storageObject = createRangeReadStorageObject(data);

        ParquetStorageObjectAdapter adapter = new ParquetStorageObjectAdapter(storageObject, footerByteCache, breaker);

        try (SeekableInputStream stream = adapter.newStream()) {
            stream.seek(5);
            ByteBuffer buf = ByteBuffer.allocate(3);
            stream.readFully(buf);
            buf.flip();
            assertEquals(6, buf.get());
            assertEquals(7, buf.get());
            assertEquals(8, buf.get());
            assertEquals(8, stream.getPos());
        }
    }

    public void testReadByteBufferAtEofReturnsMinusOne() throws IOException {
        byte[] data = new byte[] { 1, 2, 3 };
        StorageObject storageObject = createRangeReadStorageObject(data);

        ParquetStorageObjectAdapter adapter = new ParquetStorageObjectAdapter(storageObject, footerByteCache, breaker);

        try (SeekableInputStream stream = adapter.newStream()) {
            stream.seek(3);
            ByteBuffer buf = ByteBuffer.allocate(5);
            int bytesRead = stream.read(buf);
            assertEquals(-1, bytesRead);
            assertEquals(0, buf.position());
        }
    }

    public void testReadFullyByteBufferAtEofThrows() throws IOException {
        byte[] data = new byte[] { 1, 2, 3 };
        StorageObject storageObject = createRangeReadStorageObject(data);

        ParquetStorageObjectAdapter adapter = new ParquetStorageObjectAdapter(storageObject, footerByteCache, breaker);

        try (SeekableInputStream stream = adapter.newStream()) {
            stream.seek(3);
            ByteBuffer buf = ByteBuffer.allocate(5);
            expectThrows(IOException.class, () -> stream.readFully(buf));
        }
    }

    public void testDirectByteBufferLargerThanTransferBuffer() throws IOException {
        byte[] data = new byte[StorageObject.TRANSFER_BUFFER_SIZE * 3];
        randomBytes(data);
        StorageObject storageObject = createRangeReadStorageObject(data);

        ParquetStorageObjectAdapter adapter = new ParquetStorageObjectAdapter(storageObject, footerByteCache, breaker);

        try (SeekableInputStream stream = adapter.newStream()) {
            ByteBuffer buf = ByteBuffer.allocateDirect(data.length);
            assertFalse(buf.hasArray());
            stream.readFully(buf);
            buf.flip();
            for (int i = 0; i < data.length; i++) {
                assertEquals("Mismatch at position " + i, data[i], buf.get());
            }
        }
    }

    public void testSeekableInputStreamSkip() throws IOException {
        byte[] data = new byte[] { 1, 2, 3, 4, 5, 6, 7, 8, 9, 10 };
        StorageObject storageObject = createRangeReadStorageObject(data);

        ParquetStorageObjectAdapter adapter = new ParquetStorageObjectAdapter(storageObject, footerByteCache, breaker);

        try (SeekableInputStream stream = adapter.newStream()) {
            long skipped = stream.skip(3);
            assertEquals(3, skipped);
            assertEquals(3, stream.getPos());
            assertEquals(4, stream.read());
        }
    }

    /**
     * Verifies that the adapter never calls {@link StorageObject#newStream()} (full GET).
     * A stub that throws on newStream() is used; all operations succeed via newStream(pos, len) only.
     */
    public void testNoFullGetUsesOnlyRangeReads() throws IOException {
        byte[] data = new byte[] { 1, 2, 3, 4, 5, 6, 7, 8, 9, 10 };
        StorageObject rangeOnlyStorageObject = new StorageObject() {
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
            public InputStream newStream() throws IOException {
                throw new UnsupportedOperationException("Full GET not supported; use range reads only");
            }

            @Override
            public InputStream newStream(long position, long length) throws IOException {
                int pos = (int) position;
                int len = (int) Math.min(length, data.length - position);
                return new ByteArrayInputStream(data, pos, len);
            }

            @Override
            public long length() throws IOException {
                return data.length;
            }

            @Override
            public Instant lastModified() throws IOException {
                return Instant.now();
            }

            @Override
            public boolean exists() throws IOException {
                return true;
            }

            @Override
            public StoragePath path() {
                return StoragePath.of("memory://test.parquet");
            }
        };

        ParquetStorageObjectAdapter adapter = new ParquetStorageObjectAdapter(rangeOnlyStorageObject, footerByteCache, breaker);

        try (SeekableInputStream stream = adapter.newStream()) {
            assertEquals(0, stream.getPos());
            assertEquals(1, stream.read());
            assertEquals(2, stream.read());
            stream.seek(5);
            assertEquals(6, stream.read());
            stream.seek(0);
            byte[] buf = new byte[5];
            stream.readFully(buf);
            assertArrayEquals(new byte[] { 1, 2, 3, 4, 5 }, buf);
        }
    }

    /**
     * Verifies that repeated reads within the same window do not trigger additional range requests.
     * The sliding window should serve multiple reads from the same cached range.
     */
    public void testSeekWindowBehaviorReusesCachedRange() throws IOException {
        byte[] data = new byte[1024];
        randomBytes(data);
        final int[] rangeReadCount = { 0 };
        StorageObject countingStorageObject = new StorageObject() {
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
            public InputStream newStream() throws IOException {
                throw new UnsupportedOperationException("Full GET not supported");
            }

            @Override
            public InputStream newStream(long position, long length) throws IOException {
                rangeReadCount[0]++;
                int pos = (int) position;
                int len = (int) Math.min(length, data.length - position);
                return new ByteArrayInputStream(data, pos, len);
            }

            @Override
            public long length() throws IOException {
                return data.length;
            }

            @Override
            public Instant lastModified() throws IOException {
                return Instant.now();
            }

            @Override
            public boolean exists() throws IOException {
                return true;
            }

            @Override
            public StoragePath path() {
                return StoragePath.of("memory://test.parquet");
            }
        };

        ParquetStorageObjectAdapter adapter = new ParquetStorageObjectAdapter(countingStorageObject, footerByteCache, breaker);

        try (SeekableInputStream stream = adapter.newStream()) {
            stream.read();
            stream.read();
            stream.read();
            stream.seek(0);
            stream.read();
            stream.seek(10);
            stream.read();
        }

        assertTrue("Expected few range reads (window reuse); got " + rangeReadCount[0], rangeReadCount[0] <= 2);
    }

    /**
     * Each new stream fetches the tail independently; there is no cross-stream footer cache.
     */
    public void testSeparateStreamsEachFetchTailViaRangeRead() throws IOException {
        // Must exceed the sliding window so a tail seek is a suffix GET, not a whole-file fill
        // (a whole-file fill is stored in FooterByteCache only when the object fits the footer prefetch).
        byte[] data = new byte[ParquetStorageObjectAdapter.DEFAULT_WINDOW_SIZE + 1024];
        randomBytes(data);
        final int[] rangeReadCount = { 0 };
        Instant lastModified = Instant.ofEpochMilli(123);

        StorageObject countingStorageObject = new StorageObject() {
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
            public InputStream newStream() throws IOException {
                throw new UnsupportedOperationException("Full GET not supported");
            }

            @Override
            public InputStream newStream(long position, long length) throws IOException {
                rangeReadCount[0]++;
                int pos = (int) position;
                int len = (int) Math.min(length, data.length - position);
                return new ByteArrayInputStream(data, pos, len);
            }

            @Override
            public long length() throws IOException {
                return data.length;
            }

            @Override
            public Instant lastModified() throws IOException {
                return lastModified;
            }

            @Override
            public boolean exists() throws IOException {
                return true;
            }

            @Override
            public StoragePath path() {
                return StoragePath.of("memory://test.parquet");
            }
        };

        ParquetStorageObjectAdapter adapter = new ParquetStorageObjectAdapter(countingStorageObject, footerByteCache, breaker);
        long tailStart = data.length - 1024;

        byte[] first = new byte[1024];
        try (SeekableInputStream stream = adapter.newStream()) {
            stream.seek(tailStart);
            stream.readFully(first);
        }
        assertEquals(1, rangeReadCount[0]);

        byte[] second = new byte[1024];
        try (SeekableInputStream stream = adapter.newStream()) {
            stream.seek(tailStart);
            stream.readFully(second);
        }
        // Second tail read is served from the JVM-wide FooterByteCache — no additional range read
        assertEquals(1, rangeReadCount[0]);
        assertArrayEquals(first, second);
    }

    public void testReadFullyCrossesWindowBoundaries() throws IOException {
        int size = ParquetStorageObjectAdapter.DEFAULT_WINDOW_SIZE + 123;
        byte[] data = new byte[size];
        randomBytes(data);
        final int[] rangeReadCount = { 0 };

        StorageObject rangeOnlyCounting = new StorageObject() {
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
            public InputStream newStream() throws IOException {
                throw new UnsupportedOperationException("Full GET not supported");
            }

            @Override
            public InputStream newStream(long position, long length) throws IOException {
                rangeReadCount[0]++;
                int pos = (int) position;
                int len = (int) Math.min(length, data.length - position);
                return new ByteArrayInputStream(data, pos, len);
            }

            @Override
            public long length() throws IOException {
                return data.length;
            }

            @Override
            public Instant lastModified() throws IOException {
                return Instant.ofEpochMilli(0);
            }

            @Override
            public boolean exists() throws IOException {
                return true;
            }

            @Override
            public StoragePath path() {
                return StoragePath.of("memory://test.parquet");
            }
        };

        ParquetStorageObjectAdapter adapter = new ParquetStorageObjectAdapter(rangeOnlyCounting, footerByteCache, breaker);
        byte[] read = new byte[size];
        try (SeekableInputStream stream = adapter.newStream()) {
            stream.readFully(read);
        }
        assertArrayEquals(data, read);
        // Exactly 2: one full 4 MiB window, one for the remaining 123-byte tail.
        assertEquals(2, rangeReadCount[0]);
    }

    // --- Adaptive window tests ---

    public void testDefaultWindowUnchanged() throws IOException {
        byte[] data = new byte[100];
        randomBytes(data);
        StorageObject storageObject = createRangeReadStorageObject(data);

        ParquetStorageObjectAdapter adapter = new ParquetStorageObjectAdapter(storageObject, footerByteCache, breaker);

        try (SeekableInputStream stream = adapter.newStream()) {
            assertNotNull(stream);
            assertEquals(0, stream.getPos());
        }
    }

    public void testAdaptiveWindowSmallHint() throws IOException {
        byte[] data = new byte[100];
        randomBytes(data);
        StorageObject storageObject = createRangeReadStorageObject(data);

        long rangeBytes = 8 * 1024 * 1024L; // 8 MiB
        ParquetStorageObjectAdapter adapter = ParquetStorageObjectAdapter.forRange(storageObject, rangeBytes, footerByteCache, breaker);

        try (SeekableInputStream stream = adapter.newStream()) {
            assertNotNull(stream);
            byte[] buf = new byte[data.length];
            stream.readFully(buf);
            assertArrayEquals(data, buf);
        }
    }

    public void testAdaptiveWindowCappedAtMax() throws IOException {
        int size = ParquetStorageObjectAdapter.DEFAULT_WINDOW_SIZE + 123;
        byte[] data = new byte[size];
        randomBytes(data);
        final int[] maxRequestedLength = { 0 };

        StorageObject measuringStorageObject = new StorageObject() {
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
            public InputStream newStream() throws IOException {
                throw new UnsupportedOperationException("Full GET not supported");
            }

            @Override
            public InputStream newStream(long position, long length) throws IOException {
                maxRequestedLength[0] = Math.max(maxRequestedLength[0], (int) length);
                int pos = (int) position;
                int len = (int) Math.min(length, data.length - position);
                return new ByteArrayInputStream(data, pos, len);
            }

            @Override
            public long length() throws IOException {
                return data.length;
            }

            @Override
            public Instant lastModified() throws IOException {
                return Instant.ofEpochMilli(0);
            }

            @Override
            public boolean exists() throws IOException {
                return true;
            }

            @Override
            public StoragePath path() {
                return StoragePath.of("memory://test.parquet");
            }
        };

        long hugeRange = 64 * 1024 * 1024L; // 64 MiB — should be capped to MAX_WINDOW_SIZE
        ParquetStorageObjectAdapter adapter = ParquetStorageObjectAdapter.forRange(
            measuringStorageObject,
            hugeRange,
            footerByteCache,
            breaker
        );

        byte[] buf = new byte[size];
        try (SeekableInputStream stream = adapter.newStream()) {
            stream.readFully(buf);
        }
        assertArrayEquals(data, buf);
        assertTrue(
            "Requested range length should not exceed MAX_WINDOW_SIZE, got " + maxRequestedLength[0],
            maxRequestedLength[0] <= ParquetStorageObjectAdapter.MAX_WINDOW_SIZE
        );
    }

    public void testTinyFileTailSeekIsOneWholeFileGet() throws IOException {
        byte[] data = new byte[100];
        randomBytes(data);
        List<long[]> rangeGets = new ArrayList<>();
        StorageObject storage = createRecordingRangeReadStorageObject(data, rangeGets);
        ParquetStorageObjectAdapter adapter = new ParquetStorageObjectAdapter(storage, footerByteCache, breaker);

        try (SeekableInputStream stream = adapter.newStream()) {
            stream.seek(data.length - 8);
            stream.seek(0);
            byte[] buf = new byte[data.length];
            stream.readFully(buf);
            assertArrayEquals(data, buf);
        }

        assertEquals(1, rangeGets.size());
        assertEquals(0L, rangeGets.getFirst()[0]);
        assertEquals(data.length, rangeGets.getFirst()[1]);
    }

    /**
     * An object no larger than the footer prefetch is what the async footer path stores whole as its footer,
     * so the adapter's whole-file fill must store it too: a later stream over the same key is then served
     * from the cache with no GET.
     */
    public void testTinyFileWholeFileFillIsStoredAsItsFooter() throws IOException {
        byte[] data = new byte[100];
        randomBytes(data);
        List<long[]> rangeGets = new ArrayList<>();
        StorageObject storage = createRecordingRangeReadStorageObject(data, rangeGets);
        ParquetStorageObjectAdapter adapter = new ParquetStorageObjectAdapter(storage, footerByteCache, breaker);

        try (SeekableInputStream stream = adapter.newStream()) {
            stream.seek(data.length - 8);
            stream.seek(0);
            byte[] buf = new byte[data.length];
            stream.readFully(buf);
            assertArrayEquals(data, buf);
        }
        assertEquals(1, rangeGets.size());
        assertEquals(0L, rangeGets.getFirst()[0]);
        assertEquals(data.length, rangeGets.getFirst()[1]);
        assertArrayEquals("the whole-file fill is the object's footer entry", data, footerByteCache.get(adapter.cacheKey()));

        rangeGets.clear();
        try (SeekableInputStream stream = adapter.newStream()) {
            stream.seek(data.length - 8);
            stream.seek(0);
            byte[] buf = new byte[data.length];
            stream.readFully(buf);
            assertArrayEquals(data, buf);
        }
        assertEquals("a second stream is served from the stored entry", 0, rangeGets.size());
    }

    /**
     * The whole-file admission bound is the footer prefetch: an object of exactly that size is stored, one
     * byte larger is not, even though both fit the sliding window and so are filled whole from offset 0.
     * Larger whole-file fills would displace genuine footers from the byte budget.
     */
    public void testWholeFileFillIsStoredOnlyUpToTheFooterPrefetch() throws IOException {
        assertWholeFileFillStored(ParquetFormatReader.FOOTER_TAIL_PREFETCH_BYTES, true);
        assertWholeFileFillStored(ParquetFormatReader.FOOTER_TAIL_PREFETCH_BYTES + 1, false);
    }

    private void assertWholeFileFillStored(int size, boolean stored) throws IOException {
        assertThat(size, Matchers.lessThanOrEqualTo(ParquetStorageObjectAdapter.DEFAULT_WINDOW_SIZE));
        byte[] data = new byte[size];
        randomBytes(data);
        List<long[]> rangeGets = new ArrayList<>();
        StorageObject storage = createRecordingRangeReadStorageObject(data, rangeGets);
        ParquetStorageObjectAdapter adapter = new ParquetStorageObjectAdapter(storage, footerByteCache, breaker);

        try (SeekableInputStream stream = adapter.newStream()) {
            stream.seek(data.length - 8);
            byte[] trailer = new byte[8];
            stream.readFully(trailer);
        }
        assertEquals(1, rangeGets.size());
        assertEquals("a file that fits the window is filled whole", 0L, rangeGets.getFirst()[0]);
        assertEquals(data.length, rangeGets.getFirst()[1]);
        byte[] cached = footerByteCache.get(adapter.cacheKey());
        if (stored) {
            assertArrayEquals("size " + size + " is within the footer prefetch and must be stored", data, cached);
        } else {
            assertNull("size " + size + " exceeds the footer prefetch and must not occupy FooterByteCache", cached);
        }

        rangeGets.clear();
        try (SeekableInputStream stream = adapter.newStream()) {
            stream.seek(data.length - 8);
            byte[] trailer = new byte[8];
            stream.readFully(trailer);
        }
        assertEquals("size " + size, stored ? 0 : 1, rangeGets.size());
    }

    /**
     * {@link ParquetStorageObjectAdapter#refillWholeFileFooter()} stores an absent entry with one whole-file
     * GET, is a no-op while the entry lives, and stores again once the entry is gone.
     */
    public void testRefillWholeFileFooterStoresAbsentEntryOnce() throws IOException {
        byte[] data = new byte[100];
        randomBytes(data);
        List<long[]> rangeGets = new ArrayList<>();
        StorageObject storage = createRecordingRangeReadStorageObject(data, rangeGets);
        ParquetStorageObjectAdapter adapter = new ParquetStorageObjectAdapter(storage, footerByteCache, breaker);

        adapter.refillWholeFileFooter();
        assertEquals(1, rangeGets.size());
        assertEquals(0L, rangeGets.getFirst()[0]);
        assertEquals(data.length, rangeGets.getFirst()[1]);
        assertArrayEquals(data, footerByteCache.get(adapter.cacheKey()));
        assertEquals("the refill leaves no stream open", 0, adapter.trackedStreamCount());

        adapter.refillWholeFileFooter();
        assertEquals("a live entry is not read again", 1, rangeGets.size());

        footerByteCache.invalidateAll();
        adapter.refillWholeFileFooter();
        assertEquals("an expired entry is read again", 2, rangeGets.size());
        assertArrayEquals(data, footerByteCache.get(adapter.cacheKey()));
    }

    /**
     * An object with no version has no cache key at all: {@link FooterByteCache.Key#keyFor} returns null and its
     * footer is never cached. The adapter must still open and read it. The admission predicate dereferences the
     * key for its file length, so without the null conjunct this constructor throws.
     */
    public void testUnversionedObjectIsNeitherKeyedNorRefilled() throws IOException {
        byte[] data = new byte[100];
        randomBytes(data);
        List<long[]> rangeGets = new ArrayList<>();
        StorageObject unversioned = createRecordingRangeReadStorageObject(data, rangeGets, null);
        ParquetStorageObjectAdapter adapter = new ParquetStorageObjectAdapter(unversioned, footerByteCache, breaker);

        assertNull("an object with no version has no cache key", adapter.cacheKey());

        adapter.refillWholeFileFooter();
        assertEquals("nothing to refill without a key", 0, rangeGets.size());

        // The object is still readable; it just never reaches the byte cache.
        byte[] read = new byte[data.length];
        try (SeekableInputStream stream = adapter.newStream()) {
            stream.seek(0);
            stream.readFully(read);
        }
        assertArrayEquals(data, read);
        assertEquals("the read went to storage", 1, rangeGets.size());
    }

    /**
     * A refill whose read fails does not fail the caller: the bytes have no consumer of their own, so the scan
     * still runs on its own coalesced GETs. It also leaves nothing behind: no cache entry, no open stream, and
     * no window charge on the breaker or the node byte budget. Storage sees the failure twice because
     * {@code fetchWindowAt} retries a failed {@link FooterByteCache#getOrLoad} with direct I/O before giving up.
     */
    public void testRefillWholeFileFooterSwallowsAFailureAndStoresNothing() {
        byte[] data = new byte[100];
        randomBytes(data);
        AtomicInteger attempts = new AtomicInteger();
        StorageObject failing = new StorageObject() {
            @Override
            public void attachMetrics(ExternalSourceMetrics metrics, String scheme) {}

            @Override
            public StorageIdentity storageIdentity() {
                return AbstractTestStorageObject.NOOP;
            }

            @Override
            public String version() {
                return "v1";
            }

            @Override
            public InputStream newStream() {
                throw new UnsupportedOperationException("Full GET not supported in failing harness");
            }

            @Override
            public InputStream newStream(long position, long length) throws IOException {
                attempts.incrementAndGet();
                throw new IOException("simulated storage failure");
            }

            @Override
            public long length() {
                return data.length;
            }

            @Override
            public Instant lastModified() {
                return Instant.ofEpochMilli(0);
            }

            @Override
            public boolean exists() {
                return true;
            }

            @Override
            public StoragePath path() {
                return StoragePath.of("memory://refill-failure.parquet");
            }
        };
        ParquetIoWatermark watermark = new ParquetIoWatermark(64 * 1024 * 1024);
        CircuitBreaker limited = new LimitedBreaker("refill", ByteSizeValue.ofMb(16));
        ParquetStorageObjectAdapter adapter = new ParquetStorageObjectAdapter(failing, footerByteCache, limited, watermark);

        adapter.refillWholeFileFooter();
        assertEquals("the coalesced load and its direct-I/O retry", 2, attempts.get());
        assertNull("a failed refill stores nothing", footerByteCache.get(adapter.cacheKey()));
        assertEquals("the failed refill leaves no stream open", 0, adapter.trackedStreamCount());
        assertEquals("the failed refill refunds its window on the breaker", 0, limited.getUsed());
        assertEquals("the failed refill refunds its window on the node budget", 0, watermark.used());
    }

    /**
     * A budget rejection must be absorbed like a read failure. In production the outermost wrapper is
     * {@code QueryBudgetedStorageObject}, which {@code FileSourceFactory} installs above the retry layer, and its
     * permit acquisition turns a timeout into an {@link EsRejectedExecutionException} — neither an
     * {@link IOException} nor an {@code ExternalException}, so the retry layer never sees it. Left uncaught it
     * escapes the refill and fails a query that would otherwise have run, just more slowly.
     */
    public void testRefillWholeFileFooterAbsorbsABudgetRejection() {
        byte[] data = new byte[100];
        randomBytes(data);
        AtomicInteger attempts = new AtomicInteger();
        StorageObject rejecting = new StorageObject() {
            @Override
            public void attachMetrics(ExternalSourceMetrics metrics, String scheme) {}

            @Override
            public StorageIdentity storageIdentity() {
                return AbstractTestStorageObject.NOOP;
            }

            @Override
            public String version() {
                return "v1";
            }

            @Override
            public InputStream newStream() {
                throw new UnsupportedOperationException("Full GET not supported in rejecting harness");
            }

            @Override
            public InputStream newStream(long position, long length) {
                attempts.incrementAndGet();
                throw new EsRejectedExecutionException("Failed to acquire query concurrency budget permit: simulated");
            }

            @Override
            public long length() {
                return data.length;
            }

            @Override
            public Instant lastModified() {
                return Instant.ofEpochMilli(0);
            }

            @Override
            public boolean exists() {
                return true;
            }

            @Override
            public StoragePath path() {
                return StoragePath.of("memory://refill-rejected.parquet");
            }
        };
        ParquetIoWatermark watermark = new ParquetIoWatermark(64 * 1024 * 1024);
        CircuitBreaker limited = new LimitedBreaker("refill", ByteSizeValue.ofMb(16));
        ParquetStorageObjectAdapter adapter = new ParquetStorageObjectAdapter(rejecting, footerByteCache, limited, watermark);

        adapter.refillWholeFileFooter();
        assertEquals("the coalesced load and its direct-I/O retry", 2, attempts.get());
        assertNull("a rejected refill stores nothing", footerByteCache.get(adapter.cacheKey()));
        assertEquals("the rejected refill leaves no stream open", 0, adapter.trackedStreamCount());
        assertEquals("the rejected refill refunds its window on the breaker", 0, limited.getUsed());
        assertEquals("the rejected refill refunds its window on the node budget", 0, watermark.used());
    }

    /**
     * A stream that ends early must not leave a short entry behind. Every consumer reads a cache entry as the
     * file's suffix — {@code fillFromCachedTail} and {@code lookupFooterCacheHit} both derive its start as
     * {@code length - entry.length} — so a prefix cached under that contract is served at the wrong offset as
     * column-chunk data, with no error raised.
     */
    public void testRefillWholeFileFooterRejectsAShortReadAndStoresNothing() {
        byte[] data = new byte[100];
        randomBytes(data);
        AtomicInteger attempts = new AtomicInteger();
        StorageObject truncating = new StorageObject() {
            @Override
            public void attachMetrics(ExternalSourceMetrics metrics, String scheme) {}

            @Override
            public StorageIdentity storageIdentity() {
                return AbstractTestStorageObject.NOOP;
            }

            @Override
            public String version() {
                return "v1";
            }

            @Override
            public InputStream newStream() {
                throw new UnsupportedOperationException("Full GET not supported in truncating harness");
            }

            @Override
            public InputStream newStream(long position, long length) {
                attempts.incrementAndGet();
                // Ends after a third of what was asked for, without throwing.
                return new ByteArrayInputStream(data, (int) position, data.length / 3);
            }

            @Override
            public long length() {
                return data.length;
            }

            @Override
            public Instant lastModified() {
                return Instant.ofEpochMilli(0);
            }

            @Override
            public boolean exists() {
                return true;
            }

            @Override
            public StoragePath path() {
                return StoragePath.of("memory://refill-short-read.parquet");
            }
        };
        ParquetIoWatermark watermark = new ParquetIoWatermark(64 * 1024 * 1024);
        CircuitBreaker limited = new LimitedBreaker("refill-short", ByteSizeValue.ofMb(16));
        ParquetStorageObjectAdapter adapter = new ParquetStorageObjectAdapter(truncating, footerByteCache, limited, watermark);

        adapter.refillWholeFileFooter();
        assertNull("a short read stores nothing", footerByteCache.get(adapter.cacheKey()));
        assertEquals("the coalesced load and its direct-I/O retry", 2, attempts.get());
        assertEquals("the short read leaves no stream open", 0, adapter.trackedStreamCount());
        assertEquals("the short read refunds its window on the breaker", 0, limited.getUsed());
        assertEquals("the short read refunds its window on the node budget", 0, watermark.used());

        // The refill swallows the failure because nothing consumes its bytes, but a read that does need them
        // must see the early end rather than silently receive a prefix. The loader rejects the short read and
        // fetchWindowAt's direct-I/O retry reports the same event, so that is the message the caller gets.
        IOException failure = expectThrows(IOException.class, () -> {
            try (SeekableInputStream stream = adapter.newStream()) {
                stream.seek(0);
            }
        });
        assertThat(failure.getMessage(), Matchers.containsString("Unexpected end of stream"));
        assertNull("still nothing stored", footerByteCache.get(adapter.cacheKey()));
    }

    /**
     * An object that fits the footer prefetch but not the cache's per-entry budget is also never served whole,
     * so there is nothing to refill. This band is operator-reachable: {@code FOOTER_CACHE_SIZE} takes any
     * positive size, and {@code maxEntryBytes} is a quarter of it, so a 128kb budget puts the per-entry cap at
     * 32 KiB and leaves every object between that and 64 KiB here. Without the budget conjunct in
     * {@code wholeFileIsFooter} the refill would issue a whole-object GET per scan and store nothing, because
     * {@link FooterByteCache#getOrLoad} rejects an entry over the cap after reading it.
     */
    public void testRefillWholeFileFooterIsNoopAboveTheCacheEntryBudget() throws IOException {
        FooterByteCache noEntries = FooterByteCache.fromSettings(
            Settings.builder().put(ExternalSourceCacheSettings.FOOTER_CACHE_SIZE.getKey(), "1b").build()
        );
        byte[] data = new byte[100];
        randomBytes(data);
        assertThat(data.length, Matchers.lessThanOrEqualTo(ParquetFormatReader.FOOTER_TAIL_PREFETCH_BYTES));
        assertThat(
            "the object must exceed the cache's per-entry budget",
            (long) data.length,
            Matchers.greaterThan(noEntries.maxEntryBytes())
        );
        List<long[]> rangeGets = new ArrayList<>();
        StorageObject storage = createRecordingRangeReadStorageObject(data, rangeGets);
        ParquetStorageObjectAdapter adapter = new ParquetStorageObjectAdapter(storage, noEntries, breaker);

        adapter.refillWholeFileFooter();
        assertEquals("an object the cache cannot hold is never read for a refill", 0, rangeGets.size());
        assertNull(noEntries.get(adapter.cacheKey()));
    }

    /**
     * The refill absorbs a storage read failure — its bytes have no consumer, so the scan's own GETs still
     * serve the query — but it does not absorb a breaker trip. A {@link CircuitBreakingException} from its own
     * window charge means the request breaker is at its limit while the scan is about to ask for far larger
     * buffers, so it propagates rather than being hidden at {@code debug}.
     */
    public void testRefillWholeFileFooterPropagatesABreakerTrip() {
        byte[] data = new byte[100];
        randomBytes(data);
        List<long[]> rangeGets = new ArrayList<>();
        StorageObject storage = createRecordingRangeReadStorageObject(data, rangeGets);
        CircuitBreaker exhausted = new LimitedBreaker("refill-breaker", ByteSizeValue.ofBytes(1));
        ParquetStorageObjectAdapter adapter = new ParquetStorageObjectAdapter(storage, footerByteCache, exhausted);

        expectThrows(CircuitBreakingException.class, adapter::refillWholeFileFooter);
        assertEquals("the trip happens before the GET is issued", 0, rangeGets.size());
        assertNull("a tripped refill stores nothing", footerByteCache.get(adapter.cacheKey()));
        assertEquals("the tripped refill leaves no stream open", 0, adapter.trackedStreamCount());
    }

    /** An object larger than the footer prefetch is never served whole from the cache, so there is nothing to refill. */
    public void testRefillWholeFileFooterIsNoopAboveFooterPrefetch() throws IOException {
        byte[] data = new byte[ParquetFormatReader.FOOTER_TAIL_PREFETCH_BYTES + 1];
        randomBytes(data);
        List<long[]> rangeGets = new ArrayList<>();
        StorageObject storage = createRecordingRangeReadStorageObject(data, rangeGets);
        ParquetStorageObjectAdapter adapter = new ParquetStorageObjectAdapter(storage, footerByteCache, breaker);

        adapter.refillWholeFileFooter();
        assertEquals(0, rangeGets.size());
        assertNull(footerByteCache.get(adapter.cacheKey()));
    }

    public void testSeekToEndOfTinyFileDoesNotFetch() throws IOException {
        byte[] data = new byte[100];
        randomBytes(data);
        List<long[]> rangeGets = new ArrayList<>();
        StorageObject storage = createRecordingRangeReadStorageObject(data, rangeGets);
        ParquetStorageObjectAdapter adapter = new ParquetStorageObjectAdapter(storage, footerByteCache, breaker);

        try (SeekableInputStream stream = adapter.newStream()) {
            stream.seek(data.length);
            assertEquals(-1, stream.read());
        }
        assertEquals(0, rangeGets.size());
    }

    public void testAdaptiveWindowReducesRangeRequests() throws IOException {
        int size = 6 * 1024 * 1024; // 6 MiB
        byte[] data = new byte[size];
        randomBytes(data);
        final int[] rangeReadCount = { 0 };

        StorageObject countingStorageObject = new StorageObject() {
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
            public InputStream newStream() throws IOException {
                throw new UnsupportedOperationException();
            }

            @Override
            public InputStream newStream(long position, long length) throws IOException {
                rangeReadCount[0]++;
                int pos = (int) position;
                int len = (int) Math.min(length, data.length - position);
                return new ByteArrayInputStream(data, pos, len);
            }

            @Override
            public long length() throws IOException {
                return data.length;
            }

            @Override
            public Instant lastModified() throws IOException {
                return Instant.ofEpochMilli(42);
            }

            @Override
            public boolean exists() throws IOException {
                return true;
            }

            @Override
            public StoragePath path() {
                return StoragePath.of("memory://test-adaptive.parquet");
            }
        };

        // With default 4 MiB window, reading 6 MiB requires 2 range requests
        ParquetStorageObjectAdapter defaultAdapter = new ParquetStorageObjectAdapter(countingStorageObject, footerByteCache, breaker);
        byte[] buf = new byte[size];
        try (SeekableInputStream stream = defaultAdapter.newStream()) {
            stream.readFully(buf);
        }
        int defaultReads = rangeReadCount[0];

        // With 8 MiB adaptive window, reading 6 MiB requires 1 range request
        rangeReadCount[0] = 0;
        ParquetStorageObjectAdapter adaptiveAdapter = ParquetStorageObjectAdapter.forRange(
            countingStorageObject,
            8 * 1024 * 1024L,
            footerByteCache,
            breaker
        );
        try (SeekableInputStream stream = adaptiveAdapter.newStream()) {
            stream.readFully(buf);
        }
        int adaptiveReads = rangeReadCount[0];

        assertTrue(
            "Adaptive window should use fewer range reads (" + adaptiveReads + ") than default (" + defaultReads + ")",
            adaptiveReads < defaultReads
        );
    }

    private void randomBytes(byte[] data) {
        random().nextBytes(data);
    }

    private StorageObject createStorageObject(byte[] data) {
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
            public InputStream newStream() throws IOException {
                return new ByteArrayInputStream(data);
            }

            @Override
            public InputStream newStream(long position, long length) throws IOException {
                // Simple implementation that doesn't support range reads
                throw new UnsupportedOperationException("Range reads not supported in basic test");
            }

            @Override
            public long length() throws IOException {
                return data.length;
            }

            @Override
            public Instant lastModified() throws IOException {
                return Instant.now();
            }

            @Override
            public boolean exists() throws IOException {
                return true;
            }

            @Override
            public StoragePath path() {
                return StoragePath.of("memory://test.parquet");
            }
        };
    }

    public void testNoPrefetchedDataDoesNotAffectBaseline() throws IOException {
        byte[] data = new byte[1024];
        randomBytes(data);
        StorageObject storage = createRangeReadStorageObject(data);
        ParquetStorageObjectAdapter adapter = new ParquetStorageObjectAdapter(storage, footerByteCache, breaker);

        try (SeekableInputStream stream = adapter.newStream()) {
            byte[] result = new byte[100];
            stream.readFully(result);
            for (int i = 0; i < result.length; i++) {
                assertEquals(data[i], result[i]);
            }
        }
    }

    /**
     * Verifies that seek+read sequences return correct data with the sliding window. This pattern
     * mimics Parquet's column chunk access: seek to a dictionary page, read it, seek to a data
     * page, read it, possibly seek back.
     */
    public void testDirectStreamSeekAndReadCorrectness() throws IOException {
        byte[] data = new byte[64 * 1024];
        randomBytes(data);
        StorageObject storageObject = createRangeReadStorageObject(data);

        ParquetStorageObjectAdapter adapter = new ParquetStorageObjectAdapter(storageObject, footerByteCache, breaker);

        try (SeekableInputStream stream = adapter.newStream()) {
            byte[] buf = new byte[1024];
            stream.seek(4096);
            stream.readFully(buf);
            for (int i = 0; i < buf.length; i++) {
                assertEquals("Mismatch at first read offset " + i, data[4096 + i], buf[i]);
            }

            stream.seek(32768);
            stream.readFully(buf);
            for (int i = 0; i < buf.length; i++) {
                assertEquals("Mismatch at second read offset " + i, data[32768 + i], buf[i]);
            }

            stream.seek(4096);
            stream.readFully(buf);
            for (int i = 0; i < buf.length; i++) {
                assertEquals("Mismatch at re-read offset " + i, data[4096 + i], buf[i]);
            }
        }
    }

    public void testReadFullyMatchesSourceData() throws IOException {
        int size = ParquetStorageObjectAdapter.DEFAULT_WINDOW_SIZE + 12345;
        byte[] data = new byte[size];
        randomBytes(data);
        StorageObject storageObject = createRangeReadStorageObject(data);

        ParquetStorageObjectAdapter adapter = new ParquetStorageObjectAdapter(storageObject, footerByteCache, breaker);
        byte[] result = new byte[size];
        try (SeekableInputStream stream = adapter.newStream()) {
            stream.readFully(result);
        }

        assertArrayEquals(data, result);
    }

    /**
     * Verifies that interleaved seek+read of non-overlapping regions returns correct bytes.
     * This simulates Parquet reading dictionary and data pages from different column chunks within a row group.
     */
    public void testInterleavedSeekReadParity() throws IOException {
        int size = 8 * 1024 * 1024;
        byte[] data = new byte[size];
        randomBytes(data);
        StorageObject storageObject = createRangeReadStorageObject(data);

        int[][] readRegions = new int[][] {
            { 0, 4096 },
            { 1_000_000, 8192 },
            { 4_000_000, 16384 },
            { 100, 2048 },
            { 7_000_000, 4096 },
            { 2_000_000, 32768 },
            { 0, 4096 } };

        ParquetStorageObjectAdapter adapter = new ParquetStorageObjectAdapter(storageObject, footerByteCache, breaker);

        try (SeekableInputStream stream = adapter.newStream()) {
            for (int[] region : readRegions) {
                int offset = region[0];
                int len = region[1];
                byte[] buf = new byte[len];
                stream.seek(offset);
                stream.readFully(buf);
                for (int i = 0; i < len; i++) {
                    assertEquals("Mismatch at offset " + (offset + i), data[offset + i], buf[i]);
                }
            }
        }
    }

    /**
     * Verifies single-byte reads, ByteBuffer reads, and skip operations on the windowed stream.
     */
    public void testDirectStreamOperations() throws IOException {
        byte[] data = new byte[] { 1, 2, 3, 4, 5, 6, 7, 8, 9, 10 };
        StorageObject storageObject = createRangeReadStorageObject(data);

        ParquetStorageObjectAdapter adapter = new ParquetStorageObjectAdapter(storageObject, footerByteCache, breaker);

        try (SeekableInputStream stream = adapter.newStream()) {
            assertEquals(1, stream.read());
            assertEquals(1, stream.getPos());

            long skipped = stream.skip(3);
            assertEquals(3, skipped);
            assertEquals(4, stream.getPos());
            assertEquals(5, stream.read());

            stream.seek(0);
            ByteBuffer buf = ByteBuffer.allocate(5);
            stream.readFully(buf);
            buf.flip();
            assertEquals(1, buf.get());
            assertEquals(2, buf.get());
            assertEquals(3, buf.get());
            assertEquals(4, buf.get());
            assertEquals(5, buf.get());

            stream.seek(8);
            ByteBuffer directBuf = ByteBuffer.allocateDirect(2);
            stream.readFully(directBuf);
            directBuf.flip();
            assertEquals(9, directBuf.get());
            assertEquals(10, directBuf.get());

            stream.seek(10);
            assertEquals(-1, stream.read());
        }
    }

    /**
     * End-to-end regression test for dictionary-encoded SUM/COUNT correctness. Writes a Parquet file with a
     * dictionary-encoded {@code int64} column spanning multiple data pages, then fully decodes
     * it via {@link ParquetFileReader} (which drives {@link ParquetStorageObjectAdapter}) across
     * multiple iterations. Any per-iteration drift in the sum or row count would be a correctness bug.
     */
    public void testDictionaryEncodedSumCountRegression() throws IOException {
        MessageType schema = Types.buildMessage().required(PrimitiveType.PrimitiveTypeName.INT64).named("v").named("dict_regression");

        long[] dictionary = new long[] { 1L, 2L, 3L, 5L, 7L, 11L, 13L, 17L };
        int rowCount = 25_000;
        long expectedSum = 0;
        Random seeded = new Random(0xD1C77L);
        long[] expectedValues = new long[rowCount];
        for (int i = 0; i < rowCount; i++) {
            expectedValues[i] = dictionary[seeded.nextInt(dictionary.length)];
            expectedSum += expectedValues[i];
        }

        byte[] parquetData = writeDictionaryEncodedInt64Parquet(schema, expectedValues);
        StorageObject storageObject = createRangeReadStorageObject(parquetData);

        int iterations = 20;
        // Use the Hadoop-free builder; the upstream ParquetReadOptions.builder() eagerly constructs
        // HadoopParquetConfiguration, which fails in tests without Woodstox on the classpath.
        ParquetReadOptions options = PlainParquetReadOptions.builder(new PlainCompressionCodecFactory()).build();
        for (int iter = 0; iter < iterations; iter++) {
            ParquetStorageObjectAdapter adapter = new ParquetStorageObjectAdapter(storageObject, footerByteCache, breaker);

            long sum = 0;
            long count = 0;
            try (ParquetFileReader reader = ParquetFileReader.open(adapter, options)) {
                MessageColumnIO columnIO = new ColumnIOFactory().getColumnIO(schema);
                PageReadStore pages;
                while ((pages = reader.readNextRowGroup()) != null) {
                    RecordReader<Group> recordReader = columnIO.getRecordReader(pages, new GroupRecordConverter(schema));
                    long rgRows = pages.getRowCount();
                    for (long i = 0; i < rgRows; i++) {
                        Group g = recordReader.read();
                        sum += g.getLong(0, 0);
                        count++;
                    }
                }
            }

            assertEquals("COUNT mismatch on iter=" + iter, rowCount, count);
            assertEquals("SUM mismatch on iter=" + iter, expectedSum, sum);
        }
    }

    /**
     * Concurrency stress test for {@code WindowedSeekableInputStream}: one adapter shared, many streams opened from it.
     *
     * <p>Target runtime is capped at ~2s regardless of hardware to avoid flaking CI.
     */
    public void testConcurrentWindowedReadsProduceConsistentBytes() throws Exception {
        int size = ParquetStorageObjectAdapter.DEFAULT_WINDOW_SIZE * 2 + 7777;
        byte[] data = new byte[size];
        randomBytes(data);
        StorageObject storageObject = createRangeReadStorageObject(data);

        ParquetStorageObjectAdapter adapter = new ParquetStorageObjectAdapter(storageObject, footerByteCache, breaker);

        int threadCount = randomIntBetween(4, 8);
        long deadline = System.nanoTime() + TimeUnit.SECONDS.toNanos(2);
        CountDownLatch start = new CountDownLatch(1);
        AtomicReference<AssertionError> firstFailure = new AtomicReference<>();
        List<Thread> threads = new ArrayList<>(threadCount);

        for (int t = 0; t < threadCount; t++) {
            long seed = randomLong();
            Thread thread = new Thread(() -> {
                try {
                    start.await();
                    Random rnd = new Random(seed);
                    try (SeekableInputStream stream = adapter.newStream()) {
                        while (System.nanoTime() < deadline && firstFailure.get() == null) {
                            int readLen = 1 + rnd.nextInt(4096);
                            int offset = rnd.nextInt(Math.max(1, size - readLen));
                            byte[] buf = new byte[readLen];
                            stream.seek(offset);
                            stream.readFully(buf);
                            for (int i = 0; i < readLen; i++) {
                                if (buf[i] != data[offset + i]) {
                                    firstFailure.compareAndSet(
                                        null,
                                        new AssertionError(
                                            "Stale byte at offset " + (offset + i) + ": expected " + data[offset + i] + " got " + buf[i]
                                        )
                                    );
                                    return;
                                }
                            }
                        }
                    }
                } catch (Throwable e) {
                    firstFailure.compareAndSet(null, new AssertionError("Unexpected exception on stress thread", e));
                }
            }, "parquet-window-stress-" + t);
            thread.start();
            threads.add(thread);
        }

        start.countDown();
        for (Thread thread : threads) {
            thread.join(TimeUnit.SECONDS.toMillis(10));
        }

        AssertionError failure = firstFailure.get();
        if (failure != null) {
            throw failure;
        }
    }

    /**
     * Two adapters wrapping different {@link StorageObject} instances (same path and bytes, different
     * {@code lastModified}) each perform their own range reads; there is no cross-adapter cache.
     */
    public void testSeparateAdaptersDoNotShareCachedRanges() throws IOException {
        // Must exceed the sliding window so a tail seek is a suffix GET, not a whole-file fill
        // (a whole-file fill is stored in FooterByteCache only when the object fits the footer prefetch).
        byte[] data = new byte[ParquetStorageObjectAdapter.DEFAULT_WINDOW_SIZE + 1024];
        random().nextBytes(data);
        final int[] rangeReadCount = { 0 };
        final StoragePath path = StoragePath.of("memory://cache-key-test.parquet");

        StorageObject obj1 = createCountingStorageObject(data, path, Instant.ofEpochMilli(100), rangeReadCount);
        StorageObject obj2 = createCountingStorageObject(data, path, Instant.ofEpochMilli(500), rangeReadCount);

        int tailStart = data.length - 1024;
        byte[] expected = java.util.Arrays.copyOfRange(data, tailStart, data.length);

        ParquetStorageObjectAdapter first = new ParquetStorageObjectAdapter(obj1, footerByteCache, breaker);
        try (SeekableInputStream s = first.newStream()) {
            s.seek(tailStart);
            byte[] w = new byte[1024];
            s.readFully(w);
            assertArrayEquals(expected, w);
        }
        assertEquals(1, rangeReadCount[0]);

        ParquetStorageObjectAdapter second = new ParquetStorageObjectAdapter(obj2, footerByteCache, breaker);
        try (SeekableInputStream s = second.newStream()) {
            s.seek(tailStart);
            byte[] w = new byte[1024];
            s.readFully(w);
            assertArrayEquals(expected, w);
        }
        // Both adapters share the same (path, length) cache key, so the second read is a cache hit
        assertEquals("FooterByteCache coalesces reads for same path+length", 1, rangeReadCount[0]);
    }

    /**
     * Concurrent threads each open their own stream and read the tail; without a shared footer cache,
     * each stream performs its own range read(s).
     */
    public void testConcurrentTailReadsEachIssueRangeRequest() throws Exception {
        // Must exceed the sliding window so a tail seek is a suffix GET, not a whole-file fill
        // (a whole-file fill is stored in FooterByteCache only when the object fits the footer prefetch).
        byte[] data = new byte[ParquetStorageObjectAdapter.DEFAULT_WINDOW_SIZE + 1024];
        random().nextBytes(data);
        final AtomicInteger rangeReadCount = new AtomicInteger();
        final StoragePath path = StoragePath.of("memory://thundering-herd.parquet");
        final java.util.concurrent.CyclicBarrier barrier = new java.util.concurrent.CyclicBarrier(8);

        StorageObject obj = new StorageObject() {
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
            public InputStream newStream() throws IOException {
                throw new UnsupportedOperationException();
            }

            @Override
            public InputStream newStream(long position, long length) throws IOException {
                rangeReadCount.incrementAndGet();
                try {
                    Thread.sleep(50);
                } catch (InterruptedException e) {
                    Thread.currentThread().interrupt();
                }
                int pos = (int) position;
                int len = (int) Math.min(length, data.length - position);
                return new ByteArrayInputStream(data, pos, len);
            }

            @Override
            public long length() throws IOException {
                return data.length;
            }

            @Override
            public Instant lastModified() throws IOException {
                return Instant.ofEpochMilli(100);
            }

            @Override
            public boolean exists() throws IOException {
                return true;
            }

            @Override
            public StoragePath path() {
                return path;
            }
        };

        int tailStart = data.length - 1024;
        byte[] expected = java.util.Arrays.copyOfRange(data, tailStart, data.length);
        Thread[] threads = new Thread[8];
        AtomicReference<AssertionError> firstFailure = new AtomicReference<>();

        for (int i = 0; i < 8; i++) {
            threads[i] = new Thread(() -> {
                try {
                    ParquetStorageObjectAdapter adapter = new ParquetStorageObjectAdapter(obj, footerByteCache, breaker);
                    try (SeekableInputStream s = adapter.newStream()) {
                        barrier.await(5, TimeUnit.SECONDS);
                        s.seek(tailStart);
                        byte[] w = new byte[1024];
                        s.readFully(w);
                        assertArrayEquals(expected, w);
                    }
                } catch (AssertionError e) {
                    firstFailure.compareAndSet(null, e);
                } catch (Exception e) {
                    firstFailure.compareAndSet(null, new AssertionError("Unexpected exception", e));
                }
            });
            threads[i].start();
        }

        for (Thread t : threads) {
            t.join(TimeUnit.SECONDS.toMillis(10));
        }

        AssertionError failure = firstFailure.get();
        if (failure != null) {
            throw failure;
        }
        // FooterByteCache provides thundering-herd protection: concurrent tail reads for the same
        // (path, length) coalesce into a single I/O via Cache.computeIfAbsent
        assertEquals("FooterByteCache should coalesce concurrent tail reads into one range read", 1, rangeReadCount.get());
    }

    /**
     * Validates byte-for-byte correctness of the sliding-window adapter against raw data:
     * sequential reads, forward seeks, backward seeks, tail reads, and ByteBuffer reads.
     */
    public void testAdapterReadsIdenticalBytesToVanillaStream() throws IOException {
        int n = 2 * ParquetStorageObjectAdapter.DEFAULT_WINDOW_SIZE + 500;
        byte[] data = new byte[n];
        random().nextBytes(data);
        StorageObject storageObject = createRangeReadStorageObject(data);
        ParquetStorageObjectAdapter adapter = new ParquetStorageObjectAdapter(storageObject, footerByteCache, breaker);

        try (SeekableInputStream stream = adapter.newStream()) {
            byte[] got = new byte[100];
            assertEquals(100, stream.read(got));
            assertArrayEquals(java.util.Arrays.copyOfRange(data, 0, 100), got);

            stream.seek(1000);
            got = new byte[200];
            assertEquals(200, stream.read(got, 0, 200));
            assertArrayEquals(java.util.Arrays.copyOfRange(data, 1000, 1200), got);

            stream.seek(500);
            got = new byte[300];
            assertEquals(300, stream.read(got, 0, 300));
            assertArrayEquals(java.util.Arrays.copyOfRange(data, 500, 800), got);

            long endPos = data.length - 100L;
            stream.seek(endPos);
            got = new byte[100];
            stream.readFully(got);
            assertArrayEquals(java.util.Arrays.copyOfRange(data, (int) endPos, data.length), got);

            stream.seek(2048);
            ByteBuffer buf = ByteBuffer.allocate(64);
            stream.readFully(buf);
            buf.flip();
            byte[] bufBytes = new byte[64];
            buf.get(bufBytes);
            assertArrayEquals(java.util.Arrays.copyOfRange(data, 2048, 2048 + 64), bufBytes);
        }
    }

    /**
     * Validates that the {@code forRange} adaptive-window adapter reads the same bytes as the
     * default-window adapter for identical seek/read sequences.
     */
    public void testForRangeAdapterReadsIdenticalData() throws IOException {
        int n = 2 * ParquetStorageObjectAdapter.DEFAULT_WINDOW_SIZE + 500;
        byte[] data = new byte[n];
        random().nextBytes(data);
        StorageObject storage = createRangeReadStorageObject(data);
        ParquetStorageObjectAdapter defaultAdapter = new ParquetStorageObjectAdapter(storage, footerByteCache, breaker);
        ParquetStorageObjectAdapter rangeAdapter = ParquetStorageObjectAdapter.forRange(
            storage,
            8L * 1024 * 1024,
            footerByteCache,
            breaker
        );

        try (SeekableInputStream a = defaultAdapter.newStream(); SeekableInputStream b = rangeAdapter.newStream()) {
            byte[] g1 = new byte[100];
            byte[] g2 = new byte[100];
            assertEquals(a.read(g1), b.read(g2));
            assertArrayEquals(g1, g2);

            a.seek(1000);
            b.seek(1000);
            g1 = new byte[200];
            g2 = new byte[200];
            assertEquals(a.read(g1, 0, 200), b.read(g2, 0, 200));
            assertArrayEquals(g1, g2);

            a.seek(500);
            b.seek(500);
            g1 = new byte[300];
            g2 = new byte[300];
            assertEquals(a.read(g1, 0, 300), b.read(g2, 0, 300));
            assertArrayEquals(g1, g2);

            long endPos = data.length - 100L;
            a.seek(endPos);
            b.seek(endPos);
            g1 = new byte[100];
            g2 = new byte[100];
            a.readFully(g1);
            b.readFully(g2);
            assertArrayEquals(g1, g2);
        }
    }

    private StorageObject createCountingStorageObject(byte[] data, StoragePath path, Instant lastModified, int[] counter) {
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
            public InputStream newStream() throws IOException {
                throw new UnsupportedOperationException("Full GET not supported");
            }

            @Override
            public InputStream newStream(long position, long length) throws IOException {
                counter[0]++;
                int pos = (int) position;
                int len = (int) Math.min(length, data.length - position);
                return new ByteArrayInputStream(data, pos, len);
            }

            @Override
            public long length() throws IOException {
                return data.length;
            }

            @Override
            public Instant lastModified() throws IOException {
                return lastModified;
            }

            @Override
            public boolean exists() throws IOException {
                return true;
            }

            @Override
            public StoragePath path() {
                return path;
            }
        };
    }

    /**
     * Writes a Parquet file with a single {@code int64} column that is dictionary-encoded and
     * spans multiple data pages (small page size + few distinct values forces this). Used by
     * {@link #testDictionaryEncodedSumCountRegression()}.
     */
    private static byte[] writeDictionaryEncodedInt64Parquet(MessageType schema, long[] values) throws IOException {
        ByteArrayOutputStream out = new ByteArrayOutputStream();
        OutputFile outputFile = createOutputFile(out);
        SimpleGroupFactory groupFactory = new SimpleGroupFactory(schema);

        PlainParquetConfiguration conf = new PlainParquetConfiguration();
        conf.set("parquet.enable.dictionary", "true");

        try (
            ParquetWriter<Group> writer = ExampleParquetWriter.builder(outputFile)
                .withConf(conf)
                .withCodecFactory(new PlainCompressionCodecFactory())
                .withType(schema)
                .withCompressionCodec(CompressionCodecName.UNCOMPRESSED)
                .withDictionaryEncoding(true)
                .withPageSize(4 * 1024)
                .withDictionaryPageSize(64 * 1024)
                .withRowGroupSize(64 * 1024L)
                .build()
        ) {
            for (long v : values) {
                Group g = groupFactory.newGroup();
                g.add("v", v);
                writer.write(g);
            }
        }
        return out.toByteArray();
    }

    // --- Pre-warmed chunk cache tests ---

    /**
     * Reads that fall entirely inside a pre-warmed chunk must be served from memory; the
     * StorageObject must observe zero range GETs for those bytes.
     */
    public void testPreWarmedChunkServesReadFromMemory() throws IOException {
        byte[] data = new byte[1024];
        randomBytes(data);
        AtomicInteger rangeReadCount = new AtomicInteger();
        StorageObject storage = createCountingRangeReadStorageObject(data, rangeReadCount);
        ParquetStorageObjectAdapter adapter = new ParquetStorageObjectAdapter(storage, footerByteCache, breaker);

        // Pre-warm the [200, 400) range.
        java.util.NavigableMap<Long, ColumnChunkPrefetcher.PrefetchedChunk> chunks = new java.util.TreeMap<>();
        ByteBuffer warm = ByteBuffer.wrap(data, 200, 200).slice();
        chunks.put(200L, new ColumnChunkPrefetcher.PrefetchedChunk(200L, 200L, warm));
        adapter.installPreWarmedChunks(chunks);

        try (SeekableInputStream stream = adapter.newStream()) {
            stream.seek(200);
            byte[] result = new byte[200];
            stream.readFully(result);
            for (int i = 0; i < 200; i++) {
                assertEquals("Mismatch at " + i, data[200 + i], result[i]);
            }
        }
        assertEquals("Pre-warmed read must not issue any range GETs", 0, rangeReadCount.get());
    }

    /**
     * When a position falls outside any pre-warmed chunk, the stream must fall back to the normal
     * range read path so correctness is preserved even with a pre-warm cache installed.
     */
    public void testReadOutsidePreWarmedChunkFallsBackToIO() throws IOException {
        byte[] data = new byte[2048];
        randomBytes(data);
        AtomicInteger rangeReadCount = new AtomicInteger();
        StorageObject storage = createCountingRangeReadStorageObject(data, rangeReadCount);
        ParquetStorageObjectAdapter adapter = new ParquetStorageObjectAdapter(storage, footerByteCache, breaker);

        // Pre-warm the [100, 200) range only.
        java.util.NavigableMap<Long, ColumnChunkPrefetcher.PrefetchedChunk> chunks = new java.util.TreeMap<>();
        ByteBuffer warm = ByteBuffer.wrap(data, 100, 100).slice();
        chunks.put(100L, new ColumnChunkPrefetcher.PrefetchedChunk(100L, 100L, warm));
        adapter.installPreWarmedChunks(chunks);

        try (SeekableInputStream stream = adapter.newStream()) {
            // Read inside pre-warm.
            stream.seek(120);
            byte[] inside = new byte[40];
            stream.readFully(inside);
            for (int i = 0; i < 40; i++) {
                assertEquals(data[120 + i], inside[i]);
            }
            // Read outside pre-warm — must hit storage.
            stream.seek(1500);
            byte[] outside = new byte[100];
            stream.readFully(outside);
            for (int i = 0; i < 100; i++) {
                assertEquals(data[1500 + i], outside[i]);
            }
        }
        assertEquals("Read outside the pre-warmed range must trigger a single range GET", 1, rangeReadCount.get());
    }

    /**
     * Already-open streams must observe a pre-warm install that happens after their construction.
     * This matches the production wiring: parquet-mr opens the file's {@code SeekableInputStream}
     * during {@code ParquetFileReader.open}, before the caller has had a chance to install the
     * pre-warm map; without this property the optimization would be silently bypassed.
     */
    public void testInstallAfterStreamOpenAffectsAlreadyOpenStream() throws IOException {
        byte[] data = new byte[1024];
        randomBytes(data);
        AtomicInteger rangeReadCount = new AtomicInteger();
        StorageObject storage = createCountingRangeReadStorageObject(data, rangeReadCount);
        ParquetStorageObjectAdapter adapter = new ParquetStorageObjectAdapter(storage, footerByteCache, breaker);

        try (SeekableInputStream stream = adapter.newStream()) {
            // Install the pre-warm map AFTER the stream was opened — same shape as production.
            java.util.NavigableMap<Long, ColumnChunkPrefetcher.PrefetchedChunk> chunks = new java.util.TreeMap<>();
            ByteBuffer warm = ByteBuffer.wrap(data, 0, 256).slice();
            chunks.put(0L, new ColumnChunkPrefetcher.PrefetchedChunk(0L, 256L, warm));
            adapter.installPreWarmedChunks(chunks);

            byte[] result = new byte[256];
            stream.readFully(result);
            for (int i = 0; i < 256; i++) {
                assertEquals(data[i], result[i]);
            }
        }
        assertEquals("Already-open stream must observe the post-construction install", 0, rangeReadCount.get());
    }

    /**
     * A clear that happens after the open stream consumed the pre-warmed bytes must still allow
     * subsequent reads to hit storage when needed. This guards the production sequence of
     * install → row-group filter → clear.
     */
    public void testClearAfterUseFallsBackToStorage() throws IOException {
        byte[] data = new byte[2048];
        randomBytes(data);
        AtomicInteger rangeReadCount = new AtomicInteger();
        StorageObject storage = createCountingRangeReadStorageObject(data, rangeReadCount);
        ParquetStorageObjectAdapter adapter = new ParquetStorageObjectAdapter(storage, footerByteCache, breaker);

        java.util.NavigableMap<Long, ColumnChunkPrefetcher.PrefetchedChunk> chunks = new java.util.TreeMap<>();
        ByteBuffer warm = ByteBuffer.wrap(data, 0, 256).slice();
        chunks.put(0L, new ColumnChunkPrefetcher.PrefetchedChunk(0L, 256L, warm));
        adapter.installPreWarmedChunks(chunks);

        try (SeekableInputStream stream = adapter.newStream()) {
            byte[] warmBuf = new byte[256];
            stream.readFully(warmBuf);
            assertEquals(0, rangeReadCount.get());

            adapter.installPreWarmedChunks(null);

            stream.seek(1024);
            byte[] coldBuf = new byte[256];
            stream.readFully(coldBuf);
            for (int i = 0; i < 256; i++) {
                assertEquals(data[1024 + i], coldBuf[i]);
            }
            assertEquals("After clearing, reads outside the (now-detached) cache must hit storage", 1, rangeReadCount.get());
        }
    }

    /**
     * Streams created after an install observe the new map; streams created after a clear go
     * straight to the storage backend.
     */
    public void testInstallNullDisablesPreWarmForNewStreams() throws IOException {
        byte[] data = new byte[1024];
        randomBytes(data);
        AtomicInteger rangeReadCount = new AtomicInteger();
        StorageObject storage = createCountingRangeReadStorageObject(data, rangeReadCount);
        ParquetStorageObjectAdapter adapter = new ParquetStorageObjectAdapter(storage, footerByteCache, breaker);

        java.util.NavigableMap<Long, ColumnChunkPrefetcher.PrefetchedChunk> chunks = new java.util.TreeMap<>();
        ByteBuffer warm = ByteBuffer.wrap(data, 0, 256).slice();
        chunks.put(0L, new ColumnChunkPrefetcher.PrefetchedChunk(0L, 256L, warm));
        adapter.installPreWarmedChunks(chunks);
        adapter.installPreWarmedChunks(null);

        byte[] result = new byte[256];
        try (SeekableInputStream stream = adapter.newStream()) {
            stream.readFully(result);
            for (int i = 0; i < 256; i++) {
                assertEquals(data[i], result[i]);
            }
        }
        assertEquals("Stream created after clear must use real I/O", 1, rangeReadCount.get());
    }

    /**
     * Empty maps are treated identically to {@code null}: the cache is left disabled. This
     * matches the production wiring where {@link PreloadedRowGroupMetadata} returns an empty map
     * when no predicate columns were supplied.
     */
    public void testInstallEmptyMapTreatedAsCleared() throws IOException {
        byte[] data = new byte[256];
        randomBytes(data);
        AtomicInteger rangeReadCount = new AtomicInteger();
        StorageObject storage = createCountingRangeReadStorageObject(data, rangeReadCount);
        ParquetStorageObjectAdapter adapter = new ParquetStorageObjectAdapter(storage, footerByteCache, breaker);

        adapter.installPreWarmedChunks(new java.util.TreeMap<>());

        byte[] result = new byte[256];
        try (SeekableInputStream stream = adapter.newStream()) {
            stream.readFully(result);
        }
        assertEquals("Empty map must not enable the pre-warm fast path", 1, rangeReadCount.get());
    }

    private StorageObject createRecordingRangeReadStorageObject(byte[] data, List<long[]> rangeGets) {
        return createRecordingRangeReadStorageObject(data, rangeGets, "v1");
    }

    /** As {@link #createRecordingRangeReadStorageObject(byte[], List)}, with the given version. */
    private StorageObject createRecordingRangeReadStorageObject(byte[] data, List<long[]> rangeGets, String version) {
        return new StorageObject() {
            @Override
            public void attachMetrics(ExternalSourceMetrics metrics, String scheme) {}

            @Override
            public String version() {
                return version;
            }

            @Override
            public StorageIdentity storageIdentity() {
                return AbstractTestStorageObject.NOOP;
            }

            @Override
            public InputStream newStream() {
                throw new UnsupportedOperationException("Full GET not supported in recording harness");
            }

            @Override
            public InputStream newStream(long position, long length) {
                rangeGets.add(new long[] { position, length });
                int pos = (int) position;
                int len = (int) Math.min(length, data.length - position);
                return new ByteArrayInputStream(data, pos, len);
            }

            @Override
            public long length() {
                return data.length;
            }

            @Override
            public Instant lastModified() {
                return Instant.ofEpochMilli(0);
            }

            @Override
            public boolean exists() {
                return true;
            }

            @Override
            public StoragePath path() {
                return StoragePath.of("memory://window-clamp-test.parquet");
            }
        };
    }

    private StorageObject createCountingRangeReadStorageObject(byte[] data, AtomicInteger counter) {
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
                throw new UnsupportedOperationException("Full GET not supported in counting harness");
            }

            @Override
            public InputStream newStream(long position, long length) {
                counter.incrementAndGet();
                int pos = (int) position;
                int len = (int) Math.min(length, data.length - position);
                return new ByteArrayInputStream(data, pos, len);
            }

            @Override
            public long length() {
                return data.length;
            }

            @Override
            public Instant lastModified() {
                return Instant.ofEpochMilli(0);
            }

            @Override
            public boolean exists() {
                return true;
            }

            @Override
            public StoragePath path() {
                return StoragePath.of("memory://prewarm-test.parquet");
            }
        };
    }

    private static OutputFile createOutputFile(ByteArrayOutputStream out) {
        return new OutputFile() {
            @Override
            public PositionOutputStream create(long blockSizeHint) {
                return new PositionOutputStream() {
                    @Override
                    public long getPos() {
                        return out.size();
                    }

                    @Override
                    public void write(int b) {
                        out.write(b);
                    }

                    @Override
                    public void write(byte[] b, int off, int len) {
                        out.write(b, off, len);
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
                return "memory://dict-regression.parquet";
            }
        };
    }

    private StorageObject createRangeReadStorageObject(byte[] data) {
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
            public InputStream newStream() throws IOException {
                return new ByteArrayInputStream(data);
            }

            @Override
            public InputStream newStream(long position, long length) throws IOException {
                int pos = (int) position;
                int len = (int) Math.min(length, data.length - position);
                return new ByteArrayInputStream(data, pos, len);
            }

            @Override
            public long length() throws IOException {
                return data.length;
            }

            @Override
            public Instant lastModified() throws IOException {
                return Instant.now();
            }

            @Override
            public boolean exists() throws IOException {
                return true;
            }

            @Override
            public StoragePath path() {
                return StoragePath.of("memory://test.parquet");
            }
        };
    }

    @SuppressWarnings("AssertWithSideEffects")
    private static boolean assertionsEnabled() {
        boolean enabled = false;
        assert enabled = true;
        return enabled;
    }

    /**
     * Split windows snap to the two region-friendly sizes: a range that fits the default gets the default, any larger
     * range gets the cap. An in-between window (say 6 MiB) would be humongous and occupy 8 MiB of heap at 4 MiB and
     * 8 MiB G1 regions anyway.
     */
    public void testForRangeWindowSnapsToRegionFriendlySizes() {
        int defaultWindow = ParquetStorageObjectAdapter.DEFAULT_WINDOW_SIZE;
        int maxWindow = ParquetStorageObjectAdapter.MAX_WINDOW_SIZE;
        assertEquals(HeapFootprint.regionFriendlyLength(4 * 1024 * 1024), defaultWindow);
        assertEquals(HeapFootprint.regionFriendlyLength(8 * 1024 * 1024), maxWindow);
        assertEquals(defaultWindow, ParquetStorageObjectAdapter.windowSizeForRange(0));
        assertEquals(defaultWindow, ParquetStorageObjectAdapter.windowSizeForRange(randomLongBetween(1, defaultWindow)));
        assertEquals(maxWindow, ParquetStorageObjectAdapter.windowSizeForRange(defaultWindow + 1L));
        assertEquals(maxWindow, ParquetStorageObjectAdapter.windowSizeForRange(randomLongBetween(defaultWindow + 1L, maxWindow)));
        assertEquals(maxWindow, ParquetStorageObjectAdapter.windowSizeForRange(randomLongBetween(maxWindow, Long.MAX_VALUE)));
    }

}
