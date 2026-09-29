/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.datasources;

import org.elasticsearch.common.breaker.NoopCircuitBreaker;
import org.elasticsearch.common.util.BigArrays;
import org.elasticsearch.compute.data.BlockFactory;
import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.xpack.esql.datasource.csv.CsvFormatReader;
import org.elasticsearch.xpack.esql.datasource.gzip.GzipDecompressionCodec;
import org.elasticsearch.xpack.esql.datasources.spi.SegmentableFormatReader;
import org.elasticsearch.xpack.esql.datasources.spi.StorageObject;
import org.elasticsearch.xpack.esql.datasources.spi.StorageObjectMetrics;
import org.elasticsearch.xpack.esql.datasources.spi.StoragePath;
import org.hamcrest.Matchers;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.net.SocketException;
import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;
import java.time.Instant;
import java.util.List;
import java.util.Map;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.zip.GZIPOutputStream;

/**
 * End-to-end regression guard for the {@link StorageObject#abortStream(InputStream)} signal
 * propagating through the full decorator chain used in production:
 * <pre>
 *     RetryableStorageObject
 *       -> DecompressingStorageObject (gzip)
 *         -> S3-like drain-on-close raw stream
 * </pre>
 * <p>
 * The original bug was a single decorator in the chain silently swallowing the abort signal
 * (falling back to a {@code close()} which on S3 drains the entire response body). With every
 * layer correctly overriding {@code abortStream}, partial reads must not drain the raw stream.
 */
public class StorageObjectAbortChainTests extends ESTestCase {

    /**
     * Builds the production stack of decorators over a drain-simulating raw storage object,
     * reads a small prefix of a multi-MB gzipped payload, then aborts. Asserts the raw stream
     * was not drained through any layer of the chain.
     */
    public void testAbortPropagatesThroughDecoratorChainWithoutDrain() throws IOException {
        StringBuilder csv = new StringBuilder();
        for (int i = 0; i < 200_000; i++) {
            csv.append("id_").append(i).append(",name_").append(i).append(",").append(i * 1.5).append("\n");
        }
        byte[] original = csv.toString().getBytes(StandardCharsets.UTF_8);
        byte[] compressed = gzip(original);
        assertThat("compressed payload must be much larger than the prefix we read", compressed.length, Matchers.greaterThan(200_000));

        DrainSimulatingStorageObject.Tracking tracking = new DrainSimulatingStorageObject.Tracking();
        StorageObject raw = DrainSimulatingStorageObject.create(compressed, tracking);

        // Production order (inner-to-outer): decompressing wraps the S3-like raw, then retry. Outer decorators
        // delegate their abort through to the inner ones; if any layer regresses to a draining close() the
        // assertions below trip.
        StorageObject chain = new RetryableStorageObject(
            new DecompressingStorageObject(raw, new GzipDecompressionCodec()),
            new RetryPolicy(3, 1, 10)
        );

        InputStream stream = chain.newStream();

        try {
            byte[] prefix = new byte[4096];
            int n = stream.read(prefix);
            assertThat("expected to read some decompressed prefix bytes", n, Matchers.greaterThan(0));
        } finally {
            chain.abortStream(stream);
        }

        assertTrue("raw stream must have been aborted (not closed-with-drain)", tracking.aborted.get());
        assertThat(
            "abort must not drain the raw stream; consumed "
                + tracking.bytesConsumed.get()
                + " of "
                + compressed.length
                + " compressed bytes",
            tracking.bytesConsumed.get(),
            Matchers.lessThan((long) compressed.length / 2)
        );
    }

    /**
     * Full read of a gzip object through the production read chain (see {@link #readChain}). The release's
     * end-of-body read must pass through every decorator to the raw body before the abort, so S3 pools the
     * connection instead of discarding it.
     */
    public void testFullGzipReadReachesEndOfBodyBeforeAbortThroughDecoratorChain() throws IOException {
        StringBuilder csv = new StringBuilder();
        int rows = between(1, 200_000);
        for (int i = 0; i < rows; i++) {
            csv.append("id_").append(i).append(",name_").append(i).append(",").append(i * 1.5).append("\n");
        }
        byte[] original = csv.toString().getBytes(StandardCharsets.UTF_8);
        byte[] compressed = gzip(original);

        DrainSimulatingStorageObject.Tracking tracking = new DrainSimulatingStorageObject.Tracking();
        StorageObject raw = DrainSimulatingStorageObject.create(compressed, tracking);
        StorageObject chain = readChain(new RetryableStorageObject(new ConcurrencyLimitedStorageObject(raw, limiter()), retryPolicy()));

        try (InputStream stream = chain.newStream()) {
            assertArrayEquals(original, stream.readAllBytes());
        }

        assertEquals("abortStream must be invoked exactly once", 1, tracking.abortCalls.get());
        assertTrue("a fully decoded body must reach end-of-body before the abort", tracking.endOfBodyReadBeforeAbort.get());
        assertEquals(compressed.length, tracking.bytesConsumed.get());
    }

    /**
     * The release's end-of-body read is best effort: if the connection resets on it, the retry layer must not
     * resume (backoff sleep plus a new GET inside {@code close()}) for a stream that is aborted right after.
     */
    public void testEndOfBodyReadFaultAfterFullGzipReadDoesNotResume() throws IOException {
        StringBuilder csv = new StringBuilder();
        for (int i = 0; i < 10_000; i++) {
            csv.append("id_").append(i).append(",name_").append(i).append("\n");
        }
        byte[] original = csv.toString().getBytes(StandardCharsets.UTF_8);
        byte[] compressed = gzip(original);

        ResetAtEndOfBodyStorageObject raw = new ResetAtEndOfBodyStorageObject(compressed);
        RetryableStorageObject retryable = new RetryableStorageObject(new ConcurrencyLimitedStorageObject(raw, limiter()), retryPolicy());
        StorageObject chain = readChain(retryable);

        InputStream stream = chain.newStream();
        assertArrayEquals(original, stream.readAllBytes());
        // On JDK 23 to 26 GZIPInputStream probes for a next member while decoding, so that probe (not the release)
        // hits the reset and the retry layer resumes during the read itself. Only what close() adds is the release's.
        int opensBeforeClose = raw.opens.get();
        long retriesBeforeClose = retryable.metrics().retryCount();
        stream.close();

        assertTrue("the end-of-body read must have hit the reset", raw.endOfBodyFaulted.get());
        assertEquals("a fault on the end-of-body read must not re-open the object", opensBeforeClose, raw.opens.get());
        assertEquals("a fault on the end-of-body read must not count a retry", retriesBeforeClose, retryable.metrics().retryCount());
        assertEquals("abortStream must be invoked exactly once", 1, raw.abortCalls.get());
    }

    /**
     * Same as {@link #testEndOfBodyReadFaultAfterFullGzipReadDoesNotResume} through the split read chain, where
     * {@code FileSplitProvider#storageObjectForSplit} puts a {@link RangeStorageObject} view (even at offset 0)
     * between {@link DecompressingStorageObject} and the per-query budget. The decoder stops at the end of the body
     * without reading it to {@code -1} on every JDK, so only the release's end-of-body read hits the reset.
     */
    public void testEndOfBodyReadFaultThroughSplitRangeViewDoesNotResume() throws IOException {
        byte[] body = randomByteArrayOfLength(between(1, 100_000));

        ResetAtEndOfBodyStorageObject raw = new ResetAtEndOfBodyStorageObject(body);
        RetryableStorageObject retryable = new RetryableStorageObject(new ConcurrencyLimitedStorageObject(raw, limiter()), retryPolicy());
        StorageObject chain = new DecompressingStorageObject(
            new RangeStorageObject(new QueryBudgetedStorageObject(retryable, new QueryConcurrencyBudget(3, 60_000L, null)), 0, body.length),
            new StopAtLengthDecompressionCodec("test", body.length)
        );

        try (InputStream stream = chain.newStream()) {
            assertArrayEquals(body, stream.readAllBytes());
            assertFalse("the decoder must stop before the end-of-body read", raw.endOfBodyFaulted.get());
        }

        assertTrue("the end-of-body read must have hit the reset", raw.endOfBodyFaulted.get());
        assertEquals("a fault on the end-of-body read must not re-open the object", 1, raw.opens.get());
        assertEquals("a fault on the end-of-body read must not count a retry", 0, retryable.metrics().retryCount());
        assertEquals("abortStream must be invoked exactly once", 1, raw.abortCalls.get());
    }

    /**
     * Regression guard for {@link RecordBoundaryProbe#probeAt} through the same decorator chain used for
     * uncompressed text files on object storage. With little enough of its window left to transfer a boundary probe
     * deliberately does <em>not</em> abort: it opens a bounded window, then drains and closes it so the connection
     * returns to the pool for the next probe (aborting would drop the connection and cost a handshake per probe).
     * What the chain must preserve is the window's bound, so each probe transfers its own window and not a range
     * opened to end-of-file. A decorator that ignored the requested length would trip the assertion here.
     */
    public void testMacroSplitDiscoveryDrainsBoundedWindowsThroughDecoratorChain() throws IOException {
        // A stride at the drain threshold caps every window there too, so no probe has more than
        // MAX_DRAIN_BYTES left to transfer and all of them drain.
        long stride = RecordBoundaryProbe.MAX_DRAIN_BYTES;
        String row = "0123456789,0123456789,012345678\n";
        byte[] payload = row.repeat(Math.toIntExact(32 * stride / row.length())).getBytes(StandardCharsets.UTF_8);
        long fileLength = payload.length;

        DrainSimulatingStorageObject.Tracking tracking = new DrainSimulatingStorageObject.Tracking();
        StorageObject raw = DrainSimulatingStorageObject.create(payload, tracking);

        StorageObject chain = new RetryableStorageObject(raw, new RetryPolicy(3, 1, 10));

        var blockFactory = BlockFactory.builder(BigArrays.NON_RECYCLING_INSTANCE).breaker(new NoopCircuitBreaker("test")).build();
        // Plain mode: the abort-chain contract is format-agnostic; macro-split discovery now refuses non-strided
        // (default/quoted) CSV. Plain CSV keeps strided probing.
        SegmentableFormatReader csvReader = (SegmentableFormatReader) new CsvFormatReader(blockFactory).withConfig(Map.of("mode", "plain"));

        long minSegment = csvReader.minimumSegmentSize();
        List<Long> positions = RecordBoundaryProbe.stridedPositions(fileLength, stride, minSegment);
        List<Long> starts = RecordBoundaryProbe.reduce(
            RecordBoundaryProbe.stridedOutcomes(
                csvReader.recordSplitter(SegmentableFormatReader.DEFAULT_MAX_RECORD_BYTES),
                chain,
                fileLength,
                positions,
                minSegment,
                stride,
                SegmentableFormatReader.DEFAULT_MAX_RECORD_BYTES,
                RecordBoundaryProbe.DEFAULT_SPLIT_PROBE_WINDOW,
                () -> false
            )
        );

        assertThat("expected multiple macro-split boundaries", starts.size(), Matchers.greaterThan(1));
        assertEquals("a boundary probe pools its connection by draining, never aborts", 0, tracking.abortCalls.get());
        assertTrue("probe streams must be closed through the chain", tracking.closed.get());
        // The whole window, because each probe drained it, and no more than it, because the chain preserved the
        // requested length rather than opening a range to end-of-file.
        assertEquals(
            "each probe must drain its own bounded window through the chain, of a " + fileLength + " byte file",
            positions.size() * stride,
            tracking.bytesConsumed.get()
        );
    }

    /**
     * Regression guard for {@link ParallelParsingCoordinator#computeSegments}: in-file parallel parsing probes
     * record boundaries through the same decorator chain used for uncompressed object-store reads. This fixture's
     * rows are short, so a probe leaves nearly all of its window untransferred, which is more than
     * {@link RecordBoundaryProbe#MAX_DRAIN_BYTES}: every probe aborts rather than draining, and the abort must
     * reach the raw object through the chain.
     */
    public void testComputeSegmentsAbortPropagatesThroughDecoratorChainWithoutDrain() throws IOException {
        StringBuilder csv = new StringBuilder("id,name\n");
        for (int i = 0; i < 200_000; i++) {
            csv.append(i).append(",value_").append(i).append('\n');
        }
        byte[] payload = csv.toString().getBytes(StandardCharsets.UTF_8);
        long fileLength = payload.length;
        assertThat("payload must exceed minimum segment size", fileLength, Matchers.greaterThan(2L * 1024 * 1024));

        DrainSimulatingStorageObject.Tracking tracking = new DrainSimulatingStorageObject.Tracking();
        StorageObject raw = DrainSimulatingStorageObject.create(payload, tracking);

        StorageObject chain = new RetryableStorageObject(raw, new RetryPolicy(3, 1, 10));

        var blockFactory = BlockFactory.builder(BigArrays.NON_RECYCLING_INSTANCE).breaker(new NoopCircuitBreaker("test")).build();
        // Plain mode: the abort-chain contract is format-agnostic; computeSegments now refuses non-strided
        // (default/quoted) CSV. Plain CSV keeps strided probing.
        SegmentableFormatReader csvReader = (SegmentableFormatReader) new CsvFormatReader(blockFactory).withConfig(Map.of("mode", "plain"));

        List<long[]> segments = ParallelParsingCoordinator.computeSegments(csvReader, chain, fileLength, 4, csvReader.minimumSegmentSize());

        long stride = Math.max(fileLength / 4, csvReader.minimumSegmentSize());
        assertThat(
            "a probe here must be left with more than the drain threshold to transfer, or it is no longer testing the abort path",
            segmentProbeWindow(fileLength, stride),
            Matchers.greaterThan(RecordBoundaryProbe.MAX_DRAIN_BYTES)
        );
        assertThat("expected multiple parse segments", segments.size(), Matchers.greaterThan(1));
        assertTrue("each probe must abort the raw stream", tracking.abortCalls.get() >= segments.size() - 1);
        assertThat(
            "segment probes must not drain range streams; consumed "
                + tracking.bytesConsumed.get()
                + " of "
                + fileLength
                + " bytes across "
                + tracking.abortCalls.get()
                + " probes",
            tracking.bytesConsumed.get(),
            Matchers.lessThan(fileLength / 2)
        );
    }

    /**
     * The window {@link ParallelParsingCoordinator#computeSegments} opens at its first probe offset, derived the
     * way it derives it: the record cap bounds it, and is passed as the width too because segmentation advances
     * from each boundary it finds and so reads no byte twice however wide its windows are.
     */
    private static long segmentProbeWindow(long fileLength, long stride) {
        return RecordBoundaryProbe.probeWindow(
            stride,
            fileLength,
            SegmentableFormatReader.DEFAULT_MAX_RECORD_BYTES,
            SegmentableFormatReader.DEFAULT_MAX_RECORD_BYTES
        );
    }

    /**
     * The read chain for a compressed object on a blob store, outer to inner: {@link DecompressingStorageObject},
     * the per-query {@link QueryBudgetedStorageObject} ({@code FileSourceFactory}), then {@code retryable}, which the
     * caller builds as {@link RetryableStorageObject} over {@link ConcurrencyLimitedStorageObject} over the raw
     * object ({@code StorageProviderRegistry#wrapProvider}).
     */
    private static StorageObject readChain(RetryableStorageObject retryable) {
        return new DecompressingStorageObject(
            new QueryBudgetedStorageObject(retryable, new QueryConcurrencyBudget(3, 60_000L, null)),
            new GzipDecompressionCodec()
        );
    }

    private static ConcurrencyLimiter limiter() {
        return new ConcurrencyLimiter("s3", new ExternalSourceSettings.BlobStoreConcurrency(3, false));
    }

    private static RetryPolicy retryPolicy() {
        return new RetryPolicy(3, 1, 10);
    }

    /**
     * Serves {@code bytes} in full, then throws a connection reset instead of returning {@code -1}: the shape of
     * an S3 connection that drops just as the client reads the end of the body. Opens at position 0 serve the same
     * body; resume opens return an empty body.
     */
    private static final class ResetAtEndOfBodyStorageObject implements StorageObject {
        private final byte[] bytes;
        final AtomicInteger opens = new AtomicInteger();
        final AtomicInteger abortCalls = new AtomicInteger();
        final AtomicBoolean endOfBodyFaulted = new AtomicBoolean();

        ResetAtEndOfBodyStorageObject(byte[] bytes) {
            this.bytes = bytes;
        }

        @Override
        public InputStream newStream() {
            opens.incrementAndGet();
            ByteArrayInputStream body = new ByteArrayInputStream(bytes);
            return new InputStream() {
                @Override
                public int read() throws IOException {
                    byte[] one = new byte[1];
                    int n = read(one, 0, 1);
                    return n == -1 ? -1 : (one[0] & 0xFF);
                }

                @Override
                public int read(byte[] b, int off, int len) throws IOException {
                    int n = body.read(b, off, len);
                    if (n == -1) {
                        endOfBodyFaulted.set(true);
                        throw new SocketException("Connection reset");
                    }
                    return n;
                }
            };
        }

        @Override
        public InputStream newStream(long position, long length) {
            if (position == 0) {
                // The initial open through a range view; resumes open at the delivered offset.
                return newStream();
            }
            opens.incrementAndGet();
            return InputStream.nullInputStream();
        }

        @Override
        public void abortStream(InputStream stream) throws IOException {
            abortCalls.incrementAndGet();
            stream.close();
        }

        @Override
        public long length() {
            return bytes.length;
        }

        @Override
        public Instant lastModified() {
            return null;
        }

        @Override
        public boolean exists() {
            return true;
        }

        @Override
        public StoragePath path() {
            return StoragePath.of("s3://bucket/reset-at-end.csv.gz");
        }

        @Override
        public int readBytes(long position, ByteBuffer target) {
            throw new UnsupportedOperationException();
        }

        @Override
        public StorageObjectMetrics metrics() {
            return new StorageObjectMetrics(opens.get(), 0, 0, 0);
        }
    }

    private static byte[] gzip(byte[] input) throws IOException {
        ByteArrayOutputStream baos = new ByteArrayOutputStream();
        try (GZIPOutputStream gzipOut = new GZIPOutputStream(baos)) {
            gzipOut.write(input);
        }
        return baos.toByteArray();
    }
}
