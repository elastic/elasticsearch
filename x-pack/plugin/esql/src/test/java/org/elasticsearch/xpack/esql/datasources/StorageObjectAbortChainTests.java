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
import org.elasticsearch.xpack.esql.datasources.spi.StorageObject;
import org.elasticsearch.xpack.esql.datasources.spi.StoragePath;
import org.hamcrest.Matchers;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.nio.charset.StandardCharsets;
import java.time.Instant;
import java.util.List;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;
import java.util.zip.GZIPOutputStream;

/**
 * End-to-end regression guard for the {@link StorageObject#abortStream(InputStream)} signal
 * propagating through the full decorator chain used in production:
 * <pre>
 *     RetryableStorageObject
 *       -> ConcurrencyLimitedStorageObject
 *         -> QueryBudgetedStorageObject
 *           -> DecompressingStorageObject (gzip)
 *             -> S3-like drain-on-close raw stream
 * </pre>
 * <p>
 * The original bug was a single decorator in the chain silently swallowing the abort signal
 * (falling back to a {@code close()} which on S3 drains the entire response body). With every
 * layer correctly overriding {@code abortStream}, partial reads must (a) not drain the raw
 * stream and (b) release every layer's resource accounting (permits, budget).
 */
public class StorageObjectAbortChainTests extends ESTestCase {

    /**
     * Builds the production stack of decorators over a drain-simulating raw storage object,
     * reads a small prefix of a multi-MB gzipped payload, then aborts. Asserts the raw stream
     * was not drained and that every decorator released its accounting.
     */
    public void testAbortPropagatesThroughDecoratorChainWithoutDrain() throws IOException {
        StringBuilder csv = new StringBuilder();
        for (int i = 0; i < 200_000; i++) {
            csv.append("id_").append(i).append(",name_").append(i).append(",").append(i * 1.5).append("\n");
        }
        byte[] original = csv.toString().getBytes(StandardCharsets.UTF_8);
        byte[] compressed = gzip(original);
        assertThat("compressed payload must be much larger than the prefix we read", compressed.length, Matchers.greaterThan(200_000));

        AtomicLong rawBytesConsumed = new AtomicLong();
        AtomicBoolean rawAborted = new AtomicBoolean(false);
        StorageObject raw = drainSimulatingStorageObject(compressed, rawBytesConsumed, rawAborted, new AtomicInteger());

        // Production order (inner-to-outer): decompressing wraps the S3-like raw, then the
        // global hard cap, then the per-query budget, then retry. Outer decorators delegate
        // their abort through to the inner ones; if any layer regresses to a draining
        // close() the assertions below will trip.
        QueryConcurrencyBudget budget = new QueryConcurrencyBudget(4, 60_000L, null);
        ConcurrencyLimiter limiter = new ConcurrencyLimiter(4);
        StorageObject chain = new RetryableStorageObject(
            new ConcurrencyLimitedStorageObject(
                new QueryBudgetedStorageObject(new DecompressingStorageObject(raw, new GzipDecompressionCodec()), budget),
                limiter
            ),
            new RetryPolicy(3, 1, 10)
        );

        InputStream stream = chain.newStream();
        assertEquals("query budget permit must be acquired by newStream()", 1, budget.inFlight());
        assertEquals("global permit must be acquired by newStream()", 3, limiter.availablePermits());

        try {
            byte[] prefix = new byte[4096];
            int n = stream.read(prefix);
            assertThat("expected to read some decompressed prefix bytes", n, Matchers.greaterThan(0));
        } finally {
            chain.abortStream(stream);
        }

        assertTrue("raw stream must have been aborted (not closed-with-drain)", rawAborted.get());
        assertThat(
            "abort must not drain the raw stream; consumed " + rawBytesConsumed.get() + " of " + compressed.length + " compressed bytes",
            rawBytesConsumed.get(),
            Matchers.lessThan((long) compressed.length / 2)
        );
        assertEquals("query budget permit must be released by abortStream", 0, budget.inFlight());
        assertEquals("global permit must be released by abortStream", 4, limiter.availablePermits());
    }

    /**
     * Regression guard for {@link FileSplitProvider#computeRecordAlignedMacroSplitStarts}: each
     * macro-split boundary probe opens {@code newStream(pos, remaining)} and reads only a prefix,
     * so cleanup must abort (not drain) through the same decorator chain used for uncompressed
     * text files on object storage.
     */
    public void testMacroSplitDiscoveryAbortPropagatesThroughDecoratorChainWithoutDrain() throws IOException {
        StringBuilder csv = new StringBuilder("id,name\n");
        for (int i = 0; i < 200_000; i++) {
            csv.append(i).append(",value_").append(i).append('\n');
        }
        byte[] payload = csv.toString().getBytes(StandardCharsets.UTF_8);
        long fileLength = payload.length;
        assertThat("payload must exceed macro-split stride", fileLength, Matchers.greaterThan(2L * 1024 * 1024));

        AtomicLong rawBytesConsumed = new AtomicLong();
        AtomicBoolean rawAborted = new AtomicBoolean(false);
        AtomicInteger abortCalls = new AtomicInteger();
        StorageObject raw = drainSimulatingStorageObject(payload, rawBytesConsumed, rawAborted, abortCalls);

        QueryConcurrencyBudget budget = new QueryConcurrencyBudget(4, 60_000L, null);
        ConcurrencyLimiter limiter = new ConcurrencyLimiter(4);
        StorageObject chain = new RetryableStorageObject(
            new ConcurrencyLimitedStorageObject(new QueryBudgetedStorageObject(raw, budget), limiter),
            new RetryPolicy(3, 1, 10)
        );

        var blockFactory = BlockFactory.builder(BigArrays.NON_RECYCLING_INSTANCE).breaker(new NoopCircuitBreaker("test")).build();
        CsvFormatReader csvReader = new CsvFormatReader(blockFactory);

        long stride = fileLength / 4;
        List<Long> starts = FileSplitProvider.computeRecordAlignedMacroSplitStarts(csvReader, chain, fileLength, stride);

        assertThat("expected multiple macro-split boundaries", starts.size(), Matchers.greaterThan(1));
        assertTrue("each probe must abort the raw stream", abortCalls.get() >= starts.size() - 1);
        assertThat(
            "macro-split probes must not drain range streams; consumed "
                + rawBytesConsumed.get()
                + " of "
                + fileLength
                + " bytes across "
                + abortCalls.get()
                + " probes",
            rawBytesConsumed.get(),
            Matchers.lessThan(fileLength / 2)
        );
        assertEquals("query budget permits must be released after split discovery", 0, budget.inFlight());
        assertEquals("global permits must be released after split discovery", 4, limiter.availablePermits());
    }

    /**
     * Builds a {@link StorageObject} whose stream simulates Apache HttpClient's drain on
     * {@code close()}, mirroring the real S3 behaviour. The {@link StorageObject#abortStream}
     * override flips an {@code aborted} flag that suppresses the drain — exactly what the S3
     * provider does by calling {@code ResponseInputStream.abort()} instead of {@code close()}.
     */
    private static StorageObject drainSimulatingStorageObject(
        byte[] bytes,
        AtomicLong bytesConsumed,
        AtomicBoolean aborted,
        AtomicInteger abortCalls
    ) {
        return new StorageObject() {
            @Override
            public InputStream newStream() {
                return drainTrackingStream(new ByteArrayInputStream(bytes), bytesConsumed, aborted);
            }

            @Override
            public InputStream newStream(long position, long length) {
                int from = (int) position;
                int to = (int) Math.min(position + length, bytes.length);
                return drainTrackingStream(new ByteArrayInputStream(bytes, from, to - from), bytesConsumed, aborted);
            }

            @Override
            public void abortStream(InputStream stream) throws IOException {
                aborted.set(true);
                abortCalls.incrementAndGet();
                stream.close();
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
                return StoragePath.of("s3://bucket/data.csv.gz");
            }
        };
    }

    private static InputStream drainTrackingStream(ByteArrayInputStream delegate, AtomicLong bytesConsumed, AtomicBoolean aborted) {
        return new InputStream() {
            @Override
            public int read() {
                int b = delegate.read();
                if (b >= 0) bytesConsumed.incrementAndGet();
                return b;
            }

            @Override
            public int read(byte[] buf, int off, int len) {
                int n = delegate.read(buf, off, len);
                if (n > 0) bytesConsumed.addAndGet(n);
                return n;
            }

            @Override
            public void close() throws IOException {
                if (aborted.get()) {
                    return;
                }
                byte[] drain = new byte[8192];
                int n;
                while ((n = delegate.read(drain)) != -1) {
                    bytesConsumed.addAndGet(n);
                }
            }
        };
    }

    private static byte[] gzip(byte[] input) throws IOException {
        ByteArrayOutputStream baos = new ByteArrayOutputStream();
        try (GZIPOutputStream gzipOut = new GZIPOutputStream(baos)) {
            gzipOut.write(input);
        }
        return baos.toByteArray();
    }
}
