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
import org.hamcrest.Matchers;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.nio.charset.StandardCharsets;
import java.util.List;
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

        DrainSimulatingStorageObject.Tracking tracking = new DrainSimulatingStorageObject.Tracking();
        StorageObject raw = DrainSimulatingStorageObject.create(compressed, tracking);

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

        DrainSimulatingStorageObject.Tracking tracking = new DrainSimulatingStorageObject.Tracking();
        StorageObject raw = DrainSimulatingStorageObject.create(payload, tracking);

        QueryConcurrencyBudget budget = new QueryConcurrencyBudget(4, 60_000L, null);
        ConcurrencyLimiter limiter = new ConcurrencyLimiter(4);
        StorageObject chain = new RetryableStorageObject(
            new ConcurrencyLimitedStorageObject(new QueryBudgetedStorageObject(raw, budget), limiter),
            new RetryPolicy(3, 1, 10)
        );

        var blockFactory = BlockFactory.builder(BigArrays.NON_RECYCLING_INSTANCE).breaker(new NoopCircuitBreaker("test")).build();
        CsvFormatReader csvReader = new CsvFormatReader(blockFactory);

        long stride = fileLength / 4;
        List<Long> starts = FileSplitProvider.computeRecordAlignedMacroSplitStarts(
            csvReader,
            chain,
            fileLength,
            stride,
            SegmentableFormatReader.DEFAULT_MAX_RECORD_BYTES
        );

        assertThat("expected multiple macro-split boundaries", starts.size(), Matchers.greaterThan(1));
        assertTrue("each probe must abort the raw stream", tracking.abortCalls.get() >= starts.size() - 1);
        assertThat(
            "macro-split probes must not drain range streams; consumed "
                + tracking.bytesConsumed.get()
                + " of "
                + fileLength
                + " bytes across "
                + tracking.abortCalls.get()
                + " probes",
            tracking.bytesConsumed.get(),
            Matchers.lessThan(fileLength / 2)
        );
        assertEquals("query budget permits must be released after split discovery", 0, budget.inFlight());
        assertEquals("global permits must be released after split discovery", 4, limiter.availablePermits());
    }

    /**
     * Regression guard for {@link ParallelParsingCoordinator#computeSegments}: in-file parallel parsing
     * probes record boundaries through the same decorator chain used for uncompressed object-store reads.
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

        QueryConcurrencyBudget budget = new QueryConcurrencyBudget(4, 60_000L, null);
        ConcurrencyLimiter limiter = new ConcurrencyLimiter(4);
        StorageObject chain = new RetryableStorageObject(
            new ConcurrencyLimitedStorageObject(new QueryBudgetedStorageObject(raw, budget), limiter),
            new RetryPolicy(3, 1, 10)
        );

        var blockFactory = BlockFactory.builder(BigArrays.NON_RECYCLING_INSTANCE).breaker(new NoopCircuitBreaker("test")).build();
        CsvFormatReader csvReader = new CsvFormatReader(blockFactory);

        List<long[]> segments = ParallelParsingCoordinator.computeSegments(csvReader, chain, fileLength, 4, csvReader.minimumSegmentSize());

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
        assertEquals("query budget permits must be released after segment discovery", 0, budget.inFlight());
        assertEquals("global permits must be released after segment discovery", 4, limiter.availablePermits());
    }

    private static byte[] gzip(byte[] input) throws IOException {
        ByteArrayOutputStream baos = new ByteArrayOutputStream();
        try (GZIPOutputStream gzipOut = new GZIPOutputStream(baos)) {
            gzipOut.write(input);
        }
        return baos.toByteArray();
    }
}
