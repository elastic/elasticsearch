/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.benchmark.vector;

import org.apache.lucene.store.Directory;
import org.apache.lucene.store.IOContext;
import org.apache.lucene.store.IndexInput;
import org.elasticsearch.benchmark.Utils;
import org.elasticsearch.blobcache.shared.SharedBlobCacheService;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.core.IOUtils;
import org.elasticsearch.index.codec.vectors.diskbbq.CentroidIterator;
import org.elasticsearch.index.codec.vectors.diskbbq.PostingMetadata;
import org.elasticsearch.index.codec.vectors.diskbbq.PrefetchingCentroidIterator;
import org.elasticsearch.xpack.stateless.lucene.StatelessDirectoryFactory;
import org.openjdk.jmh.annotations.AuxCounters;
import org.openjdk.jmh.annotations.Benchmark;
import org.openjdk.jmh.annotations.BenchmarkMode;
import org.openjdk.jmh.annotations.Fork;
import org.openjdk.jmh.annotations.Level;
import org.openjdk.jmh.annotations.Measurement;
import org.openjdk.jmh.annotations.Mode;
import org.openjdk.jmh.annotations.OutputTimeUnit;
import org.openjdk.jmh.annotations.Param;
import org.openjdk.jmh.annotations.Scope;
import org.openjdk.jmh.annotations.Setup;
import org.openjdk.jmh.annotations.State;
import org.openjdk.jmh.annotations.TearDown;
import org.openjdk.jmh.annotations.Warmup;
import org.openjdk.jmh.infra.Blackhole;

import java.io.IOException;
import java.io.OutputStream;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.concurrent.TimeUnit;

/**
 * Microbenchmark measuring how the <em>prefetch depth</em> of
 * {@link PrefetchingCentroidIterator} affects per-query latency on the serverless
 * (stateless) blob-cache read path, and the over-fetch cost of prefetching deeper.
 *
 * <h2>What is measured</h2>
 * {@link PrefetchingCentroidIterator} wraps a delegate {@link CentroidIterator} and a posting-list
 * {@link IndexInput} slice. For each posting it calls {@code postingListSlice.prefetch(offset, length)}
 * and keeps {@code prefetchAhead} lists buffered ahead of the consumer. Production hardcodes
 * {@code prefetchAhead = 1} (the 2-arg constructor used by all three diskBBQ readers);
 * {@code ESNextDiskBBQVectorsReader} carries a TODO to prefetch more than one list "but cap it so we
 * don't fetch lists we won't end up scoring". The public 3-arg constructor
 * {@code new PrefetchingCentroidIterator(delegate, postingListSlice, prefetchAhead)} is the only way to
 * sweep depth, so this benchmark drives the iterator directly. On serverless, a posting-list read that
 * misses the local file cache triggers an asynchronous remote blob fetch; deeper prefetch overlaps those
 * fetches with consumer compute.
 *
 * <h2>Serverless machinery</h2>
 * Rather than hand-mocking a {@link Directory}, this benchmark drives the real
 * {@link StatelessDirectoryFactory} read path — the same wrapper used by
 * {@code org.elasticsearch.benchmark.stateless.AbstractStatelessQueryBenchmark}. The simulated remote
 * first-byte latency is injected via {@link StatelessDirectoryFactory#FIRST_BYTE_LATENCY_MS_PROP} and
 * cache stats are read via {@link StatelessDirectoryFactory#statsFor(Directory)}. The
 * {@link CacheCounters} auxiliary counters are copied from the stateless base so the cost axis
 * ({@code bytesDownloaded} = the latency-incurring bytes, {@code cacheMisses}) is reported alongside the
 * latency.
 *
 * <h2>Region modeling (load-bearing)</h2>
 * The real {@link SharedBlobCacheService} fetches fixed-size regions (production default 16MB), and a real
 * query visits score-ranked centroids that map to <em>scattered</em> file offsets, so consecutive visited
 * posting lists land in <em>different</em> regions and each prefetch is a distinct remote region fetch. If
 * lists were packed contiguously they would share a region (~107 × 150KB lists per 16MB region) and the
 * prefetch depth in lists would be far too shallow to ever cross a region boundary, yielding a meaningless
 * flat result. To keep the latency dynamics realistic while bounding the footprint, this benchmark shrinks
 * the region to {@code regionSizeBytes} (default 256KB) and places each posting list <em>alone</em> in its
 * own region: list {@code i} occupies {@code [i*regionSizeBytes, (i+1)*regionSizeBytes)}. This is a
 * deliberate modeling choice — production regions are 16MB; we shrink the region so each scattered visited
 * list equals exactly one fetch.
 *
 * <h2>Consumer cost</h2>
 * The consumer loop models {@code IVFVectorsReader.search} (the single real consumer): after reading each
 * posting list through the slice it burns {@code consumerComputeMicros} of CPU via
 * {@link Blackhole#consumeCPU(long)} (not {@code Thread.sleep}, so the asynchronous prefetch can overlap).
 * This is the modeled per-list scoring time the prefetch is meant to hide; the real OSQ/SIMD per-list
 * scorer cost can be calibrated from the existing {@code benchmark/vector/scorer/VectorScorer*} benchmarks.
 *
 * <h2>Hypothesis (NOT asserted)</h2>
 * <ul>
 *   <li>At {@code firstByteLatencyMs == 0} or {@code cacheState == HOT} (negative controls), latency
 *       should be ~flat across {@code prefetchAhead}.</li>
 *   <li>At {@code COLD} with high {@code firstByteLatencyMs}, latency should drop with depth up to a knee,
 *       while {@code bytesDownloaded} and {@code overFetchedLists} rise with depth — the budget trade-off
 *       the TODO is about.</li>
 * </ul>
 */
@Fork(value = 1, jvmArgsPrepend = { "--add-modules=jdk.incubator.vector" })
@State(Scope.Benchmark)
@Warmup(iterations = 3)
@Measurement(iterations = 20)
@BenchmarkMode(Mode.SingleShotTime)
@OutputTimeUnit(TimeUnit.MILLISECONDS)
public class CentroidPrefetchDepthBenchmark {

    static {
        Utils.configureBenchmarkLogging();
    }

    private static final String POSTING_FILE = "postingLists.bin";

    public enum CacheState {
        COLD,
        HOT
    }

    /** Prefetch depth sweep. {@code 1} is the production status quo (the 2-arg constructor default). */
    @Param({ "1", "2", "4", "8", "16" })
    public int prefetchAhead;

    /** Simulated remote first-byte latency. {@code 0} is the stateful/local negative control. */
    @Param({ "0", "20", "50", "100", "200" })
    public long firstByteLatencyMs;

    /** {@code HOT} pre-warms the whole working set (depth must be ~noise); {@code COLD} is where the win lives. */
    @Param({ "COLD", "HOT" })
    public CacheState cacheState;

    /** Region size: 256KB, page-aligned. One region per posting list. MUST be {@code >= perListBytes}. */
    @Param({ "262144" })
    public int regionSizeBytes;

    /** Number of posting lists, each alone in its own region (working set = numLists * regionSizeBytes). */
    @Param({ "128" })
    public int numLists;

    /** Bytes per posting list (a 384-vector list at dim768). MUST be {@code <= regionSizeBytes}. */
    @Param({ "150000" })
    public int perListBytes;

    /**
     * Modeled per-list scoring time (microseconds) that prefetch is meant to overlap. Calibrate against the
     * {@code benchmark/vector/scorer/VectorScorer*} benchmarks for the real scorer cost.
     */
    @Param({ "0", "25", "100" })
    public long consumerComputeMicros;

    /**
     * Lists the consumer actually scores ({@code < numLists}), so the consumer crosses this many distinct
     * region fetches and the remaining lists are prefetched-but-unconsumed (real over-fetch).
     */
    @Param({ "96" })
    public int consumeBudgetLists;

    private Path dataPath;
    private Path workPath;
    private Directory directory;
    private IndexInput postingListFile;
    private IndexInput postingListSlice;
    private byte[] readScratch;
    private long tokensPerMicro;

    @Setup(Level.Trial)
    public void setupTrial() throws IOException {
        if (perListBytes > regionSizeBytes) {
            throw new IllegalArgumentException(
                "perListBytes [" + perListBytes + "] must be <= regionSizeBytes [" + regionSizeBytes + "]"
            );
        }
        dataPath = Files.createTempDirectory("centroid-prefetch-bench-data");
        workPath = Files.createTempDirectory("centroid-prefetch-bench-work");
        readScratch = new byte[perListBytes];
        tokensPerMicro = calibrateTokensPerMicro();

        // One flat file of numLists region-sized slots, all zeros. Region i = [i*regionSizeBytes, (i+1)*regionSizeBytes).
        long total = (long) numLists * regionSizeBytes;
        byte[] chunk = new byte[1 << 20];
        try (OutputStream out = Files.newOutputStream(dataPath.resolve(POSTING_FILE))) {
            long remaining = total;
            while (remaining > 0) {
                int n = (int) Math.min(chunk.length, remaining);
                out.write(chunk, 0, n);
                remaining -= n;
            }
        }
    }

    @Setup(Level.Invocation)
    public void setupInvocation() throws IOException {
        System.setProperty(StatelessDirectoryFactory.FIRST_BYTE_LATENCY_MS_PROP, Long.toString(firstByteLatencyMs));
        // The cache holds exactly the whole working set (numLists regions), so HOT pre-warms everything with no eviction.
        System.setProperty(StatelessDirectoryFactory.CACHE_SIZE_BYTES_PROP, Long.toString((long) numLists * regionSizeBytes));
        // Per-invocation work-dir reset gives a COLD (empty) cache; HOT then pre-warms it below.
        IOUtils.rm(workPath);
        Files.createDirectories(workPath);

        // Shrink the cache region so each scattered posting list occupies its own region (see class javadoc).
        Settings extraNodeSettings = Settings.builder()
            .put("xpack.searchable.snapshot.shared_cache.region_size", regionSizeBytes + "b")
            .build();
        directory = StatelessDirectoryFactory.create(dataPath, workPath, extraNodeSettings);
        if (cacheState == CacheState.HOT) {
            preWarm(directory);
        }
        postingListFile = directory.openInput(POSTING_FILE, IOContext.DEFAULT);
        // Mirror production: entry.postingListSlice == postingListFile.slice("postingLists", off, len).
        postingListSlice = postingListFile.slice("postingLists", 0, postingListFile.length());
    }

    @TearDown(Level.Invocation)
    public void tearDownInvocation() throws IOException {
        IOUtils.close(postingListSlice, postingListFile, directory);
        postingListSlice = null;
        postingListFile = null;
        directory = null;
    }

    @TearDown(Level.Trial)
    public void tearDownTrial() throws IOException {
        IOUtils.rm(dataPath, workPath);
    }

    @Benchmark
    public Object runQuery(CacheCounters cacheCounters, OverFetchCounters overFetchCounters) throws IOException {
        SharedBlobCacheService.Stats before = StatelessDirectoryFactory.statsFor(directory);

        CountingCentroidIterator delegate = new CountingCentroidIterator(numLists, regionSizeBytes, perListBytes);
        PrefetchingCentroidIterator it = new PrefetchingCentroidIterator(delegate, postingListSlice, prefetchAhead);

        int consumed = 0;
        long checksum = 0;
        while (it.hasNext() && consumed < consumeBudgetLists) {
            PostingMetadata pm = it.nextPosting();
            postingListSlice.seek(pm.offset());
            // Read the posting list through the slice (incurs the injected first-byte latency on a COLD miss).
            int len = (int) pm.length();
            postingListSlice.readBytes(readScratch, 0, len);
            checksum += readScratch[0];
            // Model the per-list scoring work the prefetch is meant to overlap (burn CPU, do not sleep).
            Blackhole.consumeCPU(tokensFor(consumerComputeMicros));
            consumed++;
        }

        // Lists the delegate advanced (and therefore prefetched) but the consumer never scored.
        overFetchCounters.overFetchedLists = delegate.advances() - consumed;

        SharedBlobCacheService.Stats after = StatelessDirectoryFactory.statsFor(directory);
        if (before != null && after != null) {
            cacheCounters.bytesRead = after.readBytes() - before.readBytes();
            cacheCounters.bytesDownloaded = after.writeBytes() - before.writeBytes();
            cacheCounters.cacheMisses = after.missCount() - before.missCount();
            cacheCounters.regionWrites = after.writeCount() - before.writeCount();
        }
        return checksum;
    }

    private long tokensFor(long micros) {
        return micros * tokensPerMicro;
    }

    /**
     * Estimates how many {@link Blackhole#consumeCPU(long)} tokens correspond to one microsecond of CPU work on
     * this host, so {@code consumerComputeMicros} maps to a wall-clock-meaningful amount of busy work.
     */
    private static long calibrateTokensPerMicro() {
        for (int i = 0; i < 5; i++) {
            Blackhole.consumeCPU(100_000L);
        }
        long tokens = 2_000_000L;
        long start = System.nanoTime();
        Blackhole.consumeCPU(tokens);
        long elapsedNanos = Math.max(1, System.nanoTime() - start);
        double nanosPerToken = (double) elapsedNanos / tokens;
        return Math.max(1L, (long) (1000.0 / nanosPerToken));
    }

    private static void preWarm(Directory dir) throws IOException {
        byte[] buf = new byte[64 * 1024];
        for (String name : dir.listAll()) {
            try (IndexInput in = dir.openInput(name, IOContext.READONCE)) {
                long remaining = in.length();
                while (remaining > 0) {
                    int n = (int) Math.min(buf.length, remaining);
                    in.readBytes(buf, 0, n);
                    remaining -= n;
                }
            }
        }
    }

    /**
     * A synthetic delegate that yields {@code numLists} full posting-list records, list {@code i} alone in region
     * {@code i}, and counts its own {@link #nextPosting()} advances (used to compute over-fetch). Only
     * {@code offset()} and {@code length()} are consumed by the iterator under study.
     */
    private static final class CountingCentroidIterator implements CentroidIterator {
        private final int numLists;
        private final int regionSizeBytes;
        private final int perListBytes;
        private int next = 0;
        private int advances = 0;

        CountingCentroidIterator(int numLists, int regionSizeBytes, int perListBytes) {
            this.numLists = numLists;
            this.regionSizeBytes = regionSizeBytes;
            this.perListBytes = perListBytes;
        }

        @Override
        public boolean hasNext() {
            return next < numLists;
        }

        @Override
        public PostingMetadata nextPosting() {
            if (hasNext() == false) {
                throw new IllegalStateException("No more elements available");
            }
            long offset = (long) next * regionSizeBytes;
            next++;
            advances++;
            return new PostingMetadata(offset, perListBytes, PostingMetadata.NO_ORDINAL, 0.0f);
        }

        int advances() {
            return advances;
        }
    }

    /**
     * JMH auxiliary counters reported per invocation, copied from the stateless query benchmark base:
     * bytes read from the cache layer and bytes downloaded by the cache from the simulated blob store
     * ({@code bytesDownloaded} is the latency-incurring cost axis and naturally includes over-fetched lists).
     */
    @State(Scope.Thread)
    @AuxCounters(AuxCounters.Type.EVENTS)
    public static class CacheCounters {
        public long bytesRead;
        public long bytesDownloaded;
        public long cacheMisses;
        public long regionWrites;

        @Setup(Level.Invocation)
        public void reset() {
            bytesRead = 0;
            bytesDownloaded = 0;
            cacheMisses = 0;
            regionWrites = 0;
        }
    }

    /**
     * JMH auxiliary counter reporting the over-fetch axis: posting lists that were prefetched (advanced by the
     * delegate) but never scored by the consumer because it stopped at {@code consumeBudgetLists}.
     */
    @State(Scope.Thread)
    @AuxCounters(AuxCounters.Type.EVENTS)
    public static class OverFetchCounters {
        public long overFetchedLists;

        @Setup(Level.Invocation)
        public void reset() {
            overFetchedLists = 0;
        }
    }
}
