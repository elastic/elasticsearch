/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.benchmark.stateless;

import org.apache.lucene.index.DirectoryReader;
import org.apache.lucene.index.IndexReader;
import org.apache.lucene.index.IndexWriter;
import org.apache.lucene.index.IndexWriterConfig;
import org.apache.lucene.search.IndexSearcher;
import org.apache.lucene.store.Directory;
import org.apache.lucene.store.FSDirectory;
import org.apache.lucene.store.IOContext;
import org.apache.lucene.store.IndexInput;
import org.elasticsearch.benchmark.Utils;
import org.elasticsearch.blobcache.shared.SharedBlobCacheService;
import org.elasticsearch.core.IOUtils;
import org.elasticsearch.xpack.stateless.lucene.StatelessDirectoryFactory;
import org.openjdk.jmh.annotations.AuxCounters;
import org.openjdk.jmh.annotations.Benchmark;
import org.openjdk.jmh.annotations.BenchmarkMode;
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

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Comparator;
import java.util.concurrent.TimeUnit;
import java.util.stream.Stream;

/**
 * Base class for benchmarks that run Lucene queries against a stateless-simulated
 * Directory and compare cold-cache vs. hot-cache cost.
 *
 * <p>Each invocation runs a single query against a freshly-created
 * {@link StatelessDirectoryFactory} wrapper around an on-disk index built once
 * at trial setup. For {@link CacheState#HOT} the cache is sequentially warmed
 * before the query; for {@link CacheState#COLD} it is left empty so the query
 * exercises the simulated blob-store fetch path.
 *
 * <p>Per-invocation cache stats are exposed via {@link CacheCounters} as JMH
 * auxiliary counters: {@code bytesRead} (bytes the query consumed from the
 * cache layer) and {@code bytesDownloaded} (bytes the cache had to fetch from
 * the simulated blob store, i.e. the bytes that incurred the configured
 * first-byte latency).
 *
 * <p>To add a new query benchmark, extend this class and implement
 * {@link #indexWriterConfig()}, {@link #buildIndex(IndexWriter)} and
 * {@link #runQuery(IndexSearcher)}. Add {@link Param} fields on the concrete
 * subclass to sweep query-specific knobs (e.g. dimensionality, top-K).
 */
@State(Scope.Benchmark)
@Warmup(iterations = 3)
@Measurement(iterations = 20)
@BenchmarkMode(Mode.SingleShotTime)
@OutputTimeUnit(TimeUnit.MILLISECONDS)
public abstract class AbstractStatelessQueryBenchmark {

    static {
        Utils.configureBenchmarkLogging();
    }

    public enum CacheState {
        COLD,
        HOT
    }

    @Param({ "COLD", "HOT" })
    public CacheState cacheState;

    @Param({ "0" })
    public long firstByteLatencyMs;

    private Path dataPath;
    private Path workPath;
    protected Directory directory;
    protected IndexReader reader;
    protected IndexSearcher searcher;

    @Setup(Level.Trial)
    public final void setupTrial() throws IOException {
        dataPath = Files.createTempDirectory("stateless-bench-data");
        workPath = Files.createTempDirectory("stateless-bench-work");
        try (Directory d = FSDirectory.open(dataPath); IndexWriter w = new IndexWriter(d, indexWriterConfig())) {
            buildIndex(w);
            w.commit();
        }
    }

    @Setup(Level.Invocation)
    public final void setupInvocation() throws IOException {
        System.setProperty(StatelessDirectoryFactory.FIRST_BYTE_LATENCY_MS_PROP, Long.toString(firstByteLatencyMs));
        deleteRecursively(workPath);
        Files.createDirectories(workPath);
        directory = StatelessDirectoryFactory.create(dataPath, workPath);
        if (cacheState == CacheState.HOT) {
            preWarm(directory);
        }
        // Reader open happens in setup, not in the timed block: in production stateless
        // the reader is opened once per shard and amortized across many queries.
        reader = DirectoryReader.open(directory);
        searcher = new IndexSearcher(reader);
    }

    @TearDown(Level.Invocation)
    public final void tearDownInvocation() throws IOException {
        IOUtils.close(reader, directory);
    }

    @TearDown(Level.Trial)
    public final void tearDownTrial() throws IOException {
        deleteRecursively(dataPath);
        deleteRecursively(workPath);
    }

    @Benchmark
    public final Object runBenchmark(CacheCounters counters) throws IOException {
        SharedBlobCacheService.Stats before = StatelessDirectoryFactory.statsFor(directory);
        Object result = runQuery(searcher);
        SharedBlobCacheService.Stats after = StatelessDirectoryFactory.statsFor(directory);
        if (before != null && after != null) {
            counters.bytesRead = after.readBytes() - before.readBytes();
            counters.bytesDownloaded = after.writeBytes() - before.writeBytes();
            counters.cacheMisses = after.missCount() - before.missCount();
            counters.regionWrites = after.writeCount() - before.writeCount();
        }
        return result;
    }

    /** Subclass hook: the {@link IndexWriterConfig} (codec, merge policy) used to build the index. */
    protected abstract IndexWriterConfig indexWriterConfig();

    /** Subclass hook: write documents into the index. Called once per trial. */
    protected abstract void buildIndex(IndexWriter writer) throws IOException;

    /** Subclass hook: execute the query. Returns a value that JMH can consume to prevent dead-code elimination. */
    protected abstract Object runQuery(IndexSearcher searcher) throws IOException;

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

    private static void deleteRecursively(Path path) throws IOException {
        if (Files.exists(path) == false) {
            return;
        }
        try (Stream<Path> walk = Files.walk(path)) {
            walk.sorted(Comparator.reverseOrder()).forEach(p -> {
                try {
                    Files.deleteIfExists(p);
                } catch (IOException e) {
                    throw new UncheckedIOExceptionWrapper(e);
                }
            });
        } catch (UncheckedIOExceptionWrapper e) {
            throw e.getCause();
        }
    }

    private static final class UncheckedIOExceptionWrapper extends RuntimeException {
        UncheckedIOExceptionWrapper(IOException cause) {
            super(cause);
        }

        @Override
        public synchronized IOException getCause() {
            return (IOException) super.getCause();
        }
    }

    /**
     * JMH auxiliary counters reported per invocation: bytes read from the cache layer
     * and bytes downloaded by the cache from the simulated blob store.
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
}
