/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.benchmark.index.mapper.batch;

import org.apache.lucene.index.IndexWriter;
import org.apache.lucene.util.BytesRef;
import org.elasticsearch.benchmark.ExtraParam;
import org.elasticsearch.benchmark.index.mapper.batch.ColumnIndexer.BenchShardContext;
import org.elasticsearch.benchmark.index.mapper.batch.ColumnIndexer.CountingRecycler;
import org.elasticsearch.benchmark.index.mapper.batch.IndexingFixture.RequestBatch;
import org.elasticsearch.benchmark.index.mapper.batch.LuceneSink.SeqNoStamper;
import org.elasticsearch.benchmark.index.mapper.batch.Shapes.BenchShape;
import org.elasticsearch.benchmark.index.mapper.batch.Shapes.Expect;
import org.elasticsearch.benchmark.internal.BenchmarkLogging;
import org.elasticsearch.common.recycler.Recycler;
import org.elasticsearch.core.IOUtils;
import org.elasticsearch.index.mapper.ShardBatchMapper.BatchMapperResolution;
import org.openjdk.jmh.annotations.AuxCounters;
import org.openjdk.jmh.annotations.Benchmark;
import org.openjdk.jmh.annotations.BenchmarkMode;
import org.openjdk.jmh.annotations.Fork;
import org.openjdk.jmh.annotations.Level;
import org.openjdk.jmh.annotations.Measurement;
import org.openjdk.jmh.annotations.Mode;
import org.openjdk.jmh.annotations.OperationsPerInvocation;
import org.openjdk.jmh.annotations.OutputTimeUnit;
import org.openjdk.jmh.annotations.Param;
import org.openjdk.jmh.annotations.Scope;
import org.openjdk.jmh.annotations.Setup;
import org.openjdk.jmh.annotations.State;
import org.openjdk.jmh.annotations.TearDown;
import org.openjdk.jmh.annotations.Threads;
import org.openjdk.jmh.annotations.Warmup;

import java.io.IOException;
import java.util.EnumSet;
import java.util.concurrent.TimeUnit;

/**
 * Compares the data-node shard stage of indexing through the columnar batch path, the row fallback and classic JSON on
 * identical documents. The timed region is mapping plus the Lucene add only; the engine is bypassed, and {@code deleteAll} and
 * {@code commit} run in per-invocation and per-iteration teardowns so writer housekeeping stays out of the timing.
 */
@Fork(5)
@Warmup(iterations = 10, time = 1)
@Measurement(iterations = 5, time = 1)
@BenchmarkMode(Mode.Throughput)
@OutputTimeUnit(TimeUnit.SECONDS)
@Threads(1)
@OperationsPerInvocation(ShardIndexingBenchmark.DOCS_PER_INVOCATION)
public class ShardIndexingBenchmark {

    public static final int DOCS_PER_INVOCATION = 5000;

    @State(Scope.Benchmark)
    public abstract static class ShardFixtureState {

        @Param({ "synthetic" })
        @ExtraParam({ "columnar_stored" })
        public String sourceMode;

        @Param({ "42" })
        public long seed;

        IndexingFixture fixture;
        LuceneSink sink;
        SeqNoStamper stamper;
        BenchShardContext ctx;

        abstract String shapeName();

        abstract int docsPerRequest();

        abstract EnumSet<Expect> accepts();

        abstract boolean dryRuns();

        abstract void init();

        @Setup(Level.Trial)
        public void setUp() throws IOException {
            BenchmarkLogging.configure();
            BenchShape shape = Shapes.get(shapeName());
            if (accepts().contains(shape.expect()) == false) {
                throw new IllegalStateException(
                    "shape ["
                        + shape.name()
                        + "] expects ["
                        + shape.expect()
                        + "] but "
                        + getClass().getSimpleName()
                        + " accepts "
                        + accepts()
                );
            }
            boolean success = false;
            try {
                fixture = IndexingFixture.build(shape, sourceMode, docsPerRequest(), DOCS_PER_INVOCATION, seed);
                stamper = new SeqNoStamper();
                ctx = new BenchShardContext(fixture.mapperService(), fixture.indexSettings(), stamper.primaryTerm());
                if (dryRuns()) {
                    checkStages(shape);
                }
                init();
                sink = LuceneSink.openInTempDir(fixture.codec(), fixture.mapperService());
                success = true;
            } finally {
                if (success == false) {
                    IOUtils.closeWhileHandlingException(sink, fixture);
                    sink = null;
                    fixture = null;
                }
            }
        }

        private void checkStages(BenchShape shape) {
            RequestBatch[] requests = fixture.requests();
            for (int r = 0; r < requests.length; r++) {
                Expect actual = ColumnIndexer.dryRunStage(ctx, requests[r]);
                if (actual != shape.expect()) {
                    throw new IllegalStateException(
                        "shape ["
                            + shape.name()
                            + "] request ["
                            + r
                            + "] at docsPerRequest ["
                            + docsPerRequest()
                            + "] expected ["
                            + shape.expect()
                            + "] but the dry run reached ["
                            + actual
                            + "]"
                    );
                }
            }
        }

        @TearDown(Level.Invocation)
        public void afterInvocation() throws IOException {
            sink.afterInvocation(DOCS_PER_INVOCATION);
        }

        @TearDown(Level.Iteration)
        public void afterIteration() throws IOException {
            sink.afterIteration();
        }

        @TearDown(Level.Trial)
        public void tearDown() throws IOException {
            IOUtils.close(sink, fixture);
        }
    }

    @State(Scope.Benchmark)
    public static class ColumnState extends ShardFixtureState {

        @Param({ "long.dense.small", "long.absent10", "long.mv1to4", "double.dense.small", "metrics4" })
        @ExtraParam(
            {
                "long.dense.full",
                "integer.dense.small",
                "integer.dense.full",
                "integer.absent10",
                "integer.mv1to4",
                "short.dense.small",
                "short.dense.full",
                "short.absent10",
                "short.mv1to4",
                "byte.dense.small",
                "byte.dense.full",
                "byte.absent10",
                "byte.mv1to4",
                "double.dense.full",
                "double.absent10",
                "float.dense.small",
                "float.dense.full",
                "float.absent10",
                "half_float.dense.small",
                "half_float.dense.full",
                "half_float.absent10",
                "long.str",
                "long.dbl",
                "integer.noindex" }
        )
        public String shape;

        @Param({ "5000" })
        @ExtraParam({ "1", "10", "100", "1000" })
        public int docsPerRequest;

        CountingRecycler recycler;
        ColumnIndexer indexer;

        @Override
        String shapeName() {
            return shape;
        }

        @Override
        int docsPerRequest() {
            return docsPerRequest;
        }

        @Override
        EnumSet<Expect> accepts() {
            return EnumSet.of(Expect.COLUMN);
        }

        @Override
        boolean dryRuns() {
            return true;
        }

        @Override
        void init() {
            recycler = CountingRecycler.pooled();
            indexer = new ColumnIndexer(ctx, recycler);
        }
    }

    @State(Scope.Benchmark)
    public static class RowState extends ShardFixtureState {

        @Param({ "long.dense.small", "long.absent10", "long.mv1to4", "double.dense.small", "metrics4" })
        @ExtraParam(
            {
                "long.dense.full",
                "integer.dense.small",
                "integer.dense.full",
                "integer.absent10",
                "integer.mv1to4",
                "short.dense.small",
                "short.dense.full",
                "short.absent10",
                "short.mv1to4",
                "byte.dense.small",
                "byte.dense.full",
                "byte.absent10",
                "byte.mv1to4",
                "double.dense.full",
                "double.absent10",
                "double.mv1to4",
                "float.dense.small",
                "float.dense.full",
                "float.absent10",
                "float.mv1to4",
                "half_float.dense.small",
                "half_float.dense.full",
                "half_float.absent10",
                "half_float.mv1to4",
                "long.str",
                "long.dbl",
                "integer.noindex",
                "long.nullexplicit",
                "scaled_float.dense" }
        )
        public String shape;

        @Param({ "5000" })
        @ExtraParam({ "1", "10", "100", "1000" })
        public int docsPerRequest;

        RowIndexer batchRow;
        RowIndexer json;

        @Override
        String shapeName() {
            return shape;
        }

        @Override
        int docsPerRequest() {
            return docsPerRequest;
        }

        @Override
        EnumSet<Expect> accepts() {
            return EnumSet.allOf(Expect.class);
        }

        @Override
        boolean dryRuns() {
            return false;
        }

        @Override
        void init() {
            batchRow = new RowIndexer(fixture.mapperService(), RowIndexer.RowSourceKind.BATCH_ROW);
            json = new RowIndexer(fixture.mapperService(), RowIndexer.RowSourceKind.JSON);
        }
    }

    @State(Scope.Benchmark)
    public static class FallbackState extends ShardFixtureState {

        @Param({ "long.nullexplicit", "scaled_float.dense" })
        @ExtraParam({ "double.mv1to4", "float.mv1to4", "half_float.mv1to4" })
        public String shape;

        @Param({ "5000" })
        @ExtraParam({ "100", "1000" })
        public int docsPerRequest;

        Recycler<BytesRef> recycler;
        RowIndexer batchRow;

        @Override
        String shapeName() {
            return shape;
        }

        @Override
        int docsPerRequest() {
            return docsPerRequest;
        }

        @Override
        EnumSet<Expect> accepts() {
            return EnumSet.of(Expect.FALLBACK_RESOLVE, Expect.FALLBACK_MAP);
        }

        @Override
        boolean dryRuns() {
            return true;
        }

        @Override
        void init() {
            recycler = ColumnIndexer.pooledRecycler();
            batchRow = new RowIndexer(fixture.mapperService(), RowIndexer.RowSourceKind.BATCH_ROW);
        }
    }

    @AuxCounters(AuxCounters.Type.OPERATIONS)
    @State(Scope.Thread)
    public static class PageCounters {
        public long pagesObtained;
        public long pagesFresh;
    }

    @Benchmark
    public long column(ColumnState s, PageCounters c) throws IOException {
        long docs = s.indexer.index(s.sink.writer(), s.stamper, s.fixture.requests());
        c.pagesObtained += s.recycler.pagesObtained();
        c.pagesFresh += s.recycler.pagesFresh();
        s.recycler.reset();
        return docs;
    }

    @Benchmark
    public long rowBatch(RowState s) throws IOException {
        return s.batchRow.index(s.sink.writer(), s.stamper, s.fixture.requests());
    }

    @Benchmark
    public long rowJson(RowState s) throws IOException {
        return s.json.index(s.sink.writer(), s.stamper, s.fixture.requests());
    }

    @Benchmark
    public long columnThenRow(FallbackState s) throws IOException {
        IndexWriter writer = s.sink.writer();
        long docs = 0;
        for (RequestBatch request : s.fixture.requests()) {
            BatchMapperResolution resolution = ColumnIndexer.resolve(s.ctx, request);
            if (resolution != null && ColumnIndexer.dryRunMaps(s.ctx, request, resolution, s.recycler)) {
                throw new IllegalStateException(
                    "shape [" + s.shape + "] mapped request at offset [" + request.offset() + "] that was expected to fall back"
                );
            }
            docs += s.batchRow.indexRequest(writer, s.stamper, request);
        }
        return docs;
    }
}
