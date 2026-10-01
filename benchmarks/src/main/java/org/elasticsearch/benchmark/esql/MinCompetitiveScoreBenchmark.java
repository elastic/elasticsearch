/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.benchmark.esql;

import org.apache.lucene.analysis.core.WhitespaceAnalyzer;
import org.apache.lucene.document.Document;
import org.apache.lucene.document.Field;
import org.apache.lucene.document.TextField;
import org.apache.lucene.index.DirectoryReader;
import org.apache.lucene.index.IndexWriter;
import org.apache.lucene.index.IndexWriterConfig;
import org.apache.lucene.index.Term;
import org.apache.lucene.search.BooleanClause;
import org.apache.lucene.search.BooleanQuery;
import org.apache.lucene.search.IndexSearcher;
import org.apache.lucene.search.Query;
import org.apache.lucene.search.TermQuery;
import org.apache.lucene.store.ByteBuffersDirectory;
import org.apache.lucene.store.Directory;
import org.elasticsearch.benchmark.Utils;
import org.elasticsearch.benchmark.internal.BenchmarkLogging;
import org.elasticsearch.common.breaker.NoopCircuitBreaker;
import org.elasticsearch.common.settings.ClusterSettings;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.unit.ByteSizeValue;
import org.elasticsearch.common.util.BigArrays;
import org.elasticsearch.compute.data.BlockFactory;
import org.elasticsearch.compute.data.DocBlock;
import org.elasticsearch.compute.data.DocVector;
import org.elasticsearch.compute.data.DoubleBlock;
import org.elasticsearch.compute.data.DoubleVector;
import org.elasticsearch.compute.data.ElementType;
import org.elasticsearch.compute.data.Page;
import org.elasticsearch.compute.lucene.IndexedByShardIdFromSingleton;
import org.elasticsearch.compute.lucene.ShardContext;
import org.elasticsearch.compute.lucene.query.DataPartitioning;
import org.elasticsearch.compute.lucene.query.LuceneOperator;
import org.elasticsearch.compute.lucene.query.LuceneSliceQueue;
import org.elasticsearch.compute.lucene.query.LuceneSourceOperator;
import org.elasticsearch.compute.lucene.query.MinCompetitiveScore;
import org.elasticsearch.compute.operator.DriverContext;
import org.elasticsearch.compute.operator.topn.SharedMinCompetitive;
import org.elasticsearch.compute.operator.topn.TopNEncoder;
import org.elasticsearch.compute.operator.topn.TopNOperator;
import org.elasticsearch.compute.querydsl.query.QueryWarnings;
import org.elasticsearch.core.IOUtils;
import org.elasticsearch.core.Releasables;
import org.elasticsearch.index.mapper.BlockLoader;
import org.elasticsearch.index.mapper.MappedFieldType;
import org.elasticsearch.index.mapper.SourceLoader;
import org.elasticsearch.index.mapper.blockloader.BlockLoaderFunctionConfig;
import org.elasticsearch.index.mapper.blockloader.Warnings;
import org.elasticsearch.index.search.stats.SearchStatsSettings;
import org.elasticsearch.index.search.stats.ShardSearchStats;
import org.elasticsearch.search.sort.SortAndFormats;
import org.elasticsearch.search.sort.SortBuilder;
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

import java.io.Closeable;
import java.io.IOException;
import java.io.UncheckedIOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Optional;
import java.util.Random;
import java.util.Set;
import java.util.concurrent.TimeUnit;

/**
 * {@code FROM idx METADATA _score | WHERE <full text> AND <filter Lucene can't see> | SORT _score DESC | LIMIT N}
 * on a single data node driver: {@link LuceneSourceOperator} → filter → {@link TopNOperator}, with and without
 * feeding the TopN's bound back to Lucene through {@link MinCompetitiveScore}.
 * <p>
 * The index has Zipf distributed terms and random document lengths so BM25 scores spread out like real text.
 * {@code term}, {@code or} and {@code and} hit different Lucene bulk scorers. The filter keeps a fixed random half
 * of the documents. {@link #setup()} prints how many rows the source emitted so the time can be read next to it.
 */
@Fork(1)
@Warmup(iterations = 5, time = 1)
@Measurement(iterations = 7, time = 1)
@BenchmarkMode(Mode.AverageTime)
@OutputTimeUnit(TimeUnit.MILLISECONDS)
@State(Scope.Thread)
public class MinCompetitiveScoreBenchmark {
    static {
        BenchmarkLogging.configure();
    }

    private static final BlockFactory blockFactory = BlockFactory.builder(BigArrays.NON_RECYCLING_INSTANCE)
        .breaker(new NoopCircuitBreaker("none"))
        .build();
    private static final DriverContext driverContext = new DriverContext(BigArrays.NON_RECYCLING_INSTANCE, blockFactory, null);

    private static final String FIELD = "text";
    private static final int VOCABULARY = 10_000;
    private static final int NUM_DOCS = 500_000;
    /**
     * ES|QL sizes pages as {@code 256kb / estimated row size}. This is the order of magnitude that gives for
     * a query that extracts a few fields on top of {@code _score}.
     */
    private static final int PAGE_SIZE = 4096;
    private static final SharedMinCompetitive.KeyConfig SCORE_DESC = new SharedMinCompetitive.KeyConfig(
        ElementType.DOUBLE,
        TopNEncoder.DEFAULT_SORTABLE,
        false,
        true
    );

    static {
        // After the constants above, which the self-test uses
        if (false == "true".equals(System.getProperty("skipSelfTest"))) {
            selfTest();
        }
    }

    /**
     * {@code term} matches about a quarter of the documents, {@code or} about a third and {@code and} about 15%.
     */
    @Param({ "term", "or", "and" })
    public String query;

    @Param({ "10", "1000" })
    public int topCount;

    @Param({ "false", "true" })
    public boolean optimize;

    private Index index;

    @Setup(Level.Trial)
    public void setup() {
        index = Index.build(NUM_DOCS, 42);
        Result result = run(index, query, topCount, optimize);
        System.out.printf(
            "query=%s topCount=%d optimize=%s rows emitted by the source=%d of %d matches%n",
            query,
            topCount,
            optimize,
            result.emitted,
            run(index, query, topCount, false).emitted
        );
    }

    @TearDown(Level.Trial)
    public void tearDown() throws IOException {
        index.close();
    }

    @Benchmark
    public double run() {
        List<Double> scores = run(index, query, topCount, optimize).scores;
        return scores.isEmpty() ? 0 : scores.getLast();
    }

    /**
     * On and off must produce the same top N scores for every query and top count, and on must never emit more rows.
     */
    static void selfTest() {
        try (Index index = Index.build(20_000, 7)) {
            for (String query : Utils.possibleValues(MinCompetitiveScoreBenchmark.class, "query")) {
                for (String topCount : Utils.possibleValues(MinCompetitiveScoreBenchmark.class, "topCount")) {
                    Result off = run(index, query, Integer.parseInt(topCount), false);
                    Result on = run(index, query, Integer.parseInt(topCount), true);
                    if (off.scores.isEmpty()) {
                        throw new AssertionError("no results for [" + query + "]");
                    }
                    if (off.scores.equals(on.scores) == false) {
                        throw new AssertionError("[" + query + "] top [" + topCount + "] differs: off " + off.scores + " on " + on.scores);
                    }
                    if (on.emitted > off.emitted) {
                        throw new AssertionError("[" + query + "] emitted more with the optimization: " + on.emitted + " > " + off.emitted);
                    }
                }
            }
        } catch (IOException e) {
            throw new UncheckedIOException(e);
        }
    }

    private record Result(List<Double> scores, long emitted) {}

    private static Result run(Index index, String query, int topCount, boolean optimize) {
        SharedMinCompetitive.Supplier supplier = optimize
            ? new SharedMinCompetitive.Supplier(blockFactory.breaker(), List.of(SCORE_DESC))
            : null;
        Query luceneQuery = index.query(query);
        LuceneSourceOperator.Factory factory = new LuceneSourceOperator.Factory(
            new IndexedByShardIdFromSingleton<>(index.shardContext),
            ctx -> List.of(new LuceneSliceQueue.QueryAndTags(luceneQuery, List.of())),
            DataPartitioning.SHARD,
            DataPartitioning.AutoStrategy.DEFAULT,
            0,                       // unused for SHARD
            1,                       // taskConcurrency
            PAGE_SIZE,
            LuceneOperator.NO_LIMIT,
            true,                    // needsScore
            () -> 0L,
            1,                       // unused for SHARD
            QueryWarnings.NOOP,
            null,
            optimize ? new MinCompetitiveScore.Factory(supplier) : null
        );
        LuceneSourceOperator source = (LuceneSourceOperator) factory.get(driverContext);
        TopNOperator topN = new TopNOperator(
            blockFactory,
            blockFactory.breaker(),
            topCount,
            List.of(ElementType.DOUBLE),
            List.of(TopNEncoder.DEFAULT_SORTABLE),
            List.of(new TopNOperator.SortOrder(0, false, true)),
            PAGE_SIZE,
            Long.MAX_VALUE,
            TopNOperator.InputOrdering.NOT_SORTED,
            supplier
        );
        try {
            while (source.isFinished() == false) {
                Page page = source.getOutput();
                if (page != null) {
                    Page kept = filter(page, index.keep);
                    if (kept != null) {
                        topN.addInput(kept);
                    }
                }
            }
            topN.finish();
            List<Double> scores = new ArrayList<>(topCount);
            while (topN.isFinished() == false) {
                Page out = topN.getOutput();
                if (out == null) {
                    continue;
                }
                try {
                    DoubleBlock block = out.getBlock(0);
                    for (int p = 0; p < block.getPositionCount(); p++) {
                        scores.add(block.getDouble(p));
                    }
                } finally {
                    out.releaseBlocks();
                }
            }
            return new Result(scores, ((LuceneOperator.Status) source.status()).rowsEmitted());
        } finally {
            Releasables.close(source, topN);
        }
    }

    /**
     * Stands in for a filter ES|QL can't push to Lucene, like {@code WHERE LENGTH(title) > 10}. Keeps only
     * {@code _score} because that's all the TopN sorts on. Returns {@code null} if it keeps nothing.
     */
    private static Page filter(Page page, boolean[] keep) {
        try {
            DocVector docs = ((DocBlock) page.getBlock(0)).asVector();
            DoubleVector scores = ((DoubleBlock) page.getBlock(1)).asVector();
            try (DoubleVector.Builder kept = blockFactory.newDoubleVectorBuilder(page.getPositionCount())) {
                int count = 0;
                for (int p = 0; p < page.getPositionCount(); p++) {
                    if (keep[docs.docs().getInt(p)]) {
                        kept.appendDouble(scores.getDouble(p));
                        count++;
                    }
                }
                DoubleVector vector = kept.build();
                if (count == 0) {
                    vector.close();
                    return null;
                }
                return new Page(vector.asBlock());
            }
        } finally {
            page.releaseBlocks();
        }
    }

    /**
     * A single segment index so doc ids are global and {@link #keep} can be indexed by them directly.
     */
    private static final class Index implements Closeable {
        private final Directory directory;
        private final DirectoryReader reader;
        private final boolean[] keep;
        private final BenchmarkShardContext shardContext;

        private Index(Directory directory, DirectoryReader reader, boolean[] keep) {
            this.directory = directory;
            this.reader = reader;
            this.keep = keep;
            IndexSearcher searcher = new IndexSearcher(reader);
            searcher.setQueryCache(null);
            this.shardContext = new BenchmarkShardContext(searcher);
        }

        static Index build(int numDocs, long seed) {
            Random random = new Random(seed);
            double[] cumulative = zipf();
            Directory directory = new ByteBuffersDirectory();
            try {
                try (IndexWriter writer = new IndexWriter(directory, new IndexWriterConfig(new WhitespaceAnalyzer()))) {
                    StringBuilder text = new StringBuilder();
                    for (int d = 0; d < numDocs; d++) {
                        text.setLength(0);
                        int length = 5 + random.nextInt(46);
                        for (int t = 0; t < length; t++) {
                            text.append('t').append(sampleRank(cumulative, random)).append(' ');
                        }
                        Document doc = new Document();
                        doc.add(new TextField(FIELD, text.toString(), Field.Store.NO));
                        writer.addDocument(doc);
                    }
                    writer.forceMerge(1);
                }
                DirectoryReader reader = DirectoryReader.open(directory);
                if (reader.leaves().size() != 1) {
                    throw new AssertionError("expected a single segment but got " + reader.leaves().size());
                }
                boolean[] keep = new boolean[numDocs];
                for (int d = 0; d < numDocs; d++) {
                    keep[d] = random.nextBoolean();
                }
                return new Index(directory, reader, keep);
            } catch (IOException e) {
                throw new UncheckedIOException(e);
            }
        }

        Query query(String query) {
            return switch (query) {
                case "term" -> term(10);
                case "or" -> new BooleanQuery.Builder().add(term(10), BooleanClause.Occur.SHOULD)
                    .add(term(30), BooleanClause.Occur.SHOULD)
                    .add(term(100), BooleanClause.Occur.SHOULD)
                    .build();
                case "and" -> new BooleanQuery.Builder().add(term(3), BooleanClause.Occur.MUST)
                    .add(term(10), BooleanClause.Occur.MUST)
                    .build();
                default -> throw new IllegalArgumentException("unknown query [" + query + "]");
            };
        }

        private static Query term(int rank) {
            return new TermQuery(new Term(FIELD, "t" + rank));
        }

        /** Cumulative Zipf ({@code s = 1}) distribution over ranks {@code 1..VOCABULARY}. */
        private static double[] zipf() {
            double[] cumulative = new double[VOCABULARY];
            double sum = 0;
            for (int r = 0; r < VOCABULARY; r++) {
                sum += 1.0 / (r + 1);
                cumulative[r] = sum;
            }
            for (int r = 0; r < VOCABULARY; r++) {
                cumulative[r] /= sum;
            }
            return cumulative;
        }

        private static int sampleRank(double[] cumulative, Random random) {
            int i = Arrays.binarySearch(cumulative, random.nextDouble());
            return (i >= 0 ? i : -i - 1) + 1;
        }

        @Override
        public void close() throws IOException {
            IOUtils.close(reader, directory);
        }
    }

    /** Bare {@link ShardContext} for a scoring {@link LuceneSourceOperator} scan. */
    private static final class BenchmarkShardContext implements ShardContext {
        private final IndexSearcher searcher;
        private final ShardSearchStats stats = new ShardSearchStats(
            new SearchStatsSettings(new ClusterSettings(Settings.EMPTY, ClusterSettings.BUILT_IN_CLUSTER_SETTINGS))
        );

        private BenchmarkShardContext(IndexSearcher searcher) {
            this.searcher = searcher;
        }

        @Override
        public int index() {
            return 0;
        }

        @Override
        public IndexSearcher searcher() {
            return searcher;
        }

        @Override
        public String shardIdentifier() {
            return "bench";
        }

        @Override
        public ShardSearchStats stats() {
            return stats;
        }

        @Override
        public Optional<SortAndFormats> buildSort(List<SortBuilder<?>> sorts) {
            throw new UnsupportedOperationException("no sort in this scan");
        }

        @Override
        public SourceLoader newSourceLoader(Set<String> sourcePaths) {
            throw new UnsupportedOperationException("no _source loading in this scan");
        }

        @Override
        public BlockLoader blockLoader(
            String name,
            boolean asUnsupportedSource,
            MappedFieldType.FieldExtractPreference fieldExtractPreference,
            BlockLoaderFunctionConfig blockLoaderFunctionConfig,
            Warnings warnings,
            ByteSizeValue blockLoaderSizeOrdinals,
            ByteSizeValue blockLoaderSizeScript
        ) {
            throw new UnsupportedOperationException("no field values loaded in this scan");
        }

        @Override
        public MappedFieldType fieldType(String name) {
            throw new UnsupportedOperationException("no field types resolved in this scan");
        }

        @Override
        public void incRef() {}

        @Override
        public boolean tryIncRef() {
            return true;
        }

        @Override
        public boolean decRef() {
            return false;
        }

        @Override
        public boolean hasReferences() {
            return true;
        }
    }
}
