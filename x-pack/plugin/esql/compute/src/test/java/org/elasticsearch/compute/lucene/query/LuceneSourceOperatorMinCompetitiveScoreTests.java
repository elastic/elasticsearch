/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.compute.lucene.query;

import org.apache.lucene.document.Document;
import org.apache.lucene.document.Field;
import org.apache.lucene.document.TextField;
import org.apache.lucene.index.DirectoryReader;
import org.apache.lucene.index.IndexReader;
import org.apache.lucene.index.IndexWriter;
import org.apache.lucene.index.IndexWriterConfig;
import org.apache.lucene.index.LeafReaderContext;
import org.apache.lucene.index.NoMergePolicy;
import org.apache.lucene.index.Term;
import org.apache.lucene.search.BooleanClause;
import org.apache.lucene.search.BooleanQuery;
import org.apache.lucene.search.BoostQuery;
import org.apache.lucene.search.IndexSearcher;
import org.apache.lucene.search.Query;
import org.apache.lucene.search.ScoreDoc;
import org.apache.lucene.search.TermQuery;
import org.apache.lucene.search.similarities.BM25Similarity;
import org.apache.lucene.store.Directory;
import org.apache.lucene.tests.analysis.MockAnalyzer;
import org.apache.lucene.tests.index.RandomIndexWriter;
import org.elasticsearch.compute.data.BlockFactory;
import org.elasticsearch.compute.data.DocBlock;
import org.elasticsearch.compute.data.DocVector;
import org.elasticsearch.compute.data.DoubleBlock;
import org.elasticsearch.compute.data.DoubleVector;
import org.elasticsearch.compute.data.ElementType;
import org.elasticsearch.compute.data.IntBlock;
import org.elasticsearch.compute.data.IntVector;
import org.elasticsearch.compute.data.Page;
import org.elasticsearch.compute.lucene.IndexedByShardIdFromSingleton;
import org.elasticsearch.compute.lucene.ShardContext;
import org.elasticsearch.compute.operator.AbstractPageMappingOperator;
import org.elasticsearch.compute.operator.Driver;
import org.elasticsearch.compute.operator.DriverContext;
import org.elasticsearch.compute.operator.PageConsumerOperator;
import org.elasticsearch.compute.operator.topn.SharedGlobalTopK;
import org.elasticsearch.compute.operator.topn.SharedMinCompetitive;
import org.elasticsearch.compute.operator.topn.TopNEncoder;
import org.elasticsearch.compute.operator.topn.TopNOperator;
import org.elasticsearch.compute.querydsl.query.QueryWarnings;
import org.elasticsearch.compute.test.ComputeTestCase;
import org.elasticsearch.compute.test.OperatorTestCase;
import org.elasticsearch.compute.test.TestDriverFactory;
import org.elasticsearch.compute.test.TestDriverRunner;
import org.elasticsearch.core.IOUtils;
import org.elasticsearch.index.cache.query.TrivialQueryCachingPolicy;
import org.elasticsearch.search.internal.ContextIndexSearcher;
import org.junit.After;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Comparator;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.function.IntPredicate;

import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.greaterThan;
import static org.hamcrest.Matchers.greaterThanOrEqualTo;
import static org.hamcrest.Matchers.lessThan;
import static org.hamcrest.Matchers.lessThanOrEqualTo;

/**
 * Tests feeding a {@code SORT _score DESC} TopN's bound back into {@link LuceneSourceOperator}
 * through {@link MinCompetitiveScore}. Uses real BM25 scoring over real Lucene indices and a real
 * {@link TopNOperator} so the bound comes from the same place it does in production.
 */
public class LuceneSourceOperatorMinCompetitiveScoreTests extends ComputeTestCase {
    private static final String FIELD = "text";
    private static final String[] TERMS = { "a", "b", "c", "d", "e" };

    private final Directory directory = newDirectory();
    private IndexReader reader;

    @After
    public void closeIndex() throws IOException {
        IOUtils.close(reader, directory);
    }

    /**
     * Publish a bound directly and check that the operator emits exactly the documents scoring at
     * least the bound, including every tie, and that it told Lucene about the bound.
     */
    public void testEmitsEveryDocAtOrAboveBound() throws IOException {
        reader = randomReader(between(100, 3000));
        ShardContext ctx = bm25ShardContext(reader);
        Query query = new TermQuery(new Term(FIELD, "a"));
        List<ScoreDoc> all = allScores(ctx, query);
        assumeTrue("need matches", all.isEmpty() == false);
        float bound = all.get(between(0, all.size() - 1)).score;

        BlockFactory blockFactory = blockFactory();
        SharedMinCompetitive.Supplier supplier = scoreSupplier(blockFactory);
        try (SharedMinCompetitive channel = supplier.get()) {
            publish(blockFactory, supplier, bound);
            LuceneSourceOperator.Factory factory = factory(
                ctx,
                query,
                randomFrom(DataPartitioning.SHARD, DataPartitioning.SEGMENT, DataPartitioning.DOC),
                1,
                between(10, 500),
                new MinCompetitiveScore.Factory(supplier)
            );
            DriverContext driverContext = driverContext();
            LuceneSourceOperator source = (LuceneSourceOperator) factory.get(driverContext);
            List<ScoreDoc> emitted = new ArrayList<>();
            new TestDriverRunner().run(TestDriverFactory.create(driverContext, source, List.of(), new PageConsumerOperator(page -> {
                try {
                    collectDocsAndScores(reader.leaves(), page, emitted);
                } finally {
                    page.releaseBlocks();
                }
            })));
            OperatorTestCase.assertDriverContext(driverContext);

            Set<Integer> expected = new HashSet<>();
            for (ScoreDoc sd : all) {
                if (sd.score >= bound) {
                    expected.add(sd.doc);
                }
            }
            Set<Integer> actual = new HashSet<>();
            for (ScoreDoc sd : emitted) {
                assertTrue("emitted a doc twice " + sd.doc, actual.add(sd.doc));
                assertThat(sd.score, greaterThanOrEqualTo(bound));
            }
            assertThat(actual, equalTo(expected));
            if (bound > 0) {
                assertThat(source.minCompetitiveScoreUpdates(), greaterThan(0));
            }
        }
    }

    /**
     * Without a {@link MinCompetitiveScore} the operator must emit every match.
     */
    public void testUnchangedWithoutMinCompetitiveScore() throws IOException {
        reader = randomReader(between(1, 1000));
        ShardContext ctx = bm25ShardContext(reader);
        Query query = new TermQuery(new Term(FIELD, "a"));
        LuceneSourceOperator.Factory factory = factory(ctx, query, DataPartitioning.SHARD, 1, between(10, 500), null);
        DriverContext driverContext = driverContext();
        List<ScoreDoc> emitted = new ArrayList<>();
        new TestDriverRunner().run(
            TestDriverFactory.create(driverContext, factory.get(driverContext), List.of(), new PageConsumerOperator(page -> {
                try {
                    collectDocsAndScores(reader.leaves(), page, emitted);
                } finally {
                    page.releaseBlocks();
                }
            }))
        );
        assertThat(emitted.size(), equalTo(allScores(ctx, query).size()));
    }

    public void testRejectsLimit() throws IOException {
        reader = randomReader(10);
        ShardContext ctx = bm25ShardContext(reader);
        SharedMinCompetitive.Supplier supplier = scoreSupplier(blockFactory());
        expectThrows(
            IllegalArgumentException.class,
            () -> new LuceneSourceOperator.Factory(
                new IndexedByShardIdFromSingleton<>(ctx),
                c -> List.of(new LuceneSliceQueue.QueryAndTags(new TermQuery(new Term(FIELD, "a")), List.of())),
                DataPartitioning.SHARD,
                DataPartitioning.AutoStrategy.DEFAULT,
                LuceneOperator.SMALL_INDEX_BOUNDARY,
                1,
                100,
                10,
                true,
                () -> 0L,
                LuceneSliceQueue.MIN_DOCS_PER_SLICE,
                QueryWarnings.EMIT,
                null,
                new MinCompetitiveScore.Factory(supplier)
            )
        );
    }

    public void testRejectsNoScores() throws IOException {
        reader = randomReader(10);
        ShardContext ctx = bm25ShardContext(reader);
        SharedMinCompetitive.Supplier supplier = scoreSupplier(blockFactory());
        expectThrows(
            IllegalArgumentException.class,
            () -> new LuceneSourceOperator.Factory(
                new IndexedByShardIdFromSingleton<>(ctx),
                c -> List.of(new LuceneSliceQueue.QueryAndTags(new TermQuery(new Term(FIELD, "a")), List.of())),
                DataPartitioning.SHARD,
                DataPartitioning.AutoStrategy.DEFAULT,
                LuceneOperator.SMALL_INDEX_BOUNDARY,
                1,
                100,
                LuceneOperator.NO_LIMIT,
                false,
                () -> 0L,
                LuceneSliceQueue.MIN_DOCS_PER_SLICE,
                QueryWarnings.EMIT,
                null,
                new MinCompetitiveScore.Factory(supplier)
            )
        );
    }

    /**
     * {@code LuceneSource -> filter -> TopN(_score DESC)} over random data, queries, limits,
     * partitionings and driver counts must produce the same top N with and without the optimization.
     * Tied documents may differ, the scores may not.
     */
    public void testSameResultsAsWithoutOptimization() throws IOException {
        reader = randomReader(between(1, 5000));
        ShardContext ctx = new LuceneSourceOperatorTests.MockShardContext(reader, 0);
        Query query = randomQuery();
        int topCount = between(1, 100);
        boolean[] keep = randomKeep(reader.maxDoc());
        DataPartitioning partitioning = randomFrom(DataPartitioning.SHARD, DataPartitioning.SEGMENT, DataPartitioning.DOC);
        int taskConcurrency = between(1, 4);
        int maxPageSize = between(10, 1000);
        boolean globalMerge = randomBoolean();

        Run off = run(ctx, query, partitioning, taskConcurrency, maxPageSize, topCount, d -> keep[d], false, false);
        Run on = run(ctx, query, partitioning, taskConcurrency, maxPageSize, topCount, d -> keep[d], true, globalMerge);
        logger.info(
            "query={} topCount={} partitioning={} drivers={} globalMerge={} emitted off={} on={}",
            query,
            topCount,
            partitioning,
            taskConcurrency,
            globalMerge,
            off.emitted,
            on.emitted
        );
        assertSameTopN(off.top, on.top);
        assertThat(on.emitted, lessThanOrEqualTo(off.emitted));
    }

    /**
     * A skewed index where a few documents score far higher than the rest. Once the TopN is full
     * Lucene should skip most of the low scoring documents. This doubles as a measurement: it logs
     * how many documents the source emitted with and without the optimization.
     */
    public void testSkipsDocumentsOnSkewedIndex() throws IOException {
        int numDocs = 50_000;
        int numHot = 200;
        reader = skewedReader(numDocs, numHot);
        ShardContext ctx = bm25ShardContext(reader);
        Query query = new TermQuery(new Term(FIELD, "a"));
        int topCount = 10;
        IntPredicate keep = doc -> doc % 3 != 0;
        int maxPageSize = 1000;

        Run off = run(ctx, query, DataPartitioning.SHARD, 1, maxPageSize, topCount, keep, false, false);
        Run on = run(ctx, query, DataPartitioning.SHARD, 1, maxPageSize, topCount, keep, true, false);
        logger.info("skewed index: emitted off={} on={} updates={}", off.emitted, on.emitted, on.updates);
        assertSameTopN(off.top, on.top);
        assertThat(off.emitted, equalTo((long) numDocs));
        // Low scoring docs collected before the TopN publishes or raises its bound still come through. Still,
        // we expect to skip the overwhelming majority.
        assertThat(on.emitted, lessThan(off.emitted / 10));
        assertThat(on.updates, greaterThan(0L));
    }

    private record Run(List<ScoreDoc> top, long emitted, long updates) {}

    /**
     * Run {@code LuceneSource -> filter -> TopN(_score DESC)} on {@code taskConcurrency} drivers
     * and merge their outputs into the final top {@code topCount} like the node reduce would.
     */
    private Run run(
        ShardContext ctx,
        Query query,
        DataPartitioning partitioning,
        int taskConcurrency,
        int maxPageSize,
        int topCount,
        IntPredicate keep,
        boolean optimize,
        boolean globalMerge
    ) {
        BlockFactory blockFactory = blockFactory();
        SharedMinCompetitive.Supplier supplier = optimize ? scoreSupplier(blockFactory) : null;
        TopNOperator.GlobalTopKMergeConfig globalTopK = globalMerge && topCount > 1
            ? new TopNOperator.GlobalTopKMergeConfig(
                new SharedGlobalTopK.Supplier(blockFactory.breaker(), topCount, supplier),
                between(1, 3),
                between(1, 20000)
            )
            : null;
        LuceneSourceOperator.Factory sourceFactory = factory(
            ctx,
            query,
            partitioning,
            taskConcurrency,
            maxPageSize,
            optimize ? new MinCompetitiveScore.Factory(supplier) : null
        );
        TopNOperator.TopNOperatorFactory topNFactory = new TopNOperator.TopNOperatorFactory(
            topCount,
            List.of(ElementType.INT, ElementType.DOUBLE),
            List.of(TopNEncoder.DEFAULT_UNSORTABLE, TopNEncoder.DEFAULT_SORTABLE),
            List.of(new TopNOperator.SortOrder(1, false, true)),
            between(1, 1000),
            Long.MAX_VALUE,
            TopNOperator.InputOrdering.NOT_SORTED,
            supplier,
            globalTopK,
            null
        );
        List<ScoreDoc> results = Collections.synchronizedList(new ArrayList<>());
        List<LuceneSourceOperator> sources = new ArrayList<>();
        List<DriverContext> contexts = new ArrayList<>();
        List<Driver> drivers = new ArrayList<>();
        for (int i = 0; i < sourceFactory.taskConcurrency(); i++) {
            DriverContext driverContext = driverContext();
            contexts.add(driverContext);
            LuceneSourceOperator source = (LuceneSourceOperator) sourceFactory.get(driverContext);
            sources.add(source);
            drivers.add(
                TestDriverFactory.create(
                    driverContext,
                    source,
                    List.of(
                        new FilterAndGlobalDocOperator(driverContext.blockFactory(), ctx.searcher().getIndexReader().leaves(), keep),
                        topNFactory.get(driverContext)
                    ),
                    new PageConsumerOperator(page -> {
                        try {
                            IntBlock docs = page.getBlock(0);
                            DoubleBlock scores = page.getBlock(1);
                            for (int p = 0; p < page.getPositionCount(); p++) {
                                results.add(new ScoreDoc(docs.getInt(p), (float) scores.getDouble(p)));
                            }
                        } finally {
                            page.releaseBlocks();
                        }
                    })
                )
            );
        }
        new TestDriverRunner().run(drivers);
        contexts.forEach(OperatorTestCase::assertDriverContext);
        long emitted = 0;
        long updates = 0;
        for (LuceneSourceOperator source : sources) {
            emitted += ((LuceneOperator.Status) source.status()).rowsEmitted();
            updates += source.minCompetitiveScoreUpdates();
        }
        List<ScoreDoc> top = new ArrayList<>(results);
        top.sort(Comparator.comparingDouble((ScoreDoc sd) -> sd.score).reversed());
        return new Run(top.subList(0, Math.min(topCount, top.size())), emitted, updates);
    }

    private static void assertSameTopN(List<ScoreDoc> expected, List<ScoreDoc> actual) {
        assertThat(scores(actual), equalTo(scores(expected)));
        if (expected.isEmpty()) {
            return;
        }
        // Everything strictly better than the last kept score is fully determined. Ties at the end aren't.
        float last = expected.getLast().score;
        assertThat(docsAbove(actual, last), equalTo(docsAbove(expected, last)));
    }

    private static List<Float> scores(List<ScoreDoc> docs) {
        return docs.stream().map(sd -> sd.score).toList();
    }

    private static Set<Integer> docsAbove(List<ScoreDoc> docs, float score) {
        Set<Integer> result = new HashSet<>();
        for (ScoreDoc sd : docs) {
            if (sd.score > score) {
                result.add(sd.doc);
            }
        }
        return result;
    }

    private DriverContext driverContext() {
        BlockFactory blockFactory = blockFactory();
        return new DriverContext(blockFactory.bigArrays(), blockFactory, null);
    }

    private LuceneSourceOperator.Factory factory(
        ShardContext ctx,
        Query query,
        DataPartitioning partitioning,
        int taskConcurrency,
        int maxPageSize,
        MinCompetitiveScore.Factory minCompetitiveScore
    ) {
        return new LuceneSourceOperator.Factory(
            new IndexedByShardIdFromSingleton<>(ctx),
            c -> List.of(new LuceneSliceQueue.QueryAndTags(query, List.of())),
            partitioning,
            DataPartitioning.AutoStrategy.DEFAULT,
            LuceneOperator.SMALL_INDEX_BOUNDARY,
            taskConcurrency,
            maxPageSize,
            LuceneOperator.NO_LIMIT,
            true,
            () -> 0L,
            // Small slices so DOC partitioning really splits segments
            between(1, 1000),
            QueryWarnings.EMIT,
            null,
            minCompetitiveScore
        );
    }

    private static SharedMinCompetitive.Supplier scoreSupplier(BlockFactory blockFactory) {
        return new SharedMinCompetitive.Supplier(blockFactory.breaker(), List.of(MinCompetitiveScoreTests.SCORE_DESC));
    }

    /**
     * Publish {@code score} the way production does: through a {@link TopNOperator} whose heap is full.
     */
    private static void publish(BlockFactory blockFactory, SharedMinCompetitive.Supplier supplier, float score) {
        try (
            TopNOperator topN = new TopNOperator(
                blockFactory,
                blockFactory.breaker(),
                1,
                List.of(ElementType.DOUBLE),
                List.of(TopNEncoder.DEFAULT_SORTABLE),
                List.of(new TopNOperator.SortOrder(0, false, true)),
                100,
                Long.MAX_VALUE,
                TopNOperator.InputOrdering.NOT_SORTED,
                supplier
            )
        ) {
            topN.addInput(new Page(blockFactory.newConstantDoubleBlockWith(score, 1)));
        }
    }

    /**
     * Reference scores for every matching document, computed by Lucene itself.
     */
    private static List<ScoreDoc> allScores(ShardContext ctx, Query query) throws IOException {
        IndexSearcher searcher = ctx.searcher();
        return List.of(searcher.search(query, Math.max(1, searcher.getIndexReader().maxDoc())).scoreDocs);
    }

    private static void collectDocsAndScores(List<LeafReaderContext> leaves, Page page, List<ScoreDoc> into) {
        DocVector docs = ((DocBlock) page.getBlock(0)).asVector();
        DoubleVector scores = ((DoubleBlock) page.getBlock(1)).asVector();
        for (int p = 0; p < page.getPositionCount(); p++) {
            int globalDoc = leaves.get(docs.segments().getInt(p)).docBase + docs.docs().getInt(p);
            into.add(new ScoreDoc(globalDoc, (float) scores.getDouble(p)));
        }
    }

    private IndexReader randomReader(int numDocs) throws IOException {
        try (RandomIndexWriter writer = new RandomIndexWriter(random(), directory, newIndexWriterConfig(new MockAnalyzer(random())))) {
            for (int d = 0; d < numDocs; d++) {
                StringBuilder text = new StringBuilder();
                int length = between(1, 30);
                for (int t = 0; t < length; t++) {
                    // Skewed so some terms are much more frequent than others
                    text.append(TERMS[Math.min(TERMS.length - 1, (int) Math.floor(-Math.log(randomDouble() + 1e-9)))]).append(' ');
                }
                Document doc = new Document();
                doc.add(new TextField(FIELD, text.toString(), Field.Store.NO));
                writer.addDocument(doc);
                if (rarely()) {
                    writer.commit();
                }
            }
            return writer.getReader();
        }
    }

    /**
     * {@code numHot} short documents with many {@code a}s among long documents with a single {@code a}.
     * Force merged into a single segment so the whole run is one scorer over one set of impacts,
     * which keeps the number of skipped documents predictable.
     */
    private IndexReader skewedReader(int numDocs, int numHot) throws IOException {
        Set<Integer> hot = new HashSet<>();
        while (hot.size() < numHot) {
            hot.add(between(0, numDocs - 1));
        }
        IndexWriterConfig config = new IndexWriterConfig(new MockAnalyzer(random())).setMergePolicy(NoMergePolicy.INSTANCE)
            .setSimilarity(new BM25Similarity())
            .setMaxBufferedDocs(numDocs + 1)
            .setRAMBufferSizeMB(IndexWriterConfig.DISABLE_AUTO_FLUSH);
        try (IndexWriter writer = new IndexWriter(directory, config)) {
            for (int d = 0; d < numDocs; d++) {
                String text = hot.contains(d) ? "a a a a a a a a b" : "a " + "b ".repeat(between(10, 40));
                Document doc = new Document();
                doc.add(new TextField(FIELD, text, Field.Store.NO));
                writer.addDocument(doc);
            }
            writer.forceMerge(1);
        }
        return DirectoryReader.open(directory);
    }

    private static ShardContext bm25ShardContext(IndexReader reader) throws IOException {
        ContextIndexSearcher searcher = new ContextIndexSearcher(
            reader,
            new BM25Similarity(),
            IndexSearcher.getDefaultQueryCache(),
            TrivialQueryCachingPolicy.NEVER,
            true
        );
        return new LuceneSourceOperatorTests.MockShardContext(searcher, 0);
    }

    private Query randomQuery() {
        Query a = new TermQuery(new Term(FIELD, "a"));
        Query b = new TermQuery(new Term(FIELD, "b"));
        Query c = new TermQuery(new Term(FIELD, "c"));
        Query d = new TermQuery(new Term(FIELD, "d"));
        return switch (between(0, 5)) {
            case 0 -> a;
            case 1 -> new BooleanQuery.Builder().add(a, BooleanClause.Occur.SHOULD).add(c, BooleanClause.Occur.SHOULD).build();
            case 2 -> new BooleanQuery.Builder().add(b, BooleanClause.Occur.SHOULD)
                .add(c, BooleanClause.Occur.SHOULD)
                .add(d, BooleanClause.Occur.SHOULD)
                .build();
            case 3 -> new BooleanQuery.Builder().add(a, BooleanClause.Occur.MUST).add(c, BooleanClause.Occur.SHOULD).build();
            case 4 -> new BooleanQuery.Builder().add(c, BooleanClause.Occur.MUST).add(b, BooleanClause.Occur.FILTER).build();
            case 5 -> new BoostQuery(
                new BooleanQuery.Builder().add(a, BooleanClause.Occur.MUST).add(d, BooleanClause.Occur.MUST).build(),
                2
            );
            default -> throw new AssertionError();
        };
    }

    private boolean[] randomKeep(int maxDoc) {
        boolean[] keep = new boolean[maxDoc];
        double keepRatio = randomDoubleBetween(0.1, 1, true);
        for (int i = 0; i < maxDoc; i++) {
            keep[i] = randomDouble() < keepRatio;
        }
        return keep;
    }

    /**
     * Stands in for the filter ES|QL couldn't push to Lucene, like {@code WHERE LENGTH(title) > 10}.
     * Drops rows the predicate rejects and replaces the doc block with the global doc id so the
     * TopN output can be compared across runs.
     */
    private static class FilterAndGlobalDocOperator extends AbstractPageMappingOperator {
        private final BlockFactory blockFactory;
        private final List<LeafReaderContext> leaves;
        private final IntPredicate keep;

        FilterAndGlobalDocOperator(BlockFactory blockFactory, List<LeafReaderContext> leaves, IntPredicate keep) {
            this.blockFactory = blockFactory;
            this.leaves = leaves;
            this.keep = keep;
        }

        @Override
        protected Page process(Page page) {
            try {
                DocVector docs = ((DocBlock) page.getBlock(0)).asVector();
                DoubleVector scores = ((DoubleBlock) page.getBlock(1)).asVector();
                try (
                    IntVector.Builder ids = blockFactory.newIntVectorBuilder(page.getPositionCount());
                    DoubleVector.Builder keptScores = blockFactory.newDoubleVectorBuilder(page.getPositionCount())
                ) {
                    for (int p = 0; p < page.getPositionCount(); p++) {
                        int globalDoc = leaves.get(docs.segments().getInt(p)).docBase + docs.docs().getInt(p);
                        if (keep.test(globalDoc)) {
                            ids.appendInt(globalDoc);
                            keptScores.appendDouble(scores.getDouble(p));
                        }
                    }
                    IntBlock idBlock = ids.build().asBlock();
                    try {
                        return new Page(idBlock, keptScores.build().asBlock());
                    } catch (Exception e) {
                        idBlock.close();
                        throw e;
                    }
                }
            } finally {
                page.releaseBlocks();
            }
        }

        @Override
        public String toString() {
            return "FilterAndGlobalDocOperator";
        }
    }
}
