/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.compute.lucene.query;

import org.apache.lucene.document.Field;
import org.apache.lucene.document.SortedNumericDocValuesField;
import org.apache.lucene.document.TextField;
import org.apache.lucene.index.IndexReader;
import org.apache.lucene.index.IndexableField;
import org.apache.lucene.index.LeafReaderContext;
import org.apache.lucene.index.ReaderUtil;
import org.apache.lucene.index.SortedNumericDocValues;
import org.apache.lucene.index.Term;
import org.apache.lucene.search.BooleanClause;
import org.apache.lucene.search.BooleanQuery;
import org.apache.lucene.search.BoostQuery;
import org.apache.lucene.search.ConstantScoreQuery;
import org.apache.lucene.search.DisjunctionMaxQuery;
import org.apache.lucene.search.IndexSearcher;
import org.apache.lucene.search.LeafCollector;
import org.apache.lucene.search.Query;
import org.apache.lucene.search.Scorable;
import org.apache.lucene.search.ScoreDoc;
import org.apache.lucene.search.Sort;
import org.apache.lucene.search.SortField;
import org.apache.lucene.search.SortedNumericSortField;
import org.apache.lucene.search.TermQuery;
import org.apache.lucene.store.Directory;
import org.apache.lucene.tests.index.RandomIndexWriter;
import org.apache.lucene.tests.search.AssertingQuery;
import org.elasticsearch.common.lucene.search.Queries;
import org.elasticsearch.compute.data.DocBlock;
import org.elasticsearch.compute.data.DocVector;
import org.elasticsearch.compute.data.DoubleBlock;
import org.elasticsearch.compute.data.Page;
import org.elasticsearch.compute.lucene.IndexedByShardIdFromList;
import org.elasticsearch.compute.lucene.IndexedByShardIdFromSingleton;
import org.elasticsearch.compute.lucene.ShardContext;
import org.elasticsearch.compute.operator.Driver;
import org.elasticsearch.compute.operator.DriverContext;
import org.elasticsearch.compute.querydsl.query.QueryWarnings;
import org.elasticsearch.compute.test.ComputeTestCase;
import org.elasticsearch.compute.test.TestDriverFactory;
import org.elasticsearch.compute.test.TestDriverRunner;
import org.elasticsearch.compute.test.TestResultPageSinkOperator;
import org.elasticsearch.core.IOUtils;
import org.elasticsearch.search.DocValueFormat;
import org.elasticsearch.search.sort.FieldSortBuilder;
import org.elasticsearch.search.sort.ScoreSortBuilder;
import org.elasticsearch.search.sort.SortAndFormats;
import org.elasticsearch.search.sort.SortBuilder;
import org.junit.After;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.Optional;
import java.util.concurrent.CopyOnWriteArrayList;

import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.greaterThanOrEqualTo;
import static org.hamcrest.Matchers.sameInstance;

/**
 * Tests how the collectors of {@link LuceneTopNSourceOperator} share their minimum competitive score.
 */
public class LuceneTopNSourceOperatorCollectorTests extends ComputeTestCase {

    /**
     * A sort that a {@link LuceneSourceOperatorTests.MockShardContext}, which has no mappings, resolves the sort builders to.
     *
     * @param skipsTies whether its collectors skip the documents that tie with the lowest score of a full top N
     */
    private record TestSort(String name, Sort sort, boolean skipsTies) {
        List<SortBuilder<?>> builders() {
            // The operator reads the score from the sort values, at a position that depends on the number of sort keys
            return sort.getSort().length == 1
                ? List.of(new ScoreSortBuilder())
                : List.of(new ScoreSortBuilder(), new FieldSortBuilder("s"));
        }

        @Override
        public String toString() {
            return name;
        }
    }

    private static final SortField FIELD_SORT = new SortedNumericSortField("s", SortField.Type.LONG);

    private static final TestSort SCORE = new TestSort("score", Sort.RELEVANCE, true);
    private static final TestSort SCORE_THEN_FIELD = new TestSort("score, field", new Sort(SortField.FIELD_SCORE, FIELD_SORT), false);
    private static final TestSort SCORE_ASCENDING = new TestSort(
        "score ascending",
        new Sort(new SortField(null, SortField.Type.SCORE, true)),
        false
    );
    private static final TestSort FIELD = new TestSort("field", new Sort(FIELD_SORT), false);
    private static final TestSort FIELD_THEN_SCORE = new TestSort("field, score", new Sort(FIELD_SORT, SortField.FIELD_SCORE), false);

    private static final List<TestSort> SHARING = List.of(SCORE, SCORE_THEN_FIELD);
    private static final List<TestSort> NOT_SHARING = List.of(SCORE_ASCENDING, FIELD, FIELD_THEN_SCORE);

    private final Directory directory = newDirectory();
    private IndexReader reader;

    @After
    public void closeIndex() throws IOException {
        IOUtils.close(reader, directory);
    }

    /**
     * Once a collector has a full top N, a collector of another driver starts with the lowest score of that top N as its
     * minimum competitive score, on the same shard or on another one.
     */
    public void testFullTopNSharesItsLowestScore() throws IOException {
        TestSort sort = randomFrom(SHARING);
        int limit = between(5, 20);
        setupIndex(100);
        ShardContext ctx = createMockShardContext(0, sort);
        var provider = new LuceneTopNSourceOperator.PerShardCollectorProvider(limit, true, sort.builders());

        float lowest = collectIncreasingScores(provider.newPerShardCollector(ctx), limit + between(1, 30), limit);
        float expected = sort.skipsTies ? Math.nextUp(lowest) : lowest;
        assertThat(sort.toString(), provider.sharedScoreBound.get(), equalTo(expected));

        var other = provider.newPerShardCollector(randomBoolean() ? ctx : createMockShardContext(1, sort));
        RecordingScorable scorer = new RecordingScorable();
        other.getLeafCollector(reader.leaves().getFirst()).setScorer(scorer);
        assertThat(sort.toString(), scorer.minCompetitiveScore, equalTo(expected));
    }

    /**
     * Collectors that don't skip by score, because their sort doesn't start with the descending score, share nothing.
     */
    public void testOtherSortsShareNothing() throws IOException {
        TestSort sort = randomFrom(NOT_SHARING);
        int limit = between(5, 20);
        setupIndex(100);
        ShardContext ctx = createMockShardContext(0, sort);
        var provider = new LuceneTopNSourceOperator.PerShardCollectorProvider(limit, sort != FIELD || randomBoolean(), sort.builders());

        collectIncreasingScores(provider.newPerShardCollector(ctx), limit + between(1, 30), limit);
        assertThat(sort.toString(), provider.sharedScoreBound.get(), equalTo(0f));

        provider.sharedScoreBound.offer(randomFloat() + 1);
        RecordingScorable scorer = new RecordingScorable();
        LeafCollector leafCollector = provider.newPerShardCollector(ctx).getLeafCollector(reader.leaves().getFirst());
        leafCollector.setScorer(scorer);
        assertThat(sort.toString(), scorer.minCompetitiveScore, equalTo(0f));
    }

    /**
     * A collector whose own top N is worse than another one's sets a lower minimum competitive score than the shared one.
     * The scorer never sees it: its minimum competitive score must not go down.
     */
    public void testScorerNeverGetsALowerScore() throws IOException {
        TestSort sort = randomFrom(SHARING);
        int limit = between(5, 20);
        int hits = limit + between(1, 30);
        setupIndex(100);
        ShardContext ctx = createMockShardContext(0, sort);
        var provider = new LuceneTopNSourceOperator.PerShardCollectorProvider(limit, true, sort.builders());
        float shared = hits + between(1, 100);
        provider.sharedScoreBound.offer(shared);

        var collector = provider.newPerShardCollector(ctx);
        RecordingScorable scorer = new RecordingScorable();
        LeafReaderContext leaf = reader.leaves().getFirst();
        LeafCollector leafCollector = collector.getLeafCollector(leaf);
        leafCollector.setScorer(scorer);
        for (int doc = 0; doc < Math.min(hits, leaf.reader().maxDoc()); doc++) {
            scorer.score = doc + 1;
            leafCollector.collect(doc);
        }
        assertThat(scorer.minCompetitiveScore, equalTo(shared));
        assertThat(scorer.lowered, equalTo(false));
        assertThat(provider.sharedScoreBound.get(), equalTo(shared));
    }

    /**
     * A collector picks up a score that another collector shares while it is collecting, not only when it gets a scorer.
     */
    public void testSharedScoreIsReadWhileCollecting() throws IOException {
        setupIndex(100);
        ShardContext ctx = createMockShardContext(0, SCORE);
        // The limit is above the number of documents collected, so the collector never sets a score itself
        var provider = new LuceneTopNSourceOperator.PerShardCollectorProvider(10_000, true, SCORE.builders());
        RecordingScorable scorer = new RecordingScorable();
        scorer.score = 1;
        LeafCollector leafCollector = provider.newPerShardCollector(ctx).getLeafCollector(reader.leaves().getFirst());
        leafCollector.setScorer(scorer);
        assertThat(scorer.minCompetitiveScore, equalTo(0f));

        float shared = randomFloat() + 1;
        provider.sharedScoreBound.offer(shared);
        for (int i = 0; i < 1024; i++) {
            leafCollector.collect(0);
        }
        assertThat(scorer.minCompetitiveScore, equalTo(shared));
    }

    public void testFactoriesDoNotShare() throws IOException {
        TestSort sort = randomFrom(SHARING);
        int limit = between(5, 20);
        setupIndex(100);
        ShardContext ctx = createMockShardContext(0, sort);
        var provider = new LuceneTopNSourceOperator.PerShardCollectorProvider(limit, true, sort.builders());
        var otherProvider = new LuceneTopNSourceOperator.PerShardCollectorProvider(limit, true, sort.builders());

        collectIncreasingScores(provider.newPerShardCollector(ctx), limit + between(1, 30), limit);
        assertThat(otherProvider.sharedScoreBound.get(), equalTo(0f));
    }

    public void testOperatorsOfAFactoryShareTheProvider() throws IOException {
        setupIndex(100);
        var factory = new LuceneTopNSourceOperator.Factory(
            new IndexedByShardIdFromSingleton<>(createMockShardContext(0, SCORE)),
            c -> List.of(new LuceneSliceQueue.QueryAndTags(Queries.ALL_DOCS_INSTANCE, List.of())),
            DataPartitioning.SHARD,
            DataPartitioning.AutoStrategy.DEFAULT,
            between(1, 10),
            10000,
            between(10, 100),
            SCORE.builders(),
            randomLongBetween(10, 20),
            true,
            () -> 0L,
            QueryWarnings.EMIT
        );
        List<LuceneTopNSourceOperator> operators = new ArrayList<>();
        try {
            for (int i = between(2, 10); i > 0; i--) {
                operators.add((LuceneTopNSourceOperator) factory.get(createDriverContext()));
            }
            for (var operator : operators) {
                assertThat(operator.perShardCollectorProvider, sameInstance(factory.perShardCollectorProvider));
            }
        } finally {
            IOUtils.close(operators);
        }
    }

    /**
     * Runs a top N by descending score over several shards with several drivers. Each driver skips the documents that score
     * below a top N another driver has already filled. The best {@code limit} rows the drivers emit must still be the best
     * {@code limit} documents of all shards. Rows that tie on every sort key are interchangeable, so the sort keys of the
     * rows are compared rather than the documents.
     * <p>
     *     Some shards are a single segment of more documents than the operator scores in one go, so its scorer is handed to
     *     the collector several times, and {@link DataPartitioning#DOC} splits such a segment between drivers.
     * </p>
     */
    public void testDriversSharingAScoreEmitTopN() throws IOException {
        TestSort sort = randomFrom(SHARING);
        int numShards = between(2, 4);
        List<Directory> directories = new ArrayList<>();
        List<IndexReader> readers = new ArrayList<>();
        try {
            List<ShardContext> contexts = new ArrayList<>();
            for (int shard = 0; shard < numShards; shard++) {
                Directory shardDirectory = newDirectory();
                directories.add(shardDirectory);
                try (RandomIndexWriter writer = new RandomIndexWriter(random(), shardDirectory)) {
                    boolean large = randomBoolean();
                    for (int d = large ? between(10_000, 20_000) : between(500, 3000); d > 0; d--) {
                        List<IndexableField> doc = new ArrayList<>();
                        doc.add(new SortedNumericDocValuesField("s", d));
                        // The frequency of "a" and the length of the field vary, so the scores do
                        doc.add(new TextField("t", "a ".repeat(between(1, 20)) + "b ".repeat(between(0, 20)), Field.Store.NO));
                        writer.addDocument(doc);
                    }
                    if (large) {
                        writer.forceMerge(1);
                    }
                    readers.add(writer.getReader());
                }
                contexts.add(createMockShardContext(readers.get(shard), shard, sort));
            }
            Query query = randomScoringQuery();
            int limit = between(1, 50);
            int taskConcurrency = between(1, 8);
            var factory = new LuceneTopNSourceOperator.Factory(
                new IndexedByShardIdFromList<>(contexts),
                c -> List.of(new LuceneSliceQueue.QueryAndTags(query, List.of())),
                randomFrom(DataPartitioning.SHARD, DataPartitioning.SEGMENT, DataPartitioning.DOC),
                DataPartitioning.AutoStrategy.DEFAULT,
                taskConcurrency,
                between(10, 100),
                limit,
                sort.builders(),
                randomLongBetween(10, 20),
                true,
                () -> 0L,
                QueryWarnings.EMIT
            );

            List<SortKeys> expected = new ArrayList<>();
            for (int shard = 0; shard < numShards; shard++) {
                IndexSearcher searcher = contexts.get(shard).searcher();
                for (ScoreDoc scoreDoc : searcher.search(query, limit, new Sort(SortField.FIELD_SCORE, FIELD_SORT), true).scoreDocs) {
                    expected.add(sortKeys(sort, readers.get(shard), scoreDoc.doc, scoreDoc.score));
                }
            }
            Collections.sort(expected);
            expected = expected.subList(0, limit);

            List<Page> results = new CopyOnWriteArrayList<>();
            List<Driver> drivers = new ArrayList<>();
            for (int i = 0; i < taskConcurrency; i++) {
                DriverContext driverContext = createDriverContext();
                drivers.add(
                    TestDriverFactory.create(
                        driverContext,
                        factory.get(driverContext),
                        List.of(),
                        new TestResultPageSinkOperator(results::add)
                    )
                );
            }
            new TestDriverRunner().run(drivers);

            List<SortKeys> actual = new ArrayList<>();
            for (Page page : results) {
                DocVector docs = page.<DocBlock>getBlock(0).asVector();
                DoubleBlock scores = page.getBlock(1);
                for (int p = 0; p < page.getPositionCount(); p++) {
                    IndexReader shardReader = readers.get(docs.shards().getInt(p));
                    int doc = shardReader.leaves().get(docs.segments().getInt(p)).docBase + docs.docs().getInt(p);
                    actual.add(sortKeys(sort, shardReader, doc, (float) scores.getDouble(p)));
                }
            }
            Collections.sort(actual);
            assertThat(sort.toString(), actual.size(), greaterThanOrEqualTo(limit));
            assertThat(sort.toString(), actual.subList(0, limit), equalTo(expected));
        } finally {
            IOUtils.close(readers);
            IOUtils.close(directories);
        }
    }

    /**
     * A query that matches every document of the index of {@link #testDriversSharingAScoreEmitTopN}, with scorers that skip
     * by score in different ways. Half of the time Lucene's {@link AssertingQuery} checks how its scorer is used, among other
     * things that its minimum competitive score never goes down.
     */
    private Query randomScoringQuery() {
        Query a = new TermQuery(new Term("t", "a"));
        Query b = new TermQuery(new Term("t", "b"));
        Query query = switch (between(0, 6)) {
            case 0 -> a;
            case 1 -> new BooleanQuery.Builder().add(a, BooleanClause.Occur.SHOULD).add(b, BooleanClause.Occur.SHOULD).build();
            case 2 -> new BooleanQuery.Builder().add(a, BooleanClause.Occur.MUST).add(b, BooleanClause.Occur.SHOULD).build();
            case 3 -> new BooleanQuery.Builder().add(a, BooleanClause.Occur.FILTER).add(b, BooleanClause.Occur.SHOULD).build();
            case 4 -> new ConstantScoreQuery(a);
            case 5 -> new DisjunctionMaxQuery(List.of(a, b), randomFrom(0f, 0.3f));
            case 6 -> new BoostQuery(a, randomFloat() * 10 + 0.1f);
            default -> throw new AssertionError("unreachable");
        };
        return randomBoolean() ? new AssertingQuery(random(), query) : query;
    }

    /**
     * One driver reads two shards in which every document scores the same. Once the first shard has filled the top N, no
     * document of the second shard can beat it, so the driver collects none of them and emits {@code limit} rows in all.
     * This fails if the score stops reaching the scorer of the second shard, which would not change the top N.
     */
    public void testSecondShardCollectsNothing() throws IOException {
        int limit = between(1, 20);
        List<Directory> directories = new ArrayList<>();
        List<IndexReader> readers = new ArrayList<>();
        try {
            List<ShardContext> contexts = new ArrayList<>();
            for (int shard = 0; shard < 2; shard++) {
                Directory shardDirectory = newDirectory();
                directories.add(shardDirectory);
                try (RandomIndexWriter writer = new RandomIndexWriter(random(), shardDirectory)) {
                    for (int d = between(limit + 1, 200); d > 0; d--) {
                        writer.addDocument(List.of(new SortedNumericDocValuesField("s", 1)));
                    }
                    readers.add(writer.getReader());
                }
                contexts.add(createMockShardContext(readers.get(shard), shard, SCORE));
            }
            var factory = new LuceneTopNSourceOperator.Factory(
                new IndexedByShardIdFromList<>(contexts),
                c -> List.of(new LuceneSliceQueue.QueryAndTags(new ConstantScoreQuery(Queries.ALL_DOCS_INSTANCE), List.of())),
                DataPartitioning.SHARD,
                DataPartitioning.AutoStrategy.DEFAULT,
                1,
                between(10, 100),
                limit,
                SCORE.builders(),
                randomLongBetween(10, 20),
                true,
                () -> 0L,
                QueryWarnings.EMIT
            );
            List<Page> results = new ArrayList<>();
            DriverContext driverContext = createDriverContext();
            new TestDriverRunner().run(
                TestDriverFactory.create(driverContext, factory.get(driverContext), List.of(), new TestResultPageSinkOperator(results::add))
            );
            assertThat(results.stream().mapToInt(Page::getPositionCount).sum(), equalTo(limit));
        } finally {
            IOUtils.close(readers);
            IOUtils.close(directories);
        }
    }

    /**
     * The operator offers the lowest score of a full top N when it emits it, because the collector has not always set one.
     * A collector that sorts by more keys only sets it when it collects a competitive document after its top N is full,
     * which never happens here because all documents tie on both keys. A collector that sorts by the score alone only sets
     * it once it has collected more documents than fit, and here it collects exactly {@code limit}.
     */
    public void testFullTopNIsOfferedWhenEmitted() throws IOException {
        TestSort sort = randomFrom(SHARING);
        int limit = between(1, 20);
        float score = randomFloat() * 10 + 0.1f;
        assertThat(
            sort.toString(),
            boundAfterTopNOfTiedDocuments(sort, limit, sort == SCORE ? limit : between(limit + 1, 200), score),
            equalTo(score)
        );
    }

    /**
     * A top N that isn't full says nothing about the documents of other shards, so it offers nothing.
     */
    public void testPartialTopNOffersNothing() throws IOException {
        TestSort sort = randomFrom(SHARING);
        int limit = between(2, 20);
        assertThat(
            sort.toString(),
            boundAfterTopNOfTiedDocuments(sort, limit, between(1, limit - 1), randomFloat() * 10 + 0.1f),
            equalTo(0f)
        );
    }

    /**
     * Runs one driver over one shard of {@code docs} documents that all score {@code score} and tie on {@code s}, and returns
     * the shared bound afterwards.
     */
    private float boundAfterTopNOfTiedDocuments(TestSort sort, int limit, int docs, float score) throws IOException {
        Directory shardDirectory = newDirectory();
        IndexReader shardReader = null;
        try {
            try (RandomIndexWriter writer = new RandomIndexWriter(random(), shardDirectory)) {
                for (int d = 0; d < docs; d++) {
                    writer.addDocument(List.of(new SortedNumericDocValuesField("s", 1)));
                }
                shardReader = writer.getReader();
            }
            Query query = new BoostQuery(new ConstantScoreQuery(Queries.ALL_DOCS_INSTANCE), score);
            var factory = new LuceneTopNSourceOperator.Factory(
                new IndexedByShardIdFromSingleton<>(createMockShardContext(shardReader, 0, sort)),
                c -> List.of(new LuceneSliceQueue.QueryAndTags(query, List.of())),
                DataPartitioning.SHARD,
                DataPartitioning.AutoStrategy.DEFAULT,
                1,
                between(10, 100),
                limit,
                sort.builders(),
                randomLongBetween(10, 20),
                true,
                () -> 0L,
                QueryWarnings.EMIT
            );
            List<Page> results = runDrivers(factory, 1);
            assertThat(results.stream().mapToInt(Page::getPositionCount).sum(), equalTo(Math.min(limit, docs)));
            return factory.perShardCollectorProvider.sharedScoreBound.get();
        } finally {
            IOUtils.close(shardReader, shardDirectory);
        }
    }

    /**
     * Every document of every shard scores the same, so the second sort key alone orders the rows. A collector that has
     * filled its top N shares that score, and the other collectors must still collect the documents that tie with it:
     * the rows with the lowest {@code s} are spread over all shards.
     */
    public void testTiesOnScoreAreOrderedBySecondKeyAcrossShards() throws IOException {
        int numShards = between(2, 4);
        int limit = between(1, 30);
        List<Directory> directories = new ArrayList<>();
        List<IndexReader> readers = new ArrayList<>();
        try {
            List<ShardContext> contexts = new ArrayList<>();
            for (int shard = 0; shard < numShards; shard++) {
                Directory shardDirectory = newDirectory();
                directories.add(shardDirectory);
                try (RandomIndexWriter writer = new RandomIndexWriter(random(), shardDirectory)) {
                    // Shard i holds i, i + numShards, i + 2 * numShards, ... in a random order
                    List<Integer> values = new ArrayList<>();
                    for (int d = between(limit + 1, 200); d > 0; d--) {
                        values.add(shard + (d - 1) * numShards);
                    }
                    Collections.shuffle(values, random());
                    for (int value : values) {
                        writer.addDocument(List.of(new SortedNumericDocValuesField("s", value)));
                    }
                    readers.add(writer.getReader());
                }
                contexts.add(createMockShardContext(readers.get(shard), shard, SCORE_THEN_FIELD));
            }
            int taskConcurrency = between(1, 4);
            var factory = new LuceneTopNSourceOperator.Factory(
                new IndexedByShardIdFromList<>(contexts),
                c -> List.of(new LuceneSliceQueue.QueryAndTags(new ConstantScoreQuery(Queries.ALL_DOCS_INSTANCE), List.of())),
                randomFrom(DataPartitioning.SHARD, DataPartitioning.SEGMENT),
                DataPartitioning.AutoStrategy.DEFAULT,
                taskConcurrency,
                between(10, 100),
                limit,
                SCORE_THEN_FIELD.builders(),
                randomLongBetween(10, 20),
                true,
                () -> 0L,
                QueryWarnings.EMIT
            );
            List<Long> actual = new ArrayList<>();
            for (Page page : runDrivers(factory, taskConcurrency)) {
                DocVector docs = page.<DocBlock>getBlock(0).asVector();
                for (int p = 0; p < page.getPositionCount(); p++) {
                    IndexReader shardReader = readers.get(docs.shards().getInt(p));
                    int doc = shardReader.leaves().get(docs.segments().getInt(p)).docBase + docs.docs().getInt(p);
                    actual.add(sortKeys(SCORE_THEN_FIELD, shardReader, doc, 1f).s);
                }
            }
            Collections.sort(actual);
            List<Long> expected = new ArrayList<>();
            for (long s = 0; s < limit; s++) {
                expected.add(s);
            }
            assertThat(actual.size(), greaterThanOrEqualTo(limit));
            assertThat(actual.subList(0, limit), equalTo(expected));
        } finally {
            IOUtils.close(readers);
            IOUtils.close(directories);
        }
    }

    /**
     * Runs {@code drivers} drivers that each read from an operator of {@code factory}, and returns the pages they emit.
     */
    private List<Page> runDrivers(LuceneTopNSourceOperator.Factory factory, int drivers) {
        List<Page> results = new CopyOnWriteArrayList<>();
        List<Driver> toRun = new ArrayList<>();
        for (int i = 0; i < drivers; i++) {
            DriverContext driverContext = createDriverContext();
            toRun.add(
                TestDriverFactory.create(driverContext, factory.get(driverContext), List.of(), new TestResultPageSinkOperator(results::add))
            );
        }
        new TestDriverRunner().run(toRun);
        return results;
    }

    /**
     * The sort keys of a row: its score and, if {@code sort} has a second key, its value of {@code s}.
     */
    private record SortKeys(float score, long s) implements Comparable<SortKeys> {
        @Override
        public int compareTo(SortKeys other) {
            int cmp = Float.compare(other.score, score);
            return cmp != 0 ? cmp : Long.compare(s, other.s);
        }
    }

    private static SortKeys sortKeys(TestSort sort, IndexReader reader, int doc, float score) throws IOException {
        if (sort == SCORE) {
            return new SortKeys(score, 0);
        }
        LeafReaderContext leaf = reader.leaves().get(ReaderUtil.subIndex(doc, reader.leaves()));
        SortedNumericDocValues values = leaf.reader().getSortedNumericDocValues("s");
        assertTrue(values.advanceExact(doc - leaf.docBase));
        return new SortKeys(score, values.nextValue());
    }

    private void setupIndex(int numDocs) throws IOException {
        try (RandomIndexWriter writer = new RandomIndexWriter(random(), directory)) {
            for (int d = 0; d < numDocs; d++) {
                List<IndexableField> doc = new ArrayList<>();
                doc.add(new SortedNumericDocValuesField("s", d));
                writer.addDocument(doc);
            }
            // One leaf, so a collector that collects more than limit documents of it fills its top N
            writer.forceMerge(1);
            reader = writer.getReader();
        }
    }

    private ShardContext createMockShardContext(int shardId, TestSort sort) {
        return createMockShardContext(reader, shardId, sort);
    }

    private static ShardContext createMockShardContext(IndexReader reader, int shardId, TestSort sort) {
        return new LuceneSourceOperatorTests.MockShardContext(reader, shardId) {
            @Override
            public Optional<SortAndFormats> buildSort(List<SortBuilder<?>> sorts) {
                DocValueFormat[] formats = new DocValueFormat[sort.sort.getSort().length];
                Arrays.fill(formats, DocValueFormat.RAW);
                return Optional.of(new SortAndFormats(sort.sort, formats));
            }
        };
    }

    /**
     * Collects the first {@code hits} documents of the index with the scores 1, 2, 3, ... and returns the lowest score of
     * the resulting top {@code limit}.
     */
    private float collectIncreasingScores(LuceneTopNSourceOperator.PerShardCollector collector, int hits, int limit) throws IOException {
        int collected = 0;
        for (LeafReaderContext leaf : reader.leaves()) {
            LeafCollector leafCollector = collector.getLeafCollector(leaf);
            RecordingScorable scorer = new RecordingScorable();
            leafCollector.setScorer(scorer);
            for (int doc = 0; doc < leaf.reader().maxDoc() && collected < hits; doc++) {
                scorer.score = ++collected;
                leafCollector.collect(doc);
            }
            if (collected == hits) {
                return hits - limit + 1;
            }
        }
        throw new AssertionError("index has fewer than [" + hits + "] documents");
    }

    /**
     * A {@link Scorable} whose score the test sets, and that records the minimum competitive score it is given.
     */
    private static class RecordingScorable extends Scorable {
        float score;
        float minCompetitiveScore;
        boolean lowered;

        @Override
        public float score() {
            return score;
        }

        @Override
        public void setMinCompetitiveScore(float minScore) {
            lowered |= minScore < minCompetitiveScore;
            minCompetitiveScore = minScore;
        }
    }

    private DriverContext createDriverContext() {
        var blockFactory = blockFactory();
        return new DriverContext(blockFactory.bigArrays(), blockFactory, null);
    }
}
