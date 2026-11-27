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
import org.apache.lucene.index.Term;
import org.apache.lucene.search.FieldComparator;
import org.apache.lucene.search.FieldComparatorSource;
import org.apache.lucene.search.LeafCollector;
import org.apache.lucene.search.Pruning;
import org.apache.lucene.search.Query;
import org.apache.lucene.search.Scorable;
import org.apache.lucene.search.ScoreDoc;
import org.apache.lucene.search.Sort;
import org.apache.lucene.search.SortField;
import org.apache.lucene.search.SortedNumericSortField;
import org.apache.lucene.search.TermQuery;
import org.apache.lucene.store.Directory;
import org.apache.lucene.tests.index.RandomIndexWriter;
import org.elasticsearch.common.lucene.search.Queries;
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
import java.util.function.Function;

import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.greaterThanOrEqualTo;
import static org.hamcrest.Matchers.not;
import static org.hamcrest.Matchers.sameInstance;

/**
 * Tests how the collectors of {@link LuceneTopNSourceOperator} share their minimum competitive score.
 */
public class LuceneTopNSourceOperatorCollectorTests extends ComputeTestCase {

    /**
     * A sort that a {@link LuceneSourceOperatorTests.MockShardContext}, which has no mappings, resolves the sort builders to.
     *
     * @param sharesScore whether its collectors skip documents by score
     * @param skipsTies whether its collectors skip the documents that tie with the lowest score of a full top N
     */
    private record TestSort(String name, Function<List<SortBuilder<?>>, Sort> resolver, int keys, boolean sharesScore, boolean skipsTies) {
        List<SortBuilder<?>> builders() {
            // The operator reads the score from the sort values, at a position that depends on the number of sort keys
            return keys == 1 ? List.of(new ScoreSortBuilder()) : List.of(new ScoreSortBuilder(), new FieldSortBuilder("s"));
        }

        @Override
        public String toString() {
            return name;
        }
    }

    private static final SortField VALUE_EQUAL_FIELD = new SortedNumericSortField("s", SortField.Type.LONG);

    private static final TestSort SCORE = new TestSort("score", sorts -> Sort.RELEVANCE, 1, true, true);
    /**
     * The second key compares by value, like the sort field of a keyword.
     */
    private static final TestSort SCORE_THEN_VALUE_EQUAL_FIELD = new TestSort(
        "score, value-equal field",
        sorts -> new Sort(SortField.FIELD_SCORE, VALUE_EQUAL_FIELD),
        2,
        true,
        false
    );
    /**
     * The second key compares by identity and is built again every time the shard resolves the sort, like the sort fields
     * of numeric and date fields, whose {@code IndexFieldData.XFieldComparatorSource} doesn't override {@code equals()}.
     */
    private static final TestSort SCORE_THEN_IDENTITY_EQUAL_FIELD = new TestSort("score, identity-equal field", sorts -> {
        FieldComparatorSource source = new FieldComparatorSource() {
            @Override
            public FieldComparator<?> newComparator(String fieldname, int numHits, Pruning pruning, boolean reversed) {
                return VALUE_EQUAL_FIELD.getComparator(numHits, pruning);
            }
        };
        return new Sort(SortField.FIELD_SCORE, new SortField("s", source, false));
    }, 2, true, false);
    private static final TestSort SCORE_ASCENDING = new TestSort(
        "score ascending",
        sorts -> new Sort(new SortField(null, SortField.Type.SCORE, true)),
        1,
        false,
        false
    );
    private static final TestSort FIELD = new TestSort("field", sorts -> new Sort(VALUE_EQUAL_FIELD), 1, false, false);
    private static final TestSort FIELD_THEN_SCORE = new TestSort(
        "field, score",
        sorts -> new Sort(VALUE_EQUAL_FIELD, SortField.FIELD_SCORE),
        2,
        false,
        false
    );

    private static final List<TestSort> SHARING = List.of(SCORE, SCORE_THEN_VALUE_EQUAL_FIELD, SCORE_THEN_IDENTITY_EQUAL_FIELD);
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
        assertThat(sort.toString(), provider.sharedMinCompetitiveScore.get(), equalTo(expected));

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
        assertThat(sort.toString(), provider.sharedMinCompetitiveScore.get(), equalTo(0f));

        provider.sharedMinCompetitiveScore.offer(randomFloat() + 1);
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
        provider.sharedMinCompetitiveScore.offer(shared);

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
        assertThat(provider.sharedMinCompetitiveScore.get(), equalTo(shared));
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
        provider.sharedMinCompetitiveScore.offer(shared);
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
        assertThat(otherProvider.sharedMinCompetitiveScore.get(), equalTo(0f));
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
            ShardContext ctx = createMockShardContext(0, SCORE);
            assertThat(
                factory.perShardCollectorProvider.newPerShardCollector(ctx).collector,
                not(sameInstance(factory.perShardCollectorProvider.newPerShardCollector(ctx).collector))
            );
        } finally {
            IOUtils.close(operators);
        }
    }

    /**
     * Runs a top N by descending score over several shards with several drivers. Each driver skips the documents that score
     * below a top N another driver has already filled. The best {@code limit} rows the drivers emit must still score exactly
     * like the best {@code limit} documents of all shards. Rows that tie on the score are interchangeable, so only scores are
     * compared.
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
                    for (int d = between(500, 3000); d > 0; d--) {
                        List<IndexableField> doc = new ArrayList<>();
                        doc.add(new SortedNumericDocValuesField("s", d));
                        // The frequency of "a" and the length of the field vary, so the scores do
                        doc.add(new TextField("t", "a ".repeat(between(1, 20)) + "b ".repeat(between(0, 20)), Field.Store.NO));
                        writer.addDocument(doc);
                    }
                    readers.add(writer.getReader());
                }
                contexts.add(createMockShardContext(readers.get(shard), shard, sort));
            }
            Query query = new TermQuery(new Term("t", "a"));
            int limit = between(1, 50);
            int taskConcurrency = between(1, 8);
            var factory = new LuceneTopNSourceOperator.Factory(
                new IndexedByShardIdFromList<>(contexts),
                c -> List.of(new LuceneSliceQueue.QueryAndTags(query, List.of())),
                randomFrom(DataPartitioning.SHARD, DataPartitioning.SEGMENT),
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

            List<Float> expected = new ArrayList<>();
            for (ShardContext ctx : contexts) {
                for (ScoreDoc scoreDoc : ctx.searcher().search(query, limit).scoreDocs) {
                    expected.add(scoreDoc.score);
                }
            }
            expected.sort(Collections.reverseOrder());
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

            List<Float> actual = new ArrayList<>();
            for (Page page : results) {
                DoubleBlock scores = page.getBlock(1);
                for (int p = 0; p < page.getPositionCount(); p++) {
                    actual.add((float) scores.getDouble(p));
                }
            }
            actual.sort(Collections.reverseOrder());
            assertThat(sort.toString(), actual.size(), greaterThanOrEqualTo(limit));
            assertThat(sort.toString(), actual.subList(0, limit), equalTo(expected));
        } finally {
            IOUtils.close(readers);
            IOUtils.close(directories);
        }
    }

    private void setupIndex(int numDocs) throws IOException {
        try (RandomIndexWriter writer = new RandomIndexWriter(random(), directory)) {
            for (int d = 0; d < numDocs; d++) {
                List<IndexableField> doc = new ArrayList<>();
                doc.add(new SortedNumericDocValuesField("s", d));
                writer.addDocument(doc);
            }
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
                Sort resolved = sort.resolver.apply(sorts);
                DocValueFormat[] formats = new DocValueFormat[resolved.getSort().length];
                Arrays.fill(formats, DocValueFormat.RAW);
                return Optional.of(new SortAndFormats(resolved, formats));
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
