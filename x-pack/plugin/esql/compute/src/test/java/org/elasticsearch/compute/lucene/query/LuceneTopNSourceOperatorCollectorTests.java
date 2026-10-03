/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.compute.lucene.query;

import org.apache.lucene.document.SortedNumericDocValuesField;
import org.apache.lucene.index.IndexReader;
import org.apache.lucene.index.IndexableField;
import org.apache.lucene.index.LeafReaderContext;
import org.apache.lucene.search.FieldComparator;
import org.apache.lucene.search.FieldComparatorSource;
import org.apache.lucene.search.LeafCollector;
import org.apache.lucene.search.Pruning;
import org.apache.lucene.search.Scorable;
import org.apache.lucene.search.Sort;
import org.apache.lucene.search.SortField;
import org.apache.lucene.search.SortedNumericSelector;
import org.apache.lucene.search.SortedNumericSortField;
import org.apache.lucene.search.TopDocsCollector;
import org.apache.lucene.search.TopFieldCollector;
import org.apache.lucene.search.TopFieldCollectorManager;
import org.apache.lucene.search.TopScoreDocCollector;
import org.apache.lucene.search.comparators.LongComparator;
import org.apache.lucene.store.Directory;
import org.apache.lucene.tests.index.RandomIndexWriter;
import org.elasticsearch.common.lucene.search.Queries;
import org.elasticsearch.compute.lucene.IndexedByShardIdFromList;
import org.elasticsearch.compute.lucene.IndexedByShardIdFromSingleton;
import org.elasticsearch.compute.lucene.ShardContext;
import org.elasticsearch.compute.operator.DriverContext;
import org.elasticsearch.compute.querydsl.query.QueryWarnings;
import org.elasticsearch.compute.test.ComputeTestCase;
import org.elasticsearch.core.IOUtils;
import org.elasticsearch.search.DocValueFormat;
import org.elasticsearch.search.sort.FieldSortBuilder;
import org.elasticsearch.search.sort.ScoreSortBuilder;
import org.elasticsearch.search.sort.SortAndFormats;
import org.elasticsearch.search.sort.SortBuilder;
import org.elasticsearch.search.sort.SortBuilders;
import org.elasticsearch.search.sort.SortMode;
import org.elasticsearch.search.sort.SortOrder;
import org.junit.After;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.IdentityHashMap;
import java.util.List;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.function.Function;

import static org.hamcrest.Matchers.containsString;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.greaterThanOrEqualTo;
import static org.hamcrest.Matchers.instanceOf;
import static org.hamcrest.Matchers.lessThanOrEqualTo;
import static org.hamcrest.Matchers.not;
import static org.hamcrest.Matchers.notNullValue;
import static org.hamcrest.Matchers.nullValue;
import static org.hamcrest.Matchers.sameInstance;

public class LuceneTopNSourceOperatorCollectorTests extends ComputeTestCase {

    /**
     * What {@link #resolveSort} turns every {@link FieldSortBuilder} into. Value-equal, unlike the identity-equal sort fields a real
     * shard produces; see {@link #resolveSort}.
     */
    private static final SortField FIELD_SORT = new SortedNumericSortField("s", SortField.Type.LONG, false, SortedNumericSelector.Type.MIN);

    private final Directory directory = newDirectory();
    private final Directory emptyDirectory = newDirectory();
    private IndexReader reader;
    private IndexReader emptyReader;

    @After
    public void closeIndex() throws IOException {
        IOUtils.close(reader, emptyReader, directory, emptyDirectory);
    }

    public void testRelevanceSortCreatesTopScoreDocCollectorManager() throws IOException {
        var factory = createFactory(true, List.of(new ScoreSortBuilder()), DataPartitioning.SHARD);
        var provider = factory.perShardCollectorProvider;

        // TopScoreDocCollectorManager should be created during construction for SORT _score with needsScore=true
        assertThat(provider.topScoreDocCollectorManager, notNullValue());
        assertThat(provider.topFieldCollectorManagers.isEmpty(), equalTo(true));

        var perShardCollector = provider.newPerShardCollector(createMockShardContext(0));
        assertThat(perShardCollector.collector, instanceOf(TopScoreDocCollector.class));
    }

    /**
     * A field-first sort gets nothing from a shared manager (see {@link #testLuceneOnlySharesMinCompetitiveScoreForScoreFirstSorts}),
     * so it gets no shared manager: each collector comes from its own.
     */
    public void testFieldSortDoesNotShareManager() throws IOException {
        List<SortBuilder<?>> sorts = List.of(new FieldSortBuilder("s"));
        var factory = createFactory(randomBoolean(), sorts, DataPartitioning.SHARD);
        var provider = factory.perShardCollectorProvider;

        assertThat(provider.topFieldCollectorManagers.isEmpty(), equalTo(true));
        assertThat(provider.topScoreDocCollectorManager, nullValue());

        var ctx = createMockShardContext(0);
        var collector1 = provider.newPerShardCollector(ctx).collector;
        var collector2 = provider.newPerShardCollector(ctx).collector;
        assertThat(collector1, instanceOf(TopFieldCollector.class));
        assertThat(collector1, not(sameInstance(collector2)));
        assertThat(provider.topFieldCollectorManagers.isEmpty(), equalTo(true));
    }

    public void testScoreFirstMultiKeySortSharesManager() throws IOException {
        List<SortBuilder<?>> sorts = List.of(new ScoreSortBuilder(), new FieldSortBuilder("s"));
        var factory = createFactory(true, sorts, DataPartitioning.SHARD);
        var provider = factory.perShardCollectorProvider;

        // One shared TopFieldCollectorManager, sorting by score, the field, then doc, then score
        assertThat(provider.topFieldCollectorManagers.size(), equalTo(1));
        Sort managerSort = provider.topFieldCollectorManagers.keySet().iterator().next();
        assertThat(managerSort.getSort().length, equalTo(4));
        assertThat(managerSort.getSort()[0], equalTo(SortField.FIELD_SCORE));
        assertThat(managerSort.getSort()[1], equalTo(FIELD_SORT));
        assertThat(managerSort.getSort()[2], equalTo(SortField.FIELD_DOC));
        assertThat(managerSort.getSort()[3], equalTo(SortField.FIELD_SCORE));
        assertThat(provider.topScoreDocCollectorManager, nullValue());

        var perShardCollector = provider.newPerShardCollector(createMockShardContext(0));
        assertThat(perShardCollector.collector, instanceOf(TopFieldCollector.class));
    }

    public void testScoreAscendingSortDoesNotShareManager() throws IOException {
        List<SortBuilder<?>> sorts = List.of(new ScoreSortBuilder().order(SortOrder.ASC));
        var factory = createFactory(true, sorts, DataPartitioning.SHARD);
        var provider = factory.perShardCollectorProvider;

        assertThat(provider.topFieldCollectorManagers.isEmpty(), equalTo(true));
        assertThat(provider.topScoreDocCollectorManager, nullValue());
        assertThat(provider.newPerShardCollector(createMockShardContext(0)).collector, instanceOf(TopFieldCollector.class));
    }

    public void testSharesMinCompetitiveScore() {
        SortField scoreAsc = new SortField(null, SortField.Type.SCORE, true);
        assertThat(LuceneTopNSourceOperator.PerShardCollectorProvider.sharesMinCompetitiveScore(Sort.RELEVANCE), equalTo(true));
        assertThat(
            LuceneTopNSourceOperator.PerShardCollectorProvider.sharesMinCompetitiveScore(new Sort(SortField.FIELD_SCORE, FIELD_SORT)),
            equalTo(true)
        );
        assertThat(LuceneTopNSourceOperator.PerShardCollectorProvider.sharesMinCompetitiveScore(new Sort(scoreAsc)), equalTo(false));
        assertThat(
            LuceneTopNSourceOperator.PerShardCollectorProvider.sharesMinCompetitiveScore(new Sort(FIELD_SORT, SortField.FIELD_SCORE)),
            equalTo(false)
        );
        assertThat(
            LuceneTopNSourceOperator.PerShardCollectorProvider.sharesMinCompetitiveScore(new Sort(SortField.FIELD_DOC)),
            equalTo(false)
        );
    }

    /**
     * The point of sharing a manager: once one driver's collector has a full queue, a collector for another driver starts
     * with that driver's queue bottom as its minimum competitive score, so its scorer can skip documents immediately.
     */
    public void testSharedMinCompetitiveScoreReachesOtherCollectors() throws IOException {
        List<SortBuilder<?>> sorts = randomBoolean()
            ? List.of(new ScoreSortBuilder())
            : List.of(new ScoreSortBuilder(), new FieldSortBuilder("s"));
        int limit = randomIntBetween(5, 20);
        setupIndex(100);
        ShardContext ctx = createMockShardContext(0);
        var provider = new LuceneTopNSourceOperator.PerShardCollectorProvider(limit, true, sorts, new IndexedByShardIdFromSingleton<>(ctx));

        // The first driver collects more than limit hits with increasing scores, which fills its queue and publishes its bottom
        int hits = limit + randomIntBetween(1, 30);
        float bottom = collectIncreasingScores(provider.newPerShardCollector(ctx), hits, limit);

        // A collector for another driver picks the published score up as soon as it gets a scorer
        var other = provider.newPerShardCollector(ctx);
        RecordingScorable scorer = new RecordingScorable();
        other.getLeafCollector(reader.leaves().getFirst()).setScorer(scorer);
        assertThat(scorer.minCompetitiveScore, greaterThanOrEqualTo(bottom));
        assertThat(scorer.minCompetitiveScore, lessThanOrEqualTo(Math.nextUp(bottom)));
    }

    /**
     * Guards the assumption behind {@link LuceneTopNSourceOperator.PerShardCollectorProvider#sharesMinCompetitiveScore}: Lucene
     * doesn't share a minimum competitive score between collectors of a field-first sort, even when they come from the same
     * manager. If this starts failing after a Lucene upgrade, field-first sorts may be worth sharing a manager again.
     */
    public void testLuceneOnlySharesMinCompetitiveScoreForScoreFirstSorts() throws IOException {
        int limit = randomIntBetween(5, 20);
        setupIndex(100);
        ShardContext ctx = createMockShardContext(0);
        Sort fieldFirst = new Sort(FIELD_SORT, SortField.FIELD_DOC, SortField.FIELD_SCORE);
        var sharedManager = new TopFieldCollectorManager(fieldFirst, limit, null, 0);

        collectIncreasingScores(
            new LuceneTopNSourceOperator.PerShardCollector(ctx, sharedManager.newCollector()),
            limit + randomIntBetween(1, 30),
            limit
        );

        var other = new LuceneTopNSourceOperator.PerShardCollector(ctx, sharedManager.newCollector());
        RecordingScorable scorer = new RecordingScorable();
        other.getLeafCollector(reader.leaves().getFirst()).setScorer(scorer);
        assertThat(scorer.minCompetitiveScore, equalTo(0f));
    }

    public void testNewPerShardCollectorCreatesNewInstancesEachTime() throws IOException {
        var factory = createFactory(true, List.of(new ScoreSortBuilder()), DataPartitioning.SHARD);
        var provider = factory.perShardCollectorProvider;
        var ctx = createMockShardContext(0);

        var perShardCollector1 = provider.newPerShardCollector(ctx);
        var perShardCollector2 = provider.newPerShardCollector(ctx);
        var perShardCollector3 = provider.newPerShardCollector(ctx);

        assertThat(perShardCollector1, not(sameInstance(perShardCollector2)));
        assertThat(perShardCollector1.collector, not(sameInstance(perShardCollector2.collector)));
        assertThat(perShardCollector2.collector, not(sameInstance(perShardCollector3.collector)));
    }

    /**
     * {@code SORT _score DESC, x} is the only sort that still creates its collectors from a shared
     * {@link TopFieldCollectorManager}, whose {@code newCollector()} isn't thread-safe. Collectors created concurrently must
     * all come from that one manager and still share its minimum competitive score.
     */
    public void testConcurrentCollectorsForScoreFirstMultiKeySortShareManager() throws Exception {
        int limit = randomIntBetween(5, 20);
        setupIndex(100);
        ShardContext ctx = createMockShardContext(0);
        var provider = new LuceneTopNSourceOperator.PerShardCollectorProvider(
            limit,
            true,
            List.of(new ScoreSortBuilder(), new FieldSortBuilder("s")),
            new IndexedByShardIdFromSingleton<>(ctx)
        );

        List<TopDocsCollector<?>> collectors = createCollectorsConcurrently(provider, ctx);
        assertThat(provider.topFieldCollectorManagers.size(), equalTo(1));
        for (TopDocsCollector<?> collector : collectors) {
            assertThat(collector, instanceOf(TopFieldCollector.class));
        }

        // Two of the concurrently created collectors, one filling its queue, the other picking up the published score
        int first = randomIntBetween(0, collectors.size() - 1);
        int second = randomValueOtherThan(first, () -> randomIntBetween(0, collectors.size() - 1));
        float bottom = collectIncreasingScores(
            new LuceneTopNSourceOperator.PerShardCollector(ctx, collectors.get(first)),
            limit + randomIntBetween(1, 30),
            limit
        );
        RecordingScorable scorer = new RecordingScorable();
        new LuceneTopNSourceOperator.PerShardCollector(ctx, collectors.get(second)).getLeafCollector(reader.leaves().getFirst())
            .setScorer(scorer);
        assertThat(scorer.minCompetitiveScore, equalTo(bottom));
    }

    public void testMultipleOperatorsShareProviderWithJustScore() throws Exception {
        var factory = createFactory(true, List.of(SortBuilders.scoreSort()), DataPartitioning.SHARD);

        int numOperators = randomIntBetween(4, 10);
        var operators = new ArrayList<LuceneTopNSourceOperator>();

        try {
            for (int i = 0; i < numOperators; i++) {
                operators.add((LuceneTopNSourceOperator) factory.get(createDriverContext()));
            }

            var sharedProvider = operators.get(0).perShardCollectorProvider;
            for (var op : operators) {
                assertThat(op.perShardCollectorProvider, sameInstance(sharedProvider));
            }

            assertThat(sharedProvider, notNullValue());
            assertThat(sharedProvider.topScoreDocCollectorManager, notNullValue());

            List<TopDocsCollector<?>> collectors = createCollectorsConcurrently(sharedProvider, createMockShardContext(0));
            for (TopDocsCollector<?> collector : collectors) {
                assertThat(collector, instanceOf(TopScoreDocCollector.class));
            }
        } finally {
            IOUtils.close(operators);
        }
    }

    public void testMultipleOperatorsShareProviderWithScoreNeeded() throws Exception {
        assertMultipleOperatorsShareProvider(true);
    }

    public void testMultipleOperatorsShareProviderWithScoreNotNeeded() throws Exception {
        assertMultipleOperatorsShareProvider(false);
    }

    private void assertMultipleOperatorsShareProvider(boolean needsScore) throws Exception {
        var factory = createFactory(
            needsScore,
            List.of(SortBuilders.fieldSort("s").order(SortOrder.ASC).sortMode(SortMode.MIN)),
            DataPartitioning.SHARD
        );

        int numOperators = randomIntBetween(4, 10);
        var operators = new ArrayList<LuceneTopNSourceOperator>();

        try {
            for (int i = 0; i < numOperators; i++) {
                operators.add((LuceneTopNSourceOperator) factory.get(createDriverContext()));
            }

            var sharedProvider = operators.get(0).perShardCollectorProvider;
            for (var op : operators) {
                assertThat(op.perShardCollectorProvider, sameInstance(sharedProvider));
            }

            assertThat(sharedProvider, notNullValue());

            var ctx = createMockShardContext(0);
            var collector1 = sharedProvider.newPerShardCollector(ctx).collector;
            var collector2 = sharedProvider.newPerShardCollector(ctx).collector;
            assertThat(collector1, not(sameInstance(collector2)));

            createCollectorsConcurrently(sharedProvider, ctx);

            // Field-first sorts don't share a manager, so creating collectors leaves nothing behind
            assertThat(sharedProvider.topFieldCollectorManagers.isEmpty(), equalTo(true));
        } finally {
            IOUtils.close(operators);
        }
    }

    public void testShardsWithEqualSortsShareManager() throws IOException {
        setupIndex(100);
        int numShards = randomIntBetween(2, 5);
        List<ShardContext> contexts = new ArrayList<>();
        for (int i = 0; i < numShards; i++) {
            contexts.add(createMockShardContext(i));
        }
        var provider = new LuceneTopNSourceOperator.PerShardCollectorProvider(
            10,
            true,
            List.of(new ScoreSortBuilder(), new FieldSortBuilder("s")),
            new IndexedByShardIdFromList<>(contexts)
        );

        assertThat(provider.topFieldCollectorManagers.size(), equalTo(1));
        for (ShardContext ctx : contexts) {
            assertThat(provider.newPerShardCollector(ctx).collector, notNullValue());
        }
        assertThat(provider.topFieldCollectorManagers.size(), equalTo(1));
    }

    /**
     * Sorts backed by an {@code IndexFieldData.XFieldComparatorSource} compare by identity, because the comparator source
     * doesn't override {@code equals()}. When such a sort shares a manager, each shard must get its own, and creating
     * collectors must not add more.
     */
    public void testIdentityEqualSortsGetOneManagerPerShard() throws IOException {
        setupIndex(100);
        int numShards = randomIntBetween(2, 5);
        List<ShardContext> contexts = new ArrayList<>();
        for (int i = 0; i < numShards; i++) {
            contexts.add(
                createMockShardContext(
                    reader,
                    i,
                    sorts -> new Sort(SortField.FIELD_SCORE, new SortField("s", new NoEqualsComparatorSource(), false))
                )
            );
        }
        var provider = new LuceneTopNSourceOperator.PerShardCollectorProvider(
            10,
            true,
            List.of(new ScoreSortBuilder(), new FieldSortBuilder("s")),
            new IndexedByShardIdFromList<>(contexts)
        );

        assertThat(provider.topFieldCollectorManagers.size(), equalTo(numShards));
        int calls = randomIntBetween(10, 50);
        for (int i = 0; i < calls; i++) {
            assertThat(provider.newPerShardCollector(randomFrom(contexts)).collector, notNullValue());
        }
        assertThat(provider.topFieldCollectorManagers.size(), equalTo(numShards));
    }

    /**
     * {@link LuceneSliceQueue} never creates slices for empty shards, so the provider must not resolve their sort either.
     * Resolving it can fail, e.g. a geo distance sort on a field that is unmapped in that shard's index.
     */
    public void testEmptyShardsAreSkipped() throws IOException {
        setupIndex(100);
        setupEmptyIndex();
        ShardContext populated = createMockShardContext(0);
        ShardContext empty = createMockShardContext(emptyReader, 1, sorts -> {
            throw new IllegalArgumentException("failed to find mapper for [s] for geo distance based sort");
        });
        var provider = new LuceneTopNSourceOperator.PerShardCollectorProvider(
            10,
            randomBoolean(),
            List.of(new FieldSortBuilder("s")),
            new IndexedByShardIdFromList<>(List.of(populated, empty))
        );

        assertThat(provider.newPerShardCollector(populated).collector, notNullValue());
        var e = expectThrows(IllegalStateException.class, () -> provider.newPerShardCollector(empty));
        assertThat(e.getMessage(), containsString("no collector manager was built for shard"));
    }

    /**
     * Creates collectors for {@code ctx} from several threads at once and checks every call returned a distinct collector.
     */
    private List<TopDocsCollector<?>> createCollectorsConcurrently(
        LuceneTopNSourceOperator.PerShardCollectorProvider provider,
        ShardContext ctx
    ) {
        int numThreads = randomIntBetween(4, 8);
        int callsPerThread = randomIntBetween(5, 15);
        List<TopDocsCollector<?>> collectors = new CopyOnWriteArrayList<>();

        startInParallel(numThreads, t -> {
            for (int i = 0; i < callsPerThread; i++) {
                collectors.add(provider.newPerShardCollector(ctx).collector);
            }
        });

        Set<TopDocsCollector<?>> distinct = Collections.newSetFromMap(new IdentityHashMap<>());
        distinct.addAll(collectors);
        assertThat(distinct.size(), equalTo(numThreads * callsPerThread));
        return collectors;
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

    private void setupEmptyIndex() throws IOException {
        try (RandomIndexWriter writer = new RandomIndexWriter(random(), emptyDirectory)) {
            emptyReader = writer.getReader();
        }
    }

    private ShardContext createMockShardContext(int shardId) {
        return createMockShardContext(reader, shardId, LuceneTopNSourceOperatorCollectorTests::resolveSort);
    }

    /**
     * {@link LuceneSourceOperatorTests.MockShardContext} has no mappings to resolve sorts against, so {@code sortResolver} stands
     * in for them.
     */
    private static ShardContext createMockShardContext(IndexReader reader, int shardId, Function<List<SortBuilder<?>>, Sort> sortResolver) {
        return new LuceneSourceOperatorTests.MockShardContext(reader, shardId) {
            @Override
            public Optional<SortAndFormats> buildSort(List<SortBuilder<?>> sorts) {
                if (sorts.isEmpty()) {
                    // Matches SortBuilder.buildSort
                    return Optional.empty();
                }
                DocValueFormat[] formats = new DocValueFormat[sorts.size()];
                Arrays.fill(formats, DocValueFormat.RAW);
                return Optional.of(new SortAndFormats(sortResolver.apply(sorts), formats));
            }
        };
    }

    /**
     * Resolves sorts against the test index. Score sorts match what {@link ScoreSortBuilder} builds exactly: a descending one
     * becomes {@link SortField#FIELD_SCORE} (so a lone one is {@link Sort#RELEVANCE}), an ascending one a reversed score field.
     * Field sorts become {@link #FIELD_SORT}, a value-equal {@link SortedNumericSortField}, which is a stand-in: a real shard
     * builds a {@link SortField} with an {@code IndexFieldData.XFieldComparatorSource}, which compares by identity, so equal
     * field sorts on different shards never share a manager in production. {@link #testIdentityEqualSortsGetOneManagerPerShard}
     * covers that case.
     */
    private static Sort resolveSort(List<SortBuilder<?>> sorts) {
        SortField[] fields = new SortField[sorts.size()];
        for (int i = 0; i < fields.length; i++) {
            if (sorts.get(i) instanceof ScoreSortBuilder score) {
                fields[i] = score.order() == SortOrder.DESC ? SortField.FIELD_SCORE : new SortField(null, SortField.Type.SCORE, true);
            } else {
                fields[i] = FIELD_SORT;
            }
        }
        return new Sort(fields);
    }

    /**
     * Collects the first {@code hits} documents of the index with scores 1, 2, 3, ... and returns the score at the bottom of
     * the resulting top {@code limit}, which is the minimum competitive score the collector publishes.
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
     * A {@link Scorable} whose score the test sets directly, recording what the collector asks it to skip.
     */
    private static class RecordingScorable extends Scorable {
        float score;
        float minCompetitiveScore;

        @Override
        public float score() {
            return score;
        }

        @Override
        public void setMinCompetitiveScore(float minScore) {
            minCompetitiveScore = minScore;
        }
    }

    /**
     * Stands in for {@code IndexFieldData.XFieldComparatorSource} subclasses, which don't override {@code equals()}.
     */
    private static class NoEqualsComparatorSource extends FieldComparatorSource {
        @Override
        public FieldComparator<?> newComparator(String fieldname, int numHits, Pruning pruning, boolean reversed) {
            return new LongComparator(numHits, fieldname, null, reversed, pruning);
        }
    }

    private LuceneTopNSourceOperator.Factory createFactory(boolean needsScore, List<SortBuilder<?>> sorts, DataPartitioning partitioning)
        throws IOException {
        setupIndex(100);
        ShardContext ctx = createMockShardContext(0);
        Function<ShardContext, List<LuceneSliceQueue.QueryAndTags>> queryFunction = c -> List.of(
            new LuceneSliceQueue.QueryAndTags(Queries.ALL_DOCS_INSTANCE, List.of())
        );

        return new LuceneTopNSourceOperator.Factory(
            new IndexedByShardIdFromSingleton<>(ctx),
            queryFunction,
            partitioning,
            DataPartitioning.AutoStrategy.DEFAULT,
            randomIntBetween(1, 10),
            10000,
            randomIntBetween(10, 100),
            sorts,
            randomLongBetween(10, 20),
            needsScore,
            () -> 0L,
            QueryWarnings.EMIT
        );
    }

    private DriverContext createDriverContext() {
        var blockFactory = blockFactory();
        return new DriverContext(blockFactory.bigArrays(), blockFactory, null);
    }
}
