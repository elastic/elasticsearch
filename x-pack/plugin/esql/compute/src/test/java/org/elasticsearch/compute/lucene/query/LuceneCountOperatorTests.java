/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.compute.lucene.query;

import com.carrotsearch.randomizedtesting.annotations.ParametersFactory;

import org.apache.lucene.document.Document;
import org.apache.lucene.document.SortedNumericDocValuesField;
import org.apache.lucene.document.StringField;
import org.apache.lucene.index.IndexReader;
import org.apache.lucene.index.NoMergePolicy;
import org.apache.lucene.index.Term;
import org.apache.lucene.search.IndexSearcher;
import org.apache.lucene.search.LRUQueryCache;
import org.apache.lucene.search.QueryCachingPolicy;
import org.apache.lucene.search.ScoreMode;
import org.apache.lucene.search.TermQuery;
import org.apache.lucene.store.Directory;
import org.apache.lucene.tests.index.RandomIndexWriter;
import org.elasticsearch.common.breaker.CircuitBreakingException;
import org.elasticsearch.common.lucene.search.Queries;
import org.elasticsearch.compute.data.BooleanBlock;
import org.elasticsearch.compute.data.BooleanVector;
import org.elasticsearch.compute.data.ElementType;
import org.elasticsearch.compute.data.IntBlock;
import org.elasticsearch.compute.data.IntVector;
import org.elasticsearch.compute.data.LongBlock;
import org.elasticsearch.compute.data.LongVector;
import org.elasticsearch.compute.data.Page;
import org.elasticsearch.compute.lucene.IndexedByShardIdFromSingleton;
import org.elasticsearch.compute.lucene.ShardContext;
import org.elasticsearch.compute.operator.Driver;
import org.elasticsearch.compute.operator.DriverContext;
import org.elasticsearch.compute.querydsl.query.QueryWarnings;
import org.elasticsearch.compute.test.SourceOperatorTestCase;
import org.elasticsearch.compute.test.TestDriverFactory;
import org.elasticsearch.compute.test.TestDriverRunner;
import org.elasticsearch.compute.test.TestResultPageSinkOperator;
import org.elasticsearch.core.IOUtils;
import org.elasticsearch.indices.CrankyCircuitBreakerService;
import org.elasticsearch.search.internal.ContextIndexSearcher;
import org.elasticsearch.test.MapMatcher;
import org.hamcrest.Matcher;
import org.junit.After;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.TreeMap;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.function.Function;
import java.util.function.Supplier;

import static org.elasticsearch.test.MapMatcher.assertMap;
import static org.elasticsearch.test.MapMatcher.matchesMap;
import static org.hamcrest.Matchers.both;
import static org.hamcrest.Matchers.either;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.greaterThan;
import static org.hamcrest.Matchers.greaterThanOrEqualTo;
import static org.hamcrest.Matchers.hasSize;
import static org.hamcrest.Matchers.is;
import static org.hamcrest.Matchers.lessThanOrEqualTo;
import static org.hamcrest.Matchers.matchesRegex;

public class LuceneCountOperatorTests extends SourceOperatorTestCase {
    @ParametersFactory(argumentFormatting = "%s")
    public static Iterable<Object[]> parameters() {
        List<Object[]> parameters = new ArrayList<>();
        for (TestCase c : TestCase.values()) {
            parameters.add(new Object[] { c });
        }
        return parameters;
    }

    public enum TestCase {
        MATCH_ALL {
            @Override
            List<LuceneSliceQueue.QueryAndTags> queryAndExtra() {
                return List.of(new LuceneSliceQueue.QueryAndTags(Queries.ALL_DOCS_INSTANCE, List.of()));
            }

            @Override
            List<ElementType> tagTypes() {
                return List.of();
            }

            @Override
            void checkPages(int numDocs, int limit, List<Page> results) {
                long count = 0;
                for (Page p : results) {
                    assertThat(p.getBlockCount(), equalTo(2));
                    checkSeen(p, equalTo(1));
                    count += getCount(p);
                }
                if (limit < numDocs) {
                    assertThat(count, greaterThanOrEqualTo((long) limit));
                } else {
                    assertThat(count, equalTo((long) numDocs));
                }
            }

            @Override
            LuceneSliceQueue.PartitioningStrategy expectedPartitioning() {
                return LuceneSliceQueue.PartitioningStrategy.SHARD;
            }
        },
        MATCH_0 {
            @Override
            List<LuceneSliceQueue.QueryAndTags> queryAndExtra() {
                return List.of(new LuceneSliceQueue.QueryAndTags(SortedNumericDocValuesField.newSlowExactQuery("s", 0), List.of()));
            }

            @Override
            List<ElementType> tagTypes() {
                return List.of();
            }

            @Override
            void checkPages(int numDocs, int limit, List<Page> results) {
                long count = 0;
                for (Page p : results) {
                    assertThat(p.getBlockCount(), equalTo(2));
                    checkSeen(p, equalTo(1));
                    count += getCount(p);
                }
                assertThat(count, equalTo((long) Math.min(numDocs, 1)));
            }

            @Override
            LuceneSliceQueue.PartitioningStrategy expectedPartitioning() {
                // Doc-values scan on a small (< threshold) index: the shared cost check keeps it on the low-overhead
                // SHARD. DOC picking (scan-heavy) is covered by the forced-DOC tests and the source/TopN strategy tests.
                return LuceneSliceQueue.PartitioningStrategy.SHARD;
            }
        },
        MATCH_0_AND_1 {
            @Override
            List<LuceneSliceQueue.QueryAndTags> queryAndExtra() {
                return List.of(
                    new LuceneSliceQueue.QueryAndTags(SortedNumericDocValuesField.newSlowExactQuery("s", 0), List.of(123)),
                    new LuceneSliceQueue.QueryAndTags(SortedNumericDocValuesField.newSlowExactQuery("s", 1), List.of(456))
                );
            }

            @Override
            List<ElementType> tagTypes() {
                return List.of(ElementType.INT);
            }

            @Override
            void checkPages(int numDocs, int limit, List<Page> results) {
                Map<Integer, Long> counts = getCountsByTag(results);
                MapMatcher matcher = matchesMap();
                if (numDocs > 0) {
                    matcher = matcher.entry(123, 1L);
                }
                if (numDocs > 1) {
                    matcher = matcher.entry(456, 1L);
                }
                assertMap(counts, matcher);
            }

            @Override
            LuceneSliceQueue.PartitioningStrategy expectedPartitioning() {
                // Doc-values scan on a small (< threshold) index: the shared cost check keeps it on the low-overhead
                // SHARD. DOC picking (scan-heavy) is covered by the forced-DOC tests and the source/TopN strategy tests.
                return LuceneSliceQueue.PartitioningStrategy.SHARD;
            }
        },
        LTE_100_GT_100 {
            @Override
            List<LuceneSliceQueue.QueryAndTags> queryAndExtra() {
                return List.of(
                    new LuceneSliceQueue.QueryAndTags(SortedNumericDocValuesField.newSlowRangeQuery("s", 0, 100), List.of(123)),
                    new LuceneSliceQueue.QueryAndTags(SortedNumericDocValuesField.newSlowRangeQuery("s", 101, Long.MAX_VALUE), List.of(456))
                );
            }

            @Override
            List<ElementType> tagTypes() {
                return List.of(ElementType.INT);
            }

            @Override
            void checkPages(int numDocs, int limit, List<Page> results) {
                Map<Integer, Long> counts = getCountsByTag(results);
                MapMatcher matcher = matchesMap();
                if (limit >= numDocs) {
                    // The normal case - we don't abort early.
                    if (numDocs > 0) {
                        matcher = matcher.entry(123, (long) Math.min(numDocs, 101));
                    }
                    if (numDocs > 101) {
                        matcher = matcher.entry(456, (long) numDocs - 101);
                    }
                    assertMap(counts, matcher);
                    return;
                }
                /*
                 * The abnormal case - we abort the counting early. But this is best-effort
                 * so we *might* have a complete count. Otherwise, we'll have lower counts.
                 *
                 */
                if (counts.containsKey(123)) {
                    matcher = matcher.entry(123, both(greaterThan(0L)).and(lessThanOrEqualTo((long) Math.min(numDocs, 101))));
                }
                if (counts.containsKey(456)) {
                    matcher = matcher.entry(456, both(greaterThan(0L)).and(lessThanOrEqualTo((long) numDocs - 101)));
                }
                assertThat(counts.keySet(), hasSize(either(equalTo(1)).or(equalTo(2))));
                assertMap(counts, matcher);
            }

            @Override
            LuceneSliceQueue.PartitioningStrategy expectedPartitioning() {
                // Doc-values scan on a small (< threshold) index: the shared cost check keeps it on the low-overhead
                // SHARD. DOC picking (scan-heavy) is covered by the forced-DOC tests and the source/TopN strategy tests.
                return LuceneSliceQueue.PartitioningStrategy.SHARD;
            }
        };

        abstract List<LuceneSliceQueue.QueryAndTags> queryAndExtra();

        abstract List<ElementType> tagTypes();

        abstract void checkPages(int numDocs, int limit, List<Page> results);

        abstract LuceneSliceQueue.PartitioningStrategy expectedPartitioning();

        // TODO check for the count of count shortcuts taken
    }

    private final TestCase testCase;

    private Directory directory = newDirectory();
    private IndexReader reader;

    public LuceneCountOperatorTests(TestCase testCase) {
        this.testCase = testCase;
    }

    @After
    public void closeIndex() throws IOException {
        IOUtils.close(reader, directory);
    }

    @Override
    protected LuceneCountOperator.Factory simple(SimpleOptions options) {
        return simple(randomFrom(DataPartitioning.values()), between(1, 10_000), 100);
    }

    private LuceneCountOperator.Factory simple(DataPartitioning dataPartitioning, int numDocs, int limit) {
        int commitEvery = Math.max(1, numDocs / 10);
        try (
            RandomIndexWriter writer = new RandomIndexWriter(
                random(),
                directory,
                newIndexWriterConfig().setMergePolicy(NoMergePolicy.INSTANCE)
            )
        ) {
            for (int d = 0; d < numDocs; d++) {
                var doc = new Document();
                doc.add(new SortedNumericDocValuesField("s", d));
                writer.addDocument(doc);
                if (d % commitEvery == 0) {
                    writer.commit();
                }
            }
            reader = writer.getReader();
        } catch (IOException e) {
            throw new RuntimeException(e);
        }

        ShardContext ctx = new LuceneSourceOperatorTests.MockShardContext(reader, 0);
        Function<ShardContext, List<LuceneSliceQueue.QueryAndTags>> queryFunction = c -> testCase.queryAndExtra();
        return new LuceneCountOperator.Factory(
            new IndexedByShardIdFromSingleton<>(ctx),
            queryFunction,
            dataPartitioning,
            1,
            between(1, 8),
            testCase.tagTypes(),
            limit,
            () -> 0L,
            LuceneSliceQueue.MIN_DOCS_PER_SLICE,
            QueryWarnings.EMIT
        );
    }

    @Override
    protected Matcher<String> expectedToStringOfSimple() {
        return matchesRegex("LuceneCountOperator\\[shards = \\[test], maxPageSize = \\d+, remainingDocs=100]");
    }

    @Override
    protected Matcher<String> expectedDescriptionOfSimple() {
        return matchesRegex("LuceneCountOperator\\[dataPartitioning = (AUTO|DOC|SHARD|SEGMENT), limit = 100]");
    }

    // TODO tests for the other data partitioning configurations

    public void testSimple() {
        testSimple(this::driverContext);
    }

    public void testSimpleWithCranky() {
        try {
            testSimple(this::crankyDriverContext);
            logger.info("cranky didn't break");
        } catch (CircuitBreakingException e) {
            logger.info("broken", e);
            assertThat(e.getMessage(), equalTo(CrankyCircuitBreakerService.ERROR_MESSAGE));
        }
    }

    private void testSimple(Supplier<DriverContext> contexts) {
        int size = between(1_000, 20_000);
        int limit = randomBoolean() ? between(10, size) : Integer.MAX_VALUE;
        testCount(contexts, size, limit);
    }

    public void testEmpty() {
        testEmpty(this::driverContext);
    }

    public void testEmptyWithCranky() {
        try {
            testEmpty(this::crankyDriverContext);
            logger.info("cranky didn't break");
        } catch (CircuitBreakingException e) {
            logger.info("broken", e);
            assertThat(e.getMessage(), equalTo(CrankyCircuitBreakerService.ERROR_MESSAGE));
        }
    }

    private void testEmpty(Supplier<DriverContext> contexts) {
        int limit = randomBoolean() ? between(10, 10000) : Integer.MAX_VALUE;
        testCount(contexts, 0, limit);
    }

    /**
     * Exercises {@link DataPartitioning#DOC} partitioning explicitly, with multiple concurrent
     * drivers per shard so that several sub-segment slices share each leaf. Locks in the
     * correctness of {@link LuceneCountOperator}'s {@code coversFullLeaf} guard against the
     * Lucene-query-cache shortcut hazard (see the comment on
     * {@link LuceneCountOperator#partitioningStrategyForCount}). Without that guard, every
     * sub-range driver on the same leaf would call {@code Weight.count(leaf)} and receive the
     * full-leaf total, producing a count multiplied by the slice count.
     */
    public void testDocPartitioning() {
        int size = between(1_000, 20_000);
        int limit = randomBoolean() ? between(10, size) : Integer.MAX_VALUE;
        LuceneCountOperator.Factory factory = simple(DataPartitioning.DOC, size, limit);
        List<Page> results = new CopyOnWriteArrayList<>();
        List<Driver> drivers = new ArrayList<>();
        int taskConcurrency = between(2, 8);
        for (int i = 0; i < taskConcurrency; i++) {
            DriverContext ctx = driverContext();
            drivers.add(TestDriverFactory.create(ctx, factory.get(ctx), List.of(), new TestResultPageSinkOperator(results::add)));
        }
        new TestDriverRunner().run(drivers);
        testCase.checkPages(size, limit, results);
        for (Driver driver : drivers) {
            LuceneOperator.Status status = (LuceneOperator.Status) driver.status().completedOperators().get(0).status();
            assertNotNull(status);
            for (var strategy : status.partitioningStrategies().values()) {
                assertThat(strategy, is(LuceneSliceQueue.PartitioningStrategy.DOC));
            }
        }
    }

    /**
     * End-to-end test of DOC partitioning through the production {@link LuceneCountOperator.Factory},
     * which wires the {@code leafHasCountShortcut} guard. Uses a {@link TermQuery}, whose
     * {@code Weight.count(leaf)} returns the leaf-wide {@code docFreq} as a built-in shortcut, so the
     * guard keeps every leaf whole: each leaf becomes a single full-leaf slice, {@code coversFullLeaf}
     * fires the shortcut exactly once, and the total is exact. This locks in the keep-whole guard
     * behavior; the complementary sub-segment-split path — where {@code coversFullLeaf} must
     * <em>suppress</em> the shortcut on sub-slices that don't own the full leaf — is covered by
     * {@link #testDocPartitioningForcedSplitRespectsCountShortcut}.
     */
    public void testDocPartitioningRespectsCountShortcutSliceBoundaries() throws IOException {
        final int partitioningTaskConcurrency = 8;
        int numDocs = between(100_000, 200_000);
        try (Directory dir = newDirectory()) {
            int expectedMatches;
            // Use the default merge policy so forceMerge(1) consolidates into a single leaf, with high
            // task_concurrency so several drivers contend for it. The guard must keep that one big leaf
            // whole (a single slice). NoMergePolicy would disable forceMerge and leave many small
            // segments, each trivially its own single-leaf slice, so the keep-whole guard would never
            // be tested against a leaf that the DOC partitioner might otherwise split.
            try (RandomIndexWriter writer = new RandomIndexWriter(random(), dir, newIndexWriterConfig())) {
                int matches = 0;
                for (int d = 0; d < numDocs; d++) {
                    Document doc = new Document();
                    boolean match = d % 3 == 0;
                    doc.add(new StringField("kw", match ? "match" : "other", StringField.Store.NO));
                    if (match) {
                        matches++;
                    }
                    writer.addDocument(doc);
                }
                writer.forceMerge(1);
                expectedMatches = matches;
                try (IndexReader r = writer.getReader()) {
                    runDocPartitionedTermQueryCount(r, expectedMatches, partitioningTaskConcurrency);
                }
            }
        }
    }

    private void runDocPartitionedTermQueryCount(IndexReader r, int expectedMatches, int partitioningTaskConcurrency) {
        ShardContext ctx = new LuceneSourceOperatorTests.MockShardContext(r, 0);
        Function<ShardContext, List<LuceneSliceQueue.QueryAndTags>> queryFn = c -> List.of(
            new LuceneSliceQueue.QueryAndTags(new TermQuery(new Term("kw", "match")), List.of())
        );
        LuceneCountOperator.Factory factory = new LuceneCountOperator.Factory(
            new IndexedByShardIdFromSingleton<>(ctx),
            queryFn,
            DataPartitioning.DOC,
            1,
            partitioningTaskConcurrency,
            List.of(),
            Integer.MAX_VALUE,
            () -> 0L,
            LuceneSliceQueue.MIN_DOCS_PER_SLICE,
            QueryWarnings.EMIT
        );
        int driverCount = partitioningTaskConcurrency;
        List<Page> results = new CopyOnWriteArrayList<>();
        List<Driver> drivers = new ArrayList<>();
        for (int i = 0; i < driverCount; i++) {
            DriverContext driverCtx = driverContext();
            drivers.add(
                TestDriverFactory.create(driverCtx, factory.get(driverCtx), List.of(), new TestResultPageSinkOperator(results::add))
            );
        }
        new TestDriverRunner().run(drivers);
        long total = 0;
        for (Page p : results) {
            total += getCount(p);
        }
        assertThat("DOC-partitioned count must equal the true match count", total, equalTo((long) expectedMatches));
    }

    /**
     * Companion to {@link #testDocPartitioningRespectsCountShortcutSliceBoundaries}: the production
     * {@code leafHasCountShortcut} guard keeps a {@link TermQuery} leaf whole (its
     * {@code Weight.count(leaf)} returns the leaf-wide {@code docFreq}), so that test never splits the
     * leaf and so never exercises the {@code coversFullLeaf} protection it claims to cover. Forcing
     * {@link LuceneSliceQueue.LeafSplitGuard#NEVER} splits the leaf into sub-segment slices while the
     * count shortcut still fires per leaf — exactly the over-count hazard {@code coversFullLeaf}
     * guards against. The {@code totalSlices() > 1} assertion fails loudly if future slicing tuning
     * stops splitting, so this path can't silently fall out of coverage again.
     */
    public void testDocPartitioningForcedSplitRespectsCountShortcut() throws IOException {
        try (Directory dir = newDirectory()) {
            ForcedSplitIndex index = buildForcedSplitTermIndex(dir);
            try (IndexReader r = index.reader()) {
                runDocPartitionedTermQueryCount(
                    new LuceneSourceOperatorTests.MockShardContext(r, 0),
                    index.expectedMatches(),
                    LuceneSliceQueue.LeafSplitGuard.NEVER
                );
            }
        }
    }

    /**
     * Cached-shortcut companion to {@link #testDocPartitioningCorrectWhenQueryIsCached}, forcing
     * {@link LuceneSliceQueue.LeafSplitGuard#NEVER}. This is the genuine race {@code coversFullLeaf}
     * exists for: a query gains a leaf-wide count shortcut from the {@link LRUQueryCache} after the
     * leaf has already been split into sub-segment slices, so each sub-slice's
     * {@code Weight.count(leaf)} returns the cached leaf cardinality. Only {@code coversFullLeaf}
     * keeps the sub-slices from each applying that full-leaf total to their own range.
     */
    public void testDocPartitioningForcedSplitCorrectWhenQueryIsCached() throws IOException {
        try (Directory dir = newDirectory()) {
            ForcedSplitIndex index = buildForcedSplitTermIndex(dir);
            try (IndexReader r = index.reader()) {
                QueryCachingPolicy alwaysCache = new QueryCachingPolicy() {
                    @Override
                    public void onUse(org.apache.lucene.search.Query query) {}

                    @Override
                    public boolean shouldCache(org.apache.lucene.search.Query query) {
                        return true;
                    }
                };
                ContextIndexSearcher cachingSearcher = new ContextIndexSearcher(
                    r,
                    IndexSearcher.getDefaultSimilarity(),
                    new LRUQueryCache(100, 100 * 1024 * 1024),
                    alwaysCache,
                    true,
                    Runnable::run,
                    10,
                    50_000
                );
                // Warm the cache so Weight.count(leaf) returns the cached bitset cardinality.
                cachingSearcher.count(new TermQuery(new Term("kw", "match")));
                runDocPartitionedTermQueryCount(
                    new LuceneSourceOperatorTests.MockShardContext(cachingSearcher, 0),
                    index.expectedMatches(),
                    LuceneSliceQueue.LeafSplitGuard.NEVER
                );
            }
        }
    }

    private record ForcedSplitIndex(IndexReader reader, int expectedMatches) {}

    /**
     * Builds a single-leaf index large enough that the {@link LuceneSliceQueue.PartitioningStrategy#DOC}
     * partitioner splits it into sub-segment slices. With the {@code MIN_DOCS_PER_SLICE} floor a leaf
     * is only split once it exceeds {@code 5 * MIN_DOCS_PER_SLICE} (250k) docs, hence the doc count.
     */
    private ForcedSplitIndex buildForcedSplitTermIndex(Directory dir) throws IOException {
        int numDocs = between(260_000, 300_000);
        // Default merge policy so forceMerge(1) consolidates into a single leaf — NoMergePolicy would
        // leave many small segments, each its own driver, hiding the sub-segment-split hazard.
        try (RandomIndexWriter writer = new RandomIndexWriter(random(), dir, newIndexWriterConfig())) {
            int matches = 0;
            for (int d = 0; d < numDocs; d++) {
                Document doc = new Document();
                boolean match = d % 3 == 0;
                doc.add(new StringField("kw", match ? "match" : "other", StringField.Store.NO));
                if (match) {
                    matches++;
                }
                writer.addDocument(doc);
            }
            writer.forceMerge(1);
            return new ForcedSplitIndex(writer.getReader(), matches);
        }
    }

    /**
     * Builds a DOC-partitioned count over a {@code TermQuery} with an explicit
     * {@link LuceneSliceQueue.LeafSplitGuard} (bypassing the {@link LuceneCountOperator.Factory},
     * which hardcodes {@code leafHasCountShortcut}) and asserts the total equals
     * {@code expectedMatches}. Verifies the leaf actually splits ({@code totalSlices() > 1}) so the
     * sub-segment path is genuinely exercised.
     */
    private void runDocPartitionedTermQueryCount(ShardContext ctx, int expectedMatches, LuceneSliceQueue.LeafSplitGuard guard) {
        final int taskConcurrency = 8;
        var contexts = new IndexedByShardIdFromSingleton<>(ctx);
        Function<ShardContext, List<LuceneSliceQueue.QueryAndTags>> queryFn = c -> List.of(
            new LuceneSliceQueue.QueryAndTags(new TermQuery(new Term("kw", "match")), List.of())
        );
        LuceneSliceQueue sliceQueue = LuceneSliceQueue.create(
            contexts,
            queryFn,
            DataPartitioning.DOC,
            (c, q) -> LuceneCountOperator.partitioningStrategyForCount(c, q, (long) taskConcurrency * LuceneSliceQueue.MIN_DOCS_PER_SLICE),
            1,
            taskConcurrency,
            shardContext -> ScoreMode.COMPLETE_NO_SCORES,
            guard
        );
        assertThat("forced-split guard must produce sub-segment slices", sliceQueue.totalSlices(), greaterThan(1));
        List<Page> results = new CopyOnWriteArrayList<>();
        List<Driver> drivers = new ArrayList<>();
        for (int i = 0; i < taskConcurrency; i++) {
            DriverContext driverCtx = driverContext();
            LuceneCountOperator op = new LuceneCountOperator(
                contexts,
                driverCtx,
                sliceQueue,
                List.of(),
                Integer.MAX_VALUE,
                () -> 0L,
                QueryWarnings.EMIT
            );
            drivers.add(TestDriverFactory.create(driverCtx, op, List.of(), new TestResultPageSinkOperator(results::add)));
        }
        new TestDriverRunner().run(drivers);
        long total = 0;
        for (Page p : results) {
            total += getCount(p);
        }
        assertThat("DOC-partitioned count must equal the true match count", total, equalTo((long) expectedMatches));
    }

    private void testCount(Supplier<DriverContext> contexts, int size, int limit) {
        DataPartitioning dataPartitioning = randomFrom(DataPartitioning.values());
        LuceneCountOperator.Factory factory = simple(dataPartitioning, size, limit);
        List<Page> results = new CopyOnWriteArrayList<>();
        List<Driver> drivers = new ArrayList<>();
        int taskConcurrency = between(1, 8);
        for (int i = 0; i < taskConcurrency; i++) {
            DriverContext ctx = contexts.get();
            drivers.add(TestDriverFactory.create(ctx, factory.get(ctx), List.of(), new TestResultPageSinkOperator(results::add)));
        }
        new TestDriverRunner().run(drivers);
        assertThat(results.size(), lessThanOrEqualTo(taskConcurrency));
        testCase.checkPages(size, limit, results);
        var expectedStrategy = switch (dataPartitioning) {
            case SHARD -> LuceneSliceQueue.PartitioningStrategy.SHARD;
            case SEGMENT -> LuceneSliceQueue.PartitioningStrategy.SEGMENT;
            case DOC -> LuceneSliceQueue.PartitioningStrategy.DOC;
            case AUTO -> size >= 1 ? testCase.expectedPartitioning() : LuceneSliceQueue.PartitioningStrategy.SHARD;
        };
        for (Driver driver : drivers) {
            LuceneOperator.Status status = (LuceneOperator.Status) driver.status().completedOperators().get(0).status();
            assertNotNull(status);
            var strategies = status.partitioningStrategies();
            for (var strategy : strategies.values()) {
                assertThat(strategies.toString(), strategy, is(expectedStrategy));
            }
        }
    }

    private static long getCount(Page p) {
        LongBlock b = p.getBlock(0);
        LongVector v = b.asVector();
        assertThat(v.getPositionCount(), equalTo(1));
        assertThat(v.isConstant(), equalTo(true));
        return v.getLong(0);
    }

    private static void checkSeen(Page p, Matcher<Integer> positionCount) {
        BooleanBlock b = p.getBlock(p.getBlockCount() - 1);
        BooleanVector v = b.asVector();
        assertThat(v.getPositionCount(), positionCount);
        assertThat(v.isConstant(), equalTo(true));
        assertThat(v.getBoolean(0), equalTo(true));
    }

    private static Map<Integer, Long> getCountsByTag(List<Page> results) {
        Map<Integer, Long> totals = new TreeMap<>();
        for (Page page : results) {
            assertThat(page.getBlockCount(), equalTo(3));
            checkSeen(page, greaterThanOrEqualTo(0));
            LongBlock countsBlock = page.getBlock(page.getBlockCount() - 2);
            LongVector counts = countsBlock.asVector();
            IntBlock groupsBlock = page.getBlock(0);
            IntVector groups = groupsBlock.asVector();
            for (int p = 0; p < page.getPositionCount(); p++) {
                long count = counts.getLong(p);
                totals.compute(groups.getInt(p), (k, prev) -> prev == null ? count : (prev + count));
            }
        }
        return totals;
    }

    /**
     * Same shape as {@link #testDocPartitioningRespectsCountShortcutSliceBoundaries} but the
     * leaf-wide count shortcut comes from the {@link LRUQueryCache} rather than from a built-in
     * {@code Weight.count()}: a query with no built-in shortcut gains one once it lands in the cache,
     * because {@code CachingWrapperWeight.count(leaf)} returns the cardinality of the cached bitset.
     * The cache is warmed before the {@link LuceneCountOperator.Factory} runs, so the
     * {@code leafHasCountShortcut} guard sees the cached count at plan time and keeps the leaf whole —
     * the count then fires once and is exact. The harder race — where the query is cached only
     * <em>after</em> the leaf has already been split, so each sub-slice relies on {@code coversFullLeaf}
     * to avoid applying the leaf cardinality to its own range — is covered by
     * {@link #testDocPartitioningForcedSplitCorrectWhenQueryIsCached}.
     */
    public void testDocPartitioningCorrectWhenQueryIsCached() throws IOException {
        final int partitioningTaskConcurrency = 8;
        int numDocs = between(100_000, 200_000);
        try (Directory dir = newDirectory()) {
            int expectedMatches;
            try (RandomIndexWriter writer = new RandomIndexWriter(random(), dir, newIndexWriterConfig())) {
                int matches = 0;
                for (int d = 0; d < numDocs; d++) {
                    Document doc = new Document();
                    boolean match = d % 3 == 0;
                    doc.add(new StringField("kw", match ? "match" : "other", StringField.Store.NO));
                    if (match) {
                        matches++;
                    }
                    writer.addDocument(doc);
                }
                writer.forceMerge(1);
                expectedMatches = matches;
                try (IndexReader r = writer.getReader()) {
                    // Force the LRUQueryCache to wrap every query so Weight.count(leaf) returns the
                    // cached bitset's cardinality on subsequent calls.
                    QueryCachingPolicy alwaysCache = new QueryCachingPolicy() {
                        @Override
                        public void onUse(org.apache.lucene.search.Query query) {}

                        @Override
                        public boolean shouldCache(org.apache.lucene.search.Query query) {
                            return true;
                        }
                    };
                    ContextIndexSearcher cachingSearcher = new ContextIndexSearcher(
                        r,
                        IndexSearcher.getDefaultSimilarity(),
                        new LRUQueryCache(100, 100 * 1024 * 1024),
                        alwaysCache,
                        true,
                        Runnable::run,
                        10,
                        50_000
                    );
                    // Warm the cache so the next access uses the cached Weight.
                    cachingSearcher.count(new TermQuery(new Term("kw", "match")));

                    ShardContext ctx = new LuceneSourceOperatorTests.MockShardContext(cachingSearcher, 0);
                    Function<ShardContext, List<LuceneSliceQueue.QueryAndTags>> queryFn = c -> List.of(
                        new LuceneSliceQueue.QueryAndTags(new TermQuery(new Term("kw", "match")), List.of())
                    );
                    LuceneCountOperator.Factory factory = new LuceneCountOperator.Factory(
                        new IndexedByShardIdFromSingleton<>(ctx),
                        queryFn,
                        DataPartitioning.DOC,
                        1,
                        partitioningTaskConcurrency,
                        List.of(),
                        Integer.MAX_VALUE,
                        () -> 0L,
                        LuceneSliceQueue.MIN_DOCS_PER_SLICE,
                        QueryWarnings.EMIT
                    );
                    int driverCount = partitioningTaskConcurrency;
                    List<Page> results = new CopyOnWriteArrayList<>();
                    List<Driver> drivers = new ArrayList<>();
                    for (int i = 0; i < driverCount; i++) {
                        DriverContext driverCtx = driverContext();
                        drivers.add(
                            TestDriverFactory.create(
                                driverCtx,
                                factory.get(driverCtx),
                                List.of(),
                                new TestResultPageSinkOperator(results::add)
                            )
                        );
                    }
                    new TestDriverRunner().run(drivers);
                    long total = 0;
                    for (Page p : results) {
                        total += getCount(p);
                    }
                    assertThat(
                        "DOC-partitioned count under LRUQueryCache must equal the true match count",
                        total,
                        equalTo((long) expectedMatches)
                    );
                }
            }
        }
    }

    /**
     * Direct test of {@link LuceneOperator.LuceneScorer#coversFullLeaf(int, int, int)}: locks the
     * {@code >=} comparison against {@code Integer.MAX_VALUE}, which Lucene's
     * {@code IndexSearcher.LeafReaderContextPartition.createForEntireSegment} uses as the
     * open-ended upper bound. A future tightening to {@code ==} would break SHARD-partitioned
     * counts (silently fall back from the {@code Weight.count()} shortcut to iteration).
     */
    public void testCoversFullLeafBoundary() {
        // Whole-leaf slices: position=0, maxPosition spans the leaf.
        assertTrue("[0, leafMaxDoc) covers full leaf", LuceneOperator.LuceneScorer.coversFullLeaf(0, 1000, 1000));
        assertTrue(
            "[0, Integer.MAX_VALUE) covers full leaf (createForEntireSegment sentinel)",
            LuceneOperator.LuceneScorer.coversFullLeaf(0, Integer.MAX_VALUE, 1000)
        );

        // Sub-segment slices: any non-zero start or any short end falsifies the predicate.
        assertFalse("[500, leafMaxDoc) is a sub-slice", LuceneOperator.LuceneScorer.coversFullLeaf(500, 1000, 1000));
        assertFalse("[0, leafMaxDoc/2) is a sub-slice", LuceneOperator.LuceneScorer.coversFullLeaf(0, 500, 1000));
        assertFalse("[100, 900) is a sub-slice", LuceneOperator.LuceneScorer.coversFullLeaf(100, 900, 1000));

        // Empty leaf: trivially covered.
        assertTrue("empty leaf covered by [0, 0)", LuceneOperator.LuceneScorer.coversFullLeaf(0, 0, 0));
    }
}
