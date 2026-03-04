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
import org.apache.lucene.index.IndexReader;
import org.apache.lucene.index.NoMergePolicy;
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
import org.elasticsearch.compute.test.SourceOperatorTestCase;
import org.elasticsearch.compute.test.TestDriverFactory;
import org.elasticsearch.compute.test.TestDriverRunner;
import org.elasticsearch.compute.test.TestResultPageSinkOperator;
import org.elasticsearch.core.IOUtils;
import org.elasticsearch.indices.CrankyCircuitBreakerService;
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
        };

        abstract List<LuceneSliceQueue.QueryAndTags> queryAndExtra();

        abstract List<ElementType> tagTypes();

        abstract void checkPages(int numDocs, int limit, List<Page> results);

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
            between(1, 8),
            testCase.tagTypes(),
            limit
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
}
