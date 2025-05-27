/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.compute.lucene;

import com.carrotsearch.randomizedtesting.annotations.ParametersFactory;

import com.carrotsearch.randomizedtesting.annotations.Repeat;

import com.carrotsearch.randomizedtesting.annotations.Seed;

import org.apache.lucene.document.SortedNumericDocValuesField;
import org.apache.lucene.index.IndexReader;
import org.apache.lucene.index.IndexableField;
import org.apache.lucene.index.NoMergePolicy;
import org.apache.lucene.search.IndexSearcher;
import org.apache.lucene.search.MatchAllDocsQuery;
import org.apache.lucene.store.Directory;
import org.apache.lucene.tests.index.RandomIndexWriter;
import org.elasticsearch.common.breaker.CircuitBreakingException;
import org.elasticsearch.compute.data.DocBlock;
import org.elasticsearch.compute.data.DoubleBlock;
import org.elasticsearch.compute.data.ElementType;
import org.elasticsearch.compute.data.IntBlock;
import org.elasticsearch.compute.data.LongBlock;
import org.elasticsearch.compute.data.Page;
import org.elasticsearch.compute.operator.Driver;
import org.elasticsearch.compute.operator.DriverContext;
import org.elasticsearch.compute.operator.Operator;
import org.elasticsearch.compute.operator.PageConsumerOperator;
import org.elasticsearch.compute.operator.SinkOperator;
import org.elasticsearch.compute.operator.SourceOperator;
import org.elasticsearch.compute.test.AnyOperatorTestCase;
import org.elasticsearch.compute.test.OperatorTestCase;
import org.elasticsearch.compute.test.TestDriverFactory;
import org.elasticsearch.compute.test.TestResultPageSinkOperator;
import org.elasticsearch.core.IOUtils;
import org.elasticsearch.core.TimeValue;
import org.elasticsearch.index.cache.query.TrivialQueryCachingPolicy;
import org.elasticsearch.index.mapper.BlockLoader;
import org.elasticsearch.index.mapper.MappedFieldType;
import org.elasticsearch.index.mapper.NumberFieldMapper;
import org.elasticsearch.index.mapper.SourceLoader;
import org.elasticsearch.indices.CrankyCircuitBreakerService;
import org.elasticsearch.search.internal.ContextIndexSearcher;
import org.elasticsearch.search.sort.SortAndFormats;
import org.elasticsearch.search.sort.SortBuilder;
import org.hamcrest.Matcher;
import org.junit.After;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Optional;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.Function;

import static org.hamcrest.Matchers.both;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.greaterThanOrEqualTo;
import static org.hamcrest.Matchers.hasSize;
import static org.hamcrest.Matchers.lessThan;
import static org.hamcrest.Matchers.lessThanOrEqualTo;
import static org.hamcrest.Matchers.matchesRegex;

//@Seed("4FF2CB98F60FD89D:EBBB701671985F2B")
@Repeat(iterations = 100)
public class LuceneSourceOperatorTests extends AnyOperatorTestCase {
    private static final MappedFieldType S_FIELD = new NumberFieldMapper.NumberFieldType("s", NumberFieldMapper.NumberType.LONG);

    @ParametersFactory(argumentFormatting = "%s %s")
    public static Iterable<Object[]> parameters() {
        List<Object[]> parameters = new ArrayList<>();
        for (TestCase c : TestCase.values()) {
            for (boolean scoring : new boolean[] { false, true }) {
                parameters.add(new Object[] { c, scoring });
            }
        }
        return parameters;
    }

    public enum TestCase {
        MATCH_ALL {
            @Override
            List<LuceneSliceQueue.QueryAndTags> queryAndExtra() {
                return List.of(new LuceneSliceQueue.QueryAndTags(new MatchAllDocsQuery(), List.of()));
            }

            @Override
            void checkPages(int numDocs, int limit, int maxPageSize, List<Page> results) {
                int maxPages = Math.min(numDocs, limit);
                int minPages = (int) Math.ceil((double) maxPages / maxPageSize);
                assertThat(results, hasSize(both(greaterThanOrEqualTo(minPages)).and(lessThanOrEqualTo(maxPages))));
            }

            @Override
            int numResults(int numDocs) {
                return numDocs;
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
            void checkPages(int numDocs, int limit, int maxPageSize, List<Page> results) {
                assertThat(results, hasSize(Math.min(numDocs, 2)));
                if (results.isEmpty() == false) {
                    Page page = results.get(0);
                    IntBlock extra = page.getBlock(page.getBlockCount() - 2);
                    assertThat(extra.asVector().isConstant(), equalTo(true));
                    assertThat(extra.getInt(0), equalTo(123));
                    if (results.size() > 1) {
                        page = results.get(1);
                        extra = page.getBlock(page.getBlockCount() - 2);
                        assertThat(extra.asVector().isConstant(), equalTo(true));
                        assertThat(extra.getInt(0), equalTo(456));
                    }
                }
            }

            @Override
            int numResults(int numDocs) {
                return Math.min(numDocs, 2);
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
            void checkPages(int numDocs, int limit, int maxPageSize, List<Page> results) {
                MATCH_ALL.checkPages(numDocs, limit, maxPageSize, results);
                for (Page page : results) {
                    IntBlock extra = page.getBlock(page.getBlockCount() - 2);
                    LongBlock data = page.getBlock(page.getBlockCount() - 1);
                    for (int p = 0; p < page.getPositionCount(); p++) {
                        assertThat(extra.getInt(p), equalTo(data.getLong(p) <= 100 ? 123 : 456));
                    }
                }
            }

            @Override
            int numResults(int numDocs) {
                return numDocs;
            }
        };

        abstract List<LuceneSliceQueue.QueryAndTags> queryAndExtra();

        abstract void checkPages(int numDocs, int limit, int maxPageSize, List<Page> results);

        abstract int numResults(int numDocs);
    }

    private final TestCase testCase;
    /**
     * Do we enable scoring? We don't check the score in this test, but
     * it's nice to make sure everything else works with scoring enabled.
     */
    private final boolean scoring;

    private Directory directory = newDirectory();
    private IndexReader reader;

    public LuceneSourceOperatorTests(TestCase testCase, boolean scoring) {
        this.testCase = testCase;
        this.scoring = scoring;
    }

    @After
    public void closeIndex() throws IOException {
        IOUtils.close(reader, directory);
    }

    @Override
    protected LuceneSourceOperator.Factory simple(SimpleOptions options) {
        return simple(randomFrom(DataPartitioning.values()), between(1, 10_000), 100, scoring);
    }

    private LuceneSourceOperator.Factory simple(DataPartitioning dataPartitioning, int numDocs, int limit, boolean scoring) {
        int commitEvery = Math.max(1, numDocs / 10);
        try (
            RandomIndexWriter writer = new RandomIndexWriter(
                random(),
                directory,
                newIndexWriterConfig().setMergePolicy(NoMergePolicy.INSTANCE)
            )
        ) {
            for (int d = 0; d < numDocs; d++) {
                List<IndexableField> doc = new ArrayList<>();
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

        ShardContext ctx = new MockShardContext(reader, 0);
        Function<ShardContext, List<LuceneSliceQueue.QueryAndTags>> queryFunction = c -> testCase.queryAndExtra();
        int maxPageSize = between(10, Math.max(10, numDocs));
        int taskConcurrency = 4; // randomIntBetween(1, 4); NOCOMMIT
        return new LuceneSourceOperator.Factory(
            List.of(ctx),
            queryFunction,
            dataPartitioning,
            taskConcurrency,
            maxPageSize,
            limit,
            scoring
        );
    }

    @Override
    protected Matcher<String> expectedToStringOfSimple() {
        return matchesRegex("LuceneSourceOperator\\[shards = \\[test], maxPageSize = \\d+, remainingDocs = \\d+]");
    }

    @Override
    protected Matcher<String> expectedDescriptionOfSimple() {
        return matchesRegex(
            "LuceneSourceOperator\\["
                + "dataPartitioning = (AUTO|DOC|SHARD|SEGMENT), "
                + "maxPageSize = \\d+, "
                + "limit = 100, "
                + "needsScore = (true|false)]"
        );
    }

    public void testAutoPartitioning() {
        testSimple(DataPartitioning.AUTO);
    }

    public void testShardPartitioning() {
        testSimple(DataPartitioning.SHARD);
    }

    public void testSegmentPartitioning() {
        testSimple(DataPartitioning.SEGMENT);
    }

    public void testDocPartitioning() {
        testSimple(DataPartitioning.DOC);
    }

    private void testSimple(DataPartitioning partitioning) {
        int size = between(1_000, 20_000);
        int limit = between(10, size);
        testSimple(driverContext(), partitioning, size, limit);
    }

    public void testEarlyTermination() {
        int numDocs = 20_000; //between(1_000, 20_000); NOCOMMIT
        int limit = 100_000; //between(0, numDocs * 2); NOCOMMIT
        LuceneSourceOperator.Factory factory = simple(DataPartitioning.DOC
            // randomFrom(DataPartitioning.values()) NOCOMMIT
            , numDocs, limit, scoring);
        int taskConcurrency = factory.taskConcurrency();
        final AtomicInteger receivedRows = new AtomicInteger();
        List<Driver> drivers = new ArrayList<>();
        for (int i = 0; i < taskConcurrency; i++) {
            DriverContext driverContext = driverContext();
            SourceOperator sourceOperator = factory.get(driverContext);
            SinkOperator sinkOperator = new PageConsumerOperator(p -> {
                receivedRows.addAndGet(p.getPositionCount());
                p.releaseBlocks();
            });
            Driver driver = new Driver(
                "driver" + i,
                "test",
                "cluster",
                "node",
                0,
                0,
                driverContext,
                () -> "test",
                sourceOperator,
                List.of(),
                sinkOperator,
                TimeValue.timeValueNanos(1),
                () -> {}
            );
            drivers.add(driver);
        }
        OperatorTestCase.runDriver(drivers);
        logger.info(
            "{} received={} limit={} numResults={}",
            factory.dataPartitioning,
            receivedRows.get(),
            limit,
            testCase.numResults(numDocs)
        );
        assertThat(receivedRows.get(), equalTo(Math.min(limit, testCase.numResults(numDocs))));
    }

    public void testEmpty() {
        testSimple(driverContext(), randomFrom(DataPartitioning.values()), 0, between(10, 10_000));
    }

    public void testEmptyWithCranky() {
        try {
            testSimple(crankyDriverContext(), randomFrom(DataPartitioning.values()), 0, between(10, 10_000));
            logger.info("cranky didn't break");
        } catch (CircuitBreakingException e) {
            logger.info("broken", e);
            assertThat(e.getMessage(), equalTo(CrankyCircuitBreakerService.ERROR_MESSAGE));
        }
    }

    public void testAutoPartitioningWithCranky() {
        testWithCranky(DataPartitioning.AUTO);
    }

    public void testShardPartitioningWithCranky() {
        testWithCranky(DataPartitioning.SHARD);
    }

    public void testSegmentPartitioningWithCranky() {
        testWithCranky(DataPartitioning.SEGMENT);
    }

    public void testDocPartitioningWithCranky() {
        testWithCranky(DataPartitioning.DOC);
    }

    private void testWithCranky(DataPartitioning partitioning) {
        int size = between(1_000, 20_000);
        int limit = between(10, size);
        try {
            testSimple(crankyDriverContext(), partitioning, size, limit);
            logger.info("cranky didn't break");
        } catch (CircuitBreakingException e) {
            logger.info("broken", e);
            assertThat(e.getMessage(), equalTo(CrankyCircuitBreakerService.ERROR_MESSAGE));
        }
    }

    private void testSimple(DriverContext ctx, DataPartitioning partitioning, int numDocs, int limit) {
        LuceneSourceOperator.Factory factory = simple(partitioning, numDocs, limit, scoring);
        Operator.OperatorFactory readS = ValuesSourceReaderOperatorTests.factory(reader, S_FIELD, ElementType.LONG);

        List<Page> results = new ArrayList<>();

        OperatorTestCase.runDriver(
            TestDriverFactory.create(ctx, factory.get(ctx), List.of(readS.get(ctx)), new TestResultPageSinkOperator(results::add))
        );
        OperatorTestCase.assertDriverContext(ctx);

        for (Page page : results) {
            assertThat(page.getPositionCount(), lessThanOrEqualTo(factory.maxPageSize()));
        }

        for (Page page : results) {
            LongBlock sBlock = page.getBlock(initialBlockIndex(page));
            for (int p = 0; p < page.getPositionCount(); p++) {
                assertThat(sBlock.getLong(sBlock.getFirstValueIndex(p)), both(greaterThanOrEqualTo(0L)).and(lessThan((long) numDocs)));
            }
        }

        testCase.checkPages(numDocs, limit, factory.maxPageSize(), results);
    }

    // Returns the initial block index, ignoring the score block if scoring is enabled
    private int initialBlockIndex(Page page) {
        assert page.getBlock(0) instanceof DocBlock : "expected doc block at index 0";
        int offset = 1;
        if (scoring) {
            assert page.getBlock(1) instanceof DoubleBlock : "expected double block at index 1";
            offset++;
        }
        offset += testCase.queryAndExtra().get(0).tags().size();
        return offset;
    }

    /**
     * Creates a mock search context with the given index reader.
     * The returned mock search context can be used to test with {@link LuceneOperator}.
     */
    public static class MockShardContext implements ShardContext {
        private final int index;
        private final ContextIndexSearcher searcher;

        public MockShardContext(IndexReader reader, int index) {
            this.index = index;
            try {
                this.searcher = new ContextIndexSearcher(
                    reader,
                    IndexSearcher.getDefaultSimilarity(),
                    IndexSearcher.getDefaultQueryCache(),
                    TrivialQueryCachingPolicy.NEVER,
                    true
                );
            } catch (IOException e) {
                throw new AssertionError(e);
            }
        }

        @Override
        public int index() {
            return index;
        }

        @Override
        public IndexSearcher searcher() {
            return searcher;
        }

        @Override
        public SourceLoader newSourceLoader() {
            return SourceLoader.FROM_STORED_SOURCE;
        }

        @Override
        public BlockLoader blockLoader(
            String name,
            boolean asUnsupportedSource,
            MappedFieldType.FieldExtractPreference fieldExtractPreference
        ) {
            throw new UnsupportedOperationException();
        }

        @Override
        public Optional<SortAndFormats> buildSort(List<SortBuilder<?>> sorts) {
            return Optional.empty();
        }

        @Override
        public String shardIdentifier() {
            return "test";
        }

        @Override
        public MappedFieldType fieldType(String name) {
            throw new UnsupportedOperationException();
        }
    }
}
