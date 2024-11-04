/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.compute.lucene;

import org.apache.lucene.document.SortedNumericDocValuesField;
import org.apache.lucene.index.IndexReader;
import org.apache.lucene.index.IndexableField;
import org.apache.lucene.index.NoMergePolicy;
import org.apache.lucene.search.IndexSearcher;
import org.apache.lucene.search.MatchAllDocsQuery;
import org.apache.lucene.search.Query;
import org.apache.lucene.store.Directory;
import org.apache.lucene.tests.index.RandomIndexWriter;
import org.elasticsearch.common.breaker.CircuitBreakingException;
import org.elasticsearch.compute.data.DocBlock;
import org.elasticsearch.compute.data.DoubleBlock;
import org.elasticsearch.compute.data.ElementType;
import org.elasticsearch.compute.data.LongBlock;
import org.elasticsearch.compute.data.Page;
import org.elasticsearch.compute.operator.AnyOperatorTestCase;
import org.elasticsearch.compute.operator.Driver;
import org.elasticsearch.compute.operator.DriverContext;
import org.elasticsearch.compute.operator.Operator;
import org.elasticsearch.compute.operator.OperatorTestCase;
import org.elasticsearch.compute.operator.TestResultPageSinkOperator;
import org.elasticsearch.core.IOUtils;
import org.elasticsearch.index.cache.query.TrivialQueryCachingPolicy;
import org.elasticsearch.index.mapper.MappedFieldType;
import org.elasticsearch.index.mapper.NumberFieldMapper;
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
import java.util.function.Function;

import static org.hamcrest.Matchers.both;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.greaterThanOrEqualTo;
import static org.hamcrest.Matchers.hasSize;
import static org.hamcrest.Matchers.lessThan;
import static org.hamcrest.Matchers.lessThanOrEqualTo;
import static org.hamcrest.Matchers.matchesRegex;

public class LuceneSourceOperatorTests extends AnyOperatorTestCase {
    private static final MappedFieldType S_FIELD = new NumberFieldMapper.NumberFieldType("s", NumberFieldMapper.NumberType.LONG);
    private Directory directory = newDirectory();
    private IndexReader reader;

    @After
    public void closeIndex() throws IOException {
        IOUtils.close(reader, directory);
    }

    @Override
    protected LuceneSourceOperator.Factory simple() {
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
        Function<ShardContext, Query> queryFunction = c -> new MatchAllDocsQuery();
        int maxPageSize = between(10, Math.max(10, numDocs));
        return new LuceneSourceOperator.Factory(List.of(ctx), queryFunction, dataPartitioning, 1, maxPageSize, limit, scoring);
    }

    @Override
    protected Matcher<String> expectedToStringOfSimple() {
        return matchesRegex("LuceneSourceOperator\\[maxPageSize = \\d+, remainingDocs = \\d+]");
    }

    @Override
    protected Matcher<String> expectedDescriptionOfSimple() {
        return matchesRegex(
            "LuceneSourceOperator"
                + "\\[dataPartitioning = (DOC|SHARD|SEGMENT), maxPageSize = \\d+, limit = 100, scoreMode = (COMPLETE|COMPLETE_NO_SCORES)]"
        );
    }

    // TODO tests for the other data partitioning configurations

    public void testShardDataPartitioning() {
        int size = between(1_000, 20_000);
        int limit = between(10, size);
        testSimple(driverContext(), size, limit);
    }

    public void testEmpty() {
        testSimple(driverContext(), 0, between(10, 10_000));
    }

    public void testWithCranky() {
        try {
            testSimple(crankyDriverContext(), between(1, 10_000), 100);
            logger.info("cranky didn't break");
        } catch (CircuitBreakingException e) {
            logger.info("broken", e);
            assertThat(e.getMessage(), equalTo(CrankyCircuitBreakerService.ERROR_MESSAGE));
        }
    }

    public void testEmptyWithCranky() {
        try {
            testSimple(crankyDriverContext(), 0, between(10, 10_000));
            logger.info("cranky didn't break");
        } catch (CircuitBreakingException e) {
            logger.info("broken", e);
            assertThat(e.getMessage(), equalTo(CrankyCircuitBreakerService.ERROR_MESSAGE));
        }
    }

    public void testShardDataPartitioningWithCranky() {
        int size = between(1_000, 20_000);
        int limit = between(10, size);
        try {
            testSimple(crankyDriverContext(), size, limit);
            logger.info("cranky didn't break");
        } catch (CircuitBreakingException e) {
            logger.info("broken", e);
            assertThat(e.getMessage(), equalTo(CrankyCircuitBreakerService.ERROR_MESSAGE));
        }
    }

    private void testSimple(DriverContext ctx, int size, int limit) {
        LuceneSourceOperator.Factory factory = simple(DataPartitioning.SHARD, size, limit, scoring);
        Operator.OperatorFactory readS = ValuesSourceReaderOperatorTests.factory(reader, S_FIELD, ElementType.LONG);

        List<Page> results = new ArrayList<>();

        OperatorTestCase.runDriver(
            new Driver(ctx, factory.get(ctx), List.of(readS.get(ctx)), new TestResultPageSinkOperator(results::add), () -> {})
        );
        OperatorTestCase.assertDriverContext(ctx);

        for (Page page : results) {
            assertThat(page.getPositionCount(), lessThanOrEqualTo(factory.maxPageSize()));
        }

        for (Page page : results) {
            LongBlock sBlock = page.getBlock(initialBlockIndex(page));
            for (int p = 0; p < page.getPositionCount(); p++) {
                assertThat(sBlock.getLong(sBlock.getFirstValueIndex(p)), both(greaterThanOrEqualTo(0L)).and(lessThan((long) size)));
            }
        }
        int maxPages = Math.min(size, limit);
        int minPages = (int) Math.ceil(maxPages / factory.maxPageSize());
        assertThat(results, hasSize(both(greaterThanOrEqualTo(minPages)).and(lessThanOrEqualTo(maxPages))));
    }

    // Scores are not interesting to this test, but enabled conditionally and effectively ignored just for coverage.
    private final boolean scoring = randomBoolean();

    // Returns the initial block index, ignoring the score block if scoring is enabled
    private int initialBlockIndex(Page page) {
        assert page.getBlock(0) instanceof DocBlock : "expected doc block at index 0";
        if (scoring) {
            assert page.getBlock(1) instanceof DoubleBlock : "expected double block at index 1";
            return 2;
        } else {
            return 1;
        }
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
        public Optional<SortAndFormats> buildSort(List<SortBuilder<?>> sorts) {
            return Optional.empty();
        }

        @Override
        public String shardIdentifier() {
            return "test";
        }
    }
}
