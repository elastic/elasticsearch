/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.compute.lucene;

import org.apache.lucene.document.Document;
import org.apache.lucene.document.LongPoint;
import org.apache.lucene.index.IndexReader;
import org.apache.lucene.index.NoMergePolicy;
import org.apache.lucene.search.IndexSearcher;
import org.apache.lucene.search.MatchAllDocsQuery;
import org.apache.lucene.search.Query;
import org.apache.lucene.store.Directory;
import org.apache.lucene.tests.index.RandomIndexWriter;
import org.elasticsearch.common.breaker.CircuitBreakingException;
import org.elasticsearch.common.util.BigArrays;
import org.elasticsearch.compute.data.BooleanBlock;
import org.elasticsearch.compute.data.LongBlock;
import org.elasticsearch.compute.data.Page;
import org.elasticsearch.compute.operator.AnyOperatorTestCase;
import org.elasticsearch.compute.operator.Driver;
import org.elasticsearch.compute.operator.DriverContext;
import org.elasticsearch.compute.operator.OperatorTestCase;
import org.elasticsearch.compute.operator.TestResultPageSinkOperator;
import org.elasticsearch.core.IOUtils;
import org.elasticsearch.index.cache.query.TrivialQueryCachingPolicy;
import org.elasticsearch.index.query.SearchExecutionContext;
import org.elasticsearch.indices.CrankyCircuitBreakerService;
import org.elasticsearch.search.internal.ContextIndexSearcher;
import org.elasticsearch.search.internal.SearchContext;
import org.junit.After;

import java.io.IOException;
import java.io.UncheckedIOException;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.function.Supplier;

import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.is;
import static org.hamcrest.Matchers.lessThanOrEqualTo;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

public class LuceneCountOperatorTests extends AnyOperatorTestCase {
    private Directory directory = newDirectory();
    private IndexReader reader;

    @After
    public void closeIndex() throws IOException {
        IOUtils.close(reader, directory);
    }

    @Override
    protected LuceneCountOperator.Factory simple(BigArrays bigArrays) {
        return simple(bigArrays, randomFrom(DataPartitioning.values()), between(1, 10_000), 100);
    }

    private LuceneCountOperator.Factory simple(BigArrays bigArrays, DataPartitioning dataPartitioning, int numDocs, int limit) {
        boolean enableShortcut = randomBoolean();
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
                doc.add(new LongPoint("s", d));
                writer.addDocument(doc);
                if (enableShortcut == false && randomBoolean()) {
                    doc = new Document();
                    doc.add(new LongPoint("s", randomLongBetween(numDocs * 5L, numDocs * 10L)));
                    writer.addDocument(doc);
                }
                if (d % commitEvery == 0) {
                    writer.commit();
                }
            }
            reader = writer.getReader();
        } catch (IOException e) {
            throw new RuntimeException(e);
        }

        SearchContext ctx = mockSearchContext(reader);
        SearchExecutionContext ectx = mock(SearchExecutionContext.class);
        when(ctx.getSearchExecutionContext()).thenReturn(ectx);
        when(ectx.getIndexReader()).thenReturn(reader);
        final Query query;
        if (enableShortcut && randomBoolean()) {
            query = new MatchAllDocsQuery();
        } else {
            query = LongPoint.newRangeQuery("s", 0, numDocs);
        }
        return new LuceneCountOperator.Factory(List.of(ctx), c -> query, dataPartitioning, between(1, 8), limit);
    }

    @Override
    protected String expectedToStringOfSimple() {
        assumeFalse("can't support variable maxPageSize", true); // TODO allow testing this
        return "LuceneCountOperator[shardId=0, maxPageSize=**random**]";
    }

    @Override
    protected String expectedDescriptionOfSimple() {
        assumeFalse("can't support variable maxPageSize", true); // TODO allow testing this
        return """
            LuceneCountOperator[dataPartitioning = SHARD, maxPageSize = **random**, limit = 100, sorts = [{"s":{"order":"asc"}}]]""";
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
        LuceneCountOperator.Factory factory = simple(contexts.get().bigArrays(), dataPartitioning, size, limit);
        List<Page> results = new CopyOnWriteArrayList<>();
        List<Driver> drivers = new ArrayList<>();
        int taskConcurrency = between(1, 8);
        for (int i = 0; i < taskConcurrency; i++) {
            DriverContext ctx = contexts.get();
            drivers.add(new Driver(ctx, factory.get(ctx), List.of(), new TestResultPageSinkOperator(results::add), () -> {}));
        }
        OperatorTestCase.runDriver(drivers);
        assertThat(results.size(), lessThanOrEqualTo(taskConcurrency));
        long totalCount = 0;
        for (Page page : results) {
            assertThat(page.getPositionCount(), is(1));
            assertThat(page.getBlockCount(), is(2));
            LongBlock lb = page.getBlock(0);
            assertThat(lb.getPositionCount(), is(1));
            long count = lb.getLong(0);
            assertThat(count, lessThanOrEqualTo((long) limit));
            totalCount += count;
            BooleanBlock bb = page.getBlock(1);
            assertTrue(bb.getBoolean(0));
        }
        // We can't verify the limit
        if (size <= limit) {
            assertThat(totalCount, equalTo((long) size));
        }
    }

    /**
     * Creates a mock search context with the given index reader.
     * The returned mock search context can be used to test with {@link LuceneOperator}.
     */
    public static SearchContext mockSearchContext(IndexReader reader) {
        try {
            ContextIndexSearcher searcher = new ContextIndexSearcher(
                reader,
                IndexSearcher.getDefaultSimilarity(),
                IndexSearcher.getDefaultQueryCache(),
                TrivialQueryCachingPolicy.NEVER,
                true
            );
            SearchContext searchContext = mock(SearchContext.class);
            when(searchContext.searcher()).thenReturn(searcher);
            return searchContext;
        } catch (IOException e) {
            throw new UncheckedIOException(e);
        }
    }
}
