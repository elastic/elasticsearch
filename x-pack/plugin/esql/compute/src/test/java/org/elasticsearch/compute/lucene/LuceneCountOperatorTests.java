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
import org.apache.lucene.search.MatchAllDocsQuery;
import org.apache.lucene.search.Query;
import org.apache.lucene.store.Directory;
import org.apache.lucene.tests.index.RandomIndexWriter;
import org.elasticsearch.common.breaker.CircuitBreakingException;
import org.elasticsearch.compute.data.BooleanBlock;
import org.elasticsearch.compute.data.LongBlock;
import org.elasticsearch.compute.data.Page;
import org.elasticsearch.compute.operator.Driver;
import org.elasticsearch.compute.operator.DriverContext;
import org.elasticsearch.compute.test.AnyOperatorTestCase;
import org.elasticsearch.compute.test.OperatorTestCase;
import org.elasticsearch.compute.test.TestResultPageSinkOperator;
import org.elasticsearch.core.IOUtils;
import org.elasticsearch.indices.CrankyCircuitBreakerService;
import org.hamcrest.Matcher;
import org.junit.After;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.function.Supplier;

import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.is;
import static org.hamcrest.Matchers.lessThanOrEqualTo;
import static org.hamcrest.Matchers.matchesRegex;

public class LuceneCountOperatorTests extends AnyOperatorTestCase {
    private Directory directory = newDirectory();
    private IndexReader reader;

    @After
    public void closeIndex() throws IOException {
        IOUtils.close(reader, directory);
    }

    @Override
    protected LuceneCountOperator.Factory simple() {
        return simple(randomFrom(DataPartitioning.values()), between(1, 10_000), 100);
    }

    private LuceneCountOperator.Factory simple(DataPartitioning dataPartitioning, int numDocs, int limit) {
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

        ShardContext ctx = new LuceneSourceOperatorTests.MockShardContext(reader, 0);
        final Query query;
        if (enableShortcut && randomBoolean()) {
            query = new MatchAllDocsQuery();
        } else {
            query = LongPoint.newRangeQuery("s", 0, numDocs);
        }
        return new LuceneCountOperator.Factory(
            List.of(ctx),
            c -> List.of(new LuceneSliceQueue.QueryAndTags(query, List.of())),
            dataPartitioning,
            between(1, 8),
            limit
        );
    }

    @Override
    protected Matcher<String> expectedToStringOfSimple() {
        return matchesRegex("LuceneCountOperator\\[maxPageSize = \\d+, remainingDocs=100]");
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
            drivers.add(new Driver("test", ctx, factory.get(ctx), List.of(), new TestResultPageSinkOperator(results::add), () -> {}));
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
}
