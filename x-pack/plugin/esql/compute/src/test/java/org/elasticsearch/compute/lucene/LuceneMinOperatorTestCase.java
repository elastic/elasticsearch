/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.compute.lucene;

import org.apache.lucene.document.Document;
import org.apache.lucene.document.SortedNumericDocValuesField;
import org.apache.lucene.index.IndexReader;
import org.apache.lucene.index.IndexableField;
import org.apache.lucene.index.NoMergePolicy;
import org.apache.lucene.search.MatchAllDocsQuery;
import org.apache.lucene.search.Query;
import org.apache.lucene.store.Directory;
import org.apache.lucene.tests.index.RandomIndexWriter;
import org.elasticsearch.common.breaker.CircuitBreakingException;
import org.elasticsearch.compute.aggregation.AggregatorFunction;
import org.elasticsearch.compute.data.Block;
import org.elasticsearch.compute.data.Page;
import org.elasticsearch.compute.operator.Driver;
import org.elasticsearch.compute.operator.DriverContext;
import org.elasticsearch.compute.test.AnyOperatorTestCase;
import org.elasticsearch.compute.test.OperatorTestCase;
import org.elasticsearch.compute.test.TestResultPageSinkOperator;
import org.elasticsearch.core.IOUtils;
import org.elasticsearch.core.Releasables;
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

public abstract class LuceneMinOperatorTestCase extends AnyOperatorTestCase {

    protected interface NumberTypeTest {

        IndexableField newPointField();

        IndexableField newDocValuesField();

        void assertPage(Page page);

        AggregatorFunction newAggregatorFunction(DriverContext context);

        void assertMinValue(Block block, boolean exactResult);

    }

    protected abstract NumberTypeTest getNumberTypeTest();

    protected abstract LuceneMinFactory.NumberType getNumberType();

    protected static final String FIELD_NAME = "field";
    private final Directory directory = newDirectory();
    private IndexReader reader;

    @After
    public void closeIndex() throws IOException {
        IOUtils.close(reader, directory);
    }

    @Override
    protected LuceneMinFactory simple() {
        return simple(getNumberTypeTest(), randomFrom(DataPartitioning.values()), between(1, 10_000), 100);
    }

    private LuceneMinFactory simple(NumberTypeTest numberTypeTest, DataPartitioning dataPartitioning, int numDocs, int limit) {
        final boolean enableShortcut = randomBoolean();
        final boolean enableMultiValue = randomBoolean();
        final int commitEvery = Math.max(1, numDocs / 10);
        try (
            RandomIndexWriter writer = new RandomIndexWriter(
                random(),
                directory,
                newIndexWriterConfig().setMergePolicy(NoMergePolicy.INSTANCE)
            )
        ) {

            for (int d = 0; d < numDocs; d++) {
                final var numValues = enableMultiValue ? randomIntBetween(1, 5) : 1;
                final var doc = new Document();
                for (int i = 0; i < numValues; i++) {
                    if (enableShortcut) {
                        doc.add(numberTypeTest.newPointField());
                    } else {
                        doc.add(numberTypeTest.newDocValuesField());
                    }
                }
                writer.addDocument(doc);
                if (d % commitEvery == 0) {
                    writer.commit();
                }
            }
            reader = writer.getReader();
        } catch (IOException e) {
            throw new RuntimeException(e);
        }

        final ShardContext ctx = new LuceneSourceOperatorTests.MockShardContext(reader, 0);
        final Query query;
        if (enableShortcut && randomBoolean()) {
            query = new MatchAllDocsQuery();
        } else {
            query = SortedNumericDocValuesField.newSlowRangeQuery(FIELD_NAME, Long.MIN_VALUE, Long.MAX_VALUE);
        }
        return new LuceneMinFactory(
            List.of(ctx),
            c -> List.of(new LuceneSliceQueue.QueryAndTags(query, List.of())),
            dataPartitioning,
            between(1, 8),
            FIELD_NAME,
            getNumberType(),
            limit
        );
    }

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
        testMin(contexts, size, limit);
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
        testMin(contexts, 0, limit);
    }

    private void testMin(Supplier<DriverContext> contexts, int size, int limit) {
        DataPartitioning dataPartitioning = randomFrom(DataPartitioning.values());
        NumberTypeTest numberTypeTest = getNumberTypeTest();
        LuceneMinFactory factory = simple(numberTypeTest, dataPartitioning, size, limit);
        List<Page> results = new CopyOnWriteArrayList<>();
        List<Driver> drivers = new ArrayList<>();
        int taskConcurrency = between(1, 8);
        for (int i = 0; i < taskConcurrency; i++) {
            DriverContext ctx = contexts.get();
            drivers.add(new Driver("test", ctx, factory.get(ctx), List.of(), new TestResultPageSinkOperator(results::add), () -> {}));
        }
        OperatorTestCase.runDriver(drivers);
        assertThat(results.size(), lessThanOrEqualTo(taskConcurrency));

        try (AggregatorFunction aggregatorFunction = numberTypeTest.newAggregatorFunction(contexts.get())) {
            for (Page page : results) {
                assertThat(page.getPositionCount(), is(1)); // one row
                assertThat(page.getBlockCount(), is(2)); // two blocks
                numberTypeTest.assertPage(page);
                aggregatorFunction.addIntermediateInput(page);
            }

            final Block[] result = new Block[1];
            try {
                aggregatorFunction.evaluateFinal(result, 0, contexts.get());
                if (result[0].areAllValuesNull() == false) {
                    boolean exactResult = size <= limit;
                    numberTypeTest.assertMinValue(result[0], exactResult);
                }
            } finally {
                Releasables.close(result);
            }
        }
    }

    @Override
    protected final Matcher<String> expectedToStringOfSimple() {
        return matchesRegex("LuceneMinMaxOperator\\[maxPageSize = \\d+, remainingDocs=100]");
    }

    @Override
    protected final Matcher<String> expectedDescriptionOfSimple() {
        return matchesRegex(
            "LuceneMinOperator\\[type = "
                + getNumberType().name()
                + ", dataPartitioning = (AUTO|DOC|SHARD|SEGMENT), fieldName = "
                + FIELD_NAME
                + ", limit = 100]"
        );
    }
}
