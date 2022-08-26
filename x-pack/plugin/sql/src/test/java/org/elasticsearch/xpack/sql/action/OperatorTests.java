/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.sql.action;

import org.apache.lucene.document.Document;
import org.apache.lucene.document.NumericDocValuesField;
import org.apache.lucene.index.IndexReader;
import org.apache.lucene.search.MatchAllDocsQuery;
import org.apache.lucene.store.Directory;
import org.apache.lucene.tests.index.RandomIndexWriter;
import org.elasticsearch.common.util.BigArrays;
import org.elasticsearch.core.TimeValue;
import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.threadpool.TestThreadPool;
import org.elasticsearch.threadpool.ThreadPool;
import org.elasticsearch.xpack.sql.action.compute.data.LongBlock;
import org.elasticsearch.xpack.sql.action.compute.data.Page;
import org.elasticsearch.xpack.sql.action.compute.lucene.LuceneSourceOperator;
import org.elasticsearch.xpack.sql.action.compute.lucene.NumericDocValuesExtractor;
import org.elasticsearch.xpack.sql.action.compute.operator.Driver;
import org.elasticsearch.xpack.sql.action.compute.operator.LongAvgGroupingOperator;
import org.elasticsearch.xpack.sql.action.compute.operator.LongAvgOperator;
import org.elasticsearch.xpack.sql.action.compute.operator.LongGroupingOperator;
import org.elasticsearch.xpack.sql.action.compute.operator.LongMaxOperator;
import org.elasticsearch.xpack.sql.action.compute.operator.LongTransformerOperator;
import org.elasticsearch.xpack.sql.action.compute.operator.Operator;
import org.elasticsearch.xpack.sql.action.compute.operator.PageConsumerOperator;
import org.elasticsearch.xpack.sql.action.compute.operator.exchange.ExchangeSink;
import org.elasticsearch.xpack.sql.action.compute.operator.exchange.ExchangeSinkOperator;
import org.elasticsearch.xpack.sql.action.compute.operator.exchange.ExchangeSource;
import org.elasticsearch.xpack.sql.action.compute.operator.exchange.ExchangeSourceOperator;
import org.elasticsearch.xpack.sql.action.compute.operator.exchange.PassthroughExchanger;
import org.elasticsearch.xpack.sql.action.compute.operator.exchange.RandomExchanger;
import org.elasticsearch.xpack.sql.action.compute.operator.exchange.RandomUnionSourceOperator;
import org.junit.After;
import org.junit.Before;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.Executor;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;

public class OperatorTests extends ESTestCase {

    private ThreadPool threadPool;

    @Before
    public void setUp() throws Exception {
        super.setUp();
        threadPool = new TestThreadPool("OperatorTests");
    }

    @After
    public void tearDown() throws Exception {
        ThreadPool.terminate(threadPool, 30, TimeUnit.SECONDS);
        super.tearDown();
    }

    class RandomLongBlockSourceOperator implements Operator {

        boolean finished;

        @Override
        public Page getOutput() {
            if (random().nextInt(100) < 1) {
                finish();
            }
            final int size = randomIntBetween(1, 10);
            final long[] array = new long[size];
            for (int i = 0; i < array.length; i++) {
                array[i] = randomLongBetween(0, 5);
            }
            return new Page(new LongBlock(array, array.length));
        }

        @Override
        public boolean isFinished() {
            return finished;
        }

        @Override
        public void finish() {
            finished = true;
        }

        @Override
        public boolean needsInput() {
            return false;
        }

        @Override
        public void addInput(Page page) {
            throw new UnsupportedOperationException();
        }

        @Override
        public void close() {

        }
    }

    public void testOperators() {
        Driver driver = new Driver(
            List.of(
                new RandomLongBlockSourceOperator(),
                new LongTransformerOperator(0, i -> i + 1),
                new LongGroupingOperator(1, BigArrays.NON_RECYCLING_INSTANCE),
                new LongMaxOperator(2),
                new PageConsumerOperator(page -> logger.info("New page: {}", page))
            ),
            () -> {}
        );
        driver.run();
    }

    public void testOperatorsWithLucene() throws IOException {
        int numDocs = 100000;
        try (Directory dir = newDirectory(); RandomIndexWriter w = new RandomIndexWriter(random(), dir)) {
            Document doc = new Document();
            NumericDocValuesField docValuesField = new NumericDocValuesField("value", 0);
            for (int i = 0; i < numDocs; i++) {
                doc.clear();
                docValuesField.setLongValue(i);
                doc.add(docValuesField);
                w.addDocument(doc);
            }
            w.commit();

            try (IndexReader reader = w.getReader()) {
                AtomicInteger pageCount = new AtomicInteger();
                AtomicInteger rowCount = new AtomicInteger();
                AtomicReference<Page> lastPage = new AtomicReference<>();

                // implements cardinality on value field
                Driver driver = new Driver(
                    List.of(
                        new LuceneSourceOperator(reader, 0, new MatchAllDocsQuery()),
                        new NumericDocValuesExtractor(reader, 0, 1, 2, "value"),
                        new LongGroupingOperator(3, BigArrays.NON_RECYCLING_INSTANCE),
                        new LongMaxOperator(4), // returns highest group number
                        new LongTransformerOperator(0, i -> i + 1), // adds +1 to group number (which start with 0) to get group count
                        new PageConsumerOperator(page -> {
                            logger.info("New page: {}", page);
                            pageCount.incrementAndGet();
                            rowCount.addAndGet(page.getPositionCount());
                            lastPage.set(page);
                        })
                    ),
                    () -> {}
                );
                driver.run();
                assertEquals(1, pageCount.get());
                assertEquals(1, rowCount.get());
                assertEquals(numDocs, lastPage.get().getBlock(1).getLong(0));
            }
        }
    }

    public void testOperatorsWithLuceneSlicing() throws IOException {
        int numDocs = 100000;
        try (Directory dir = newDirectory(); RandomIndexWriter w = new RandomIndexWriter(random(), dir)) {
            Document doc = new Document();
            NumericDocValuesField docValuesField = new NumericDocValuesField("value", 0);
            for (int i = 0; i < numDocs; i++) {
                doc.clear();
                docValuesField.setLongValue(i);
                doc.add(docValuesField);
                w.addDocument(doc);
            }
            if (randomBoolean()) {
                w.forceMerge(randomIntBetween(1, 10));
            }
            w.commit();

            try (IndexReader reader = w.getReader()) {
                AtomicInteger rowCount = new AtomicInteger();

                List<Driver> drivers = new ArrayList<>();
                for (LuceneSourceOperator luceneSourceOperator : new LuceneSourceOperator(reader, 0, new MatchAllDocsQuery()).docSlice(
                    randomIntBetween(1, 10)
                )) {
                    drivers.add(
                        new Driver(
                            List.of(
                                luceneSourceOperator,
                                new NumericDocValuesExtractor(reader, 0, 1, 2, "value"),
                                new PageConsumerOperator(page -> rowCount.addAndGet(page.getPositionCount()))
                            ),
                            () -> {}
                        )
                    );
                }
                Driver.runToCompletion(threadPool.executor(ThreadPool.Names.SEARCH), drivers);
                assertEquals(numDocs, rowCount.get());
            }
        }
    }

    public void testOperatorsWithPassthroughExchange() {
        ExchangeSource exchangeSource = new ExchangeSource();

        Driver driver1 = new Driver(
            List.of(
                new RandomLongBlockSourceOperator(),
                new LongTransformerOperator(0, i -> i + 1),
                new ExchangeSinkOperator(
                    new ExchangeSink(new PassthroughExchanger(exchangeSource, Integer.MAX_VALUE), sink -> exchangeSource.finish())
                )
            ),
            () -> {}
        );

        Driver driver2 = new Driver(
            List.of(
                new ExchangeSourceOperator(exchangeSource),
                new LongGroupingOperator(1, BigArrays.NON_RECYCLING_INSTANCE),
                new PageConsumerOperator(page -> logger.info("New page: {}", page))
            ),
            () -> {}
        );

        Driver.runToCompletion(randomExecutor(), List.of(driver1, driver2));
    }

    private Executor randomExecutor() {
        return threadPool.executor(randomFrom(ThreadPool.Names.SAME, ThreadPool.Names.GENERIC, ThreadPool.Names.SEARCH));
    }

    public void testOperatorsWithRandomExchange() {
        ExchangeSource exchangeSource1 = new ExchangeSource();
        ExchangeSource exchangeSource2 = new ExchangeSource();

        Driver driver1 = new Driver(
            List.of(
                new RandomLongBlockSourceOperator(),
                new LongTransformerOperator(0, i -> i + 1),
                new ExchangeSinkOperator(
                    new ExchangeSink(
                        new RandomExchanger(List.of(p -> exchangeSource1.addPage(p, () -> {}), p -> exchangeSource2.addPage(p, () -> {}))),
                        sink -> {
                            exchangeSource1.finish();
                            exchangeSource2.finish();
                        }
                    )
                )
            ),
            () -> {}
        );

        ExchangeSource exchangeSource3 = new ExchangeSource();
        ExchangeSource exchangeSource4 = new ExchangeSource();

        Driver driver2 = new Driver(
            List.of(
                new ExchangeSourceOperator(exchangeSource1),
                new LongGroupingOperator(1, BigArrays.NON_RECYCLING_INSTANCE),
                new ExchangeSinkOperator(
                    new ExchangeSink(new PassthroughExchanger(exchangeSource3, Integer.MAX_VALUE), s -> exchangeSource3.finish())
                )
            ),
            () -> {}
        );

        Driver driver3 = new Driver(
            List.of(
                new ExchangeSourceOperator(exchangeSource2),
                new LongMaxOperator(1),
                new ExchangeSinkOperator(
                    new ExchangeSink(new PassthroughExchanger(exchangeSource4, Integer.MAX_VALUE), s -> exchangeSource4.finish())
                )
            ),
            () -> {}
        );

        Driver driver4 = new Driver(
            List.of(
                new RandomUnionSourceOperator(List.of(exchangeSource3, exchangeSource4)),
                new PageConsumerOperator(page -> logger.info("New page with #blocks: {}", page.getBlockCount()))
            ),
            () -> {}
        );

        Driver.runToCompletion(randomExecutor(), List.of(driver1, driver2, driver3, driver4));
    }

    public void testOperatorsAsync() {
        Driver driver = new Driver(
            List.of(
                new RandomLongBlockSourceOperator(),
                new LongTransformerOperator(0, i -> i + 1),
                new LongGroupingOperator(1, BigArrays.NON_RECYCLING_INSTANCE),
                new LongMaxOperator(2),
                new PageConsumerOperator(page -> logger.info("New page: {}", page))
            ),
            () -> {}
        );

        while (driver.isFinished() == false) {
            logger.info("Run a couple of steps");
            driver.run(TimeValue.MAX_VALUE, 10);
        }
    }

    // Trivial test with small input
    public void testBasicAvgOperators() {
        AtomicInteger pageCount = new AtomicInteger();
        AtomicInteger rowCount = new AtomicInteger();
        AtomicReference<Page> lastPage = new AtomicReference<>();

        Driver driver = new Driver(
            List.of(
                new ListLongBlockSourceOperator(List.of(1L, 2L, 3L, 4L, 5L, 6L, 7L, 8L, 9L)),
                new LongAvgOperator(0), // partial reduction
                new LongAvgOperator(0, 1), // final reduction
                new PageConsumerOperator(page -> {
                    logger.info("New page: {}", page);
                    pageCount.incrementAndGet();
                    rowCount.addAndGet(page.getPositionCount());
                    lastPage.set(page);
                })
            ),
            () -> {}
        );
        driver.run();
        assertEquals(1, pageCount.get());
        assertEquals(1, rowCount.get());
        assertEquals(5, lastPage.get().getBlock(0).getLong(0));
    }

    // Trivial test with small input
    public void testBasicAvgGroupingOperators() {
        AtomicInteger pageCount = new AtomicInteger();
        AtomicInteger rowCount = new AtomicInteger();
        AtomicReference<Page> lastPage = new AtomicReference<>();

        var source = new LongTupleBlockSourceOperator(
            List.of(9L, 5L, 9L, 5L, 9L, 5L, 9L, 5L, 9L),  // groups
            List.of(1L, 1L, 2L, 1L, 3L, 1L, 4L, 1L, 5L)   // values
        );

        Driver driver = new Driver(
            List.of(
                source,
                new LongGroupingOperator(0, BigArrays.NON_RECYCLING_INSTANCE),
                new LongAvgGroupingOperator(1, 0),
                new PageConsumerOperator(page -> {
                    logger.info("New page: {}", page);
                    pageCount.incrementAndGet();
                    rowCount.addAndGet(page.getPositionCount());
                    lastPage.set(page);
                })
            ),
            () -> {}
        );
        driver.run();
        assertEquals(1, pageCount.get());
        assertEquals(2, rowCount.get());

        // expect [5 - avg1 , 9 - avg3] - groups (order agnostic)
        assertEquals(5, lastPage.get().getBlock(0).getLong(0));  // expect [5, 9] - order agnostic
        assertEquals(9, lastPage.get().getBlock(0).getLong(1));
        assertEquals(1, lastPage.get().getBlock(1).getLong(0));
        assertEquals(3, lastPage.get().getBlock(1).getLong(1));
    }

    /**
     * A source operator whose output is the given long values. This operator produces a single
     * Page with two Blocks. The first Block contains the long values from the first list, in order.
     * The second Block contains the long values from the second list, in order.
     */
    class LongTupleBlockSourceOperator implements Operator {

        private final List<Long> firstValues;
        private final List<Long> secondValues;

        LongTupleBlockSourceOperator(List<Long> firstValues, List<Long> secondValues) {
            assert firstValues.size() == secondValues.size();
            this.firstValues = firstValues;
            this.secondValues = secondValues;
        }

        boolean finished;

        @Override
        public Page getOutput() {
            // all in one page for now
            finished = true;
            LongBlock firstBlock = new LongBlock(firstValues.stream().mapToLong(Long::longValue).toArray(), firstValues.size());
            LongBlock secondBlock = new LongBlock(secondValues.stream().mapToLong(Long::longValue).toArray(), secondValues.size());
            return new Page(firstBlock, secondBlock);
        }

        @Override
        public void close() {}

        @Override
        public boolean isFinished() {
            return finished;
        }

        @Override
        public void finish() {
            finished = true;
        }

        @Override
        public boolean needsInput() {
            return false;
        }

        @Override
        public void addInput(Page page) {
            throw new UnsupportedOperationException();
        }
    }

    /**
     * A source operator whose output is the given long values. This operator produces a single
     * Page with a single Block. The Block contains the long values from the given list, in order.
     */
    class ListLongBlockSourceOperator implements Operator {

        private final List<Long> values;

        ListLongBlockSourceOperator(List<Long> values) {
            this.values = values;
        }

        boolean finished;

        @Override
        public Page getOutput() {
            // all in one page, for now
            finished = true;
            return new Page(new LongBlock(values.stream().mapToLong(Long::longValue).toArray(), values.size()));
        }

        @Override
        public void close() {}

        @Override
        public boolean isFinished() {
            return finished;
        }

        @Override
        public void finish() {
            finished = true;
        }

        @Override
        public boolean needsInput() {
            return false;
        }

        @Override
        public void addInput(Page page) {
            throw new UnsupportedOperationException();
        }
    }
}
