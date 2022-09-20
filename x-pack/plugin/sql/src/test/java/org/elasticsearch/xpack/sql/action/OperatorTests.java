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
import org.elasticsearch.xpack.sql.action.compute.aggregation.Aggregator;
import org.elasticsearch.xpack.sql.action.compute.aggregation.AggregatorFunction;
import org.elasticsearch.xpack.sql.action.compute.aggregation.AggregatorMode;
import org.elasticsearch.xpack.sql.action.compute.aggregation.GroupingAggregator;
import org.elasticsearch.xpack.sql.action.compute.aggregation.GroupingAggregatorFunction;
import org.elasticsearch.xpack.sql.action.compute.data.Block;
import org.elasticsearch.xpack.sql.action.compute.data.LongBlock;
import org.elasticsearch.xpack.sql.action.compute.data.Page;
import org.elasticsearch.xpack.sql.action.compute.lucene.LuceneSourceOperator;
import org.elasticsearch.xpack.sql.action.compute.lucene.NumericDocValuesExtractor;
import org.elasticsearch.xpack.sql.action.compute.operator.AggregationOperator;
import org.elasticsearch.xpack.sql.action.compute.operator.Driver;
import org.elasticsearch.xpack.sql.action.compute.operator.HashAggregationOperator;
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
import java.util.Collections;
import java.util.List;
import java.util.concurrent.Executor;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;
import java.util.stream.IntStream;
import java.util.stream.LongStream;

import static java.util.stream.Collectors.toList;
import static java.util.stream.Collectors.toMap;
import static java.util.stream.Collectors.toSet;

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

    // Basic aggregator test with small(ish) input
    public void testBasicAggOperators() {
        AtomicInteger pageCount = new AtomicInteger();
        AtomicInteger rowCount = new AtomicInteger();
        AtomicReference<Page> lastPage = new AtomicReference<>();

        var rawValues = LongStream.range(0, 100_000).boxed().collect(toList());
        // shuffling provides a basic level of randomness to otherwise quite boring data
        Collections.shuffle(rawValues, random());
        var source = new SequenceLongBlockSourceOperator(rawValues);

        Driver driver = new Driver(
            List.of(
                source,
                new AggregationOperator(
                    List.of(
                        new Aggregator(AggregatorFunction.doubleAvg, AggregatorMode.INITIAL, 0),
                        new Aggregator(AggregatorFunction.longAvg, AggregatorMode.INITIAL, 0),
                        new Aggregator(AggregatorFunction.count, AggregatorMode.INITIAL, 0),
                        new Aggregator(AggregatorFunction.max, AggregatorMode.INITIAL, 0),
                        new Aggregator(AggregatorFunction.sum, AggregatorMode.INITIAL, 0)
                    )
                ),
                new AggregationOperator(
                    List.of(
                        new Aggregator(AggregatorFunction.doubleAvg, AggregatorMode.INTERMEDIATE, 0),
                        new Aggregator(AggregatorFunction.longAvg, AggregatorMode.INTERMEDIATE, 1),
                        new Aggregator(AggregatorFunction.count, AggregatorMode.INTERMEDIATE, 2),
                        new Aggregator(AggregatorFunction.max, AggregatorMode.INTERMEDIATE, 3),
                        new Aggregator(AggregatorFunction.sum, AggregatorMode.INTERMEDIATE, 4)
                    )
                ),
                new AggregationOperator(
                    List.of(
                        new Aggregator(AggregatorFunction.doubleAvg, AggregatorMode.FINAL, 0),
                        new Aggregator(AggregatorFunction.longAvg, AggregatorMode.FINAL, 1),
                        new Aggregator(AggregatorFunction.count, AggregatorMode.FINAL, 2),
                        new Aggregator(AggregatorFunction.max, AggregatorMode.FINAL, 3),
                        new Aggregator(AggregatorFunction.sum, AggregatorMode.FINAL, 4)
                    )
                ),
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
        // assert average
        assertEquals(49_999.5, lastPage.get().getBlock(0).getDouble(0), 0.0);
        // assert average
        assertEquals(49_999.5, lastPage.get().getBlock(1).getDouble(0), 0.0);
        // assert count
        assertEquals(100_000, lastPage.get().getBlock(2).getLong(0));
        // assert max
        assertEquals(99_999.0, lastPage.get().getBlock(3).getDouble(0), 0.0);
        // assert sum
        assertEquals(4.99995E9, lastPage.get().getBlock(4).getDouble(0), 0.0);
    }

    // Tests avg aggregators with multiple intermediate partial blocks.
    public void testIntermediateAvgOperators() {
        Operator source = new SequenceLongBlockSourceOperator(LongStream.range(0, 100_000).boxed().toList());
        List<Page> rawPages = drainSourceToPages(source);

        Aggregator partialAggregator = null;
        List<Aggregator> partialAggregators = new ArrayList<>();
        for (Page inputPage : rawPages) {
            if (partialAggregator == null || random().nextBoolean()) {
                partialAggregator = new Aggregator(AggregatorFunction.doubleAvg, AggregatorMode.INITIAL, 0);
                partialAggregators.add(partialAggregator);
            }
            partialAggregator.processPage(inputPage);
        }
        List<Block> partialBlocks = partialAggregators.stream().map(Aggregator::evaluate).toList();

        Aggregator interAggregator = null;
        List<Aggregator> intermediateAggregators = new ArrayList<>();
        for (Block block : partialBlocks) {
            if (interAggregator == null || random().nextBoolean()) {
                interAggregator = new Aggregator(AggregatorFunction.doubleAvg, AggregatorMode.INTERMEDIATE, 0);
                intermediateAggregators.add(interAggregator);
            }
            interAggregator.processPage(new Page(block));
        }
        List<Block> intermediateBlocks = intermediateAggregators.stream().map(Aggregator::evaluate).toList();

        var finalAggregator = new Aggregator(AggregatorFunction.doubleAvg, AggregatorMode.FINAL, 0);
        intermediateBlocks.stream().forEach(b -> finalAggregator.processPage(new Page(b)));
        Block resultBlock = finalAggregator.evaluate();
        assertEquals(49_999.5, resultBlock.getDouble(0), 0);
    }

    // Tests that overflows throw during summation.
    public void testSumLongOverflow() {
        Operator source = new SequenceLongBlockSourceOperator(List.of(Long.MAX_VALUE, 1L), 2);
        List<Page> rawPages = drainSourceToPages(source);

        Aggregator aggregator = new Aggregator(AggregatorFunction.sum, AggregatorMode.SINGLE, 0);
        logger.info(rawPages);
        ArithmeticException ex = expectThrows(ArithmeticException.class, () -> {
            for (Page page : rawPages) {
                // rawPages.forEach(aggregator::processPage);
                logger.info("processing page: {}", page);
                aggregator.processPage(page);
            }
        });
        assertTrue(ex.getMessage().contains("overflow"));
    }

    private static List<Page> drainSourceToPages(Operator source) {
        List<Page> rawPages = new ArrayList<>();
        Page page;
        while ((page = source.getOutput()) != null) {
            rawPages.add(page);
        }
        assert rawPages.size() > 0;
        // shuffling provides a basic level of randomness to otherwise quite boring data
        Collections.shuffle(rawPages, random());
        return rawPages;
    }

    /** Tuple of groupId and respective value. Both of which are of type long. */
    record LongGroupPair(long groupId, long value) {}

    // Basic test with small(ish) input
    public void testBasicAvgGroupingOperators() {
        AtomicInteger pageCount = new AtomicInteger();
        AtomicInteger rowCount = new AtomicInteger();
        AtomicReference<Page> lastPage = new AtomicReference<>();

        final int cardinality = 10;
        final long initialGroupId = 10_000L;
        final long initialValue = 0L;

        // create a list of group/value pairs. Each group has 100 monotonically increasing values.
        // Higher groupIds have higher sets of values, e.g. logical group1, values 0...99;
        // group2, values 100..199, etc. This way we can assert average values given the groupId.
        List<LongGroupPair> values = new ArrayList<>();
        long group = initialGroupId;
        long value = initialValue;
        for (int i = 0; i < cardinality; i++) {
            for (int j = 0; j < 100; j++) {
                values.add(new LongGroupPair(group, value++));
            }
            group++;
        }
        // shuffling provides a basic level of randomness to otherwise quite boring data
        Collections.shuffle(values, random());
        var source = new GroupPairBlockSourceOperator(values, 99);

        Driver driver = new Driver(
            List.of(
                source,
                new HashAggregationOperator(
                    0, // group by channel
                    List.of(new GroupingAggregator(GroupingAggregatorFunction.avg, AggregatorMode.SINGLE, 1)),
                    BigArrays.NON_RECYCLING_INSTANCE
                ),
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
        assertEquals(10, rowCount.get());
        assertEquals(2, lastPage.get().getBlockCount());

        final Block groupIdBlock = lastPage.get().getBlock(0);
        assertEquals(cardinality, groupIdBlock.getPositionCount());
        var expectedGroupIds = LongStream.range(initialGroupId, initialGroupId + cardinality).boxed().collect(toSet());
        var actualGroupIds = IntStream.range(0, groupIdBlock.getPositionCount()).mapToLong(groupIdBlock::getLong).boxed().collect(toSet());
        assertEquals(expectedGroupIds, actualGroupIds);

        final Block valuesBlock = lastPage.get().getBlock(1);
        assertEquals(cardinality, valuesBlock.getPositionCount());
        var expectedValues = IntStream.range(0, cardinality).boxed().collect(toMap(i -> initialGroupId + i, i -> 49.5 + (i * 100)));
        var actualValues = IntStream.range(0, cardinality).boxed().collect(toMap(groupIdBlock::getLong, valuesBlock::getDouble));
        assertEquals(expectedValues, actualValues);
    }

    /**
     * A source operator whose output is the given group tuple values. This operator produces pages
     * with two Blocks. The first Block contains the groupId long values. The second Block contains
     * the respective groupId {@link LongGroupPair#value()}. The returned pages preserve the order
     * of values as given in the in initial list.
     */
    class GroupPairBlockSourceOperator extends AbstractBlockSourceOperator {

        private static final int MAX_PAGE_POSITIONS = 8 * 1024;

        private final List<LongGroupPair> values;

        GroupPairBlockSourceOperator(List<LongGroupPair> values) {
            this(values, MAX_PAGE_POSITIONS);
        }

        GroupPairBlockSourceOperator(List<LongGroupPair> values, int maxPagePositions) {
            super(maxPagePositions);
            this.values = values;
        }

        @Override
        Page createPage(int positionOffset, int length) {
            final long[] groupsBlock = new long[length];
            final long[] valuesBlock = new long[length];
            for (int i = 0; i < length; i++) {
                LongGroupPair item = values.get(positionOffset + i);
                groupsBlock[i] = item.groupId();
                valuesBlock[i] = item.value();
            }
            currentPosition += length;
            return new Page(new LongBlock(groupsBlock, length), new LongBlock(valuesBlock, length));
        }

        @Override
        int remaining() {
            return values.size() - currentPosition;
        }
    }

    /**
     * A source operator whose output is the given long values. This operator produces pages
     * containing a single Block. The Block contains the long values from the given list, in order.
     */
    class SequenceLongBlockSourceOperator extends AbstractBlockSourceOperator {

        static final int MAX_PAGE_POSITIONS = 8 * 1024;

        private final long[] values;

        SequenceLongBlockSourceOperator(List<Long> values) {
            this(values, MAX_PAGE_POSITIONS);
        }

        SequenceLongBlockSourceOperator(List<Long> values, int maxPagePositions) {
            super(maxPagePositions);
            this.values = values.stream().mapToLong(Long::longValue).toArray();
        }

        protected Page createPage(int positionOffset, int length) {
            final long[] array = new long[length];
            for (int i = 0; i < length; i++) {
                array[i] = values[positionOffset + i];
            }
            currentPosition += length;
            return new Page(new LongBlock(array, array.length));
        }

        int remaining() {
            return values.length - currentPosition;
        }
    }

    /**
     * An abstract source operator. Implementations of this operator produce pages with a random
     * number of positions up to a maximum of the given maxPagePositions positions.
     */
    abstract class AbstractBlockSourceOperator implements Operator {

        boolean finished;

        /** The position of the next element to output. */
        int currentPosition;

        final int maxPagePositions;

        AbstractBlockSourceOperator(int maxPagePositions) {
            this.maxPagePositions = maxPagePositions;
        }

        /** The number of remaining elements that this source operator will produce. */
        abstract int remaining();

        /** Creates a page containing a block with {@code length} positions, from the given position offset. */
        abstract Page createPage(int positionOffset, int length);

        @Override
        public Page getOutput() {
            if (finished) {
                return null;
            }
            if (remaining() <= 0) {
                finish();
                return null;
            }
            int length = Math.min(randomInt(maxPagePositions), remaining());
            return createPage(currentPosition, length);
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
