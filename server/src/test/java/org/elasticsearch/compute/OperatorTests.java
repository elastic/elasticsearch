/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.compute;

import org.apache.lucene.document.Document;
import org.apache.lucene.document.Field;
import org.apache.lucene.document.LongPoint;
import org.apache.lucene.document.NumericDocValuesField;
import org.apache.lucene.index.DirectoryReader;
import org.apache.lucene.index.IndexReader;
import org.apache.lucene.index.LeafReaderContext;
import org.apache.lucene.index.Term;
import org.apache.lucene.search.Collector;
import org.apache.lucene.search.IndexSearcher;
import org.apache.lucene.search.LeafCollector;
import org.apache.lucene.search.MatchAllDocsQuery;
import org.apache.lucene.search.Query;
import org.apache.lucene.search.Scorable;
import org.apache.lucene.search.ScoreMode;
import org.apache.lucene.store.Directory;
import org.apache.lucene.tests.index.RandomIndexWriter;
import org.apache.lucene.util.BytesRef;
import org.elasticsearch.common.util.BigArrays;
import org.elasticsearch.common.util.concurrent.ConcurrentCollections;
import org.elasticsearch.compute.aggregation.Aggregator;
import org.elasticsearch.compute.aggregation.AggregatorMode;
import org.elasticsearch.compute.aggregation.BlockHash;
import org.elasticsearch.compute.aggregation.GroupingAggregator;
import org.elasticsearch.compute.aggregation.GroupingAggregatorFunction;
import org.elasticsearch.compute.data.Block;
import org.elasticsearch.compute.data.LongArrayBlock;
import org.elasticsearch.compute.data.Page;
import org.elasticsearch.compute.lucene.LuceneSourceOperator;
import org.elasticsearch.compute.lucene.ValuesSourceReaderOperator;
import org.elasticsearch.compute.operator.AggregationOperator;
import org.elasticsearch.compute.operator.Driver;
import org.elasticsearch.compute.operator.EvalOperator;
import org.elasticsearch.compute.operator.FilterOperator;
import org.elasticsearch.compute.operator.HashAggregationOperator;
import org.elasticsearch.compute.operator.LimitOperator;
import org.elasticsearch.compute.operator.LongGroupingOperator;
import org.elasticsearch.compute.operator.LongMaxOperator;
import org.elasticsearch.compute.operator.LongTransformerOperator;
import org.elasticsearch.compute.operator.Operator;
import org.elasticsearch.compute.operator.PageConsumerOperator;
import org.elasticsearch.compute.operator.SourceOperator;
import org.elasticsearch.compute.operator.TopNOperator;
import org.elasticsearch.compute.operator.exchange.ExchangeSink;
import org.elasticsearch.compute.operator.exchange.ExchangeSinkOperator;
import org.elasticsearch.compute.operator.exchange.ExchangeSource;
import org.elasticsearch.compute.operator.exchange.ExchangeSourceOperator;
import org.elasticsearch.compute.operator.exchange.PassthroughExchanger;
import org.elasticsearch.compute.operator.exchange.RandomExchanger;
import org.elasticsearch.compute.operator.exchange.RandomUnionSourceOperator;
import org.elasticsearch.core.CheckedConsumer;
import org.elasticsearch.core.TimeValue;
import org.elasticsearch.core.Tuple;
import org.elasticsearch.index.fielddata.IndexNumericFieldData;
import org.elasticsearch.index.fielddata.plain.SortedNumericIndexFieldData;
import org.elasticsearch.index.mapper.KeywordFieldMapper;
import org.elasticsearch.index.mapper.Uid;
import org.elasticsearch.search.aggregations.support.CoreValuesSourceType;
import org.elasticsearch.search.aggregations.support.ValuesSource;
import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.threadpool.TestThreadPool;
import org.elasticsearch.threadpool.ThreadPool;
import org.junit.After;
import org.junit.Before;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Comparator;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.Executor;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.BiFunction;
import java.util.function.Function;
import java.util.function.Predicate;
import java.util.stream.IntStream;
import java.util.stream.LongStream;

import static java.util.stream.Collectors.toList;
import static java.util.stream.Collectors.toMap;
import static java.util.stream.Collectors.toSet;
import static org.elasticsearch.compute.aggregation.AggregatorFunctionProviders.avgDouble;
import static org.elasticsearch.compute.aggregation.AggregatorFunctionProviders.avgLong;
import static org.elasticsearch.compute.aggregation.AggregatorFunctionProviders.count;
import static org.elasticsearch.compute.aggregation.AggregatorFunctionProviders.max;
import static org.elasticsearch.compute.aggregation.AggregatorFunctionProviders.sum;
import static org.elasticsearch.compute.aggregation.AggregatorMode.FINAL;
import static org.elasticsearch.compute.aggregation.AggregatorMode.INITIAL;
import static org.elasticsearch.compute.aggregation.AggregatorMode.INTERMEDIATE;
import static org.elasticsearch.compute.aggregation.AggregatorMode.SINGLE;
import static org.hamcrest.Matchers.contains;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.hasSize;

@Experimental
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

    class RandomLongBlockSourceOperator extends SourceOperator {

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
            return new Page(new LongArrayBlock(array, array.length));
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
        public void close() {

        }
    }

    public void testOperators() {
        Driver driver = new Driver(
            new RandomLongBlockSourceOperator(),
            List.of(
                new LongTransformerOperator(0, i -> i + 1),
                new LongGroupingOperator(1, BigArrays.NON_RECYCLING_INSTANCE),
                new LongMaxOperator(2)
            ),
            new PageConsumerOperator(page -> logger.info("New page: {}", page)),
            () -> {}
        );
        driver.run();
    }

    public void testOperatorsWithLucene() throws IOException {
        final String fieldName = "value";
        final int numDocs = 100000;
        try (Directory dir = newDirectory(); RandomIndexWriter w = new RandomIndexWriter(random(), dir)) {
            Document doc = new Document();
            NumericDocValuesField docValuesField = new NumericDocValuesField(fieldName, 0);
            for (int i = 0; i < numDocs; i++) {
                doc.clear();
                docValuesField.setLongValue(i);
                doc.add(docValuesField);
                w.addDocument(doc);
            }
            w.commit();

            ValuesSource vs = new ValuesSource.Numeric.FieldData(
                new SortedNumericIndexFieldData(
                    fieldName,
                    IndexNumericFieldData.NumericType.LONG,
                    IndexNumericFieldData.NumericType.LONG.getValuesSourceType(),
                    null
                )
            );

            try (IndexReader reader = w.getReader()) {
                AtomicInteger pageCount = new AtomicInteger();
                AtomicInteger rowCount = new AtomicInteger();
                AtomicReference<Page> lastPage = new AtomicReference<>();

                // implements cardinality on value field
                Driver driver = new Driver(
                    new LuceneSourceOperator(reader, 0, new MatchAllDocsQuery()),
                    List.of(
                        new ValuesSourceReaderOperator(
                            List.of(CoreValuesSourceType.NUMERIC),
                            List.of(vs),
                            List.of(reader),
                            0,
                            1,
                            2,
                            fieldName
                        ),
                        new LongGroupingOperator(3, BigArrays.NON_RECYCLING_INSTANCE),
                        new LongMaxOperator(4), // returns highest group number
                        new LongTransformerOperator(0, i -> i + 1) // adds +1 to group number (which start with 0) to get group count
                    ),
                    new PageConsumerOperator(page -> {
                        logger.info("New page: {}", page);
                        pageCount.incrementAndGet();
                        rowCount.addAndGet(page.getPositionCount());
                        lastPage.set(page);
                    }),
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
        final String fieldName = "value";
        final int numDocs = 100000;
        try (Directory dir = newDirectory(); RandomIndexWriter w = new RandomIndexWriter(random(), dir)) {
            Document doc = new Document();
            NumericDocValuesField docValuesField = new NumericDocValuesField(fieldName, 0);
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

            ValuesSource vs = new ValuesSource.Numeric.FieldData(
                new SortedNumericIndexFieldData(
                    fieldName,
                    IndexNumericFieldData.NumericType.LONG,
                    IndexNumericFieldData.NumericType.LONG.getValuesSourceType(),
                    null
                )
            );

            try (IndexReader reader = w.getReader()) {
                AtomicInteger rowCount = new AtomicInteger();

                List<Driver> drivers = new ArrayList<>();
                for (LuceneSourceOperator luceneSourceOperator : new LuceneSourceOperator(reader, 0, new MatchAllDocsQuery()).docSlice(
                    randomIntBetween(1, 10)
                )) {
                    drivers.add(
                        new Driver(
                            luceneSourceOperator,
                            List.of(
                                new ValuesSourceReaderOperator(
                                    List.of(CoreValuesSourceType.NUMERIC),
                                    List.of(vs),
                                    List.of(reader),
                                    0,
                                    1,
                                    2,
                                    fieldName
                                )
                            ),
                            new PageConsumerOperator(page -> rowCount.addAndGet(page.getPositionCount())),
                            () -> {}
                        )
                    );
                }
                Driver.runToCompletion(threadPool.executor(ThreadPool.Names.SEARCH), drivers);
                assertEquals(numDocs, rowCount.get());
            }
        }
    }

    public void testQueryOperator() throws IOException {
        Map<BytesRef, Long> docs = new HashMap<>();
        CheckedConsumer<DirectoryReader, IOException> verifier = reader -> {
            final long from = randomBoolean() ? Long.MIN_VALUE : randomLongBetween(0, 10000);
            final long to = randomBoolean() ? Long.MAX_VALUE : randomLongBetween(from, from + 10000);
            final Query query = LongPoint.newRangeQuery("pt", from, to);
            final String partition = randomFrom("shard", "segment", "doc");
            final List<LuceneSourceOperator> queryOperators = switch (partition) {
                case "shard" -> List.of(new LuceneSourceOperator(reader, 0, query));
                case "segment" -> new LuceneSourceOperator(reader, 0, query).segmentSlice();
                case "doc" -> new LuceneSourceOperator(reader, 0, query).docSlice(randomIntBetween(1, 10));
                default -> throw new AssertionError("unknown partition [" + partition + "]");
            };
            List<Driver> drivers = new ArrayList<>();
            Set<Integer> actualDocIds = Collections.newSetFromMap(ConcurrentCollections.newConcurrentMap());
            for (LuceneSourceOperator queryOperator : queryOperators) {
                PageConsumerOperator docCollector = new PageConsumerOperator(page -> {
                    Block idBlock = page.getBlock(0);
                    Block segmentBlock = page.getBlock(1);
                    for (int i = 0; i < idBlock.getPositionCount(); i++) {
                        int docBase = reader.leaves().get(segmentBlock.getInt(i)).docBase;
                        int docId = docBase + idBlock.getInt(i);
                        assertTrue("duplicated docId=" + docId, actualDocIds.add(docId));
                    }
                });
                drivers.add(new Driver(queryOperator, List.of(), docCollector, () -> {}));
            }
            Driver.runToCompletion(threadPool.executor(ThreadPool.Names.SEARCH), drivers);
            Set<Integer> expectedDocIds = searchForDocIds(reader, query);
            assertThat("query=" + query + ", partition=" + partition, actualDocIds, equalTo(expectedDocIds));
        };

        try (Directory dir = newDirectory(); RandomIndexWriter w = new RandomIndexWriter(random(), dir)) {
            int numDocs = randomIntBetween(0, 10_000);
            for (int i = 0; i < numDocs; i++) {
                Document d = new Document();
                long point = randomLongBetween(0, 5000);
                d.add(new LongPoint("pt", point));
                BytesRef id = Uid.encodeId("id-" + randomIntBetween(0, 5000));
                d.add(new Field("id", id, KeywordFieldMapper.Defaults.FIELD_TYPE));
                if (docs.put(id, point) != null) {
                    w.updateDocument(new Term("id", id), d);
                } else {
                    w.addDocument(d);
                }
            }
            try (DirectoryReader reader = w.getReader()) {
                verifier.accept(reader);
            }
        }
    }

    public void testOperatorsWithPassthroughExchange() {
        ExchangeSource exchangeSource = new ExchangeSource();

        Driver driver1 = new Driver(
            new RandomLongBlockSourceOperator(),
            List.of(new LongTransformerOperator(0, i -> i + 1)),
            new ExchangeSinkOperator(
                new ExchangeSink(new PassthroughExchanger(exchangeSource, Integer.MAX_VALUE), sink -> exchangeSource.finish())
            ),
            () -> {}
        );

        Driver driver2 = new Driver(
            new ExchangeSourceOperator(exchangeSource),
            List.of(new LongGroupingOperator(1, BigArrays.NON_RECYCLING_INSTANCE)),
            new PageConsumerOperator(page -> logger.info("New page: {}", page)),
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
            new RandomLongBlockSourceOperator(),
            List.of(new LongTransformerOperator(0, i -> i + 1)),
            new ExchangeSinkOperator(
                new ExchangeSink(
                    new RandomExchanger(List.of(p -> exchangeSource1.addPage(p, () -> {}), p -> exchangeSource2.addPage(p, () -> {}))),
                    sink -> {
                        exchangeSource1.finish();
                        exchangeSource2.finish();
                    }
                )
            ),
            () -> {}
        );

        ExchangeSource exchangeSource3 = new ExchangeSource();
        ExchangeSource exchangeSource4 = new ExchangeSource();

        Driver driver2 = new Driver(
            new ExchangeSourceOperator(exchangeSource1),
            List.of(new LongGroupingOperator(1, BigArrays.NON_RECYCLING_INSTANCE)),
            new ExchangeSinkOperator(
                new ExchangeSink(new PassthroughExchanger(exchangeSource3, Integer.MAX_VALUE), s -> exchangeSource3.finish())
            ),
            () -> {}
        );

        Driver driver3 = new Driver(
            new ExchangeSourceOperator(exchangeSource2),
            List.of(new LongMaxOperator(1)),
            new ExchangeSinkOperator(
                new ExchangeSink(new PassthroughExchanger(exchangeSource4, Integer.MAX_VALUE), s -> exchangeSource4.finish())
            ),
            () -> {}
        );

        Driver driver4 = new Driver(
            new RandomUnionSourceOperator(List.of(exchangeSource3, exchangeSource4)),
            List.of(),
            new PageConsumerOperator(page -> logger.info("New page with #blocks: {}", page.getBlockCount())),
            () -> {}
        );

        Driver.runToCompletion(randomExecutor(), List.of(driver1, driver2, driver3, driver4));
    }

    public void testOperatorsAsync() {
        Driver driver = new Driver(
            new RandomLongBlockSourceOperator(),
            List.of(
                new LongTransformerOperator(0, i -> i + 1),
                new LongGroupingOperator(1, BigArrays.NON_RECYCLING_INSTANCE),
                new LongMaxOperator(2)
            ),
            new PageConsumerOperator(page -> logger.info("New page: {}", page)),
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
            source,
            List.of(
                new AggregationOperator(
                    List.of(
                        new Aggregator(avgDouble(), INITIAL, 0),
                        new Aggregator(avgLong(), INITIAL, 0),
                        new Aggregator(count(), INITIAL, 0),
                        new Aggregator(max(), INITIAL, 0),
                        new Aggregator(sum(), INITIAL, 0)
                    )
                ),
                new AggregationOperator(
                    List.of(
                        new Aggregator(avgDouble(), INTERMEDIATE, 0),
                        new Aggregator(avgLong(), INTERMEDIATE, 1),
                        new Aggregator(count(), INTERMEDIATE, 2),
                        new Aggregator(max(), INTERMEDIATE, 3),
                        new Aggregator(sum(), INTERMEDIATE, 4)
                    )
                ),
                new AggregationOperator(
                    List.of(
                        new Aggregator(avgDouble(), FINAL, 0),
                        new Aggregator(avgLong(), FINAL, 1),
                        new Aggregator(count(), FINAL, 2),
                        new Aggregator(max(), FINAL, 3),
                        new Aggregator(sum(), FINAL, 4)
                    )
                )
            ),
            new PageConsumerOperator(page -> {
                logger.info("New page: {}", page);
                pageCount.incrementAndGet();
                rowCount.addAndGet(page.getPositionCount());
                lastPage.set(page);
            }),
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
                partialAggregator = new Aggregator(avgDouble(), INITIAL, 0);
                partialAggregators.add(partialAggregator);
            }
            partialAggregator.processPage(inputPage);
        }
        List<Block> partialBlocks = partialAggregators.stream().map(Aggregator::evaluate).toList();

        Aggregator interAggregator = null;
        List<Aggregator> intermediateAggregators = new ArrayList<>();
        for (Block block : partialBlocks) {
            if (interAggregator == null || random().nextBoolean()) {
                interAggregator = new Aggregator(avgDouble(), INTERMEDIATE, 0);
                intermediateAggregators.add(interAggregator);
            }
            interAggregator.processPage(new Page(block));
        }
        List<Block> intermediateBlocks = intermediateAggregators.stream().map(Aggregator::evaluate).toList();

        var finalAggregator = new Aggregator(avgDouble(), FINAL, 0);
        intermediateBlocks.stream().forEach(b -> finalAggregator.processPage(new Page(b)));
        Block resultBlock = finalAggregator.evaluate();
        logger.info("resultBlock: " + resultBlock);
        assertEquals(49_999.5, resultBlock.getDouble(0), 0);
    }

    public void testOperatorsWithLuceneGroupingCount() throws IOException {
        final String fieldName = "value";
        final int numDocs = 100000;
        try (Directory dir = newDirectory(); RandomIndexWriter w = new RandomIndexWriter(random(), dir)) {
            Document doc = new Document();
            NumericDocValuesField docValuesField = new NumericDocValuesField(fieldName, 0);
            for (int i = 0; i < numDocs; i++) {
                doc.clear();
                docValuesField.setLongValue(i);
                doc.add(docValuesField);
                w.addDocument(doc);
            }
            w.commit();

            ValuesSource vs = new ValuesSource.Numeric.FieldData(
                new SortedNumericIndexFieldData(
                    fieldName,
                    IndexNumericFieldData.NumericType.LONG,
                    IndexNumericFieldData.NumericType.LONG.getValuesSourceType(),
                    null
                )
            );

            try (IndexReader reader = w.getReader()) {
                AtomicInteger pageCount = new AtomicInteger();
                AtomicInteger rowCount = new AtomicInteger();
                AtomicReference<Page> lastPage = new AtomicReference<>();

                // implements cardinality on value field
                Driver driver = new Driver(
                    new LuceneSourceOperator(reader, 0, new MatchAllDocsQuery()),
                    List.of(
                        new ValuesSourceReaderOperator(
                            List.of(CoreValuesSourceType.NUMERIC),
                            List.of(vs),
                            List.of(reader),
                            0,
                            1,
                            2,
                            fieldName
                        ),
                        new HashAggregationOperator(
                            3, // group by channel
                            List.of(new GroupingAggregator(GroupingAggregatorFunction.count, INITIAL, 3)),
                            BlockHash.newLongHash(BigArrays.NON_RECYCLING_INSTANCE)
                        ),
                        new HashAggregationOperator(
                            0, // group by channel
                            List.of(new GroupingAggregator(GroupingAggregatorFunction.count, INTERMEDIATE, 1)),
                            BlockHash.newLongHash(BigArrays.NON_RECYCLING_INSTANCE)
                        ),
                        new HashAggregationOperator(
                            0, // group by channel
                            List.of(new GroupingAggregator(GroupingAggregatorFunction.count, FINAL, 1)),
                            BlockHash.newLongHash(BigArrays.NON_RECYCLING_INSTANCE)
                        )
                    ),
                    new PageConsumerOperator(page -> {
                        logger.info("New page: {}", page);
                        pageCount.incrementAndGet();
                        rowCount.addAndGet(page.getPositionCount());
                        lastPage.set(page);
                    }),
                    () -> {}
                );
                driver.run();
                assertEquals(1, pageCount.get());
                assertEquals(2, lastPage.get().getBlockCount());
                assertEquals(numDocs, rowCount.get());
                Block valuesBlock = lastPage.get().getBlock(1);
                for (int i = 0; i < numDocs; i++) {
                    assertEquals(1, valuesBlock.getLong(i));
                }
            }
        }
    }

    // Tests that overflows throw during summation.
    public void testSumLongOverflow() {
        Operator source = new SequenceLongBlockSourceOperator(List.of(Long.MAX_VALUE, 1L), 2);
        List<Page> rawPages = drainSourceToPages(source);

        Aggregator aggregator = new Aggregator(sum(), SINGLE, 0);
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
    // @com.carrotsearch.randomizedtesting.annotations.Repeat(iterations = 10000)
    public void testBasicGroupingOperators() {
        AtomicInteger pageCount = new AtomicInteger();
        AtomicInteger rowCount = new AtomicInteger();
        AtomicReference<Page> lastPage = new AtomicReference<>();

        final int cardinality = 20;
        final long initialGroupId = 1_000L;
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
            source,
            List.of(
                new HashAggregationOperator(
                    0, // group by channel
                    List.of(
                        new GroupingAggregator(GroupingAggregatorFunction.avg, INITIAL, 1),
                        new GroupingAggregator(GroupingAggregatorFunction.max, INITIAL, 1),
                        new GroupingAggregator(GroupingAggregatorFunction.min, INITIAL, 1),
                        new GroupingAggregator(GroupingAggregatorFunction.sum, INITIAL, 1),
                        new GroupingAggregator(GroupingAggregatorFunction.count, INITIAL, 1)
                    ),
                    BlockHash.newLongHash(BigArrays.NON_RECYCLING_INSTANCE)
                ),
                new HashAggregationOperator(
                    0, // group by channel
                    List.of(
                        new GroupingAggregator(GroupingAggregatorFunction.avg, INTERMEDIATE, 1),
                        new GroupingAggregator(GroupingAggregatorFunction.max, INTERMEDIATE, 2),
                        new GroupingAggregator(GroupingAggregatorFunction.min, INTERMEDIATE, 3),
                        new GroupingAggregator(GroupingAggregatorFunction.sum, INTERMEDIATE, 4),
                        new GroupingAggregator(GroupingAggregatorFunction.count, INTERMEDIATE, 5)
                    ),
                    BlockHash.newLongHash(BigArrays.NON_RECYCLING_INSTANCE)
                ),
                new HashAggregationOperator(
                    0, // group by channel
                    List.of(
                        new GroupingAggregator(GroupingAggregatorFunction.avg, FINAL, 1),
                        new GroupingAggregator(GroupingAggregatorFunction.max, FINAL, 2),
                        new GroupingAggregator(GroupingAggregatorFunction.min, FINAL, 3),
                        new GroupingAggregator(GroupingAggregatorFunction.sum, FINAL, 4),
                        new GroupingAggregator(GroupingAggregatorFunction.count, FINAL, 5)
                    ),
                    BlockHash.newLongHash(BigArrays.NON_RECYCLING_INSTANCE)
                )
            ),
            new PageConsumerOperator(page -> {
                logger.info("New page: {}", page);
                pageCount.incrementAndGet();
                rowCount.addAndGet(page.getPositionCount());
                lastPage.set(page);
            }),
            () -> {}
        );
        driver.run();
        assertEquals(1, pageCount.get());
        assertEquals(cardinality, rowCount.get());
        assertEquals(6, lastPage.get().getBlockCount());

        final Block groupIdBlock = lastPage.get().getBlock(0);
        assertEquals(cardinality, groupIdBlock.getPositionCount());
        var expectedGroupIds = LongStream.range(initialGroupId, initialGroupId + cardinality).boxed().collect(toSet());
        var actualGroupIds = IntStream.range(0, groupIdBlock.getPositionCount()).mapToLong(groupIdBlock::getLong).boxed().collect(toSet());
        assertEquals(expectedGroupIds, actualGroupIds);

        // assert average
        final Block avgValuesBlock = lastPage.get().getBlock(1);
        assertEquals(cardinality, avgValuesBlock.getPositionCount());
        var expectedAvgValues = IntStream.range(0, cardinality).boxed().collect(toMap(i -> initialGroupId + i, i -> 49.5 + (i * 100)));
        var actualAvgValues = IntStream.range(0, cardinality).boxed().collect(toMap(groupIdBlock::getLong, avgValuesBlock::getDouble));
        assertEquals(expectedAvgValues, actualAvgValues);

        // assert max
        final Block maxValuesBlock = lastPage.get().getBlock(2);
        assertEquals(cardinality, maxValuesBlock.getPositionCount());
        var expectedMaxValues = IntStream.range(0, cardinality).boxed().collect(toMap(i -> initialGroupId + i, i -> 99.0 + (i * 100)));
        var actualMaxValues = IntStream.range(0, cardinality).boxed().collect(toMap(groupIdBlock::getLong, maxValuesBlock::getDouble));
        assertEquals(expectedMaxValues, actualMaxValues);

        // assert min
        final Block minValuesBlock = lastPage.get().getBlock(3);
        assertEquals(cardinality, minValuesBlock.getPositionCount());
        var expectedMinValues = IntStream.range(0, cardinality).boxed().collect(toMap(i -> initialGroupId + i, i -> i * 100d));
        var actualMinValues = IntStream.range(0, cardinality).boxed().collect(toMap(groupIdBlock::getLong, minValuesBlock::getDouble));
        assertEquals(expectedMinValues, actualMinValues);

        // assert sum
        final Block sumValuesBlock = lastPage.get().getBlock(4);
        assertEquals(cardinality, sumValuesBlock.getPositionCount());
        var expectedSumValues = IntStream.range(0, cardinality)
            .boxed()
            .collect(toMap(i -> initialGroupId + i, i -> (double) IntStream.range(i * 100, (i * 100) + 100).sum()));
        var actualSumValues = IntStream.range(0, cardinality).boxed().collect(toMap(groupIdBlock::getLong, sumValuesBlock::getDouble));
        assertEquals(expectedSumValues, actualSumValues);

        // assert count
        final Block countValuesBlock = lastPage.get().getBlock(5);
        assertEquals(cardinality, countValuesBlock.getPositionCount());
        var expectedCountValues = IntStream.range(0, cardinality).boxed().collect(toMap(i -> initialGroupId + i, i -> 100L));
        var actualCountValues = IntStream.range(0, cardinality).boxed().collect(toMap(groupIdBlock::getLong, countValuesBlock::getLong));
        assertEquals(expectedCountValues, actualCountValues);
    }

    // Tests grouping avg aggregations with multiple intermediate partial blocks.
    // @com.carrotsearch.randomizedtesting.annotations.Repeat(iterations = 10000)
    public void testGroupingIntermediateAvgOperators() {
        // expected values based on the group/value pairs described in testGroupingIntermediateOperators
        Function<Integer, Double> expectedValueGenerator = i -> 49.5 + (i * 100);
        testGroupingIntermediateOperators(GroupingAggregatorFunction.avg, expectedValueGenerator);
    }

    // Tests grouping max aggregations with multiple intermediate partial blocks.
    // @com.carrotsearch.randomizedtesting.annotations.Repeat(iterations = 10000)
    public void testGroupingIntermediateMaxOperators() {
        // expected values based on the group/value pairs described in testGroupingIntermediateOperators
        Function<Integer, Double> expectedValueGenerator = i -> (99.0 + (i * 100));
        testGroupingIntermediateOperators(GroupingAggregatorFunction.max, expectedValueGenerator);
    }

    // Tests grouping min aggregations with multiple intermediate partial blocks.
    // @com.carrotsearch.randomizedtesting.annotations.Repeat(iterations = 10000)
    public void testGroupingIntermediateMinOperators() {
        // expected values based on the group/value pairs described in testGroupingIntermediateOperators
        Function<Integer, Double> expectedValueGenerator = i -> i * 100d;
        testGroupingIntermediateOperators(GroupingAggregatorFunction.min, expectedValueGenerator);
    }

    // Tests grouping sum aggregations with multiple intermediate partial blocks.
    // @com.carrotsearch.randomizedtesting.annotations.Repeat(iterations = 10000)
    public void testGroupingIntermediateSumOperators() {
        // expected values based on the group/value pairs described in testGroupingIntermediateOperators
        Function<Integer, Double> expectedValueGenerator = i -> (double) IntStream.range(i * 100, (i * 100) + 100).sum();
        testGroupingIntermediateOperators(GroupingAggregatorFunction.sum, expectedValueGenerator);
    }

    public void testMaxOperatorsNegative() {
        AtomicInteger pageCount = new AtomicInteger();
        AtomicInteger rowCount = new AtomicInteger();
        AtomicReference<Page> lastPage = new AtomicReference<>();

        var rawValues = LongStream.rangeClosed(randomIntBetween(-100, -51), -50).boxed().collect(toList());
        // shuffling provides a basic level of randomness to otherwise quite boring data
        Collections.shuffle(rawValues, random());
        var source = new SequenceLongBlockSourceOperator(rawValues);

        Driver driver = new Driver(
            source,
            List.of(
                new AggregationOperator(List.of(new Aggregator(max(), INITIAL, 0))),
                new AggregationOperator(List.of(new Aggregator(max(), INTERMEDIATE, 0))),
                new AggregationOperator(List.of(new Aggregator(max(), FINAL, 0)))
            ),
            new PageConsumerOperator(page -> {
                logger.info("New page: {}", page);
                pageCount.incrementAndGet();
                rowCount.addAndGet(page.getPositionCount());
                lastPage.set(page);
            }),
            () -> {}
        );
        driver.run();
        assertEquals(1, pageCount.get());
        assertEquals(1, lastPage.get().getBlockCount());
        assertEquals(1, rowCount.get());
        // assert max
        assertEquals(-50, lastPage.get().getBlock(0).getDouble(0), 0.0);
    }

    // Tests grouping aggregations with multiple intermediate partial blocks.
    private void testGroupingIntermediateOperators(
        BiFunction<AggregatorMode, Integer, GroupingAggregatorFunction> aggFunction,
        Function<Integer, Double> expectedValueGenerator
    ) {
        final int cardinality = 13;
        final long initialGroupId = 100_000L;
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
        List<Page> rawPages = drainSourceToPages(source);

        HashAggregationOperator partialAggregatorOperator = null;
        List<Operator> partialAggregatorOperators = new ArrayList<>();
        for (Page inputPage : rawPages) {
            if (partialAggregatorOperator == null || random().nextBoolean()) {
                partialAggregatorOperator = new HashAggregationOperator(
                    0, // group by channel
                    List.of(new GroupingAggregator(aggFunction, INITIAL, 1)),
                    BlockHash.newLongHash(BigArrays.NON_RECYCLING_INSTANCE)
                );
                partialAggregatorOperators.add(partialAggregatorOperator);
            }
            partialAggregatorOperator.addInput(inputPage);
        }
        List<Page> partialPages = partialAggregatorOperators.stream().peek(Operator::finish).map(Operator::getOutput).toList();

        HashAggregationOperator interAggregatorOperator = null;
        List<Operator> interAggregatorOperators = new ArrayList<>();
        for (Page page : partialPages) {
            if (interAggregatorOperator == null || random().nextBoolean()) {
                interAggregatorOperator = new HashAggregationOperator(
                    0, // group by channel
                    List.of(new GroupingAggregator(aggFunction, INTERMEDIATE, 1)),
                    BlockHash.newLongHash(BigArrays.NON_RECYCLING_INSTANCE)
                );
                interAggregatorOperators.add(interAggregatorOperator);
            }
            interAggregatorOperator.addInput(page);
        }
        List<Page> intermediatePages = interAggregatorOperators.stream().peek(Operator::finish).map(Operator::getOutput).toList();

        HashAggregationOperator finalAggregationOperator = new HashAggregationOperator(
            0, // group by channel
            List.of(new GroupingAggregator(aggFunction, FINAL, 1)),
            BlockHash.newLongHash(BigArrays.NON_RECYCLING_INSTANCE)
        );
        intermediatePages.stream().forEach(finalAggregationOperator::addInput);
        finalAggregationOperator.finish();
        Page finalPage = finalAggregationOperator.getOutput();
        logger.info("Final page: {}", finalPage);

        assertEquals(cardinality, finalPage.getPositionCount());
        assertEquals(2, finalPage.getBlockCount());

        final Block groupIdBlock = finalPage.getBlock(0);
        assertEquals(cardinality, finalPage.getPositionCount());
        var expectedGroupIds = LongStream.range(initialGroupId, initialGroupId + cardinality).boxed().collect(toSet());
        var actualGroupIds = IntStream.range(0, groupIdBlock.getPositionCount()).mapToLong(groupIdBlock::getLong).boxed().collect(toSet());
        assertEquals(expectedGroupIds, actualGroupIds);

        final Block valuesBlock = finalPage.getBlock(1);
        assertEquals(cardinality, valuesBlock.getPositionCount());
        var expectedValues = IntStream.range(0, cardinality).boxed().collect(toMap(i -> initialGroupId + i, expectedValueGenerator));
        var actualValues = IntStream.range(0, cardinality).boxed().collect(toMap(groupIdBlock::getLong, valuesBlock::getDouble));
        assertEquals(expectedValues, actualValues);
    }

    public void testFilterOperator() {
        var positions = 1000;
        var values = randomList(positions, positions, ESTestCase::randomLong);
        Predicate<Long> condition = l -> l % 2 == 0;

        var results = new ArrayList<Long>();

        var driver = new Driver(
            new SequenceLongBlockSourceOperator(values),
            List.of(new FilterOperator((page, position) -> condition.test(page.getBlock(0).getLong(position)))),
            new PageConsumerOperator(page -> {
                Block block = page.getBlock(0);
                for (int i = 0; i < page.getPositionCount(); i++) {
                    results.add(block.getLong(i));
                }
            }),
            () -> {}
        );

        driver.run();

        assertThat(results, contains(values.stream().filter(condition).toArray()));
    }

    public void testFilterEvalFilter() {
        var positions = 1000;
        var values = randomList(positions, positions, ESTestCase::randomLong);
        Predicate<Long> condition1 = l -> l % 2 == 0;
        Function<Long, Long> transformation = l -> l + 1;
        Predicate<Long> condition2 = l -> l % 3 == 0;

        var results = new ArrayList<Tuple<Long, Long>>();

        var driver = new Driver(
            new SequenceLongBlockSourceOperator(values),
            List.of(
                new FilterOperator((page, position) -> condition1.test(page.getBlock(0).getLong(position))),
                new EvalOperator((page, position) -> transformation.apply(page.getBlock(0).getLong(position)), Long.TYPE),
                new FilterOperator((page, position) -> condition2.test(page.getBlock(1).getLong(position)))
            ),
            new PageConsumerOperator(page -> {
                Block block1 = page.getBlock(0);
                Block block2 = page.getBlock(1);
                for (int i = 0; i < page.getPositionCount(); i++) {
                    results.add(Tuple.tuple(block1.getLong(i), block2.getLong(i)));
                }
            }),
            () -> {}
        );

        driver.run();

        assertThat(
            results,
            contains(
                values.stream()
                    .filter(condition1)
                    .map(l -> Tuple.tuple(l, transformation.apply(l)))
                    .filter(t -> condition2.test(t.v2()))
                    .toArray()
            )
        );
    }

    public void testLimitOperator() {
        var positions = 100;
        var limit = randomIntBetween(90, 101);
        var values = randomList(positions, positions, ESTestCase::randomLong);

        var results = new ArrayList<Long>();

        var driver = new Driver(
            new SequenceLongBlockSourceOperator(values, 100),
            List.of(new LimitOperator(limit)),
            new PageConsumerOperator(page -> {
                Block block = page.getBlock(0);
                for (int i = 0; i < page.getPositionCount(); i++) {
                    results.add(block.getLong(i));
                }
            }),
            () -> {}
        );

        driver.run();

        assertThat(results, contains(values.stream().limit(limit).toArray()));
    }

    public void testRandomTopN() {
        for (boolean asc : List.of(true, false)) {
            int limit = randomIntBetween(1, 20);
            List<Long> inputValues = randomList(0, 5000, ESTestCase::randomLong);
            Comparator<Long> comparator = asc ? Comparator.naturalOrder() : Comparator.reverseOrder();
            List<Long> expectedValues = inputValues.stream().sorted(comparator).limit(limit).toList();
            List<Long> outputValues = topN(inputValues, limit, asc);
            assertThat(outputValues, equalTo(expectedValues));
        }
    }

    public void testBasicTopN() {
        List<Long> values = List.of(2L, 1L, 4L, 5L, 10L, 20L, 4L, 100L);
        assertThat(topN(values, 1, true), equalTo(List.of(1L)));
        assertThat(topN(values, 1, false), equalTo(List.of(100L)));
        assertThat(topN(values, 2, true), equalTo(List.of(1L, 2L)));
        assertThat(topN(values, 2, false), equalTo(List.of(100L, 20L)));
        assertThat(topN(values, 3, true), equalTo(List.of(1L, 2L, 4L)));
        assertThat(topN(values, 3, false), equalTo(List.of(100L, 20L, 10L)));
        assertThat(topN(values, 4, true), equalTo(List.of(1L, 2L, 4L, 4L)));
        assertThat(topN(values, 4, false), equalTo(List.of(100L, 20L, 10L, 5L)));
        assertThat(topN(values, 5, true), equalTo(List.of(1L, 2L, 4L, 4L, 5L)));
        assertThat(topN(values, 5, false), equalTo(List.of(100L, 20L, 10L, 5L, 4L)));
    }

    private List<Long> topN(List<Long> inputValues, int limit, boolean ascendingOrder) {
        List<Long> outputValues = new ArrayList<>();
        Driver driver = new Driver(
            new SequenceLongBlockSourceOperator(inputValues, randomIntBetween(1, 1000)),
            List.of(new TopNOperator(0, ascendingOrder, limit, true)),
            new PageConsumerOperator(page -> {
                Block block = page.getBlock(0);
                for (int i = 0; i < block.getPositionCount(); i++) {
                    outputValues.add(block.getLong(i));
                }
            }),
            () -> {}
        );
        driver.run();
        assertThat(outputValues, hasSize(Math.min(limit, inputValues.size())));
        return outputValues;
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
            return new Page(new LongArrayBlock(groupsBlock, length), new LongArrayBlock(valuesBlock, length));
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
            return new Page(new LongArrayBlock(array, array.length));
        }

        int remaining() {
            return values.length - currentPosition;
        }
    }

    /**
     * An abstract source operator. Implementations of this operator produce pages with a random
     * number of positions up to a maximum of the given maxPagePositions positions.
     */
    abstract class AbstractBlockSourceOperator extends SourceOperator {

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
    }

    private static Set<Integer> searchForDocIds(IndexReader reader, Query query) throws IOException {
        IndexSearcher searcher = new IndexSearcher(reader);
        Set<Integer> docIds = new HashSet<>();
        searcher.search(query, new Collector() {
            @Override
            public LeafCollector getLeafCollector(LeafReaderContext context) {
                return new LeafCollector() {
                    @Override
                    public void setScorer(Scorable scorer) {

                    }

                    @Override
                    public void collect(int doc) {
                        int docId = context.docBase + doc;
                        assertTrue(docIds.add(docId));
                    }
                };
            }

            @Override
            public ScoreMode scoreMode() {
                return ScoreMode.COMPLETE_NO_SCORES;
            }
        });
        return docIds;
    }
}
