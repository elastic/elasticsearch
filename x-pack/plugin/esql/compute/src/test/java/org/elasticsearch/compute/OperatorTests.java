/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.compute;

import org.apache.lucene.document.Document;
import org.apache.lucene.document.DoubleDocValuesField;
import org.apache.lucene.document.Field;
import org.apache.lucene.document.LongPoint;
import org.apache.lucene.document.NumericDocValuesField;
import org.apache.lucene.document.SortedDocValuesField;
import org.apache.lucene.document.SortedSetDocValuesField;
import org.apache.lucene.index.DirectoryReader;
import org.apache.lucene.index.IndexReader;
import org.apache.lucene.index.LeafReaderContext;
import org.apache.lucene.index.SortedSetDocValues;
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
import org.apache.lucene.tests.store.BaseDirectoryWrapper;
import org.apache.lucene.util.BytesRef;
import org.elasticsearch.action.support.ListenableActionFuture;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.util.BigArrays;
import org.elasticsearch.common.util.MockBigArrays;
import org.elasticsearch.common.util.MockPageCacheRecycler;
import org.elasticsearch.common.util.concurrent.ConcurrentCollections;
import org.elasticsearch.compute.aggregation.BlockHash;
import org.elasticsearch.compute.aggregation.GroupingAggregator;
import org.elasticsearch.compute.aggregation.GroupingAggregatorFunction;
import org.elasticsearch.compute.ann.Experimental;
import org.elasticsearch.compute.data.Block;
import org.elasticsearch.compute.data.BlockBuilder;
import org.elasticsearch.compute.data.Page;
import org.elasticsearch.compute.lucene.LuceneDocRef;
import org.elasticsearch.compute.lucene.LuceneSourceOperator;
import org.elasticsearch.compute.lucene.ValueSourceInfo;
import org.elasticsearch.compute.lucene.ValuesSourceReaderOperator;
import org.elasticsearch.compute.operator.Driver;
import org.elasticsearch.compute.operator.EvalOperator;
import org.elasticsearch.compute.operator.FilterOperator;
import org.elasticsearch.compute.operator.HashAggregationOperator;
import org.elasticsearch.compute.operator.LimitOperator;
import org.elasticsearch.compute.operator.LongGroupingOperator;
import org.elasticsearch.compute.operator.LongMaxOperator;
import org.elasticsearch.compute.operator.LongTransformerOperator;
import org.elasticsearch.compute.operator.Operator;
import org.elasticsearch.compute.operator.OrdinalsGroupingOperator;
import org.elasticsearch.compute.operator.PageConsumerOperator;
import org.elasticsearch.compute.operator.SequenceLongBlockSourceOperator;
import org.elasticsearch.compute.operator.SourceOperator;
import org.elasticsearch.compute.operator.TopNOperator;
import org.elasticsearch.compute.operator.TopNOperator.SortOrder;
import org.elasticsearch.compute.operator.TupleBlockSourceOperator;
import org.elasticsearch.compute.operator.exchange.ExchangeSink;
import org.elasticsearch.compute.operator.exchange.ExchangeSinkOperator;
import org.elasticsearch.compute.operator.exchange.ExchangeSource;
import org.elasticsearch.compute.operator.exchange.ExchangeSourceOperator;
import org.elasticsearch.compute.operator.exchange.PassthroughExchanger;
import org.elasticsearch.compute.operator.exchange.RandomExchanger;
import org.elasticsearch.compute.operator.exchange.RandomUnionSourceOperator;
import org.elasticsearch.core.CheckedConsumer;
import org.elasticsearch.core.Releasables;
import org.elasticsearch.core.TimeValue;
import org.elasticsearch.core.Tuple;
import org.elasticsearch.index.fielddata.IndexFieldDataCache;
import org.elasticsearch.index.fielddata.IndexNumericFieldData;
import org.elasticsearch.index.fielddata.SortedBinaryDocValues;
import org.elasticsearch.index.fielddata.plain.SortedDoublesIndexFieldData;
import org.elasticsearch.index.fielddata.plain.SortedNumericIndexFieldData;
import org.elasticsearch.index.fielddata.plain.SortedSetOrdinalsIndexFieldData;
import org.elasticsearch.index.mapper.KeywordFieldMapper;
import org.elasticsearch.index.mapper.Uid;
import org.elasticsearch.indices.breaker.NoneCircuitBreakerService;
import org.elasticsearch.search.aggregations.support.CoreValuesSourceType;
import org.elasticsearch.search.aggregations.support.ValuesSource;
import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.threadpool.TestThreadPool;
import org.elasticsearch.threadpool.ThreadPool;
import org.junit.After;
import org.junit.Before;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
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
import java.util.function.Function;
import java.util.function.LongUnaryOperator;
import java.util.function.Predicate;

import static org.elasticsearch.compute.aggregation.AggregatorMode.FINAL;
import static org.elasticsearch.compute.aggregation.AggregatorMode.INITIAL;
import static org.elasticsearch.compute.aggregation.AggregatorMode.INTERMEDIATE;
import static org.elasticsearch.core.Tuple.tuple;
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
            BlockBuilder blockBuilder = BlockBuilder.newLongBlockBuilder(size);
            for (int i = 0; i < size; i++) {
                blockBuilder.appendLong(randomLongBetween(0, 5));
            }
            return new Page(blockBuilder.build());
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
        try (
            Driver driver = new Driver(
                new RandomLongBlockSourceOperator(),
                List.of(new LongTransformerOperator(0, i -> i + 1), new LongGroupingOperator(1, bigArrays()), new LongMaxOperator(2)),
                new PageConsumerOperator(page -> logger.info("New page: {}", page)),
                () -> {}
            )
        ) {
            driver.run();
        }
    }

    public void testOperatorsWithLucene() throws IOException {
        BigArrays bigArrays = bigArrays();
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
                try (
                    Driver driver = new Driver(
                        new LuceneSourceOperator(reader, 0, new MatchAllDocsQuery()),
                        List.of(
                            new ValuesSourceReaderOperator(
                                List.of(new ValueSourceInfo(CoreValuesSourceType.NUMERIC, vs, reader)),
                                new LuceneDocRef(0, 1, 2)
                            ),
                            new LongGroupingOperator(3, bigArrays),
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
                    )
                ) {
                    driver.run();
                }
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
                try {
                    for (LuceneSourceOperator luceneSourceOperator : new LuceneSourceOperator(reader, 0, new MatchAllDocsQuery()).docSlice(
                        randomIntBetween(1, 10)
                    )) {
                        drivers.add(
                            new Driver(
                                luceneSourceOperator,
                                List.of(
                                    new ValuesSourceReaderOperator(
                                        List.of(new ValueSourceInfo(CoreValuesSourceType.NUMERIC, vs, reader)),
                                        new LuceneDocRef(0, 1, 2)
                                    )
                                ),
                                new PageConsumerOperator(page -> rowCount.addAndGet(page.getPositionCount())),
                                () -> {}
                            )
                        );
                    }
                    runToCompletion(threadPool.executor(ThreadPool.Names.SEARCH), drivers);
                } finally {
                    Releasables.close(drivers);
                }
                assertEquals(numDocs, rowCount.get());
            }
        }
    }

    public void testValuesSourceReaderOperatorWithLNulls() throws IOException {
        final int numDocs = 100_000;
        try (Directory dir = newDirectory(); RandomIndexWriter w = new RandomIndexWriter(random(), dir)) {
            Document doc = new Document();
            NumericDocValuesField intField = new NumericDocValuesField("i", 0);
            NumericDocValuesField longField = new NumericDocValuesField("j", 0);
            NumericDocValuesField doubleField = new DoubleDocValuesField("d", 0);
            String kwFieldName = "kw";
            for (int i = 0; i < numDocs; i++) {
                doc.clear();
                intField.setLongValue(i);
                doc.add(intField);
                if (i % 100 != 0) { // Do not set field for every 100 values
                    longField.setLongValue(i);
                    doc.add(longField);
                    doubleField.setDoubleValue(i);
                    doc.add(doubleField);
                    doc.add(new SortedDocValuesField(kwFieldName, new BytesRef("kw=" + i)));
                }
                w.addDocument(doc);
            }
            w.commit();

            ValuesSource intVs = new ValuesSource.Numeric.FieldData(
                new SortedNumericIndexFieldData(
                    intField.name(),
                    IndexNumericFieldData.NumericType.INT,
                    IndexNumericFieldData.NumericType.INT.getValuesSourceType(),
                    null
                )
            );
            ValuesSource longVs = new ValuesSource.Numeric.FieldData(
                new SortedNumericIndexFieldData(
                    longField.name(),
                    IndexNumericFieldData.NumericType.LONG,
                    IndexNumericFieldData.NumericType.LONG.getValuesSourceType(),
                    null
                )
            );
            ValuesSource doubleVs = new ValuesSource.Numeric.FieldData(
                new SortedDoublesIndexFieldData(
                    doubleField.name(),
                    IndexNumericFieldData.NumericType.DOUBLE,
                    IndexNumericFieldData.NumericType.DOUBLE.getValuesSourceType(),
                    null
                )
            );
            var breakerService = new NoneCircuitBreakerService();
            var cache = new IndexFieldDataCache.None();
            ValuesSource keywordVs = new ValuesSource.Bytes.FieldData(
                new SortedSetOrdinalsIndexFieldData(cache, kwFieldName, CoreValuesSourceType.KEYWORD, breakerService, null)
            );

            try (IndexReader reader = w.getReader()) {
                // implements cardinality on value field
                var luceneDocRef = new LuceneDocRef(0, 1, 2);
                Driver driver = new Driver(
                    new LuceneSourceOperator(reader, 0, new MatchAllDocsQuery()),
                    List.of(
                        new ValuesSourceReaderOperator(
                            List.of(new ValueSourceInfo(CoreValuesSourceType.NUMERIC, intVs, reader)),
                            luceneDocRef
                        ),
                        new ValuesSourceReaderOperator(
                            List.of(new ValueSourceInfo(CoreValuesSourceType.NUMERIC, longVs, reader)),
                            luceneDocRef
                        ),
                        new ValuesSourceReaderOperator(
                            List.of(new ValueSourceInfo(CoreValuesSourceType.NUMERIC, doubleVs, reader)),
                            luceneDocRef
                        ),
                        new ValuesSourceReaderOperator(
                            List.of(new ValueSourceInfo(CoreValuesSourceType.KEYWORD, keywordVs, reader)),
                            luceneDocRef
                        )
                    ),
                    new PageConsumerOperator(page -> {
                        logger.debug("New page: {}", page);
                        Block intValuesBlock = page.getBlock(3);
                        Block longValuesBlock = page.getBlock(4);
                        Block doubleValuesBlock = page.getBlock(5);
                        Block keywordValuesBlock = page.getBlock(6);

                        for (int i = 0; i < page.getPositionCount(); i++) {
                            assertFalse(intValuesBlock.isNull(i));
                            long j = intValuesBlock.getLong(i);
                            // Every 100 documents we set fields to null
                            boolean fieldIsEmpty = j % 100 == 0;
                            assertEquals(fieldIsEmpty, longValuesBlock.isNull(i));
                            assertEquals(fieldIsEmpty, doubleValuesBlock.isNull(i));
                            assertEquals(fieldIsEmpty, keywordValuesBlock.isNull(i));
                        }
                    }),
                    () -> {}
                );
                driver.run();
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
            try {
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
                runToCompletion(threadPool.executor(ThreadPool.Names.SEARCH), drivers);
                Set<Integer> expectedDocIds = searchForDocIds(reader, query);
                assertThat("query=" + query + ", partition=" + partition, actualDocIds, equalTo(expectedDocIds));
            } finally {
                Releasables.close(drivers);
            }
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
        BigArrays bigArrays = bigArrays();
        ExchangeSource exchangeSource = new ExchangeSource();

        try (
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
                List.of(new LongGroupingOperator(1, bigArrays)),
                new PageConsumerOperator(page -> logger.info("New page: {}", page)),
                () -> {}
            )
        ) {
            runToCompletion(randomExecutor(), List.of(driver1, driver2));
            // TODO where is the assertion here?
        }
    }

    private Executor randomExecutor() {
        return threadPool.executor(randomFrom(ThreadPool.Names.SAME, ThreadPool.Names.GENERIC, ThreadPool.Names.SEARCH));
    }

    public void testOperatorsWithRandomExchange() {
        BigArrays bigArrays = bigArrays();
        ExchangeSource exchangeSource1 = new ExchangeSource();
        ExchangeSource exchangeSource2 = new ExchangeSource();
        ExchangeSource exchangeSource3 = new ExchangeSource();
        ExchangeSource exchangeSource4 = new ExchangeSource();

        try (
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
            Driver driver2 = new Driver(
                new ExchangeSourceOperator(exchangeSource1),
                List.of(new LongGroupingOperator(1, bigArrays)),
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
            )
        ) {
            runToCompletion(randomExecutor(), List.of(driver1, driver2, driver3, driver4));
        }
    }

    public void testOperatorsAsync() {
        try (
            Driver driver = new Driver(
                new RandomLongBlockSourceOperator(),
                List.of(new LongTransformerOperator(0, i -> i + 1), new LongGroupingOperator(1, bigArrays()), new LongMaxOperator(2)),
                new PageConsumerOperator(page -> logger.info("New page: {}", page)),
                () -> {}
            )
        ) {
            while (driver.isFinished() == false) {
                logger.info("Run a couple of steps");
                driver.run(TimeValue.MAX_VALUE, 10);
            }
            // TODO is the assertion that it finishes?
        }
    }

    public void testOperatorsWithLuceneGroupingCount() throws IOException {
        BigArrays bigArrays = bigArrays();
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
                try (
                    Driver driver = new Driver(
                        new LuceneSourceOperator(reader, 0, new MatchAllDocsQuery()),
                        List.of(
                            new ValuesSourceReaderOperator(
                                List.of(new ValueSourceInfo(CoreValuesSourceType.NUMERIC, vs, reader)),
                                new LuceneDocRef(0, 1, 2)
                            ),
                            new HashAggregationOperator(
                                3, // group by channel
                                List.of(
                                    new GroupingAggregator.GroupingAggregatorFactory(
                                        bigArrays,
                                        GroupingAggregatorFunction.COUNT,
                                        INITIAL,
                                        3
                                    )
                                ),
                                () -> BlockHash.newLongHash(bigArrays)
                            ),
                            new HashAggregationOperator(
                                0, // group by channel
                                List.of(
                                    new GroupingAggregator.GroupingAggregatorFactory(
                                        bigArrays,
                                        GroupingAggregatorFunction.COUNT,
                                        INTERMEDIATE,
                                        1
                                    )
                                ),
                                () -> BlockHash.newLongHash(bigArrays)
                            ),
                            new HashAggregationOperator(
                                0, // group by channel
                                List.of(
                                    new GroupingAggregator.GroupingAggregatorFactory(bigArrays, GroupingAggregatorFunction.COUNT, FINAL, 1)
                                ),
                                () -> BlockHash.newLongHash(bigArrays)
                            )
                        ),
                        new PageConsumerOperator(page -> {
                            logger.info("New page: {}", page);
                            pageCount.incrementAndGet();
                            rowCount.addAndGet(page.getPositionCount());
                            lastPage.set(page);
                        }),
                        () -> {}
                    )
                ) {
                    driver.run();
                }
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

    public void testGroupingWithOrdinals() throws IOException {
        final String gField = "g";
        final int numDocs = between(100, 10000);
        final Map<BytesRef, Long> expectedCounts = new HashMap<>();
        int keyLength = randomIntBetween(1, 10);
        try (BaseDirectoryWrapper dir = newDirectory(); RandomIndexWriter writer = new RandomIndexWriter(random(), dir)) {
            for (int i = 0; i < numDocs; i++) {
                Document doc = new Document();
                BytesRef key = new BytesRef(randomByteArrayOfLength(keyLength));
                SortedSetDocValuesField docValuesField = new SortedSetDocValuesField(gField, key);
                doc.add(docValuesField);
                writer.addDocument(doc);
                expectedCounts.compute(key, (k, v) -> v == null ? 1 : v + 1);
            }
            writer.commit();
            Map<BytesRef, Long> actualCounts = new HashMap<>();
            BigArrays bigArrays = bigArrays();
            try (DirectoryReader reader = writer.getReader()) {
                Driver driver = new Driver(
                    new LuceneSourceOperator(reader, 0, new MatchAllDocsQuery()),
                    List.of(
                        new MapPageOperator(p -> p.appendBlock(BlockBuilder.newConstantIntBlockWith(1, p.getPositionCount()))),
                        new OrdinalsGroupingOperator(
                            List.of(
                                new ValueSourceInfo(
                                    CoreValuesSourceType.KEYWORD,
                                    randomBoolean() ? getOrdinalsValuesSource(gField) : getBytesValuesSource(gField),
                                    reader
                                )
                            ),
                            new LuceneDocRef(0, 1, 2),
                            List.of(
                                new GroupingAggregator.GroupingAggregatorFactory(bigArrays, GroupingAggregatorFunction.COUNT, INITIAL, 3)
                            ),
                            bigArrays
                        ),
                        new HashAggregationOperator(
                            0, // group by channel
                            List.of(
                                new GroupingAggregator.GroupingAggregatorFactory(bigArrays, GroupingAggregatorFunction.COUNT, FINAL, 1)
                            ),
                            () -> BlockHash.newBytesRefHash(bigArrays)
                        )
                    ),
                    new PageConsumerOperator(page -> {
                        Block keys = page.getBlock(0);
                        Block counts = page.getBlock(1);
                        for (int i = 0; i < keys.getPositionCount(); i++) {
                            BytesRef spare = new BytesRef();
                            actualCounts.put(keys.getBytesRef(i, spare), counts.getLong(i));
                        }
                    }),
                    () -> {}
                );
                driver.run();
                assertThat(actualCounts, equalTo(expectedCounts));
            }
        }
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

    public void testFilterOperator() {
        var positions = 1000;
        var values = randomList(positions, positions, ESTestCase::randomLong);
        Predicate<Long> condition = l -> l % 2 == 0;

        var results = new ArrayList<Long>();

        try (
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
            )
        ) {
            driver.run();
        }

        assertThat(results, contains(values.stream().filter(condition).toArray()));
    }

    public void testFilterEvalFilter() {
        var positions = 1000;
        var values = randomList(positions, positions, ESTestCase::randomLong);
        Predicate<Long> condition1 = l -> l % 2 == 0;
        Function<Long, Long> transformation = l -> l + 1;
        Predicate<Long> condition2 = l -> l % 3 == 0;

        var results = new ArrayList<Tuple<Long, Long>>();

        try (
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
                        results.add(tuple(block1.getLong(i), block2.getLong(i)));
                    }
                }),
                () -> {}
            )
        ) {
            driver.run();
        }

        assertThat(
            results,
            contains(
                values.stream()
                    .filter(condition1)
                    .map(l -> tuple(l, transformation.apply(l)))
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

        try (
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
            )
        ) {
            driver.run();
        }

        assertThat(results, contains(values.stream().limit(limit).toArray()));
    }

    public void testRandomTopN() {
        for (boolean asc : List.of(true, false)) {
            int limit = randomIntBetween(1, 20);
            List<Long> inputValues = randomList(0, 5000, ESTestCase::randomLong);
            Comparator<Long> comparator = asc ? Comparator.naturalOrder() : Comparator.reverseOrder();
            List<Long> expectedValues = inputValues.stream().sorted(comparator).limit(limit).toList();
            List<Long> outputValues = topN(inputValues, limit, asc, false);
            assertThat(outputValues, equalTo(expectedValues));
        }
    }

    public void testBasicTopN() {
        List<Long> values = Arrays.asList(2L, 1L, 4L, null, 5L, 10L, null, 20L, 4L, 100L);
        assertThat(topN(values, 1, true, false), equalTo(Arrays.asList(1L)));
        assertThat(topN(values, 1, false, false), equalTo(Arrays.asList(100L)));
        assertThat(topN(values, 2, true, false), equalTo(Arrays.asList(1L, 2L)));
        assertThat(topN(values, 2, false, false), equalTo(Arrays.asList(100L, 20L)));
        assertThat(topN(values, 3, true, false), equalTo(Arrays.asList(1L, 2L, 4L)));
        assertThat(topN(values, 3, false, false), equalTo(Arrays.asList(100L, 20L, 10L)));
        assertThat(topN(values, 4, true, false), equalTo(Arrays.asList(1L, 2L, 4L, 4L)));
        assertThat(topN(values, 4, false, false), equalTo(Arrays.asList(100L, 20L, 10L, 5L)));
        assertThat(topN(values, 100, true, false), equalTo(Arrays.asList(1L, 2L, 4L, 4L, 5L, 10L, 20L, 100L, null, null)));
        assertThat(topN(values, 100, false, false), equalTo(Arrays.asList(100L, 20L, 10L, 5L, 4L, 4L, 2L, 1L, null, null)));
        assertThat(topN(values, 1, true, true), equalTo(Arrays.asList(new Long[] { null })));
        assertThat(topN(values, 1, false, true), equalTo(Arrays.asList(new Long[] { null })));
        assertThat(topN(values, 2, true, true), equalTo(Arrays.asList(null, null)));
        assertThat(topN(values, 2, false, true), equalTo(Arrays.asList(null, null)));
        assertThat(topN(values, 3, true, true), equalTo(Arrays.asList(null, null, 1L)));
        assertThat(topN(values, 3, false, true), equalTo(Arrays.asList(null, null, 100L)));
        assertThat(topN(values, 4, true, true), equalTo(Arrays.asList(null, null, 1L, 2L)));
        assertThat(topN(values, 4, false, true), equalTo(Arrays.asList(null, null, 100L, 20L)));
        assertThat(topN(values, 100, true, true), equalTo(Arrays.asList(null, null, 1L, 2L, 4L, 4L, 5L, 10L, 20L, 100L)));
        assertThat(topN(values, 100, false, true), equalTo(Arrays.asList(null, null, 100L, 20L, 10L, 5L, 4L, 4L, 2L, 1L)));
    }

    private List<Long> topN(List<Long> inputValues, int limit, boolean ascendingOrder, boolean nullsFirst) {
        return topNTwoColumns(
            inputValues.stream().map(v -> tuple(v, 0L)).toList(),
            limit,
            List.of(new SortOrder(0, ascendingOrder, nullsFirst))
        ).stream().map(Tuple::v1).toList();
    }

    public void testTopNTwoColumns() {
        List<Tuple<Long, Long>> values = Arrays.asList(tuple(1L, 1L), tuple(1L, 2L), tuple(null, null), tuple(null, 1L), tuple(1L, null));
        assertThat(
            topNTwoColumns(values, 5, List.of(new SortOrder(0, true, false), new SortOrder(1, true, false))),
            equalTo(List.of(tuple(1L, 1L), tuple(1L, 2L), tuple(1L, null), tuple(null, 1L), tuple(null, null)))
        );
        assertThat(
            topNTwoColumns(values, 5, List.of(new SortOrder(0, true, true), new SortOrder(1, true, false))),
            equalTo(List.of(tuple(null, 1L), tuple(null, null), tuple(1L, 1L), tuple(1L, 2L), tuple(1L, null)))
        );
        assertThat(
            topNTwoColumns(values, 5, List.of(new SortOrder(0, true, false), new SortOrder(1, true, true))),
            equalTo(List.of(tuple(1L, null), tuple(1L, 1L), tuple(1L, 2L), tuple(null, null), tuple(null, 1L)))
        );
    }

    private List<Tuple<Long, Long>> topNTwoColumns(List<Tuple<Long, Long>> inputValues, int limit, List<SortOrder> sortOrders) {
        List<Tuple<Long, Long>> outputValues = new ArrayList<>();
        try (
            Driver driver = new Driver(
                new TupleBlockSourceOperator(inputValues, randomIntBetween(1, 1000)),
                List.of(new TopNOperator(limit, sortOrders)),
                new PageConsumerOperator(page -> {
                    Block block1 = page.getBlock(0);
                    Block block2 = page.getBlock(1);
                    for (int i = 0; i < block1.getPositionCount(); i++) {
                        outputValues.add(tuple(block1.isNull(i) ? null : block1.getLong(i), block2.isNull(i) ? null : block2.getLong(i)));
                    }
                }),
                () -> {}
            )
        ) {
            driver.run();
        }
        assertThat(outputValues, hasSize(Math.min(limit, inputValues.size())));
        return outputValues;
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

    static ValuesSource.Bytes.WithOrdinals getOrdinalsValuesSource(String field) {
        return new ValuesSource.Bytes.WithOrdinals() {
            @Override
            public SortedBinaryDocValues bytesValues(LeafReaderContext context) {
                throw new UnsupportedOperationException();
            }

            @Override
            public SortedSetDocValues ordinalsValues(LeafReaderContext context) throws IOException {
                return context.reader().getSortedSetDocValues(field);
            }

            @Override
            public SortedSetDocValues globalOrdinalsValues(LeafReaderContext context) {
                throw new UnsupportedOperationException();
            }

            @Override
            public boolean supportsGlobalOrdinalsMapping() {
                throw new UnsupportedOperationException();
            }

            @Override
            public LongUnaryOperator globalOrdinalsMapping(LeafReaderContext context) {
                throw new UnsupportedOperationException();
            }
        };
    }

    static ValuesSource.Bytes getBytesValuesSource(String field) {
        return new ValuesSource.Bytes() {
            @Override
            public SortedBinaryDocValues bytesValues(LeafReaderContext context) throws IOException {
                SortedSetDocValues dv = context.reader().getSortedSetDocValues(field);
                return new SortedBinaryDocValues() {
                    @Override
                    public boolean advanceExact(int doc) throws IOException {
                        return dv.advanceExact(doc);
                    }

                    @Override
                    public int docValueCount() {
                        return dv.docValueCount();
                    }

                    @Override
                    public BytesRef nextValue() throws IOException {
                        return dv.lookupOrd(dv.nextOrd());
                    }
                };
            }
        };
    }

    static class MapPageOperator implements Operator {
        private Page output;
        private final Function<Page, Page> fn;
        private boolean finished = false;

        MapPageOperator(Function<Page, Page> fn) {
            this.fn = fn;
        }

        @Override
        public boolean needsInput() {
            return output == null;
        }

        @Override
        public void addInput(Page page) {
            output = fn.apply(page);
        }

        @Override
        public void finish() {
            finished = true;
        }

        @Override
        public boolean isFinished() {
            return finished && output == null;
        }

        @Override
        public Page getOutput() {
            Page p = output;
            output = null;
            return p;
        }

        @Override
        public void close() {

        }
    }

    /**
     * Creates a {@link BigArrays} that tracks releases but doesn't throw circuit breaking exceptions.
     */
    private BigArrays bigArrays() {
        return new MockBigArrays(new MockPageCacheRecycler(Settings.EMPTY), new NoneCircuitBreakerService());
    }

    public static void runToCompletion(Executor executor, List<Driver> drivers) {
        ListenableActionFuture<List<Driver.Result>> future = new ListenableActionFuture<>();
        Driver.start(executor, drivers, future::onResponse);
        RuntimeException e = Driver.Result.collectFailures(future.actionGet());
        if (e != null) {
            throw e;
        }
    }
}
