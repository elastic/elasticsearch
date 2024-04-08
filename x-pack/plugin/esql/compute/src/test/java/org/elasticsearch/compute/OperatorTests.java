/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.compute;

import org.apache.lucene.document.Document;
import org.apache.lucene.document.Field;
import org.apache.lucene.document.LongPoint;
import org.apache.lucene.document.SortedSetDocValuesField;
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
import org.apache.lucene.tests.store.BaseDirectoryWrapper;
import org.apache.lucene.util.BytesRef;
import org.elasticsearch.common.breaker.CircuitBreakingException;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.unit.ByteSizeValue;
import org.elasticsearch.common.util.BigArrays;
import org.elasticsearch.common.util.MockBigArrays;
import org.elasticsearch.common.util.MockPageCacheRecycler;
import org.elasticsearch.common.util.concurrent.ConcurrentCollections;
import org.elasticsearch.compute.aggregation.CountAggregatorFunction;
import org.elasticsearch.compute.aggregation.blockhash.BlockHash;
import org.elasticsearch.compute.data.Block;
import org.elasticsearch.compute.data.BlockFactory;
import org.elasticsearch.compute.data.BytesRefBlock;
import org.elasticsearch.compute.data.DocBlock;
import org.elasticsearch.compute.data.DocVector;
import org.elasticsearch.compute.data.ElementType;
import org.elasticsearch.compute.data.IntVector;
import org.elasticsearch.compute.data.LongBlock;
import org.elasticsearch.compute.data.Page;
import org.elasticsearch.compute.lucene.DataPartitioning;
import org.elasticsearch.compute.lucene.LuceneOperator;
import org.elasticsearch.compute.lucene.LuceneSourceOperator;
import org.elasticsearch.compute.lucene.LuceneSourceOperatorTests;
import org.elasticsearch.compute.lucene.ShardContext;
import org.elasticsearch.compute.lucene.ValuesSourceReaderOperator;
import org.elasticsearch.compute.operator.AbstractPageMappingOperator;
import org.elasticsearch.compute.operator.Driver;
import org.elasticsearch.compute.operator.DriverContext;
import org.elasticsearch.compute.operator.HashAggregationOperator;
import org.elasticsearch.compute.operator.LimitOperator;
import org.elasticsearch.compute.operator.Operator;
import org.elasticsearch.compute.operator.OperatorTestCase;
import org.elasticsearch.compute.operator.OrdinalsGroupingOperator;
import org.elasticsearch.compute.operator.PageConsumerOperator;
import org.elasticsearch.compute.operator.SequenceLongBlockSourceOperator;
import org.elasticsearch.core.CheckedConsumer;
import org.elasticsearch.core.Releasables;
import org.elasticsearch.index.mapper.KeywordFieldMapper;
import org.elasticsearch.index.mapper.MapperServiceTestCase;
import org.elasticsearch.index.mapper.SourceLoader;
import org.elasticsearch.index.mapper.Uid;
import org.elasticsearch.indices.breaker.NoneCircuitBreakerService;
import org.elasticsearch.test.ESTestCase;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

import static org.elasticsearch.compute.aggregation.AggregatorMode.FINAL;
import static org.elasticsearch.compute.aggregation.AggregatorMode.INITIAL;
import static org.elasticsearch.compute.operator.OperatorTestCase.randomPageSize;
import static org.hamcrest.Matchers.contains;
import static org.hamcrest.Matchers.empty;
import static org.hamcrest.Matchers.equalTo;

// TODO: Move these tests to the right test classes.
public class OperatorTests extends MapperServiceTestCase {

    public void testQueryOperator() throws IOException {
        Map<BytesRef, Long> docs = new HashMap<>();
        CheckedConsumer<DirectoryReader, IOException> verifier = reader -> {
            final long from = randomBoolean() ? Long.MIN_VALUE : randomLongBetween(0, 10000);
            final long to = randomBoolean() ? Long.MAX_VALUE : randomLongBetween(from, from + 10000);
            final Query query = LongPoint.newRangeQuery("pt", from, to);
            LuceneOperator.Factory factory = luceneOperatorFactory(reader, query, LuceneOperator.NO_LIMIT);
            List<Driver> drivers = new ArrayList<>();
            try {
                Set<Integer> actualDocIds = ConcurrentCollections.newConcurrentSet();
                for (int t = 0; t < factory.taskConcurrency(); t++) {
                    PageConsumerOperator docCollector = new PageConsumerOperator(page -> {
                        DocVector docVector = page.<DocBlock>getBlock(0).asVector();
                        IntVector doc = docVector.docs();
                        IntVector segment = docVector.segments();
                        for (int i = 0; i < doc.getPositionCount(); i++) {
                            int docBase = reader.leaves().get(segment.getInt(i)).docBase;
                            int docId = docBase + doc.getInt(i);
                            assertTrue("duplicated docId=" + docId, actualDocIds.add(docId));
                        }
                    });
                    DriverContext driverContext = driverContext();
                    drivers.add(new Driver(driverContext, factory.get(driverContext), List.of(), docCollector, () -> {}));
                }
                OperatorTestCase.runDriver(drivers);
                Set<Integer> expectedDocIds = searchForDocIds(reader, query);
                assertThat("query=" + query, actualDocIds, equalTo(expectedDocIds));
                drivers.stream().map(Driver::driverContext).forEach(OperatorTests::assertDriverContext);
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

    public void testGroupingWithOrdinals() throws Exception {
        DriverContext driverContext = driverContext();
        BlockFactory blockFactory = driverContext.blockFactory();

        final String gField = "g";
        final int numDocs = 2856; // between(100, 10000);
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
            boolean shuffleDocs = randomBoolean();
            Operator shuffleDocsOperator = new AbstractPageMappingOperator() {
                @Override
                protected Page process(Page page) {
                    if (shuffleDocs == false) {
                        return page;
                    }
                    DocVector docVector = (DocVector) page.getBlock(0).asVector();
                    int positionCount = docVector.getPositionCount();
                    IntVector shards = null;
                    IntVector segments = null;
                    IntVector docs = null;
                    try (
                        IntVector.Builder shardsBuilder = blockFactory.newIntVectorBuilder(positionCount);
                        IntVector.Builder segmentsBuilder = blockFactory.newIntVectorBuilder(positionCount);
                        IntVector.Builder docsBuilder = blockFactory.newIntVectorBuilder(positionCount);
                    ) {
                        List<Integer> docIds = new ArrayList<>(positionCount);
                        for (int i = 0; i < positionCount; i++) {
                            shardsBuilder.appendInt(docVector.shards().getInt(i));
                            segmentsBuilder.appendInt(docVector.segments().getInt(i));
                            docIds.add(docVector.docs().getInt(i));
                        }
                        shards = shardsBuilder.build();
                        segments = segmentsBuilder.build();
                        Collections.shuffle(docIds, random());
                        for (Integer d : docIds) {
                            docsBuilder.appendInt(d);
                        }
                        docs = docsBuilder.build();
                    } finally {
                        if (docs == null) {
                            Releasables.closeExpectNoException(docVector, shards, segments);
                        } else {
                            Releasables.closeExpectNoException(docVector);
                        }
                    }
                    Block[] blocks = new Block[page.getBlockCount()];
                    blocks[0] = new DocVector(shards, segments, docs, false).asBlock();
                    for (int i = 1; i < blocks.length; i++) {
                        blocks[i] = page.getBlock(i);
                    }
                    return new Page(blocks);
                }

                @Override
                public String toString() {
                    return "ShuffleDocs";
                }
            };

            try (DirectoryReader reader = writer.getReader()) {
                Driver driver = new Driver(
                    driverContext,
                    luceneOperatorFactory(reader, new MatchAllDocsQuery(), LuceneOperator.NO_LIMIT).get(driverContext),
                    List.of(shuffleDocsOperator, new AbstractPageMappingOperator() {
                        @Override
                        protected Page process(Page page) {
                            return page.appendBlock(driverContext.blockFactory().newConstantIntBlockWith(1, page.getPositionCount()));
                        }

                        @Override
                        public String toString() {
                            return "Add(1)";
                        }
                    },
                        new OrdinalsGroupingOperator(
                            shardIdx -> new KeywordFieldMapper.KeywordFieldType("g").blockLoader(null),
                            List.of(new ValuesSourceReaderOperator.ShardContext(reader, () -> SourceLoader.FROM_STORED_SOURCE)),
                            ElementType.BYTES_REF,
                            0,
                            gField,
                            List.of(CountAggregatorFunction.supplier(List.of(1)).groupingAggregatorFactory(INITIAL)),
                            randomPageSize(),
                            driverContext
                        ),
                        new HashAggregationOperator(
                            List.of(CountAggregatorFunction.supplier(List.of(1, 2)).groupingAggregatorFactory(FINAL)),
                            () -> BlockHash.build(
                                List.of(new HashAggregationOperator.GroupSpec(0, ElementType.BYTES_REF)),
                                driverContext.blockFactory(),
                                randomPageSize(),
                                false
                            ),
                            driverContext
                        )
                    ),
                    new PageConsumerOperator(page -> {
                        BytesRefBlock keys = page.getBlock(0);
                        LongBlock counts = page.getBlock(1);
                        for (int i = 0; i < keys.getPositionCount(); i++) {
                            BytesRef spare = new BytesRef();
                            keys.getBytesRef(i, spare);
                            actualCounts.put(BytesRef.deepCopyOf(spare), counts.getLong(i));
                        }
                        page.releaseBlocks();
                    }),
                    () -> {}
                );
                OperatorTestCase.runDriver(driver);
                assertThat(actualCounts, equalTo(expectedCounts));
                assertDriverContext(driverContext);
                org.elasticsearch.common.util.MockBigArrays.ensureAllArraysAreReleased();
            }
        }
        assertThat(blockFactory.breaker().getUsed(), equalTo(0L));
    }

    public void testLimitOperator() {
        var positions = 100;
        var limit = randomIntBetween(90, 101);
        var values = randomList(positions, positions, ESTestCase::randomLong);

        var results = new ArrayList<Long>();
        DriverContext driverContext = driverContext();
        try (
            var driver = new Driver(
                driverContext,
                new SequenceLongBlockSourceOperator(driverContext.blockFactory(), values, 100),
                List.of((new LimitOperator.Factory(limit)).get(driverContext)),
                new PageConsumerOperator(page -> {
                    LongBlock block = page.getBlock(0);
                    for (int i = 0; i < page.getPositionCount(); i++) {
                        results.add(block.getLong(i));
                    }
                }),
                () -> {}
            )
        ) {
            OperatorTestCase.runDriver(driver);
        }

        assertThat(results, contains(values.stream().limit(limit).toArray()));
        assertDriverContext(driverContext);
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

    /**
     * Creates a {@link BigArrays} that tracks releases but doesn't throw circuit breaking exceptions.
     */
    private BigArrays bigArrays() {
        return new MockBigArrays(new MockPageCacheRecycler(Settings.EMPTY), new NoneCircuitBreakerService());
    }

    /**
     * A {@link DriverContext} that won't throw {@link CircuitBreakingException}.
     */
    protected final DriverContext driverContext() {
        var breaker = new MockBigArrays.LimitedBreaker("esql-test-breaker", ByteSizeValue.ofGb(1));
        return new DriverContext(bigArrays(), BlockFactory.getInstance(breaker, bigArrays()));
    }

    public static void assertDriverContext(DriverContext driverContext) {
        assertTrue(driverContext.isFinished());
        assertThat(driverContext.getSnapshot().releasables(), empty());
    }

    static LuceneOperator.Factory luceneOperatorFactory(IndexReader reader, Query query, int limit) {
        final ShardContext searchContext = new LuceneSourceOperatorTests.MockShardContext(reader, 0);
        return new LuceneSourceOperator.Factory(
            List.of(searchContext),
            ctx -> query,
            randomFrom(DataPartitioning.values()),
            randomIntBetween(1, 10),
            randomPageSize(),
            limit
        );
    }
}
