/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.compute.lucene.query;

import org.apache.lucene.document.Document;
import org.apache.lucene.document.SortedNumericDocValuesField;
import org.apache.lucene.index.DirectoryReader;
import org.apache.lucene.index.IndexReader;
import org.apache.lucene.index.IndexWriter;
import org.apache.lucene.index.IndexWriterConfig;
import org.apache.lucene.index.IndexableField;
import org.apache.lucene.index.LeafReaderContext;
import org.apache.lucene.index.NoMergePolicy;
import org.apache.lucene.search.FilterWeight;
import org.apache.lucene.search.IndexSearcher;
import org.apache.lucene.search.Query;
import org.apache.lucene.search.QueryVisitor;
import org.apache.lucene.search.ScoreMode;
import org.apache.lucene.search.ScorerSupplier;
import org.apache.lucene.search.Weight;
import org.apache.lucene.store.Directory;
import org.elasticsearch.common.lucene.search.Queries;
import org.elasticsearch.compute.data.BytesRefBlock;
import org.elasticsearch.compute.data.DocBlock;
import org.elasticsearch.compute.data.DocVector;
import org.elasticsearch.compute.data.IntBlock;
import org.elasticsearch.compute.data.IntVector;
import org.elasticsearch.compute.data.LongBlock;
import org.elasticsearch.compute.data.Page;
import org.elasticsearch.compute.lucene.IndexedByShardIdFromList;
import org.elasticsearch.compute.lucene.PartialLeafReaderContext;
import org.elasticsearch.compute.operator.Driver;
import org.elasticsearch.compute.operator.DriverContext;
import org.elasticsearch.compute.operator.Limiter;
import org.elasticsearch.compute.operator.PageConsumerOperator;
import org.elasticsearch.compute.operator.SinkOperator;
import org.elasticsearch.compute.operator.SourceOperator;
import org.elasticsearch.compute.querydsl.query.QueryWarnings;
import org.elasticsearch.compute.test.SourceOperatorTestCase;
import org.elasticsearch.compute.test.TestDriverRunner;
import org.elasticsearch.core.IOUtils;
import org.elasticsearch.core.Releasables;
import org.elasticsearch.core.TimeValue;
import org.hamcrest.Matcher;
import org.junit.After;
import org.junit.Before;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.atomic.AtomicInteger;

import static org.hamcrest.Matchers.either;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.greaterThanOrEqualTo;
import static org.hamcrest.Matchers.instanceOf;
import static org.hamcrest.Matchers.matchesRegex;

public class TimeSeriesSourceOperatorTests extends SourceOperatorTestCase {

    private final List<Directory> directories = new ArrayList<>();
    private final List<IndexReader> readers = new ArrayList<>();

    @Before
    public void setUpDirectory() throws Exception {}

    @After
    public void closeIndex() throws IOException {
        IOUtils.close(readers);
        IOUtils.close(directories);
        readers.clear();
        directories.clear();
    }

    @Override
    protected TimeSeriesSourceOperator.Factory simple(SimpleOptions options) {
        if (readers.isEmpty()) {
            var dir = newDirectory();
            directories.add(dir);
            readers.add(loadIndex(dir, between(1, 100)));
        }
        try {
            return simple(List.of(new LuceneSliceQueue.QueryAndTags(Queries.ALL_DOCS_INSTANCE, List.of())));
        } catch (Exception e) {
            throw new AssertionError(e);
        }
    }

    TimeSeriesSourceOperator.Factory simple(List<LuceneSliceQueue.QueryAndTags> queryAndTags) throws Exception {
        List<LuceneSourceOperatorTests.MockShardContext> contexts = new ArrayList<>();
        for (int i = 0; i < readers.size(); i++) {
            contexts.add(new LuceneSourceOperatorTests.MockShardContext(readers.get(i), i));
        }
        return new TimeSeriesSourceOperator.Factory(
            new IndexedByShardIdFromList<>(contexts),
            c -> queryAndTags,
            DataPartitioning.AUTO,
            LuceneOperator.SMALL_INDEX_BOUNDARY,
            randomIntBetween(1, 4),
            between(10, 100),
            between(1, 100),
            () -> 0L,
            LuceneSliceQueue.MIN_DOCS_PER_SLICE,
            QueryWarnings.EMIT
        );
    }

    static IndexReader loadIndex(Directory directory, int numDocs) {
        IndexWriterConfig config = newIndexWriterConfig();
        try {
            try (var writer = new IndexWriter(directory, config)) {
                for (int d = 0; d < numDocs; d++) {
                    List<IndexableField> doc = new ArrayList<>();
                    doc.add(new SortedNumericDocValuesField("f", d));
                    writer.addDocument(doc);
                    if (random().nextInt(100) < 20) {
                        writer.commit();
                    }
                }
                writer.commit();
            }
            return DirectoryReader.open(directory);
        } catch (IOException e) {
            throw new AssertionError(e);
        }
    }

    @Override
    protected Matcher<String> expectedToStringOfSimple() {
        return matchesRegex("TimeSeriesSourceOperator\\[shards = \\[test], maxPageSize = \\d+, remainingDocs = \\d+]");
    }

    @Override
    protected Matcher<String> expectedDescriptionOfSimple() {
        return matchesRegex("TimeSeriesSourceOperator\\[maxPageSize = \\d+, limit = \\d+]");
    }

    public void testReusesScorersAcrossSlices() throws IOException {
        final int[] buildScorerCount = new int[2];
        class CountingScorerQuery extends Query {
            @Override
            public Weight createWeight(IndexSearcher searcher, ScoreMode scoreMode, float boost) throws IOException {
                return new FilterWeight(this, Queries.ALL_DOCS_INSTANCE.createWeight(searcher, scoreMode, boost)) {
                    @Override
                    public ScorerSupplier scorerSupplier(LeafReaderContext context) throws IOException {
                        buildScorerCount[context.ord]++;
                        return super.scorerSupplier(context);
                    }
                };
            }

            @Override
            public String toString(String field) {
                return "ScorerCountingQuery";
            }

            @Override
            public void visit(QueryVisitor visitor) {
                visitor.visitLeaf(this);
            }

            @Override
            public boolean equals(Object obj) {
                return this == obj;
            }

            @Override
            public int hashCode() {
                return System.identityHashCode(this);
            }
        }

        try (Directory directory = newDirectory()) {
            try (IndexWriter writer = new IndexWriter(directory, new IndexWriterConfig().setMergePolicy(NoMergePolicy.INSTANCE))) {
                for (int segment = 0; segment < 2; segment++) {
                    int numDocs = between(100, 200);
                    for (int doc = 0; doc < numDocs; doc++) {
                        writer.addDocument(new Document());
                    }
                    writer.commit();
                }
            }
            try (DirectoryReader reader = DirectoryReader.open(directory)) {
                assertThat(reader.leaves().size(), equalTo(2));
                var context = new LuceneSourceOperatorTests.MockShardContext(reader, 0);
                var query = new CountingScorerQuery();
                Weight weight = query.createWeight(context.searcher(), ScoreMode.COMPLETE_NO_SCORES, 1.0f);
                List<LuceneSlice> slices = new ArrayList<>();
                List<Integer> expectedDocs = new ArrayList<>();
                int gapBetweenSlices = between(0, 5);
                for (int slice = 0; slice < 2; slice++) {
                    int minDoc = slice * (10 + gapBetweenSlices);
                    int maxDoc = minDoc + 10;
                    List<PartialLeafReaderContext> leaves = new ArrayList<>();
                    for (LeafReaderContext leaf : reader.leaves()) {
                        leaves.add(new PartialLeafReaderContext(leaf, minDoc, maxDoc));
                    }
                    slices.add(new LuceneSlice(slice, slice == 0, context, leaves, weight, List.of(), LuceneSlice.NEVER_BLOCKED));
                    for (int doc = minDoc; doc < maxDoc; doc++) {
                        expectedDocs.add(doc);
                    }
                }
                List<List<Integer>> docsPerLeaf = List.of(new ArrayList<>(), new ArrayList<>());
                var queue = new LuceneSliceQueue(i -> context, slices, Map.of("test", LuceneSliceQueue.PartitioningStrategy.TIME_SERIES));
                try (
                    var operator = new TimeSeriesSourceOperator(
                        new IndexedByShardIdFromList<>(List.of(context)),
                        driverContext(),
                        between(1, 10),
                        queue,
                        LuceneOperator.NO_LIMIT,
                        Limiter.NO_LIMIT,
                        () -> 0L,
                        QueryWarnings.EMIT
                    )
                ) {
                    while (operator.isFinished() == false) {
                        Page page = operator.getOutput();
                        if (page == null) {
                            continue;
                        }
                        try {
                            DocVector docVector = page.<DocBlock>getBlock(0).asVector();
                            for (int p = 0; p < docVector.getPositionCount(); p++) {
                                docsPerLeaf.get(docVector.segments().getInt(p)).add(docVector.docs().getInt(p));
                            }
                        } finally {
                            Releasables.close(page);
                        }
                    }
                }
                assertThat(docsPerLeaf, equalTo(List.of(expectedDocs, expectedDocs)));
                assertArrayEquals(new int[] { 1, 1 }, buildScorerCount);
            }
        }
    }

    public void testSliceIndex() throws Exception {
        int numShards = between(1, 10);
        for (int i = 0; i < numShards; i++) {
            var dir = newDirectory();
            directories.add(dir);
            readers.add(loadIndex(dir, between(10, 100)));
        }
        var factory = simple(
            List.of(
                new LuceneSliceQueue.QueryAndTags(SortedNumericDocValuesField.newSlowExactQuery("f", 3), List.of("t1")),
                new LuceneSliceQueue.QueryAndTags(SortedNumericDocValuesField.newSlowExactQuery("f", 5), List.of("t2")),
                new LuceneSliceQueue.QueryAndTags(SortedNumericDocValuesField.newSlowExactQuery("f", 7), List.of("t3"))
            )
        );
        DriverContext driverContext = driverContext();
        SourceOperator sourceOperator = factory.get(driverContext);
        AtomicInteger lastSliceIndex = new AtomicInteger(-1);
        SinkOperator sinkOperator = new PageConsumerOperator(p -> {
            try {
                // _doc, _slice_index, future_max_timestamp, tag
                assertThat(p.getBlockCount(), equalTo(4));
                assertThat(p.getBlock(0), instanceOf(DocBlock.class));

                IntBlock sliceIndexBlock = p.getBlock(1);
                IntVector sliceIndexVector = sliceIndexBlock.asVector();
                assertNotNull(sliceIndexVector);
                assertTrue(sliceIndexVector.isConstant());
                int sliceIndex = sliceIndexVector.getInt(0);
                assertThat(sliceIndex, greaterThanOrEqualTo(0));
                // slice index is monotonic
                assertThat(sliceIndex, either(equalTo(lastSliceIndex.get())).or(equalTo(lastSliceIndex.get() + 1)));
                lastSliceIndex.set(sliceIndex);

                LongBlock futureTimestampBlock = p.getBlock(2);
                var longVector = futureTimestampBlock.asVector();
                assertNotNull(longVector);
                assertTrue(longVector.isConstant());
                assertThat(longVector.getLong(0), equalTo(Long.MAX_VALUE));

                BytesRefBlock tagBlock = p.getBlock(3);
                var tagVector = tagBlock.asVector();
                assertNotNull(tagVector);
            } finally {
                p.releaseBlocks();
            }
        });
        Driver driver = new Driver(
            "driver",
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
        new TestDriverRunner().run(driver);
        // assertThat(lastSliceIndex.get(), equalTo(numShards * 3 - 1));
    }
}
