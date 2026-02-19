/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.compute.lucene.query;

import org.apache.lucene.document.SortedNumericDocValuesField;
import org.apache.lucene.index.DirectoryReader;
import org.apache.lucene.index.IndexReader;
import org.apache.lucene.index.IndexWriter;
import org.apache.lucene.index.IndexWriterConfig;
import org.apache.lucene.index.IndexableField;
import org.apache.lucene.store.Directory;
import org.elasticsearch.common.lucene.search.Queries;
import org.elasticsearch.compute.data.BytesRefBlock;
import org.elasticsearch.compute.data.DocBlock;
import org.elasticsearch.compute.data.IntBlock;
import org.elasticsearch.compute.data.IntVector;
import org.elasticsearch.compute.data.LongBlock;
import org.elasticsearch.compute.lucene.IndexedByShardIdFromList;
import org.elasticsearch.compute.operator.Driver;
import org.elasticsearch.compute.operator.DriverContext;
import org.elasticsearch.compute.operator.PageConsumerOperator;
import org.elasticsearch.compute.operator.SinkOperator;
import org.elasticsearch.compute.operator.SourceOperator;
import org.elasticsearch.compute.test.SourceOperatorTestCase;
import org.elasticsearch.compute.test.TestDriverRunner;
import org.elasticsearch.core.IOUtils;
import org.elasticsearch.core.TimeValue;
import org.hamcrest.Matcher;
import org.junit.After;
import org.junit.Before;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
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
            randomIntBetween(1, 4),
            between(10, 100),
            between(1, 100)
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
