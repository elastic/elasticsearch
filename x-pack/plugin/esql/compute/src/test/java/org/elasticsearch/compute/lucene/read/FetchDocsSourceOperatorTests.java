/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.compute.lucene.read;

import org.apache.lucene.document.SortedNumericDocValuesField;
import org.apache.lucene.index.DirectoryReader;
import org.apache.lucene.index.IndexReader;
import org.apache.lucene.index.IndexWriter;
import org.apache.lucene.index.LeafReaderContext;
import org.apache.lucene.index.NoMergePolicy;
import org.apache.lucene.index.SortedNumericDocValues;
import org.apache.lucene.store.Directory;
import org.elasticsearch.compute.data.BlockFactory;
import org.elasticsearch.compute.data.DocBlock;
import org.elasticsearch.compute.data.DocVector;
import org.elasticsearch.compute.data.ElementType;
import org.elasticsearch.compute.data.LongBlock;
import org.elasticsearch.compute.data.Page;
import org.elasticsearch.compute.lucene.IndexedByShardId;
import org.elasticsearch.compute.lucene.IndexedByShardIdFromList;
import org.elasticsearch.compute.operator.DriverContext;
import org.elasticsearch.compute.operator.Operator;
import org.elasticsearch.compute.test.ComputeTestCase;
import org.elasticsearch.core.AbstractRefCounted;
import org.elasticsearch.core.RefCounted;
import org.elasticsearch.index.mapper.MappedFieldType;
import org.elasticsearch.index.mapper.NumberFieldMapper;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.atomic.AtomicInteger;

import static org.hamcrest.Matchers.containsString;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.nullValue;

public class FetchDocsSourceOperatorTests extends ComputeTestCase {
    // the one reference its owner holds, like the shard contexts of a data node
    private final AbstractRefCounted shard = AbstractRefCounted.of(() -> {});

    public void testRejectsUnsortedDocs() {
        IllegalArgumentException e = expectThrows(
            IllegalArgumentException.class,
            () -> new FetchDocsSourceOperator.ShardDocs(0, new int[] { 0, 0 }, new int[] { 7, 3 })
        );
        assertThat(e.getMessage(), containsString("[0, 3] follows [0, 7]"));
        expectThrows(
            IllegalArgumentException.class,
            () -> new FetchDocsSourceOperator.ShardDocs(0, new int[] { 1, 0 }, new int[] { 3, 7 })
        );
    }

    public void testRejectsDuplicates() {
        expectThrows(
            IllegalArgumentException.class,
            () -> new FetchDocsSourceOperator.ShardDocs(0, new int[] { 2, 2 }, new int[] { 5, 5 })
        );
    }

    public void testRejectsMismatchedArrays() {
        expectThrows(IllegalArgumentException.class, () -> new FetchDocsSourceOperator.ShardDocs(0, new int[] { 0 }, new int[] { 1, 2 }));
        expectThrows(IllegalArgumentException.class, () -> new FetchDocsSourceOperator.ShardDocs(0, new int[] { -1 }, new int[] { 1 }));
    }

    /**
     * Every page is one run of one segment, the shape {@link ValuesSourceReaderOperator} loads with a single reader.
     */
    public void testOnePagePerSegmentRun() {
        BlockFactory blockFactory = blockFactory();
        FetchDocsSourceOperator.ShardDocs docs = new FetchDocsSourceOperator.ShardDocs(
            0,
            new int[] { 0, 0, 0, 2, 3, 3 },
            new int[] { 1, 5, 9, 4, 0, 8 }
        );
        List<int[]> pages = new ArrayList<>();
        try (FetchDocsSourceOperator source = new FetchDocsSourceOperator(blockFactory, refCounteds(), docs, 100)) {
            while (source.isFinished() == false) {
                Page page = source.getOutput();
                try {
                    DocVector vector = page.<DocBlock>getBlock(0).asVector();
                    assertTrue(vector.singleSegment());
                    assertTrue(vector.singleSegmentNonDecreasing());
                    assertFalse(vector.mayContainDuplicates());
                    assertThat("the page holds its shard", shard.refCount(), equalTo(2));
                    int[] values = new int[vector.getPositionCount() + 1];
                    values[0] = vector.segments().getInt(0);
                    for (int p = 0; p < vector.getPositionCount(); p++) {
                        values[p + 1] = vector.docs().getInt(p);
                    }
                    pages.add(values);
                } finally {
                    page.releaseBlocks();
                }
            }
            assertThat(source.status(), equalTo(new FetchDocsSourceOperator.Status(3, 6, 3)));
        }
        assertThat(pages.size(), equalTo(3));
        assertArrayEquals(new int[] { 0, 1, 5, 9 }, pages.get(0));
        assertArrayEquals(new int[] { 2, 4 }, pages.get(1));
        assertArrayEquals(new int[] { 3, 0, 8 }, pages.get(2));
        assertThat("released pages return their shard", shard.refCount(), equalTo(1));
    }

    /**
     * Random documents of a real index with several segments, loaded the way a fetch driver loads them. Every value
     * must be the one Lucene holds for the document, in the order the request listed them.
     */
    public void testValuesSourceReaderLoadsTheAskedDocuments() throws IOException {
        try (Directory directory = newDirectory()) {
            try (IndexWriter writer = new IndexWriter(directory, newIndexWriterConfig().setMergePolicy(NoMergePolicy.INSTANCE))) {
                for (int d = between(50, 300); d > 0; d--) {
                    writer.addDocument(List.of(new SortedNumericDocValuesField("key", randomLong())));
                    if (randomInt(19) == 0) {
                        writer.flush();
                    }
                }
            }
            try (IndexReader reader = DirectoryReader.open(directory)) {
                List<Integer> segments = new ArrayList<>();
                List<Integer> docs = new ArrayList<>();
                List<Long> expected = new ArrayList<>();
                for (LeafReaderContext leaf : reader.leaves()) {
                    SortedNumericDocValues keys = leaf.reader().getSortedNumericDocValues("key");
                    for (int doc = 0; doc < leaf.reader().maxDoc(); doc++) {
                        if (randomInt(3) == 0) {
                            assertTrue(keys.advanceExact(doc));
                            segments.add(leaf.ord);
                            docs.add(doc);
                            expected.add(keys.nextValue());
                        }
                    }
                }
                FetchDocsSourceOperator.ShardDocs shardDocs = new FetchDocsSourceOperator.ShardDocs(
                    0,
                    segments.stream().mapToInt(Integer::intValue).toArray(),
                    docs.stream().mapToInt(Integer::intValue).toArray()
                );
                MappedFieldType key = new NumberFieldMapper.NumberFieldType("key", NumberFieldMapper.NumberType.LONG);
                DriverContext driverContext = driverContext();
                List<Long> loaded = new ArrayList<>();
                try (
                    FetchDocsSourceOperator source = new FetchDocsSourceOperator(
                        driverContext.blockFactory(),
                        refCounteds(),
                        shardDocs,
                        between(1, 20)
                    );
                    Operator values = ValuesSourceReaderOperatorTests.factory(reader, key, ElementType.LONG).get(driverContext)
                ) {
                    while (source.isFinished() == false) {
                        values.addInput(source.getOutput());
                        drain(values, loaded);
                    }
                    values.finish();
                    drain(values, loaded);
                    assertThat(source.status().segmentRuns(), equalTo((int) segments.stream().distinct().count()));
                }
                assertThat(loaded, equalTo(expected));
            }
        }
    }

    private static void drain(Operator values, List<Long> loaded) {
        for (Page page = values.getOutput(); page != null; page = values.getOutput()) {
            try {
                LongBlock block = page.getBlock(1);
                for (int p = 0; p < block.getPositionCount(); p++) {
                    loaded.add(block.getLong(block.getFirstValueIndex(p)));
                }
            } finally {
                page.releaseBlocks();
            }
        }
    }

    public void testLongRunsSpanSeveralPages() {
        BlockFactory blockFactory = blockFactory();
        int docCount = between(10, 100);
        int maxPageSize = between(1, 9);
        int[] segments = new int[docCount];
        int[] docIds = new int[docCount];
        for (int i = 0; i < docCount; i++) {
            docIds[i] = i * 3;
        }
        FetchDocsSourceOperator.ShardDocs docs = new FetchDocsSourceOperator.ShardDocs(0, segments, docIds);
        int emitted = 0;
        int pageCount = 0;
        try (FetchDocsSourceOperator source = new FetchDocsSourceOperator(blockFactory, refCounteds(), docs, maxPageSize)) {
            while (source.isFinished() == false) {
                Page page = source.getOutput();
                assertThat(page.getPositionCount() <= maxPageSize, equalTo(true));
                emitted += page.getPositionCount();
                pageCount++;
                page.releaseBlocks();
            }
            assertThat("a run split across pages is still one run", source.status().segmentRuns(), equalTo(1));
        }
        assertThat(emitted, equalTo(docCount));
        assertThat(pageCount, equalTo((docCount + maxPageSize - 1) / maxPageSize));
    }

    /**
     * An empty page would never move the cursor, so both the factory and the operator refuse a page size below one.
     */
    public void testRejectsPagesWithoutRows() {
        int maxPageSize = between(-5, 0);
        IllegalArgumentException e = expectThrows(
            IllegalArgumentException.class,
            () -> new FetchDocsSourceOperator(blockFactory(), refCounteds(), null, maxPageSize)
        );
        assertThat(e.getMessage(), equalTo("maxPageSize must be positive but was [" + maxPageSize + "]"));
        expectThrows(IllegalArgumentException.class, () -> new FetchDocsSourceOperator.Factory(null, maxPageSize));
    }

    public void testShardDocsDescribeThemselvesWithoutTheirArrays() {
        FetchDocsSourceOperator.ShardDocs docs = new FetchDocsSourceOperator.ShardDocs(3, new int[] { 0, 1 }, new int[] { 4, 2 });
        assertThat(docs.toString(), equalTo("ShardDocs[shard=3, docs=2]"));
    }

    public void testNoShardEmitsNothing() {
        try (FetchDocsSourceOperator source = new FetchDocsSourceOperator(blockFactory(), refCounteds(), null, 10)) {
            assertTrue(source.isFinished());
            assertThat(source.getOutput(), nullValue());
        }
    }

    public void testFinishStops() {
        FetchDocsSourceOperator.ShardDocs docs = new FetchDocsSourceOperator.ShardDocs(0, new int[] { 0, 1 }, new int[] { 0, 0 });
        try (FetchDocsSourceOperator source = new FetchDocsSourceOperator(blockFactory(), refCounteds(), docs, 10)) {
            source.getOutput().releaseBlocks();
            source.finish();
            assertTrue(source.isFinished());
            assertThat(source.getOutput(), nullValue());
        }
    }

    public void testFactoryClaimsAShardPerDriver() {
        FetchDocsSourceOperator.ShardDocs docs = new FetchDocsSourceOperator.ShardDocs(0, new int[] { 0 }, new int[] { 4 });
        AtomicInteger claims = new AtomicInteger();
        FetchDocsSourceOperator.ShardDocsProvider provider = new FetchDocsSourceOperator.ShardDocsProvider() {
            @Override
            public FetchDocsSourceOperator.ShardDocs claim(DriverContext driverContext) {
                return claims.getAndIncrement() == 0 ? docs : null;
            }

            @Override
            public IndexedByShardId<? extends RefCounted> refCounteds() {
                return FetchDocsSourceOperatorTests.this.refCounteds();
            }
        };
        FetchDocsSourceOperator.Factory factory = new FetchDocsSourceOperator.Factory(provider, 10);
        assertThat(factory.describe(), equalTo("FetchDocsSourceOperator[maxPageSize=10]"));
        try (var first = factory.get(driverContext()); var second = factory.get(driverContext())) {
            first.getOutput().releaseBlocks();
            assertTrue(first.isFinished());
            assertTrue("the second driver got no shard", second.isFinished());
        }
    }

    private DriverContext driverContext() {
        BlockFactory blockFactory = blockFactory();
        return new DriverContext(blockFactory.bigArrays(), blockFactory, null);
    }

    private IndexedByShardId<AbstractRefCounted> refCounteds() {
        return new IndexedByShardIdFromList<>(List.of(shard));
    }
}
