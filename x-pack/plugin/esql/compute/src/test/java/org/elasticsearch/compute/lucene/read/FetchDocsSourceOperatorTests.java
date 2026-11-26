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
import org.elasticsearch.compute.data.IntBlock;
import org.elasticsearch.compute.data.LongBlock;
import org.elasticsearch.compute.data.Page;
import org.elasticsearch.compute.lucene.IndexedByShardId;
import org.elasticsearch.compute.lucene.IndexedByShardIdFromList;
import org.elasticsearch.compute.operator.DriverContext;
import org.elasticsearch.compute.operator.Operator;
import org.elasticsearch.compute.test.ComputeTestCase;
import org.elasticsearch.core.AbstractRefCounted;
import org.elasticsearch.index.mapper.MappedFieldType;
import org.elasticsearch.index.mapper.NumberFieldMapper;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.stream.IntStream;

import static org.hamcrest.Matchers.containsString;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.hasSize;
import static org.hamcrest.Matchers.lessThanOrEqualTo;
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
     * Every page is one slice, the shape {@link ValuesSourceReaderOperator} loads with a single reader, and its second
     * block is the position of the slice.
     */
    public void testOnePagePerSlice() {
        BlockFactory blockFactory = blockFactory();
        FetchDocsSourceOperator.ShardDocs docs = new FetchDocsSourceOperator.ShardDocs(
            0,
            new int[] { 0, 0, 0, 2, 3, 3 },
            new int[] { 1, 5, 9, 4, 0, 8 }
        );
        List<int[]> pages = new ArrayList<>();
        try (FetchDocsSourceOperator source = new FetchDocsSourceOperator(blockFactory, refCounteds(), slices(docs))) {
            while (source.isFinished() == false) {
                Page page = source.getOutput();
                if (page == null) {
                    continue;
                }
                try {
                    DocVector vector = page.<DocBlock>getBlock(0).asVector();
                    assertTrue(vector.singleSegment());
                    assertTrue(vector.singleSegmentNonDecreasing());
                    assertFalse(vector.mayContainDuplicates());
                    assertThat("the page holds its shard", shard.refCount(), equalTo(2));
                    IntBlock position = page.getBlock(1);
                    assertTrue(position.asVector().isConstant());
                    int[] values = new int[vector.getPositionCount() + 2];
                    values[0] = position.getInt(0);
                    values[1] = vector.segments().getInt(0);
                    for (int p = 0; p < vector.getPositionCount(); p++) {
                        values[p + 2] = vector.docs().getInt(p);
                    }
                    pages.add(values);
                } finally {
                    page.releaseBlocks();
                }
            }
            assertThat(source.status(), equalTo(new FetchDocsSourceOperator.Status(3, 6, 3)));
        }
        assertThat(pages.size(), equalTo(3));
        assertArrayEquals(new int[] { 0, 0, 1, 5, 9 }, pages.get(0));
        assertArrayEquals(new int[] { 1, 2, 4 }, pages.get(1));
        assertArrayEquals(new int[] { 2, 3, 0, 8 }, pages.get(2));
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
                        slices(shardDocs)
                    );
                    Operator values = ValuesSourceReaderOperatorTests.factory(reader, key, ElementType.LONG).get(driverContext)
                ) {
                    while (source.isFinished() == false) {
                        Page page = source.getOutput();
                        if (page != null) {
                            values.addInput(page);
                            drain(values, loaded);
                        }
                    }
                    values.finish();
                    drain(values, loaded);
                    // one driver alone keeps to the slice after its last, so it moves to each segment once
                    assertThat(source.status().segmentRuns(), equalTo((int) segments.stream().distinct().count()));
                }
                assertThat(loaded, equalTo(expected));
            }
        }
    }

    private static void drain(Operator values, List<Long> loaded) {
        for (Page page = values.getOutput(); page != null; page = values.getOutput()) {
            try {
                // the _doc column, the position of the slice, then the loaded values
                LongBlock block = page.getBlock(2);
                for (int p = 0; p < block.getPositionCount(); p++) {
                    loaded.add(block.getLong(block.getFirstValueIndex(p)));
                }
            } finally {
                page.releaseBlocks();
            }
        }
    }

    /**
     * A long run of a segment comes out in slices, not as one page.
     */
    public void testALongRunComesOutInSlices() {
        BlockFactory blockFactory = blockFactory();
        int docCount = between(1000, 20_000);
        int[] segments = new int[docCount];
        int[] docIds = new int[docCount];
        for (int i = 0; i < docCount; i++) {
            docIds[i] = i * 3;
        }
        FetchSliceQueue slices = slices(new FetchDocsSourceOperator.ShardDocs(0, segments, docIds));
        int pages = 0;
        try (FetchDocsSourceOperator source = new FetchDocsSourceOperator(blockFactory, refCounteds(), slices)) {
            while (source.isFinished() == false) {
                Page page = source.getOutput();
                if (page != null) {
                    assertThat(page.getPositionCount(), lessThanOrEqualTo(FetchSliceQueue.SLICE_DOCS));
                    page.releaseBlocks();
                    pages++;
                }
            }
            assertThat(pages, equalTo(Math.ceilDiv(docCount, FetchSliceQueue.SLICE_DOCS)));
            assertThat(pages, equalTo(slices.sliceCount()));
            assertThat(source.status(), equalTo(new FetchDocsSourceOperator.Status(pages, docCount, 1)));
        }
    }

    public void testShardDocsDescribeThemselvesWithoutTheirArrays() {
        FetchDocsSourceOperator.ShardDocs docs = new FetchDocsSourceOperator.ShardDocs(3, new int[] { 0, 1 }, new int[] { 4, 2 });
        assertThat(docs.toString(), equalTo("ShardDocs[shard=3, docs=2]"));
    }

    public void testNoSlicesEmitNothing() {
        try (FetchDocsSourceOperator source = new FetchDocsSourceOperator(blockFactory(), refCounteds(), slices())) {
            assertThat(source.getOutput(), nullValue());
            assertTrue(source.isFinished());
        }
    }

    /**
     * One source alone emits the slices of several shards in request order, each shard after the one before. A shard
     * without documents adds no page.
     */
    public void testEmitsSeveralShardsOneAfterTheOther() {
        AbstractRefCounted second = AbstractRefCounted.of(() -> {});
        AbstractRefCounted third = AbstractRefCounted.of(() -> {});
        IndexedByShardId<AbstractRefCounted> shards = new IndexedByShardIdFromList<>(List.of(shard, second, third));
        FetchSliceQueue slices = slices(
            new FetchDocsSourceOperator.ShardDocs(2, new int[] { 0, 1 }, new int[] { 3, 0 }),
            new FetchDocsSourceOperator.ShardDocs(1, new int[0], new int[0]),
            new FetchDocsSourceOperator.ShardDocs(0, new int[] { 4 }, new int[] { 9 })
        );
        List<int[]> pages = new ArrayList<>();
        try (FetchDocsSourceOperator source = new FetchDocsSourceOperator(blockFactory(), shards, slices)) {
            while (source.isFinished() == false) {
                Page page = source.getOutput();
                if (page == null) {
                    continue;
                }
                try {
                    DocVector vector = page.<DocBlock>getBlock(0).asVector();
                    assertThat(vector.getPositionCount(), equalTo(1));
                    pages.add(new int[] { vector.shards().getInt(0), vector.segments().getInt(0), vector.docs().getInt(0) });
                } finally {
                    page.releaseBlocks();
                }
            }
            assertThat(source.status(), equalTo(new FetchDocsSourceOperator.Status(3, 3, 3)));
        }
        assertThat(pages, hasSize(3));
        assertArrayEquals(new int[] { 2, 0, 3 }, pages.get(0));
        assertArrayEquals(new int[] { 2, 1, 0 }, pages.get(1));
        assertArrayEquals(new int[] { 0, 4, 9 }, pages.get(2));
        assertThat("released pages return their shards", shard.refCount() + second.refCount() + third.refCount(), equalTo(3));
    }

    /**
     * The sources of several drivers share the slices of a request, and each slice goes to one of them.
     */
    public void testSourcesThatShareTheSlicesEmitEachSliceOnce() {
        int docCount = between(200, 2_000);
        int step = between(1, 10);
        int[] segments = new int[docCount];
        int[] docIds = new int[docCount];
        for (int i = 0; i < docCount; i++) {
            segments[i] = i / 300;
            docIds[i] = i * step;
        }
        FetchSliceQueue slices = slices(new FetchDocsSourceOperator.ShardDocs(0, segments, docIds));
        List<FetchDocsSourceOperator> sources = new ArrayList<>();
        for (int s = between(2, 4); s > 0; s--) {
            sources.add(new FetchDocsSourceOperator(blockFactory(), refCounteds(), slices));
        }
        List<Integer> positions = new ArrayList<>();
        long docsEmitted = 0;
        while (sources.stream().allMatch(FetchDocsSourceOperator::isFinished) == false) {
            FetchDocsSourceOperator source = randomFrom(sources);
            Page page = source.getOutput();
            if (page != null) {
                positions.add(page.<IntBlock>getBlock(1).getInt(0));
                docsEmitted += page.getPositionCount();
                page.releaseBlocks();
            }
        }
        sources.forEach(FetchDocsSourceOperator::close);
        assertThat(positions.stream().sorted().toList(), equalTo(IntStream.range(0, slices.sliceCount()).boxed().toList()));
        assertThat(docsEmitted, equalTo((long) docCount));
        assertThat("released pages return their shard", shard.refCount(), equalTo(1));
    }

    public void testFinishStops() {
        FetchDocsSourceOperator.ShardDocs docs = new FetchDocsSourceOperator.ShardDocs(0, new int[] { 0, 1 }, new int[] { 0, 0 });
        try (FetchDocsSourceOperator source = new FetchDocsSourceOperator(blockFactory(), refCounteds(), slices(docs))) {
            source.getOutput().releaseBlocks();
            source.finish();
            assertTrue(source.isFinished());
            assertThat(source.getOutput(), nullValue());
        }
    }

    private static FetchSliceQueue slices(FetchDocsSourceOperator.ShardDocs... shards) {
        return FetchSliceQueue.create(List.of(shards));
    }

    private DriverContext driverContext() {
        BlockFactory blockFactory = blockFactory();
        return new DriverContext(blockFactory.bigArrays(), blockFactory, null);
    }

    private IndexedByShardId<AbstractRefCounted> refCounteds() {
        return new IndexedByShardIdFromList<>(List.of(shard));
    }
}
