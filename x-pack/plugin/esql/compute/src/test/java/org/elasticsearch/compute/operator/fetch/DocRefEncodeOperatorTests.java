/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.compute.operator.fetch;

import org.elasticsearch.compute.data.BatchMetadata;
import org.elasticsearch.compute.data.BlockFactory;
import org.elasticsearch.compute.data.BlockUtils;
import org.elasticsearch.compute.data.DocBlock;
import org.elasticsearch.compute.data.DocRefBlock;
import org.elasticsearch.compute.data.DocRefOrigin;
import org.elasticsearch.compute.data.DocVector;
import org.elasticsearch.compute.data.IntVector;
import org.elasticsearch.compute.data.LongBlock;
import org.elasticsearch.compute.data.Page;
import org.elasticsearch.compute.lucene.IndexedByShardId;
import org.elasticsearch.compute.lucene.IndexedByShardIdFromList;
import org.elasticsearch.compute.test.ComputeTestCase;
import org.elasticsearch.compute.test.RandomBlock;
import org.elasticsearch.core.RefCounted;

import java.util.Arrays;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.Function;

import static org.hamcrest.Matchers.containsString;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.sameInstance;

public class DocRefEncodeOperatorTests extends ComputeTestCase {
    private final CountingRefCounted[] shards = { new CountingRefCounted(), new CountingRefCounted(), new CountingRefCounted() };
    private final DocRefOrigin[] origins = {
        RandomBlock.randomDocRefOrigin(),
        RandomBlock.randomDocRefOrigin(),
        RandomBlock.randomDocRefOrigin() };

    public void testOneShardSharesTheVectors() {
        BlockFactory blockFactory = blockFactory();
        int positions = between(1, 100);
        DocBlock doc = docBlock(blockFactory, blockFactory.newConstantIntVector(1, positions), positions);
        IntVector segments = doc.asVector().segments();
        IntVector docs = doc.asVector().docs();
        LongBlock other = blockFactory.newConstantLongBlockWith(randomLong(), positions);
        try (Page out = encode(blockFactory, new Page(doc, other), allOrigins())) {
            DocRefBlock refs = out.getBlock(0);
            assertThat(refs.asVector().segments(), sameInstance(segments));
            assertThat(refs.asVector().docs(), sameInstance(docs));
            assertTrue(refs.asVector().singleOrigin());
            assertThat(refs.asVector().origins().size(), equalTo(1));
            assertThat(refs.asVector().origin(0), equalTo(origins[1]));
            assertThat("the other columns pass through", out.getBlock(1), sameInstance(other));
            assertThat("the page released its shard while the references live on", shards[1].count.get(), equalTo(1));
        }
    }

    /**
     * A TopN builds the shard vector as an array even when every row comes from one shard. The rows still need no
     * ordinal on the wire.
     */
    public void testOneShardFromATopN() {
        BlockFactory blockFactory = blockFactory();
        int positions = between(2, 100);
        try (DocBlock.Builder builder = DocBlock.newBlockBuilder(blockFactory, positions)) {
            for (int p = 0; p < positions; p++) {
                builder.appendShard(2).appendSegment(0).appendDoc(p);
            }
            DocBlock doc = builder.shardRefCounters(refCounteds()).build();
            assertFalse(doc.asVector().shards().isConstant());
            try (Page out = encode(blockFactory, new Page(doc), allOrigins())) {
                DocRefBlock refs = out.getBlock(0);
                assertTrue(refs.asVector().originOrdinals().isConstant());
                assertThat(refs.asVector().origin(positions - 1), equalTo(origins[2]));
            }
        }
    }

    public void testManyShardsInTheOrderRowsReferenceThem() {
        BlockFactory blockFactory = blockFactory();
        int[] rowShards = { 2, 0, 2, 1 };
        try (DocBlock.Builder builder = DocBlock.newBlockBuilder(blockFactory, rowShards.length)) {
            for (int p = 0; p < rowShards.length; p++) {
                builder.appendShard(rowShards[p]).appendSegment(p).appendDoc(10 * p);
            }
            try (Page out = encode(blockFactory, new Page(builder.shardRefCounters(refCounteds()).build()), allOrigins())) {
                DocRefBlock refs = out.getBlock(0);
                assertThat(refs.asVector().origins().size(), equalTo(3));
                assertThat(refs.asVector().origins().get(0), equalTo(origins[2]));
                assertThat(refs.asVector().origins().get(1), equalTo(origins[0]));
                assertThat(refs.asVector().origins().get(2), equalTo(origins[1]));
                for (int p = 0; p < rowShards.length; p++) {
                    assertThat(BlockUtils.toJavaObject(refs, p), equalTo(new BlockUtils.DocRef(origins[rowShards[p]], p, 10 * p)));
                }
            }
        }
        for (CountingRefCounted shard : shards) {
            assertThat(shard.count.get(), equalTo(1));
        }
    }

    /**
     * Production tombstones the contexts of shards that already closed. Reading their origin would fail, and nothing needs
     * it, because no row references them.
     */
    public void testReadsOnlyTheOriginsRowsReference() {
        BlockFactory blockFactory = blockFactory();
        IndexedByShardId<DocRefOrigin> withTombstone = view(slot -> {
            if (slot == 2) {
                throw new AssertionError("read the origin of a slot no row references");
            }
            return origins[slot];
        });
        try (DocBlock.Builder builder = DocBlock.newBlockBuilder(blockFactory, 2)) {
            builder.appendShard(0).appendSegment(0).appendDoc(0);
            builder.appendShard(1).appendSegment(0).appendDoc(0);
            try (Page out = encode(blockFactory, new Page(builder.shardRefCounters(refCounteds()).build()), withTombstone)) {
                assertThat(out.<DocRefBlock>getBlock(0).asVector().origins().size(), equalTo(2));
            }
        }
    }

    /**
     * An empty page still has to lose its {@link DocBlock}, which can't be written to the wire.
     */
    public void testEmptyPageKeepsItsBatchMetadata() {
        BlockFactory blockFactory = blockFactory();
        BatchMetadata metadata = new BatchMetadata(7, 3, true);
        DocBlock doc = docBlock(blockFactory, blockFactory.newConstantIntVector(0, 0), 0);
        try (Page out = encode(blockFactory, new Page(metadata, doc), allOrigins())) {
            assertThat(out.getPositionCount(), equalTo(0));
            assertThat(out.<DocRefBlock>getBlock(0).getPositionCount(), equalTo(0));
            assertThat(out.batchMetadata(), equalTo(metadata));
        }
    }

    public void testBatchMarkerPassesThrough() {
        BlockFactory blockFactory = blockFactory();
        Page marker = Page.createBatchMarkerPage(7, 3);
        try (Page out = encode(blockFactory, marker, allOrigins())) {
            assertThat(out, sameInstance(marker));
        }
    }

    public void testRejectsOtherBlocks() {
        BlockFactory blockFactory = blockFactory();
        DocRefEncodeOperator operator = new DocRefEncodeOperator(blockFactory, 0, allOrigins());
        try (operator) {
            Page page = new Page(blockFactory.newConstantNullBlock(3));
            IllegalStateException e = expectThrows(IllegalStateException.class, () -> operator.addInput(page));
            assertThat(e.getMessage(), containsString("expected _doc at channel [0] but got [NULL]"));
        }
    }

    public void testBreakerTripReleasesTheInput() {
        BlockFactory input = blockFactory();
        testWithCrankyBlockFactory(cranky -> {
            int[] rowShards = { 0, 1, 2, 0 };
            try (DocBlock.Builder builder = DocBlock.newBlockBuilder(input, rowShards.length)) {
                for (int p = 0; p < rowShards.length; p++) {
                    builder.appendShard(rowShards[p]).appendSegment(0).appendDoc(p);
                }
                Page page = new Page(builder.shardRefCounters(refCounteds()).build());
                encode(cranky, page, allOrigins()).releaseBlocks();
            }
        });
        for (CountingRefCounted shard : shards) {
            assertThat(shard.count.get(), equalTo(1));
        }
    }

    private Page encode(BlockFactory blockFactory, Page page, IndexedByShardId<DocRefOrigin> originsView) {
        try (DocRefEncodeOperator operator = new DocRefEncodeOperator(blockFactory, 0, originsView)) {
            operator.addInput(page);
            operator.finish();
            Page out = operator.getOutput();
            assertTrue(operator.isFinished());
            return out;
        }
    }

    private DocBlock docBlock(BlockFactory blockFactory, IntVector shardVector, int positions) {
        IntVector segments = blockFactory.newConstantIntVector(0, positions);
        IntVector docs;
        try (IntVector.FixedBuilder builder = blockFactory.newIntVectorFixedBuilder(positions)) {
            for (int p = 0; p < positions; p++) {
                builder.appendInt(p);
            }
            docs = builder.build();
        }
        return new DocVector(refCounteds(), shardVector, segments, docs, DocVector.config()).asBlock();
    }

    private IndexedByShardId<CountingRefCounted> refCounteds() {
        return new IndexedByShardIdFromList<>(Arrays.asList(shards));
    }

    private IndexedByShardId<DocRefOrigin> allOrigins() {
        return view(slot -> origins[slot]);
    }

    private IndexedByShardId<DocRefOrigin> view(Function<Integer, DocRefOrigin> bySlot) {
        return new IndexedByShardId<>() {
            @Override
            public DocRefOrigin get(int shardId) {
                return bySlot.apply(shardId);
            }

            @Override
            public Iterable<? extends DocRefOrigin> iterable() {
                throw new AssertionError("only single slots are read");
            }

            @Override
            public int size() {
                return origins.length;
            }

            @Override
            public <S> IndexedByShardId<S> map(Function<DocRefOrigin, S> mapper) {
                throw new AssertionError("not mapped");
            }
        };
    }

    /**
     * Starts with the one reference its owner holds, like the shard contexts of a data node.
     */
    private static class CountingRefCounted implements RefCounted {
        final AtomicInteger count = new AtomicInteger(1);

        @Override
        public void incRef() {
            count.incrementAndGet();
        }

        @Override
        public boolean tryIncRef() {
            count.incrementAndGet();
            return true;
        }

        @Override
        public boolean decRef() {
            return count.decrementAndGet() == 0;
        }

        @Override
        public boolean hasReferences() {
            return count.get() > 0;
        }
    }
}
