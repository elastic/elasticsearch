/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.compute.data;

import org.elasticsearch.compute.lucene.IndexedByShardId;
import org.elasticsearch.compute.lucene.IndexedByShardIdFromList;
import org.elasticsearch.compute.test.ComputeTestCase;
import org.elasticsearch.compute.test.RandomBlock;
import org.elasticsearch.core.RefCounted;
import org.elasticsearch.core.Releasables;

import java.util.Arrays;
import java.util.List;
import java.util.concurrent.atomic.AtomicInteger;

import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.sameInstance;

/**
 * {@link DocVector#detach} and the checks that keep rows that left their node away from the shard contexts of a node.
 */
public class DocVectorDetachTests extends ComputeTestCase {
    private final CountingRefCounted[] shards = { new CountingRefCounted(), new CountingRefCounted(), new CountingRefCounted() };
    private final List<DocOrigin> origins = RandomBlock.randomDocOrigins(3);

    public void testOneShardSharesTheVectors() {
        BlockFactory blockFactory = blockFactory();
        int positions = between(1, 100);
        DocVector detached;
        IntVector segments;
        IntVector docs;
        try (DocVector local = docVector(blockFactory, blockFactory.newConstantIntVector(1, positions), positions)) {
            segments = local.segments();
            docs = local.docs();
            detached = local.detach(origins::get);
        }
        try (detached) {
            assertThat(detached.segments(), sameInstance(segments));
            assertThat(detached.docs(), sameInstance(docs));
            assertTrue(detached.shards().isConstant());
            assertThat(detached.origins().size(), equalTo(1));
            assertThat(detached.origin(positions - 1), equalTo(origins.get(1)));
            assertThat("the detached rows pin no shard", shards[1].count.get(), equalTo(1));
        }
    }

    /**
     * A TopN builds the shard vector as an array even when every row comes from one shard. The detached rows still get
     * constant ordinals, so they need no ordinal on the wire.
     */
    public void testOneShardFromATopN() {
        BlockFactory blockFactory = blockFactory();
        int positions = between(2, 100);
        try (DocBlock.Builder builder = DocBlock.newBlockBuilder(blockFactory, positions)) {
            for (int p = 0; p < positions; p++) {
                builder.appendShard(2).appendSegment(0).appendDoc(p);
            }
            try (
                DocBlock local = builder.shardRefCounters(refCounteds()).build();
                DocVector detached = local.asVector().detach(origins::get)
            ) {
                assertFalse(local.asVector().shards().isConstant());
                assertTrue(detached.shards().isConstant());
                assertThat(detached.origins().size(), equalTo(1));
                assertThat(detached.origin(positions - 1), equalTo(origins.get(2)));
            }
        }
    }

    public void testManyShardsInTheOrderRowsReferenceThem() {
        BlockFactory blockFactory = blockFactory();
        int[] rowShards = { 2, 0, 2, 1 };
        int[] reads = new int[shards.length];
        try (DocBlock.Builder builder = DocBlock.newBlockBuilder(blockFactory, rowShards.length)) {
            for (int p = 0; p < rowShards.length; p++) {
                builder.appendShard(rowShards[p]).appendSegment(p).appendDoc(10 * p);
            }
            try (DocBlock local = builder.shardRefCounters(refCounteds()).build(); DocVector detached = local.asVector().detach(slot -> {
                reads[slot]++;
                return origins.get(slot);
            })) {
                assertThat(detached.origins().size(), equalTo(3));
                assertThat(detached.origins().get(0), equalTo(origins.get(2)));
                assertThat(detached.origins().get(1), equalTo(origins.get(0)));
                assertThat(detached.origins().get(2), equalTo(origins.get(1)));
                for (int p = 0; p < rowShards.length; p++) {
                    assertThat(detached.origin(p), equalTo(origins.get(rowShards[p])));
                    assertThat(detached.segments().getInt(p), equalTo(p));
                    assertThat(detached.docs().getInt(p), equalTo(10 * p));
                }
            }
        }
        assertThat("each slot's origin is read once", reads, equalTo(new int[] { 1, 1, 1 }));
        for (CountingRefCounted shard : shards) {
            assertThat(shard.count.get(), equalTo(1));
        }
    }

    /**
     * A data node tombstones the contexts of shards that already closed. Reading their origin would fail, and nothing
     * needs it, because no row references them.
     */
    public void testReadsOnlyTheOriginsRowsReference() {
        BlockFactory blockFactory = blockFactory();
        try (DocBlock.Builder builder = DocBlock.newBlockBuilder(blockFactory, 2)) {
            builder.appendShard(0).appendSegment(0).appendDoc(0);
            builder.appendShard(1).appendSegment(0).appendDoc(0);
            try (DocBlock local = builder.shardRefCounters(refCounteds()).build(); DocVector detached = local.asVector().detach(slot -> {
                if (slot == 2) {
                    throw new AssertionError("read the origin of a slot no row references");
                }
                return origins.get(slot);
            })) {
                assertThat(detached.origins().size(), equalTo(2));
            }
        }
    }

    public void testEmpty() {
        BlockFactory blockFactory = blockFactory();
        try (
            DocVector local = docVector(blockFactory, blockFactory.newConstantIntVector(0, 0), 0);
            DocVector detached = local.detach(slot -> {
                throw new AssertionError("no row references a slot");
            })
        ) {
            assertThat(detached.getPositionCount(), equalTo(0));
            assertThat(detached.origins().size(), equalTo(0));
        }
    }

    public void testKeepsMayContainDuplicates() {
        BlockFactory blockFactory = blockFactory();
        boolean mayContainDuplicates = randomBoolean();
        IntVector docs = mayContainDuplicates ? blockFactory.newConstantIntVector(7, 3) : blockFactory.newIntRangeVector(0, 3);
        try (
            DocVector local = new DocVector(
                refCounteds(),
                blockFactory.newConstantIntVector(0, 3),
                blockFactory.newConstantIntVector(0, 3),
                docs,
                DocVector.config().mayContainDuplicates(mayContainDuplicates)
            );
            DocVector detached = local.detach(origins::get)
        ) {
            assertThat(detached.mayContainDuplicates(), equalTo(mayContainDuplicates));
        }
    }

    public void testDetachesOnce() {
        BlockFactory blockFactory = blockFactory();
        try (
            DocVector local = docVector(blockFactory, blockFactory.newConstantIntVector(0, 1), 1);
            DocVector detached = local.detach(origins::get)
        ) {
            IllegalStateException e = expectThrows(IllegalStateException.class, () -> detached.detach(origins::get));
            assertThat(e.getMessage(), equalTo("DocVector is already detached"));
        }
    }

    public void testBreakerTripReleasesEverything() {
        int[] rowShards = { 0, 1, 2, 0 };
        testWithCrankyBlockFactory(cranky -> {
            try (DocBlock.Builder builder = DocBlock.newBlockBuilder(cranky, rowShards.length)) {
                for (int p = 0; p < rowShards.length; p++) {
                    builder.appendShard(rowShards[p]).appendSegment(0).appendDoc(p);
                }
                try (DocBlock local = builder.shardRefCounters(refCounteds()).build()) {
                    local.asVector().detach(origins::get).close();
                }
            }
        });
        for (CountingRefCounted shard : shards) {
            assertThat(shard.count.get(), equalTo(1));
        }
    }

    public void testRowsOfThisNodeHaveNoOrigin() {
        BlockFactory blockFactory = blockFactory();
        try (DocVector local = docVector(blockFactory, blockFactory.newConstantIntVector(0, 1), 1)) {
            assertNull(local.origins());
            local.ensureAttached("test");
            IllegalStateException e = expectThrows(IllegalStateException.class, () -> local.origin(0));
            assertThat(e.getMessage(), equalTo("DocVector has no origins"));
        }
    }

    /**
     * The ordinals of rows that left their node would name whatever shard holds that slot on the node that reads them.
     */
    public void testRowsThatLeftTheirNodeCantBeReadThroughShardContexts() {
        BlockFactory blockFactory = blockFactory();
        try (
            DocVector local = docVector(blockFactory, blockFactory.newConstantIntVector(0, 1), 1);
            DocVector detached = local.detach(origins::get)
        ) {
            IllegalStateException e = expectThrows(IllegalStateException.class, () -> detached.ensureAttached("reader"));
            assertThat(e.getMessage(), equalTo("[reader] can't read a detached DocVector"));
        }
    }

    public void testRowsWithOriginsPinNoShard() {
        BlockFactory blockFactory = blockFactory();
        IntVector ordinals = blockFactory.newConstantIntVector(0, 1);
        IntVector segments = blockFactory.newConstantIntVector(0, 1);
        IntVector docs = blockFactory.newConstantIntVector(0, 1);
        try {
            DocVector.Config config = DocVector.config().origins(DocOrigins.of(origins));
            IllegalArgumentException e = expectThrows(
                IllegalArgumentException.class,
                () -> new DocVector(refCounteds(), ordinals, segments, docs, config)
            );
            assertThat(e.getMessage(), equalTo("DocVector with origins can't reference shards"));
        } finally {
            Releasables.close(ordinals, segments, docs);
        }
    }

    private DocVector docVector(BlockFactory blockFactory, IntVector shardVector, int positions) {
        IntVector segments = blockFactory.newConstantIntVector(0, positions);
        IntVector docs = blockFactory.newIntRangeVector(0, positions);
        return new DocVector(refCounteds(), shardVector, segments, docs, DocVector.config());
    }

    private IndexedByShardId<CountingRefCounted> refCounteds() {
        return new IndexedByShardIdFromList<>(Arrays.asList(shards));
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
