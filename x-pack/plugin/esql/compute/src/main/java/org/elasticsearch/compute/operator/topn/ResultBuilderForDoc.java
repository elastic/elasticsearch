/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.compute.operator.topn;

import org.apache.lucene.util.ArrayUtil;
import org.apache.lucene.util.BytesRef;
import org.elasticsearch.common.util.BigArrays;
import org.elasticsearch.common.util.LongHash;
import org.elasticsearch.compute.data.Block;
import org.elasticsearch.compute.data.BlockFactory;
import org.elasticsearch.compute.data.DocBlock;
import org.elasticsearch.compute.data.DocOrigin;
import org.elasticsearch.compute.data.DocOriginTable;
import org.elasticsearch.compute.data.DocOrigins;
import org.elasticsearch.compute.data.DocVector;
import org.elasticsearch.compute.lucene.AlwaysReferencedIndexedByShardId;
import org.elasticsearch.core.Nullable;
import org.elasticsearch.core.Releasable;
import org.elasticsearch.core.Releasables;

import java.util.ArrayList;
import java.util.List;

class ResultBuilderForDoc implements ResultBuilder {
    private static final int[] NO_OWNED_REFS = new int[0];

    private final DocVectorEncoder encoder;
    private final DocBlock.Builder builder;
    /**
     * The origins of the block for rows that left their node, {@code null} for rows of this node.
     */
    @Nullable
    private final BlockOrigins blockOrigins;

    /**
     * The number of shard refs this builder owns, indexed by shard id. {@link #decodeValue} takes one
     * ref per row. The {@link DocVector} that {@link #build} returns adopts all of them. If we never
     * return that vector then {@link #close} releases them. Shard ids are bounded by the number of
     * shards the query reads, so this array stays small.
     */
    private int[] ownedRefsPerShard = NO_OWNED_REFS;

    ResultBuilderForDoc(BlockFactory blockFactory, DocVectorEncoder encoder, int positions) {
        this.encoder = encoder;
        DocOriginTable table = encoder.origins();
        this.blockOrigins = table == null ? null : new BlockOrigins(table, blockFactory.bigArrays());
        boolean success = false;
        try {
            this.builder = DocBlock.newBlockBuilder(blockFactory, positions);
            success = true;
        } finally {
            if (success == false) {
                Releasables.closeExpectNoException(blockOrigins);
            }
        }
    }

    @Override
    public void decodeKey(BytesRef keys, boolean asc) {
        throw new AssertionError("_doc can't be a key");
    }

    @Override
    public void decodeValue(BytesRef values) {
        int shard = encoder.decodeInt(values);
        int segment = encoder.decodeInt(values);
        int doc = encoder.decodeInt(values);

        if (blockOrigins == null) {
            if (shard >= ownedRefsPerShard.length) {
                ownedRefsPerShard = ArrayUtil.grow(ownedRefsPerShard, shard + 1);
            }
            // Since rows can be closed before build is called, we need to increment the ref count to ensure the shard context isn't
            // closed.
            encoder.refCounteds().get(shard).mustIncRef();
            // Count the ref before appending so close releases it if an append throws
            ownedRefsPerShard[shard]++;
            builder.appendShard(shard);
        } else {
            builder.appendShard(blockOrigins.blockOrdinal(shard));
        }
        builder.appendSegment(segment);
        builder.appendDoc(doc);
    }

    @Override
    public Block build() {
        // TODO figure out when we don't need to set mayContainDuplicates
        if (blockOrigins != null) {
            DocVector.Config config = DocVector.config().origins(blockOrigins.build()).mayContainDuplicates();
            return builder.shardRefCounters(AlwaysReferencedIndexedByShardId.INSTANCE).build(config);
        }
        DocVector.Config config = DocVector.config().dontIncrementShardRefCounts().mayContainDuplicates();
        Block block = builder.shardRefCounters(encoder.refCounteds()).build(config);
        ownedRefsPerShard = NO_OWNED_REFS;
        return block;
    }

    @Override
    public long estimatedBytes() {
        return builder.estimatedBytes() + (blockOrigins == null ? 0 : blockOrigins.estimatedBytes());
    }

    @Override
    public String toString() {
        return "ValueExtractorForDoc";
    }

    @Override
    public void close() {
        Releasables.closeExpectNoException(this::releaseOwnedRefs, builder, blockOrigins);
    }

    private void releaseOwnedRefs() {
        for (int shard = 0; shard < ownedRefsPerShard.length; shard++) {
            for (int r = 0; r < ownedRefsPerShard[shard]; r++) {
                encoder.refCounteds().get(shard).decRef();
            }
        }
        ownedRefsPerShard = NO_OWNED_REFS;
    }

    /**
     * Numbers the origins of one block in the order its rows first reference them. TopN builds a block for every output
     * page, so the memory here follows the origins the page references rather than every origin the operator saw.
     */
    private static final class BlockOrigins implements Releasable {
        private final DocOriginTable table;
        /**
         * The block ordinal of each table ordinal seen so far, as the id the hash gives it. The hash gives ids in the
         * order keys first arrive, which is the order of {@link #origins}.
         */
        private final LongHash blockOrdinals;
        private final List<DocOrigin> origins = new ArrayList<>();
        private long originBytes;

        BlockOrigins(DocOriginTable table, BigArrays bigArrays) {
            this.table = table;
            this.blockOrdinals = new LongHash(1, bigArrays);
        }

        /**
         * The ordinal of the block's dictionary for the origin numbered {@code tableOrdinal} in the table of the operator.
         */
        int blockOrdinal(int tableOrdinal) {
            long ordinal = blockOrdinals.add(tableOrdinal);
            if (ordinal < 0) {
                return Math.toIntExact(-1 - ordinal);
            }
            DocOrigin origin = table.get(tableOrdinal);
            origins.add(origin);
            originBytes += origin.ramBytesUsed();
            return Math.toIntExact(ordinal);
        }

        /**
         * The bytes the {@link DocOrigins} that {@link #build} returns will charge to the breaker.
         */
        long estimatedBytes() {
            return DocOrigins.ramBytesEstimated(origins.size(), originBytes);
        }

        DocOrigins build() {
            return DocOrigins.of(origins);
        }

        @Override
        public void close() {
            blockOrdinals.close();
        }
    }
}
