/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.compute.operator.topn;

import org.apache.lucene.util.ArrayUtil;
import org.apache.lucene.util.BytesRef;
import org.elasticsearch.compute.data.Block;
import org.elasticsearch.compute.data.BlockFactory;
import org.elasticsearch.compute.data.DocBlock;
import org.elasticsearch.compute.data.DocVector;
import org.elasticsearch.core.Releasables;

class ResultBuilderForDoc implements ResultBuilder {
    private static final int[] NO_OWNED_REFS = new int[0];

    private final DocVectorEncoder encoder;
    private final DocBlock.Builder builder;

    /**
     * The number of shard refs this builder owns, indexed by shard id. {@link #decodeValue} takes one
     * ref per row. The {@link DocVector} that {@link #build} returns adopts all of them. If we never
     * return that vector then {@link #close} releases them. Shard ids are bounded by the number of
     * shards the query reads, so this array stays small.
     */
    private int[] ownedRefsPerShard = NO_OWNED_REFS;

    ResultBuilderForDoc(BlockFactory blockFactory, DocVectorEncoder encoder, int positions) {
        this.encoder = encoder;
        this.builder = DocBlock.newBlockBuilder(blockFactory, positions);
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

        if (shard >= ownedRefsPerShard.length) {
            ownedRefsPerShard = ArrayUtil.grow(ownedRefsPerShard, shard + 1);
        }
        // Since rows can be closed before build is called, we need to increment the ref count to ensure the shard context isn't closed.
        encoder.refCounteds().get(shard).mustIncRef();
        // Count the ref before appending so close releases it if an append throws
        ownedRefsPerShard[shard]++;

        builder.appendShard(shard);
        builder.appendSegment(segment);
        builder.appendDoc(doc);
    }

    @Override
    public Block build() {
        DocVector.Config config = DocVector.config().dontIncrementShardRefCounts().mayContainDuplicates();
        // TODO figure out when we don't need to set mayContainDuplicates
        Block block = builder.shardRefCounters(encoder.refCounteds()).build(config);
        ownedRefsPerShard = NO_OWNED_REFS;
        return block;
    }

    @Override
    public long estimatedBytes() {
        return builder.estimatedBytes();
    }

    @Override
    public String toString() {
        return "ValueExtractorForDoc";
    }

    @Override
    public void close() {
        Releasables.closeExpectNoException(this::releaseOwnedRefs, builder);
    }

    private void releaseOwnedRefs() {
        for (int shard = 0; shard < ownedRefsPerShard.length; shard++) {
            for (int r = 0; r < ownedRefsPerShard[shard]; r++) {
                encoder.refCounteds().get(shard).decRef();
            }
        }
        ownedRefsPerShard = NO_OWNED_REFS;
    }
}
