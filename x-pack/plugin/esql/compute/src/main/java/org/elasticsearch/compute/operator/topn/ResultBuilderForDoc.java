/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.compute.operator.topn;

import org.apache.lucene.util.BytesRef;
import org.elasticsearch.compute.data.Block;
import org.elasticsearch.compute.data.BlockFactory;
import org.elasticsearch.compute.data.DocVector;
import org.elasticsearch.compute.data.IntVector;
import org.elasticsearch.compute.lucene.ShardRefCounted;
import org.elasticsearch.compute.operator.DriverContext;
import org.elasticsearch.core.Nullable;
import org.elasticsearch.core.RefCounted;
import org.elasticsearch.core.Releasables;

import java.util.HashMap;
import java.util.Map;

class ResultBuilderForDoc implements ResultBuilder {
    private final BlockFactory blockFactory;
    private final int[] shards;
    /** {@link org.elasticsearch.compute.lucene.ShardContext#globalIndex()}. */
    private int globalShard = DocVector.NO_GLOBAL_SHARD;
    private final int[] segments;
    private final int[] docs;
    private final DriverContext.Phase phase;
    private int position;
    private @Nullable RefCounted nextRefCounted;
    private final Map<Integer, RefCounted> refCounted = new HashMap<>();

    ResultBuilderForDoc(BlockFactory blockFactory, int positions, DriverContext.Phase phase) {
        // TODO use fixed length builders
        this.blockFactory = blockFactory;
        this.shards = new int[positions];
        this.segments = new int[positions];
        this.docs = new int[positions];
        this.phase = phase;
    }

    @Override
    public void decodeKey(BytesRef keys) {
        throw new AssertionError("_doc can't be a key");
    }

    @Override
    public void setNextRefCounted(RefCounted nextRefCounted) {
        this.nextRefCounted = nextRefCounted;
        // Since rows can be closed before build is called, we need to increment the ref count to ensure the shard context isn't closed.
        this.nextRefCounted.mustIncRef();
    }

    @Override
    public void decodeValue(BytesRef values) {
        if (nextRefCounted == null) {
            throw new IllegalStateException("setNextRefCounted must be set before each decodeValue call");
        }
        shards[position] = TopNEncoder.DEFAULT_UNSORTABLE.decodeInt(values);
        int globalShard = TopNEncoder.DEFAULT_UNSORTABLE.decodeInt(values);
        switch (phase) {
            case OTHER -> {
                // We need to keep the global shard around for the reduce phase. However, since this we are in (presumably) local data
                // phase, all shards should share the same global shard.
                if (this.globalShard == DocVector.NO_GLOBAL_SHARD) {
                    this.globalShard = globalShard;
                } else {
                    if (globalShard != this.globalShard) {
                        throw new IllegalStateException(
                            "global shard must be the same for all rows, but got " + globalShard + " != " + this.globalShard
                        );
                    }
                }
            }
            // Swap the local index with the global one, so it can be used by later field extractors.
            case NODE_REDUCE -> shards[position] = globalShard;
        }
        segments[position] = TopNEncoder.DEFAULT_UNSORTABLE.decodeInt(values);
        docs[position] = TopNEncoder.DEFAULT_UNSORTABLE.decodeInt(values);
        refCounted.putIfAbsent(shards[position], nextRefCounted);
        position++;
        nextRefCounted = null;
    }

    @Override
    public Block build() {
        boolean success = false;
        IntVector shardsVector = null;
        IntVector segmentsVector = null;
        try {
            shardsVector = blockFactory.newIntArrayVector(shards, position);
            segmentsVector = blockFactory.newIntArrayVector(segments, position);
            var docsVector = blockFactory.newIntArrayVector(docs, position);
            DocVector docVector = new DocVector(
                new ShardRefCountedMap(refCounted),
                shardsVector,
                globalShard,
                segmentsVector,
                docsVector,
                null
            );
            var docsBlock = docVector.asBlock();
            success = true;
            return docsBlock;
        } finally {
            // The DocVector constructor already incremented the relevant RefCounted, so we can now decrement them since we incremented them
            // in setNextRefCounted.
            refCounted.values().forEach(RefCounted::decRef);
            if (success == false) {
                Releasables.closeExpectNoException(shardsVector, segmentsVector);
            }
        }
    }

    private record ShardRefCountedMap(Map<Integer, RefCounted> refCounters) implements ShardRefCounted {
        @Override
        public RefCounted get(int shardId) {
            return refCounters.get(shardId);
        }
    }

    @Override
    public String toString() {
        return "ValueExtractorForDoc";
    }

    @Override
    public void close() {
        // TODO memory accounting
    }
}
