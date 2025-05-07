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
import org.elasticsearch.core.Nullable;
import org.elasticsearch.core.RefCounted;
import org.elasticsearch.core.Releasables;

import java.util.HashMap;
import java.util.Map;

class ResultBuilderForDoc implements ResultBuilder {
    private final BlockFactory blockFactory;
    private final int[] shards;
    private final int[] segments;
    private final int[] docs;
    private int position;
    private @Nullable RefCounted nextRefCounted;
    private final Map<Integer, RefCounted> refCounted = new HashMap<>();

    ResultBuilderForDoc(BlockFactory blockFactory, int positions) {
        // TODO use fixed length builders
        this.blockFactory = blockFactory;
        this.shards = new int[positions];
        this.segments = new int[positions];
        this.docs = new int[positions];
    }

    @Override
    public void decodeKey(BytesRef keys) {
        throw new AssertionError("_doc can't be a key");
    }

    void setNextRefCounted(RefCounted nextRefCounted) {
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
            var docsBlock = new DocVector(new ShardRefCountedMap(refCounted), shardsVector, segmentsVector, docsVector, null).asBlock();
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
