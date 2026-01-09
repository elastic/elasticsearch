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
import org.elasticsearch.core.Releasables;

class ResultBuilderForDoc implements ResultBuilder {
    private final BlockFactory blockFactory;
    private final DocVectorEncoder encoder;
    private final int[] shards;
    private final int[] segments;
    private final int[] docs;
    private int position;

    ResultBuilderForDoc(BlockFactory blockFactory, DocVectorEncoder encoder, int positions) {
        // TODO use fixed length builders
        this.blockFactory = blockFactory;
        this.encoder = encoder;
        this.shards = new int[positions];
        this.segments = new int[positions];
        this.docs = new int[positions];
    }

    @Override
    public void decodeKey(BytesRef keys) {
        throw new AssertionError("_doc can't be a key");
    }

    @Override
    public void decodeValue(BytesRef values) {
        int shard = encoder.decodeInt(values);
        shards[position] = shard;
        // Since rows can be closed before build is called, we need to increment the ref count to ensure the shard context isn't closed.
        encoder.refCounteds().get(shard).mustIncRef();
        segments[position] = encoder.decodeInt(values);
        docs[position] = encoder.decodeInt(values);
        position++;
    }

    @Override
    public Block build() {
        boolean success = false;
        IntVector shardsVector = null;
        IntVector segmentsVector = null;
        IntVector docsVector = null;

        try {
            shardsVector = blockFactory.newIntArrayVector(shards, position);
            segmentsVector = blockFactory.newIntArrayVector(segments, position);
            docsVector = blockFactory.newIntArrayVector(docs, position);
            var docsBlock = DocVector.withoutIncrementingShardRefCounts(encoder.refCounteds(), shardsVector, segmentsVector, docsVector)
                .asBlock();
            success = true;
            return docsBlock;
        } finally {
            if (success == false) {
                Releasables.closeExpectNoException(shardsVector, segmentsVector, docsVector);
            }
        }
    }

    @Override
    public String toString() {
        return "ValueExtractorForDoc";
    }

    @Override
    public void close() {
        // TODO Memory accounting
    }
}
