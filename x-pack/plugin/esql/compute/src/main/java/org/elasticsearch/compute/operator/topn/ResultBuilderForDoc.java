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

class ResultBuilderForDoc implements ResultBuilder {
    private final DocVectorEncoder encoder;
    private final DocVector.FixedBuilder builder;

    ResultBuilderForDoc(BlockFactory blockFactory, DocVectorEncoder encoder, int positions) {
        this.encoder = encoder;
        this.builder = DocVector.newFixedBuilder(blockFactory, positions);
    }

    @Override
    public void decodeKey(BytesRef keys) {
        throw new AssertionError("_doc can't be a key");
    }

    @Override
    public void decodeValue(BytesRef values) {
        int shard = encoder.decodeInt(values);
        int segment = encoder.decodeInt(values);
        int doc = encoder.decodeInt(values);

        // Since rows can be closed before build is called, we need to increment the ref count to ensure the shard context isn't closed.
        encoder.refCounteds().get(shard).mustIncRef();

        builder.append(shard, segment, doc);
    }

    @Override
    public Block build() {
        DocVector.Config config = DocVector.config().dontIncrementShardRefCounts().mayContainDuplicates();
        // TODO figure out when we don't need to set mayContainDuplicates
        return builder.shardRefCounters(encoder.refCounteds()).build(config).asBlock();
    }

    @Override
    public String toString() {
        return "ValueExtractorForDoc";
    }

    @Override
    public void close() {
        builder.close();
    }
}
