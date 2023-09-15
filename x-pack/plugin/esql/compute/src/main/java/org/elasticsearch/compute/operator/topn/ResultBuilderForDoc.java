/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.compute.operator.topn;

import org.apache.lucene.util.BytesRef;
import org.elasticsearch.compute.data.Block;
import org.elasticsearch.compute.data.DocVector;
import org.elasticsearch.compute.data.IntArrayVector;

class ResultBuilderForDoc implements ResultBuilder {
    private final int[] shards;
    private final int[] segments;
    private final int[] docs;
    private int position;

    ResultBuilderForDoc(int positions) {
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
        shards[position] = TopNEncoder.DEFAULT_UNSORTABLE.decodeInt(values);
        segments[position] = TopNEncoder.DEFAULT_UNSORTABLE.decodeInt(values);
        docs[position] = TopNEncoder.DEFAULT_UNSORTABLE.decodeInt(values);
        position++;
    }

    @Override
    public Block build() {
        return new DocVector(
            new IntArrayVector(shards, position),
            new IntArrayVector(segments, position),
            new IntArrayVector(docs, position),
            null
        ).asBlock();
    }

    @Override
    public String toString() {
        return "ValueExtractorForDoc";
    }
}
