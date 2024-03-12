/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.core.inference.rank;

import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.search.rank.RankDoc;

import java.io.IOException;

public class TextSimilarityRankDoc extends RankDoc {

    public TextSimilarityRankDoc(int doc, float score, int shardIndex) {
        super(doc, score, shardIndex);
    }

    public TextSimilarityRankDoc(StreamInput in) throws IOException {
        super(in);
    }

    @Override
    protected void doWriteTo(StreamOutput out) throws IOException {}

    @Override
    protected boolean doEquals(RankDoc rd) {
        return true;
    }

    @Override
    protected int doHashCode() {
        return 0;
    }

    @Override
    public String toString() {
        return "TextSimilarityRankDoc{" +
            "rank=" + rank +
            ", score=" + score +
            ", doc=" + doc +
            ", shardIndex=" + shardIndex +
            '}';
    }
}
