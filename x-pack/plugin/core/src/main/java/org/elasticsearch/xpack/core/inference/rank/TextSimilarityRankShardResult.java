/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.core.inference.rank;

import org.elasticsearch.TransportVersion;
import org.elasticsearch.TransportVersions;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.search.rank.RankShardResult;

import java.io.IOException;
import java.util.Objects;

public class TextSimilarityRankShardResult implements RankShardResult {

    public final TextSimilarityRankDoc[] rankDocs;

    public TextSimilarityRankShardResult(TextSimilarityRankDoc[] rankDocs) {
        this.rankDocs = Objects.requireNonNull(rankDocs);
    }

    public TextSimilarityRankShardResult(StreamInput in) throws IOException {
        this.rankDocs = in.readArray(TextSimilarityRankDoc::new, TextSimilarityRankDoc[]::new);
    }

    @Override
    public String getWriteableName() {
        return "text_similarity";
    }

    @Override
    public TransportVersion getMinimalSupportedVersion() {
        return TransportVersions.V_8_11_X;
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        out.writeArray(rankDocs);
    }

}
