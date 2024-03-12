/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.core.inference.rank;

import org.apache.lucene.search.Query;
import org.elasticsearch.TransportVersion;
import org.elasticsearch.TransportVersions;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.search.rank.RankBuilder;
import org.elasticsearch.search.rank.RankCoordinatorContext;
import org.elasticsearch.search.rank.RankShardContext;
import org.elasticsearch.xcontent.XContentBuilder;

import java.io.IOException;
import java.util.List;

public class TextSimilarityRankBuilder extends RankBuilder {

    private final String field;
    private final String modelText;
    private final String modelId;

    public TextSimilarityRankBuilder(String field, String modelId, String modelText, int windowSize) {
        super(windowSize);
        this.field = field;
        this.modelId = modelId;
        this.modelText = modelText;
    }

    public TextSimilarityRankBuilder(StreamInput in) throws IOException {
        super(in);
        this.field = in.readString();
        this.modelId = in.readString();
        this.modelText = in.readString();
    }

    @Override
    public String getWriteableName() {
        return "text_similarity_rank_builder";
    }

    @Override
    public TransportVersion getMinimalSupportedVersion() {
        return TransportVersions.V_8_11_X;
    }

    @Override
    protected void doWriteTo(StreamOutput out) throws IOException {
        out.writeString(field);
        out.writeString(modelId);
        out.writeString(modelText);
    }

    @Override
    protected void doXContent(XContentBuilder builder, Params params) throws IOException {
        throw new UnsupportedOperationException("not supported");
    }

    @Override
    public RankShardContext buildRankShardContext(List<Query> queries, int from) {
        return new TextSimilarityRankShardContext(queries, from, windowSize());
    }

    @Override
    public RankCoordinatorContext buildRankCoordinatorContext(int size, int from) {
        return new TextSimilarityRankCoordinatorContext(size, from, windowSize());
    }

    @Override
    protected boolean doEquals(RankBuilder other) {
        return false;
    }

    @Override
    protected int doHashCode() {
        return 0;
    }

}
