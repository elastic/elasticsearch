/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.inference.rank.textsimilarity;

import org.apache.lucene.search.Explanation;
import org.elasticsearch.TransportVersion;
import org.elasticsearch.TransportVersions;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.search.rank.RankDoc;
import org.elasticsearch.xcontent.XContentBuilder;

import java.io.IOException;
import java.util.Objects;

public class TextSimilarityRankDoc extends RankDoc {

    public static final String NAME = "text_similarity_rank_doc";

    public final String inferenceId;
    public final String field;

    public TextSimilarityRankDoc(int doc, float score, int shardIndex, String inferenceId, String field) {
        super(doc, score, shardIndex);
        this.inferenceId = inferenceId;
        this.field = field;
    }

    public TextSimilarityRankDoc(StreamInput in) throws IOException {
        super(in);
        inferenceId = in.readString();
        field = in.readString();
    }

    @Override
    public Explanation explain(Explanation[] sources, String[] queryNames) {
        final String queryAlias = queryNames[0] == null ? "" : "[" + queryNames[0] + "]";
        return Explanation.match(
            score,
            "text_similarity_reranker match using inference endpoint: ["
                + inferenceId
                + "] on document field: ["
                + field
                + "] matching on source query "
                + queryAlias,
            sources
        );
    }

    @Override
    public void doWriteTo(StreamOutput out) throws IOException {
        out.writeString(inferenceId);
        out.writeString(field);
    }

    @Override
    public boolean doEquals(RankDoc rd) {
        TextSimilarityRankDoc tsrd = (TextSimilarityRankDoc) rd;
        return Objects.equals(inferenceId, tsrd.inferenceId) && Objects.equals(field, tsrd.field);
    }

    @Override
    public int doHashCode() {
        return Objects.hash(inferenceId, field);
    }

    @Override
    public String toString() {
        return "TextSimilarityRankDoc{"
            + "doc="
            + doc
            + ", shardIndex="
            + shardIndex
            + ", score="
            + score
            + ", inferenceId="
            + inferenceId
            + ", field="
            + field
            + '}';
    }

    @Override
    public String getWriteableName() {
        return NAME;
    }

    @Override
    protected void doToXContent(XContentBuilder builder, Params params) throws IOException {
        builder.field("inferenceId", inferenceId);
        builder.field("field", field);
    }

    @Override
    public TransportVersion getMinimalSupportedVersion() {
        return TransportVersions.TEXT_SIMILARITY_RERANKER_QUERY_REWRITE;
    }
}
