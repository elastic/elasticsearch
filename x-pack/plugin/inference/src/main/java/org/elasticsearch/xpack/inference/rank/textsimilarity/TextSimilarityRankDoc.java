/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.inference.rank.textsimilarity;

import org.apache.lucene.search.Explanation;
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

    public TextSimilarityRankDoc(int doc, float score, int shardIndex) {
        this(doc, score, shardIndex, null, null);
    }

    public TextSimilarityRankDoc(int doc, float score, int shardIndex, String inferenceId, String field) {
        super(doc, score, shardIndex);
        this.inferenceId = inferenceId;
        this.field = field;
    }

    public TextSimilarityRankDoc(StreamInput in) throws IOException {
        super(in);
        if (in.getTransportVersion().onOrAfter(TransportVersions.RANK_DOC_OPTIONAL_METADATA_FOR_EXPLAIN)) {
            inferenceId = in.readOptionalString();
            field = in.readOptionalString();
        } else {
            inferenceId = in.readString();
            field = in.readString();
        }
    }

    @Override
    public Explanation explain(Explanation[] sources, String[] queryNames) {
        assert inferenceId != null && field != null;
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
        if (out.getTransportVersion().onOrAfter(TransportVersions.RANK_DOC_OPTIONAL_METADATA_FOR_EXPLAIN)) {
            out.writeOptionalString(inferenceId);
            out.writeOptionalString(field);
        } else {
            out.writeString(inferenceId == null ? "" : inferenceId);
            out.writeString(field == null ? "" : field);
        }
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
        if (inferenceId != null) {
            builder.field("inferenceId", inferenceId);
        }
        if (field != null) {
            builder.field("field", field);
        }
    }
}
