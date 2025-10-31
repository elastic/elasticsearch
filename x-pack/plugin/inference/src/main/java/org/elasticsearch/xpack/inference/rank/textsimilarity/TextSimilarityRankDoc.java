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
import org.elasticsearch.features.NodeFeature;
import org.elasticsearch.search.rank.RankDoc;
import org.elasticsearch.xcontent.XContentBuilder;

import java.io.IOException;
import java.util.Objects;

public class TextSimilarityRankDoc extends RankDoc {

    public static final NodeFeature TEXT_SIMILARITY_RANK_DOC_EXPLAIN_CHUNKS = new NodeFeature("text_similarity_rank_doc_explain_chunks");
    private static final TransportVersion TEXT_SIMILARITY_RANK_DOC_EXPLAIN_CHUNKS_VERSION = TransportVersion.fromName(
        "text_similarity_rank_docs_explain_chunks"
    );

    public static final String NAME = "text_similarity_rank_doc";

    public final String inferenceId;
    public final String field;
    public final ChunkScorerConfig chunkScorerConfig;

    public TextSimilarityRankDoc(int doc, float score, int shardIndex) {
        this(doc, score, shardIndex, null, null, null);
    }

    public TextSimilarityRankDoc(
        int doc,
        float score,
        int shardIndex,
        String inferenceId,
        String field,
        ChunkScorerConfig chunkScorerConfig
    ) {
        super(doc, score, shardIndex);
        this.inferenceId = inferenceId;
        this.field = field;
        this.chunkScorerConfig = chunkScorerConfig;
    }

    public TextSimilarityRankDoc(StreamInput in) throws IOException {
        super(in);
        if (in.getTransportVersion().supports(TransportVersions.V_8_18_0)) {
            inferenceId = in.readOptionalString();
            field = in.readOptionalString();
        } else {
            inferenceId = in.readString();
            field = in.readString();
        }

        if (in.getTransportVersion().supports(TEXT_SIMILARITY_RANK_DOC_EXPLAIN_CHUNKS_VERSION)) {
            boolean hasChunkScorerConfig = in.readBoolean();
            chunkScorerConfig = hasChunkScorerConfig ? new ChunkScorerConfig(in) : null;
        } else {
            chunkScorerConfig = null;
        }
    }

    @Override
    public Explanation explain(Explanation[] sources, String[] queryNames) {
        assert inferenceId != null && field != null;
        final String queryAlias = queryNames[0] == null ? "" : "[" + queryNames[0] + "]";

        StringBuilder sb = new StringBuilder();

        sb.append("text_similarity_reranker match using inference endpoint: [")
            .append(inferenceId)
            .append("] on document field: [")
            .append(field)
            .append("] matching on source query ")
            .append(queryAlias);

        if (chunkScorerConfig != null) {
            sb.append("and rescoring considering only top [").append(chunkScorerConfig.sizeOrDefault()).append("] best chunks");
        }

        return Explanation.match(score, sb.toString(), sources);
    }

    @Override
    public void doWriteTo(StreamOutput out) throws IOException {
        if (out.getTransportVersion().supports(TransportVersions.V_8_18_0)) {
            out.writeOptionalString(inferenceId);
            out.writeOptionalString(field);
        } else {
            out.writeString(inferenceId == null ? "" : inferenceId);
            out.writeString(field == null ? "" : field);
        }
        if (out.getTransportVersion().supports(TEXT_SIMILARITY_RANK_DOC_EXPLAIN_CHUNKS_VERSION)) {
            boolean hasChunkScorerConfig = chunkScorerConfig != null;
            out.writeBoolean(hasChunkScorerConfig);
            if (hasChunkScorerConfig) {
                chunkScorerConfig.writeTo(out);
            }
        }
    }

    @Override
    public boolean doEquals(RankDoc rd) {
        TextSimilarityRankDoc tsrd = (TextSimilarityRankDoc) rd;
        return Objects.equals(inferenceId, tsrd.inferenceId)
            && Objects.equals(field, tsrd.field)
            && Objects.equals(chunkScorerConfig, tsrd.chunkScorerConfig);
    }

    @Override
    public int doHashCode() {
        return Objects.hash(inferenceId, field, chunkScorerConfig);
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
            + ", chunkScorerConfig="
            + chunkScorerConfig
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
        if (chunkScorerConfig != null) {
            builder.field("chunkScorerConfig", chunkScorerConfig);
        }
    }
}
