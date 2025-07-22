/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.inference.rank.textsimilarity;

import org.apache.lucene.search.Explanation;
import org.apache.lucene.search.Query;
import org.elasticsearch.TransportVersion;
import org.elasticsearch.TransportVersions;
import org.elasticsearch.client.internal.Client;
import org.elasticsearch.common.Strings;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.index.query.MatchQueryBuilder;
import org.elasticsearch.index.query.QueryBuilder;
import org.elasticsearch.index.query.QueryRewriteContext;
import org.elasticsearch.license.License;
import org.elasticsearch.license.LicensedFeature;
import org.elasticsearch.search.rank.RankBuilder;
import org.elasticsearch.search.rank.RankDoc;
import org.elasticsearch.search.rank.context.QueryPhaseRankCoordinatorContext;
import org.elasticsearch.search.rank.context.QueryPhaseRankShardContext;
import org.elasticsearch.search.rank.context.RankFeaturePhaseRankCoordinatorContext;
import org.elasticsearch.search.rank.context.RankFeaturePhaseRankShardContext;
import org.elasticsearch.search.rank.feature.RankFeatureDoc;
import org.elasticsearch.search.rank.feature.RerankSnippetInput;
import org.elasticsearch.xcontent.XContentBuilder;

import java.io.IOException;
import java.util.List;
import java.util.Objects;

import static org.elasticsearch.xpack.inference.rank.textsimilarity.TextSimilarityRankRetrieverBuilder.FAILURES_ALLOWED_FIELD;
import static org.elasticsearch.xpack.inference.rank.textsimilarity.TextSimilarityRankRetrieverBuilder.FIELD_FIELD;
import static org.elasticsearch.xpack.inference.rank.textsimilarity.TextSimilarityRankRetrieverBuilder.INFERENCE_ID_FIELD;
import static org.elasticsearch.xpack.inference.rank.textsimilarity.TextSimilarityRankRetrieverBuilder.INFERENCE_TEXT_FIELD;
import static org.elasticsearch.xpack.inference.rank.textsimilarity.TextSimilarityRankRetrieverBuilder.MIN_SCORE_FIELD;
import static org.elasticsearch.xpack.inference.rank.textsimilarity.TextSimilarityRankRetrieverBuilder.SNIPPETS_FIELD;
import static org.elasticsearch.xpack.inference.services.elasticsearch.ElasticsearchInternalService.DEFAULT_RERANK_ID;
import static org.elasticsearch.xpack.inference.services.elasticsearch.ElasticsearchInternalService.RERANKER_ID;

/**
 * A {@code RankBuilder} that enables ranking with text similarity model inference. Supports parameters for configuring the inference call.
 */
public class TextSimilarityRankBuilder extends RankBuilder {

    public static final String NAME = "text_similarity_reranker";

    /**
     * The default token size limit of the Elastic reranker is 512.
     */
    private static final int RERANK_TOKEN_SIZE_LIMIT = 512;

    /**
     * 4096 is a safe default token size limit for other reranker models.
     * Reranker models with smaller token limits will be truncated.
     */
    private static final int DEFAULT_TOKEN_SIZE_LIMIT = 4_096;

    public static final LicensedFeature.Momentary TEXT_SIMILARITY_RERANKER_FEATURE = LicensedFeature.momentary(
        null,
        "text-similarity-reranker",
        License.OperationMode.ENTERPRISE
    );

    private final String inferenceId;
    private final String inferenceText;
    private final String field;
    private final Float minScore;
    private final boolean failuresAllowed;
    private final RerankSnippetInput rerankSnippetInput;

    public TextSimilarityRankBuilder(
        String field,
        String inferenceId,
        String inferenceText,
        int rankWindowSize,
        Float minScore,
        boolean failuresAllowed,
        RerankSnippetInput rerankSnippetInput
    ) {
        super(rankWindowSize);
        this.inferenceId = inferenceId;
        this.inferenceText = inferenceText;
        this.field = field;
        this.minScore = minScore;
        this.failuresAllowed = failuresAllowed;
        this.rerankSnippetInput = rerankSnippetInput;
    }

    public TextSimilarityRankBuilder(StreamInput in) throws IOException {
        super(in);
        // rankWindowSize deserialization is handled by the parent class RankBuilder
        this.inferenceId = in.readString();
        this.inferenceText = in.readString();
        this.field = in.readString();
        this.minScore = in.readOptionalFloat();
        if (in.getTransportVersion().isPatchFrom(TransportVersions.RERANKER_FAILURES_ALLOWED_8_19)
            || in.getTransportVersion().onOrAfter(TransportVersions.RERANKER_FAILURES_ALLOWED)) {
            this.failuresAllowed = in.readBoolean();
        } else {
            this.failuresAllowed = false;
        }
        if (in.getTransportVersion().onOrAfter(TransportVersions.RERANK_SNIPPETS)) {
            this.rerankSnippetInput = in.readOptionalWriteable(RerankSnippetInput::new);
        } else {
            this.rerankSnippetInput = null;
        }
    }

    @Override
    public String getWriteableName() {
        return NAME;
    }

    @Override
    public TransportVersion getMinimalSupportedVersion() {
        return TransportVersions.V_8_15_0;
    }

    @Override
    public void doWriteTo(StreamOutput out) throws IOException {
        // rankWindowSize serialization is handled by the parent class RankBuilder
        out.writeString(inferenceId);
        out.writeString(inferenceText);
        out.writeString(field);
        out.writeOptionalFloat(minScore);
        if (out.getTransportVersion().isPatchFrom(TransportVersions.RERANKER_FAILURES_ALLOWED_8_19)
            || out.getTransportVersion().onOrAfter(TransportVersions.RERANKER_FAILURES_ALLOWED)) {
            out.writeBoolean(failuresAllowed);
        }
        if (out.getTransportVersion().onOrAfter(TransportVersions.RERANK_SNIPPETS)) {
            out.writeOptionalWriteable(rerankSnippetInput);
        }
    }

    @Override
    public void doXContent(XContentBuilder builder, Params params) throws IOException {
        // this object is not parsed, but it sometimes needs to be output as xcontent
        // rankWindowSize serialization is handled by the parent class RankBuilder
        builder.field(INFERENCE_ID_FIELD.getPreferredName(), inferenceId);
        builder.field(INFERENCE_TEXT_FIELD.getPreferredName(), inferenceText);
        builder.field(FIELD_FIELD.getPreferredName(), field);
        if (minScore != null) {
            builder.field(MIN_SCORE_FIELD.getPreferredName(), minScore);
        }
        if (failuresAllowed) {
            builder.field(FAILURES_ALLOWED_FIELD.getPreferredName(), true);
        }
        if (rerankSnippetInput != null) {
            builder.field(SNIPPETS_FIELD.getPreferredName(), rerankSnippetInput);
        }
    }

    @Override
    public RankBuilder rewrite(QueryRewriteContext queryRewriteContext) throws IOException {
        TextSimilarityRankBuilder rewritten = this;
        if (rerankSnippetInput != null) {
            QueryBuilder snippetQueryBuilder = rerankSnippetInput.snippetQueryBuilder();
            if (snippetQueryBuilder == null) {
                rewritten = new TextSimilarityRankBuilder(
                    field,
                    inferenceId,
                    inferenceText,
                    rankWindowSize(),
                    minScore,
                    failuresAllowed,
                    new RerankSnippetInput(
                        rerankSnippetInput.numSnippets(),
                        rerankSnippetInput.inferenceText(),
                        rerankSnippetInput.tokenSizeLimit(),
                        new MatchQueryBuilder(field, inferenceText)
                    )
                );
            } else {
                QueryBuilder rewrittenSnippetQueryBuilder = snippetQueryBuilder.rewrite(queryRewriteContext);
                if (snippetQueryBuilder != rewrittenSnippetQueryBuilder) {
                    rewritten = new TextSimilarityRankBuilder(
                        field,
                        inferenceId,
                        inferenceText,
                        rankWindowSize(),
                        minScore,
                        failuresAllowed,
                        new RerankSnippetInput(
                            rerankSnippetInput.numSnippets(),
                            rerankSnippetInput.inferenceText(),
                            rerankSnippetInput.tokenSizeLimit(),
                            rewrittenSnippetQueryBuilder
                        )
                    );
                }
            }
        }

        return rewritten;
    }

    @Override
    public boolean isCompoundBuilder() {
        return false;
    }

    @Override
    public Explanation explainHit(Explanation baseExplanation, RankDoc scoreDoc, List<String> queryNames) {
        if (scoreDoc == null) {
            return baseExplanation;
        }
        if (false == baseExplanation.isMatch()) {
            return baseExplanation;
        }

        assert scoreDoc instanceof RankFeatureDoc : "ScoreDoc is not an instance of RankFeatureDoc";
        RankFeatureDoc rrfRankDoc = (RankFeatureDoc) scoreDoc;

        return Explanation.match(
            rrfRankDoc.score,
            "rank after reranking: ["
                + rrfRankDoc.rank
                + "] with score: ["
                + rrfRankDoc.score
                + "], using inference endpoint: ["
                + inferenceId
                + "] on document field: ["
                + field
                + "]",
            baseExplanation
        );
    }

    @Override
    public QueryPhaseRankShardContext buildQueryPhaseShardContext(List<Query> queries, int from) {
        return null;
    }

    @Override
    public QueryPhaseRankCoordinatorContext buildQueryPhaseCoordinatorContext(int size, int from) {
        return null;
    }

    @Override
    public RankFeaturePhaseRankShardContext buildRankFeaturePhaseShardContext() {
        return new TextSimilarityRerankingRankFeaturePhaseRankShardContext(field, rerankSnippetInput);
    }

    @Override
    public RankFeaturePhaseRankCoordinatorContext buildRankFeaturePhaseCoordinatorContext(int size, int from, Client client) {
        return new TextSimilarityRankFeaturePhaseRankCoordinatorContext(
            size,
            from,
            rankWindowSize(),
            client,
            inferenceId,
            inferenceText,
            minScore,
            failuresAllowed,
            rerankSnippetInput != null
                ? new RerankSnippetInput(rerankSnippetInput.numSnippets, inferenceText, tokenSizeLimit(inferenceId))
                : null
        );
    }

    /**
     * @return The token size limit to apply to this rerank context.
     * TODO: This should be pulled from the inference endpoint when available, not hardcoded.
     */
    public static Integer tokenSizeLimit(String inferenceId) {
        if (inferenceId.equals(DEFAULT_RERANK_ID) || inferenceId.equals(RERANKER_ID)) {
            return RERANK_TOKEN_SIZE_LIMIT;
        }
        return DEFAULT_TOKEN_SIZE_LIMIT;
    }

    public String field() {
        return field;
    }

    public String inferenceId() {
        return inferenceId;
    }

    public String inferenceText() {
        return inferenceText;
    }

    public Float minScore() {
        return minScore;
    }

    public boolean failuresAllowed() {
        return failuresAllowed;
    }

    @Override
    protected boolean doEquals(RankBuilder other) {
        TextSimilarityRankBuilder that = (TextSimilarityRankBuilder) other;
        return Objects.equals(inferenceId, that.inferenceId)
            && Objects.equals(inferenceText, that.inferenceText)
            && Objects.equals(field, that.field)
            && Objects.equals(minScore, that.minScore)
            && failuresAllowed == that.failuresAllowed
            && Objects.equals(rerankSnippetInput, that.rerankSnippetInput);
    }

    @Override
    protected int doHashCode() {
        return Objects.hash(inferenceId, inferenceText, field, minScore, failuresAllowed, rerankSnippetInput);
    }

    @Override
    public String toString() {
        return Strings.toString(this);
    }
}
