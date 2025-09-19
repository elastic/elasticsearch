/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.inference.queries;

import org.apache.lucene.search.join.ScoreMode;
import org.elasticsearch.TransportVersions;
import org.elasticsearch.action.ResolvedIndices;
import org.elasticsearch.cluster.metadata.IndexMetadata;
import org.elasticsearch.cluster.metadata.InferenceFieldMetadata;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.index.mapper.MappedFieldType;
import org.elasticsearch.index.query.MatchNoneQueryBuilder;
import org.elasticsearch.index.query.QueryBuilder;
import org.elasticsearch.index.query.QueryBuilders;
import org.elasticsearch.index.query.QueryRewriteContext;
import org.elasticsearch.inference.InferenceResults;
import org.elasticsearch.inference.MinimalServiceSettings;
import org.elasticsearch.inference.TaskType;
import org.elasticsearch.inference.WeightedToken;
import org.elasticsearch.plugins.internal.rewriter.QueryRewriteInterceptor;
import org.elasticsearch.xpack.core.ml.inference.results.TextExpansionResults;
import org.elasticsearch.xpack.core.ml.search.SparseVectorQueryBuilder;
import org.elasticsearch.xpack.inference.mapper.SemanticTextField;
import org.elasticsearch.xpack.inference.mapper.SemanticTextFieldMapper;

import java.io.IOException;
import java.util.Collection;
import java.util.List;
import java.util.Map;

public class InterceptedInferenceSparseVectorQueryBuilder extends InterceptedInferenceQueryBuilder<SparseVectorQueryBuilder> {
    public static final String NAME = "intercepted_inference_sparse_vector";

    @SuppressWarnings("deprecation")
    private static final QueryRewriteInterceptor BWC_INTERCEPTOR = new LegacySemanticSparseVectorQueryRewriteInterceptor();

    public InterceptedInferenceSparseVectorQueryBuilder(SparseVectorQueryBuilder originalQuery) {
        super(originalQuery);
    }

    public InterceptedInferenceSparseVectorQueryBuilder(StreamInput in) throws IOException {
        super(in);
    }

    public InterceptedInferenceSparseVectorQueryBuilder(
        InterceptedInferenceQueryBuilder<SparseVectorQueryBuilder> other,
        Map<String, InferenceResults> inferenceResultsMap
    ) {
        super(other, inferenceResultsMap);
    }

    @Override
    protected Map<String, Float> getFields() {
        return Map.of(getField(), 1.0f);
    }

    @Override
    protected String getQuery() {
        return originalQuery.getQuery();
    }

    @Override
    protected String getInferenceIdOverride() {
        return originalQuery.getInferenceId();
    }

    @Override
    protected void coordinatorNodeValidate(ResolvedIndices resolvedIndices) {
        // Check if we are querying any non-inference fields
        Collection<IndexMetadata> indexMetadataCollection = resolvedIndices.getConcreteLocalIndicesMetadata().values();
        for (IndexMetadata indexMetadata : indexMetadataCollection) {
            InferenceFieldMetadata inferenceFieldMetadata = indexMetadata.getInferenceFields().get(getField());
            if (inferenceFieldMetadata == null && originalQuery.getQuery() != null && originalQuery.getInferenceId() == null) {
                // We are querying a non-inference field and need to generate inference results for a query string, but the inference ID
                // was not specified
                throw new IllegalArgumentException(
                    SparseVectorQueryBuilder.INFERENCE_ID_FIELD.getPreferredName() + " required to perform vector search on query string"
                );
            }
        }
    }

    @Override
    protected QueryBuilder doRewriteBwC(QueryRewriteContext queryRewriteContext) {
        QueryBuilder rewritten = this;
        if (queryRewriteContext.getMinTransportVersion().before(TransportVersions.NEW_SEMANTIC_QUERY_INTERCEPTORS)) {
            rewritten = BWC_INTERCEPTOR.interceptAndRewrite(queryRewriteContext, originalQuery);
        }

        return rewritten;
    }

    @Override
    protected QueryBuilder copy(Map<String, InferenceResults> inferenceResultsMap) {
        return new InterceptedInferenceSparseVectorQueryBuilder(this, inferenceResultsMap);
    }

    @Override
    protected QueryBuilder queryFields(
        Map<String, Float> inferenceFields,
        Map<String, Float> nonInferenceFields,
        QueryRewriteContext indexMetadataContext
    ) {
        QueryBuilder rewritten;
        MappedFieldType fieldType = indexMetadataContext.getFieldType(getField());
        if (fieldType == null) {
            rewritten = new MatchNoneQueryBuilder();
        } else if (fieldType instanceof SemanticTextFieldMapper.SemanticTextFieldType semanticTextFieldType) {
            rewritten = querySemanticTextField(semanticTextFieldType);
        } else {
            rewritten = queryNonSemanticTextField();
        }

        return rewritten;
    }

    @Override
    protected boolean resolveWildcards() {
        return false;
    }

    @Override
    protected boolean useDefaultFields() {
        return false;
    }

    @Override
    public String getWriteableName() {
        return NAME;
    }

    private String getField() {
        return originalQuery.getFieldName();
    }

    private QueryBuilder querySemanticTextField(SemanticTextFieldMapper.SemanticTextFieldType semanticTextFieldType) {
        MinimalServiceSettings modelSettings = semanticTextFieldType.getModelSettings();
        if (modelSettings == null) {
            // No inference results have been indexed yet
            return new MatchNoneQueryBuilder();
        } else if (modelSettings.taskType() != TaskType.SPARSE_EMBEDDING) {
            throw new IllegalArgumentException("Field [" + getField() + "] does not use a [" + TaskType.SPARSE_EMBEDDING + "] model");
        }

        List<WeightedToken> queryVector = originalQuery.getQueryVectors();
        if (queryVector == null) {
            String inferenceId = originalQuery.getInferenceId();
            if (inferenceId == null) {
                inferenceId = semanticTextFieldType.getSearchInferenceId();
            }

            queryVector = getQueryVector(inferenceId);
        }

        SparseVectorQueryBuilder innerSparseVectorQuery = new SparseVectorQueryBuilder(
            SemanticTextField.getEmbeddingsFieldName(getField()),
            queryVector,
            null,
            null,
            originalQuery.shouldPruneTokens(),
            originalQuery.getTokenPruningConfig()
        );

        return QueryBuilders.nestedQuery(SemanticTextField.getChunksFieldName(getField()), innerSparseVectorQuery, ScoreMode.Max)
            .boost(originalQuery.boost())
            .queryName(originalQuery.queryName());
    }

    private QueryBuilder queryNonSemanticTextField() {
        List<WeightedToken> queryVector = originalQuery.getQueryVectors();
        if (queryVector == null) {
            String inferenceId = originalQuery.getInferenceId();
            if (inferenceId == null) {
                throw new IllegalArgumentException("Either query vector or inference ID must be specified");
            }

            queryVector = getQueryVector(inferenceId);
        }

        return new SparseVectorQueryBuilder(
            getField(),
            queryVector,
            null,
            null,
            originalQuery.shouldPruneTokens(),
            originalQuery.getTokenPruningConfig()
        ).boost(originalQuery.boost()).queryName(originalQuery.queryName());
    }

    private List<WeightedToken> getQueryVector(String inferenceId) {
        InferenceResults inferenceResults = inferenceResultsMap.get(inferenceId);
        if (inferenceResults == null) {
            throw new IllegalStateException("Could not find inference results from inference endpoint [" + inferenceId + "]");
        } else if (inferenceResults instanceof TextExpansionResults == false) {
            throw new IllegalArgumentException(
                "Expected query inference results to be of type ["
                    + TextExpansionResults.NAME
                    + "], got ["
                    + inferenceResults.getWriteableName()
                    + "]. Are you specifying a compatible inference endpoint? Has the inference endpoint configuration changed?"
            );
        }

        TextExpansionResults textExpansionResults = (TextExpansionResults) inferenceResults;
        return textExpansionResults.getWeightedTokens();
    }
}
