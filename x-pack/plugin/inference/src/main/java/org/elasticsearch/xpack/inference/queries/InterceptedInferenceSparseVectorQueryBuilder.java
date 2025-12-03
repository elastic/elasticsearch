/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.inference.queries;

import org.apache.lucene.search.join.ScoreMode;
import org.elasticsearch.TransportVersion;
import org.elasticsearch.action.ResolvedIndices;
import org.elasticsearch.action.support.PlainActionFuture;
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

import static org.elasticsearch.transport.RemoteClusterAware.LOCAL_CLUSTER_GROUP_KEY;

public class InterceptedInferenceSparseVectorQueryBuilder extends InterceptedInferenceQueryBuilder<SparseVectorQueryBuilder> {
    public static final String NAME = "intercepted_inference_sparse_vector";

    @SuppressWarnings("deprecation")
    private static final QueryRewriteInterceptor BWC_INTERCEPTOR = new LegacySemanticSparseVectorQueryRewriteInterceptor();

    private static final TransportVersion NEW_SEMANTIC_QUERY_INTERCEPTORS = TransportVersion.fromName("new_semantic_query_interceptors");

    public InterceptedInferenceSparseVectorQueryBuilder(SparseVectorQueryBuilder originalQuery) {
        super(originalQuery);
    }

    public InterceptedInferenceSparseVectorQueryBuilder(
        SparseVectorQueryBuilder originalQuery,
        Map<FullyQualifiedInferenceId, InferenceResults> inferenceResultsMap
    ) {
        super(originalQuery, inferenceResultsMap);
    }

    public InterceptedInferenceSparseVectorQueryBuilder(StreamInput in) throws IOException {
        super(in);
    }

    private InterceptedInferenceSparseVectorQueryBuilder(
        InterceptedInferenceQueryBuilder<SparseVectorQueryBuilder> other,
        Map<FullyQualifiedInferenceId, InferenceResults> inferenceResultsMap,
        PlainActionFuture<InferenceQueryUtils.InferenceInfo> inferenceInfoFuture,
        boolean interceptedCcsRequest
    ) {
        super(other, inferenceResultsMap, inferenceInfoFuture, interceptedCcsRequest);
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
    protected FullyQualifiedInferenceId getInferenceIdOverride() {
        FullyQualifiedInferenceId override = null;
        String originalInferenceId = originalQuery.getInferenceId();
        if (originalInferenceId != null) {
            override = new FullyQualifiedInferenceId(LOCAL_CLUSTER_GROUP_KEY, originalInferenceId);
        }

        return override;
    }

    @Override
    protected boolean preInferenceCoordinatorNodeValidate(ResolvedIndices resolvedIndices) {
        // Check if we are querying any non-inference fields
        int inferenceFieldsQueried = 0;
        Collection<IndexMetadata> indexMetadataCollection = resolvedIndices.getConcreteLocalIndicesMetadata().values();
        for (IndexMetadata indexMetadata : indexMetadataCollection) {
            InferenceFieldMetadata inferenceFieldMetadata = indexMetadata.getInferenceFields().get(getField());
            if (inferenceFieldMetadata == null) {
                missingInferenceIdOverrideCheck();
            } else {
                inferenceFieldsQueried++;
            }
        }

        // We can skip remote cluster inference info gathering if:
        // - Inference fields are queried locally, guaranteeing that the query will be intercepted
        // - The inference ID override or query vector is set. In either case, remote cluster inference results are not required.
        return inferenceFieldsQueried > 0 && (getInferenceIdOverride() != null || originalQuery.getQueryVectors() != null);
    }

    @Override
    protected void postInferenceCoordinatorNodeValidate(InferenceQueryUtils.InferenceInfo inferenceInfo) {
        // Detect if we are querying any non-inference fields locally or remotely. We can do this by comparing the inference field count to
        // the index count. Since the sparse vector query is a single-field query, they should match if we are querying only inference
        // fields.
        if (inferenceInfo.inferenceFieldCount() < inferenceInfo.indexCount()) {
            missingInferenceIdOverrideCheck();
        }
    }

    @Override
    protected QueryBuilder doRewriteBwC(QueryRewriteContext queryRewriteContext) throws IOException {
        QueryBuilder rewritten = this;
        if (queryRewriteContext.getMinTransportVersion().supports(NEW_SEMANTIC_QUERY_INTERCEPTORS) == false) {
            rewritten = BWC_INTERCEPTOR.interceptAndRewrite(queryRewriteContext, originalQuery);
        }

        return rewritten;
    }

    @Override
    protected InterceptedInferenceQueryBuilder<SparseVectorQueryBuilder> copy(
        Map<FullyQualifiedInferenceId, InferenceResults> inferenceResultsMap,
        PlainActionFuture<InferenceQueryUtils.InferenceInfo> inferenceInfoFuture,
        boolean interceptedCcsRequest
    ) {
        return new InterceptedInferenceSparseVectorQueryBuilder(this, inferenceResultsMap, inferenceInfoFuture, interceptedCcsRequest);
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
            rewritten = querySemanticTextField(indexMetadataContext.getLocalClusterAlias(), semanticTextFieldType);
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

    private QueryBuilder querySemanticTextField(String clusterAlias, SemanticTextFieldMapper.SemanticTextFieldType semanticTextFieldType) {
        MinimalServiceSettings modelSettings = semanticTextFieldType.getModelSettings();
        if (modelSettings == null) {
            // No inference results have been indexed yet
            return new MatchNoneQueryBuilder();
        } else if (modelSettings.taskType() != TaskType.SPARSE_EMBEDDING) {
            throw new IllegalArgumentException("Field [" + getField() + "] does not use a [" + TaskType.SPARSE_EMBEDDING + "] model");
        }

        List<WeightedToken> queryVector = originalQuery.getQueryVectors();
        if (queryVector == null) {
            FullyQualifiedInferenceId fullyQualifiedInferenceId = getInferenceIdOverride();
            if (fullyQualifiedInferenceId == null) {
                fullyQualifiedInferenceId = new FullyQualifiedInferenceId(clusterAlias, semanticTextFieldType.getSearchInferenceId());
            }

            queryVector = getQueryVector(fullyQualifiedInferenceId);
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
            FullyQualifiedInferenceId fullyQualifiedInferenceId = getInferenceIdOverride();
            if (fullyQualifiedInferenceId == null) {
                throw new IllegalArgumentException("Either query vector or inference ID must be specified");
            }

            queryVector = getQueryVector(fullyQualifiedInferenceId);
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

    private List<WeightedToken> getQueryVector(FullyQualifiedInferenceId fullyQualifiedInferenceId) {
        InferenceResults inferenceResults = inferenceResultsMap.get(fullyQualifiedInferenceId);
        if (inferenceResults == null) {
            throw new IllegalStateException("Could not find inference results from inference endpoint [" + fullyQualifiedInferenceId + "]");
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

    private void missingInferenceIdOverrideCheck() {
        if (originalQuery.getQuery() != null && originalQuery.getInferenceId() == null) {
            throw new IllegalArgumentException(
                SparseVectorQueryBuilder.INFERENCE_ID_FIELD.getPreferredName() + " required to perform vector search on query string"
            );
        }
    }
}
