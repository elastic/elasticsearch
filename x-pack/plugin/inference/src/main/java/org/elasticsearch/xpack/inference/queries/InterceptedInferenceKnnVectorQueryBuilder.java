/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.inference.queries;

import org.apache.lucene.search.join.ScoreMode;
import org.apache.lucene.util.SetOnce;
import org.elasticsearch.TransportVersion;
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
import org.elasticsearch.plugins.internal.rewriter.QueryRewriteInterceptor;
import org.elasticsearch.search.vectors.KnnVectorQueryBuilder;
import org.elasticsearch.search.vectors.QueryVectorBuilder;
import org.elasticsearch.search.vectors.VectorData;
import org.elasticsearch.xpack.core.ml.inference.results.MlDenseEmbeddingResults;
import org.elasticsearch.xpack.core.ml.vectors.TextEmbeddingQueryVectorBuilder;
import org.elasticsearch.xpack.inference.mapper.SemanticTextField;
import org.elasticsearch.xpack.inference.mapper.SemanticTextFieldMapper;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.Map;

import static org.elasticsearch.transport.RemoteClusterAware.LOCAL_CLUSTER_GROUP_KEY;

public class InterceptedInferenceKnnVectorQueryBuilder extends InterceptedInferenceQueryBuilder<KnnVectorQueryBuilder> {
    public static final String NAME = "intercepted_inference_knn";

    @SuppressWarnings("deprecation")
    private static final QueryRewriteInterceptor BWC_INTERCEPTOR = new LegacySemanticKnnVectorQueryRewriteInterceptor();

    private static final TransportVersion NEW_SEMANTIC_QUERY_INTERCEPTORS = TransportVersion.fromName("new_semantic_query_interceptors");

    public InterceptedInferenceKnnVectorQueryBuilder(KnnVectorQueryBuilder originalQuery) {
        super(originalQuery);
    }

    public InterceptedInferenceKnnVectorQueryBuilder(
        KnnVectorQueryBuilder originalQuery,
        Map<FullyQualifiedInferenceId, InferenceResults> inferenceResultsMap
    ) {
        super(originalQuery, inferenceResultsMap);
    }

    public InterceptedInferenceKnnVectorQueryBuilder(StreamInput in) throws IOException {
        super(in);
    }

    private InterceptedInferenceKnnVectorQueryBuilder(
        InterceptedInferenceQueryBuilder<KnnVectorQueryBuilder> other,
        Map<FullyQualifiedInferenceId, InferenceResults> inferenceResultsMap,
        SetOnce<Map<FullyQualifiedInferenceId, InferenceResults>> inferenceResultsMapSupplier,
        boolean ccsRequest
    ) {
        super(other, inferenceResultsMap, inferenceResultsMapSupplier, ccsRequest);
    }

    @Override
    protected Map<String, Float> getFields() {
        return Map.of(getField(), 1.0f);
    }

    @Override
    protected String getQuery() {
        String query = null;
        QueryVectorBuilder queryVectorBuilder = originalQuery.queryVectorBuilder();
        if (queryVectorBuilder instanceof TextEmbeddingQueryVectorBuilder textEmbeddingQueryVectorBuilder) {
            query = textEmbeddingQueryVectorBuilder.getModelText();
        }

        return query;
    }

    @Override
    protected FullyQualifiedInferenceId getInferenceIdOverride() {
        String modelId = getQueryVectorBuilderModelId();
        return modelId != null ? new FullyQualifiedInferenceId(LOCAL_CLUSTER_GROUP_KEY, modelId) : null;
    }

    @Override
    protected InterceptedInferenceQueryBuilder<KnnVectorQueryBuilder> customDoRewriteGetInferenceResults(
        QueryRewriteContext queryRewriteContext
    ) throws IOException {
        // knn query may contain filters that are also intercepted.
        // We need to rewrite those here so that we can get inference results for them too.
        return rewriteFilterQueries(queryRewriteContext);
    }

    private InterceptedInferenceQueryBuilder<KnnVectorQueryBuilder> rewriteFilterQueries(QueryRewriteContext queryRewriteContext)
        throws IOException {
        boolean filtersChanged = false;
        List<QueryBuilder> rewrittenFilters = new ArrayList<>(originalQuery.filterQueries().size());
        for (QueryBuilder filter : originalQuery.filterQueries()) {
            QueryBuilder rewrittenFilter = filter.rewrite(queryRewriteContext);
            if (rewrittenFilter != filter) {
                filtersChanged = true;
            }
            rewrittenFilters.add(rewrittenFilter);
        }
        if (filtersChanged) {
            originalQuery.setFilterQueries(rewrittenFilters);
            return copy(inferenceResultsMap, inferenceResultsMapSupplier, ccsRequest);
        }
        return this;
    }

    @Override
    protected void coordinatorNodeValidate(ResolvedIndices resolvedIndices) {
        if (originalQuery.queryVector() == null && originalQuery.queryVectorBuilder() instanceof TextEmbeddingQueryVectorBuilder == false) {
            // This should never happen because either query vector or query vector builder must be non-null, which is enforced by the
            // KnnVectorQueryBuilder constructor. The only query vector builder used in production is TextEmbeddingQueryVectorBuilder,
            // thus if it is not this type it is null.
            // We could throw here _if_ we add a new query vector builder type and forget to update this class to support it, which would
            // be a server-side error.
            throw new IllegalStateException(
                "No [" + TextEmbeddingQueryVectorBuilder.NAME + "] query vector builder or query vector specified"
            );
        }

        // Check if we are querying any non-inference fields
        Collection<IndexMetadata> indexMetadataCollection = resolvedIndices.getConcreteLocalIndicesMetadata().values();
        for (IndexMetadata indexMetadata : indexMetadataCollection) {
            InferenceFieldMetadata inferenceFieldMetadata = indexMetadata.getInferenceFields().get(getField());
            if (inferenceFieldMetadata == null) {
                QueryVectorBuilder queryVectorBuilder = originalQuery.queryVectorBuilder();
                if (queryVectorBuilder instanceof TextEmbeddingQueryVectorBuilder textEmbeddingQueryVectorBuilder
                    && textEmbeddingQueryVectorBuilder.getModelId() == null) {
                    throw new IllegalArgumentException("[model_id] must not be null.");
                }
            }
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
    protected InterceptedInferenceQueryBuilder<KnnVectorQueryBuilder> copy(
        Map<FullyQualifiedInferenceId, InferenceResults> inferenceResultsMap,
        SetOnce<Map<FullyQualifiedInferenceId, InferenceResults>> inferenceResultsMapSupplier,
        boolean ccsRequest
    ) {
        return new InterceptedInferenceKnnVectorQueryBuilder(this, inferenceResultsMap, inferenceResultsMapSupplier, ccsRequest);
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

    private String getQueryVectorBuilderModelId() {
        String modelId = null;
        QueryVectorBuilder queryVectorBuilder = originalQuery.queryVectorBuilder();
        if (queryVectorBuilder instanceof TextEmbeddingQueryVectorBuilder textEmbeddingQueryVectorBuilder) {
            modelId = textEmbeddingQueryVectorBuilder.getModelId();
        }

        return modelId;
    }

    private QueryBuilder querySemanticTextField(String clusterAlias, SemanticTextFieldMapper.SemanticTextFieldType semanticTextFieldType) {
        MinimalServiceSettings modelSettings = semanticTextFieldType.getModelSettings();
        if (modelSettings == null) {
            // No inference results have been indexed yet
            return new MatchNoneQueryBuilder();
        } else if (modelSettings.taskType() != TaskType.TEXT_EMBEDDING) {
            throw new IllegalArgumentException("Field [" + getField() + "] does not use a [" + TaskType.TEXT_EMBEDDING + "] model");
        }

        VectorData queryVector = originalQuery.queryVector();
        if (queryVector == null) {
            FullyQualifiedInferenceId fullyQualifiedInferenceId = getInferenceIdOverride();
            if (fullyQualifiedInferenceId == null) {
                fullyQualifiedInferenceId = new FullyQualifiedInferenceId(clusterAlias, semanticTextFieldType.getSearchInferenceId());
            }

            MlDenseEmbeddingResults textEmbeddingResults = getTextEmbeddingResults(fullyQualifiedInferenceId);
            queryVector = new VectorData(textEmbeddingResults.getInferenceAsFloat());
        }

        KnnVectorQueryBuilder innerKnnQuery = new KnnVectorQueryBuilder(
            SemanticTextField.getEmbeddingsFieldName(getField()),
            queryVector,
            originalQuery.k(),
            originalQuery.numCands(),
            originalQuery.visitPercentage(),
            originalQuery.rescoreVectorBuilder(),
            originalQuery.getVectorSimilarity()
        );
        innerKnnQuery.addFilterQueries(originalQuery.filterQueries());

        return QueryBuilders.nestedQuery(SemanticTextField.getChunksFieldName(getField()), innerKnnQuery, ScoreMode.Max)
            .boost(originalQuery.boost())
            .queryName(originalQuery.queryName());
    }

    private QueryBuilder queryNonSemanticTextField() {
        VectorData queryVector = originalQuery.queryVector();
        if (queryVector == null) {
            FullyQualifiedInferenceId fullyQualifiedInferenceId = getInferenceIdOverride();
            if (fullyQualifiedInferenceId == null) {
                // This should never happen because we validate that either query vector or a valid query vector builder is specified in:
                // - The KnnVectorQueryBuilder constructor
                // - coordinatorNodeValidate
                throw new IllegalStateException("No query vector or query vector builder model ID specified");
            }

            MlDenseEmbeddingResults textEmbeddingResults = getTextEmbeddingResults(fullyQualifiedInferenceId);
            queryVector = new VectorData(textEmbeddingResults.getInferenceAsFloat());
        }

        KnnVectorQueryBuilder knnQuery = new KnnVectorQueryBuilder(
            getField(),
            queryVector,
            originalQuery.k(),
            originalQuery.numCands(),
            originalQuery.visitPercentage(),
            originalQuery.rescoreVectorBuilder(),
            originalQuery.getVectorSimilarity()
        ).boost(originalQuery.boost()).queryName(originalQuery.queryName());
        knnQuery.addFilterQueries(originalQuery.filterQueries());

        return knnQuery;
    }

    private MlDenseEmbeddingResults getTextEmbeddingResults(FullyQualifiedInferenceId fullyQualifiedInferenceId) {
        InferenceResults inferenceResults = inferenceResultsMap.get(fullyQualifiedInferenceId);
        if (inferenceResults == null) {
            throw new IllegalStateException("Could not find inference results from inference endpoint [" + fullyQualifiedInferenceId + "]");
        } else if (inferenceResults instanceof MlDenseEmbeddingResults == false) {
            throw new IllegalArgumentException(
                "Expected query inference results to be of type ["
                    + MlDenseEmbeddingResults.NAME
                    + "], got ["
                    + inferenceResults.getWriteableName()
                    + "]. Are you specifying a compatible inference endpoint? Has the inference endpoint configuration changed?"
            );
        }

        return (MlDenseEmbeddingResults) inferenceResults;
    }
}
