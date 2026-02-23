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
import org.elasticsearch.action.support.PlainActionFuture;
import org.elasticsearch.cluster.metadata.IndexMetadata;
import org.elasticsearch.cluster.metadata.InferenceFieldMetadata;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
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
import org.elasticsearch.search.vectors.QueryVectorBuilderAsyncAction;
import org.elasticsearch.search.vectors.VectorData;
import org.elasticsearch.xcontent.XContentBuilder;
import org.elasticsearch.xpack.core.ml.inference.results.MlDenseEmbeddingResults;
import org.elasticsearch.xpack.core.ml.vectors.TextEmbeddingQueryVectorBuilder;
import org.elasticsearch.xpack.inference.mapper.SemanticTextField;
import org.elasticsearch.xpack.inference.mapper.SemanticTextFieldMapper;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.Map;

public class InterceptedInferenceKnnVectorQueryBuilder extends InterceptedInferenceQueryBuilder<KnnVectorQueryBuilder> {
    public static final String NAME = "intercepted_inference_knn";

    @SuppressWarnings("deprecation")
    private static final QueryRewriteInterceptor BWC_INTERCEPTOR = new LegacySemanticKnnVectorQueryRewriteInterceptor();

    private static final TransportVersion NEW_SEMANTIC_QUERY_INTERCEPTORS = TransportVersion.fromName("new_semantic_query_interceptors");

    private final SetOnce<float[]> queryVectorSupplier;

    public InterceptedInferenceKnnVectorQueryBuilder(KnnVectorQueryBuilder originalQuery) {
        super(originalQuery);
        this.queryVectorSupplier = null;
    }

    public InterceptedInferenceKnnVectorQueryBuilder(
        KnnVectorQueryBuilder originalQuery,
        Map<FullyQualifiedInferenceId, InferenceResults> inferenceResultsMap
    ) {
        super(originalQuery, inferenceResultsMap);
        this.queryVectorSupplier = null;
    }

    public InterceptedInferenceKnnVectorQueryBuilder(StreamInput in) throws IOException {
        super(in);
        this.queryVectorSupplier = null;
    }

    private InterceptedInferenceKnnVectorQueryBuilder(
        InterceptedInferenceQueryBuilder<KnnVectorQueryBuilder> other,
        Map<FullyQualifiedInferenceId, InferenceResults> inferenceResultsMap,
        PlainActionFuture<InferenceQueryUtils.InferenceInfo> inferenceInfoFuture,
        boolean interceptedCcsRequest
    ) {
        super(other, inferenceResultsMap, inferenceInfoFuture, interceptedCcsRequest);
        this.queryVectorSupplier = null;
    }

    private InterceptedInferenceKnnVectorQueryBuilder(
        InterceptedInferenceQueryBuilder<KnnVectorQueryBuilder> other,
        KnnVectorQueryBuilder originalQuery,
        SetOnce<float[]> queryVectorSupplier
    ) {
        super(originalQuery, other.inferenceResultsMap, other.inferenceInfoFuture, other.interceptedCcsRequest);
        this.queryVectorSupplier = queryVectorSupplier;
    }

    @Override
    protected void doWriteTo(StreamOutput out) throws IOException {
        if (queryVectorSupplier != null) {
            throw new IllegalStateException("Cannot serialize query vector supplier. Missing a rewriteAndFetch?");
        }
        super.doWriteTo(out);
    }

    @Override
    protected void doXContent(XContentBuilder builder, Params params) throws IOException {
        if (queryVectorSupplier != null) {
            throw new IllegalStateException("Cannot serialize query vector supplier. Missing a rewriteAndFetch?");
        }
        super.doXContent(builder, params);
    }

    @Override
    protected Map<String, Float> getFields() {
        return Map.of(getField(), 1.0f);
    }

    @Override
    protected String getQuery() {
        if (queryVectorSupplier != null) {
            // We are in the process of rewriting a standalone query vector builder to generate a query vector. Return null to prevent
            // InferenceQueryUtils from attempting to generate inference results based on the query text.
            return null;
        }

        String query = null;
        QueryVectorBuilder queryVectorBuilder = originalQuery.queryVectorBuilder();
        if (queryVectorBuilder instanceof TextEmbeddingQueryVectorBuilder textEmbeddingQueryVectorBuilder) {
            query = textEmbeddingQueryVectorBuilder.getModelText();
        } else if (queryVectorBuilder != null) {
            throw new IllegalStateException("Query vector builder should have been rewritten to a query vector");
        }

        return query;
    }

    @Override
    protected InterceptedInferenceQueryBuilder<KnnVectorQueryBuilder> customDoRewriteWaitForInferenceResults(
        QueryRewriteContext queryRewriteContext
    ) throws IOException {
        InterceptedInferenceKnnVectorQueryBuilder rewritten = this;

        // knn query may contain filters that are also intercepted.
        // We need to rewrite those here so that we can get inference results for them too.
        rewritten = rewriteFilterQueries(rewritten, queryRewriteContext);

        // If present, rewrite a complete & valid query vector builder to generate the query vector
        rewritten = rewriteQueryVectorBuilder(rewritten, queryRewriteContext);

        return rewritten;
    }

    private static InterceptedInferenceKnnVectorQueryBuilder rewriteFilterQueries(
        InterceptedInferenceKnnVectorQueryBuilder queryBuilder,
        QueryRewriteContext queryRewriteContext
    ) throws IOException {
        KnnVectorQueryBuilder originalQuery = queryBuilder.originalQuery;

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
            return queryBuilder.copy(
                queryBuilder.inferenceResultsMap,
                queryBuilder.inferenceInfoFuture,
                queryBuilder.interceptedCcsRequest
            );
        }
        return queryBuilder;
    }

    private static InterceptedInferenceKnnVectorQueryBuilder rewriteQueryVectorBuilder(
        InterceptedInferenceKnnVectorQueryBuilder queryBuilder,
        QueryRewriteContext queryRewriteContext
    ) {
        final KnnVectorQueryBuilder originalQuery = queryBuilder.originalQuery;
        final SetOnce<float[]> queryVectorSupplier = queryBuilder.queryVectorSupplier;

        if (queryVectorSupplier != null) {
            if (queryVectorSupplier.get() == null) {
                return queryBuilder;
            }

            KnnVectorQueryBuilder rewrittenOriginalQuery = new KnnVectorQueryBuilder(
                originalQuery.getFieldName(),
                queryVectorSupplier.get(),
                originalQuery.k(),
                originalQuery.numCands(),
                originalQuery.visitPercentage(),
                originalQuery.rescoreVectorBuilder(),
                originalQuery.getVectorSimilarity()
            ).boost(originalQuery.boost()).queryName(originalQuery.queryName()).addFilterQueries(originalQuery.filterQueries());

            return new InterceptedInferenceKnnVectorQueryBuilder(queryBuilder, rewrittenOriginalQuery, null);
        }

        QueryVectorBuilder queryVectorBuilder = originalQuery.queryVectorBuilder();
        if (queryVectorBuilder != null) {
            boolean registerAction = false;
            if (queryVectorBuilder instanceof TextEmbeddingQueryVectorBuilder tevb) {
                // TextEmbeddingQueryVectorBuilder is a special case. If a model ID is set, we register an action to generate
                // the query vector. If not, the model text will be returned via getQuery() so that InferenceQueryUtils can
                // generate the appropriate inference results for the inferred inference ID(s).
                if (tevb.getModelId() != null) {
                    registerAction = true;
                }
            } else {
                // We register an action to generate the query vector for all other query vector builders. If they cannot, buildVector()
                // should throw an error indicating why.
                registerAction = true;
            }

            if (registerAction) {
                SetOnce<float[]> newQueryVectorSupplier = new SetOnce<>();
                queryRewriteContext.registerUniqueAsyncAction(
                    new QueryVectorBuilderAsyncAction(queryVectorBuilder),
                    newQueryVectorSupplier::set
                );
                return new InterceptedInferenceKnnVectorQueryBuilder(queryBuilder, originalQuery, newQueryVectorSupplier);
            }
        }

        return queryBuilder;
    }

    @Override
    protected boolean preInferenceCoordinatorNodeValidate(ResolvedIndices resolvedIndices) {
        // Check the field types we are querying locally
        int nonInferenceFieldsQueried = 0;
        int inferenceFieldsQueried = 0;
        Collection<IndexMetadata> indexMetadataCollection = resolvedIndices.getConcreteLocalIndicesMetadata().values();
        for (IndexMetadata indexMetadata : indexMetadataCollection) {
            InferenceFieldMetadata inferenceFieldMetadata = indexMetadata.getInferenceFields().get(getField());
            if (inferenceFieldMetadata == null) {
                nonInferenceFieldsQueried++;
            } else {
                inferenceFieldsQueried++;
            }
        }

        validateQueryVectorBuilder(nonInferenceFieldsQueried > 0);

        // We can skip remote cluster inference info gathering if:
        // - Inference fields are queried locally, guaranteeing that the query will be intercepted
        // - A standalone query vector builder or query vector is set. In either case, remote cluster inference results are not required.
        return inferenceFieldsQueried > 0 && (hasStandaloneQueryVectorBuilder() || originalQuery.queryVector() != null);
    }

    @Override
    protected void postInferenceCoordinatorNodeValidate(InferenceQueryUtils.InferenceInfo inferenceInfo) {
        // Detect if we are querying any non-inference fields locally or remotely. We can do this by comparing the inference field count to
        // the index count. Since the knn query is a single-field query, they should match if we are querying only inference fields.
        if (inferenceInfo.inferenceFieldCount() < inferenceInfo.indexCount()) {
            validateQueryVectorBuilder(true);
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
    protected InterceptedInferenceKnnVectorQueryBuilder copy(
        Map<FullyQualifiedInferenceId, InferenceResults> inferenceResultsMap,
        PlainActionFuture<InferenceQueryUtils.InferenceInfo> inferenceInfoFuture,
        boolean interceptedCcsRequest
    ) {
        return new InterceptedInferenceKnnVectorQueryBuilder(this, inferenceResultsMap, inferenceInfoFuture, interceptedCcsRequest);
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
        } else if (modelSettings.taskType() != TaskType.TEXT_EMBEDDING) {
            throw new IllegalArgumentException("Field [" + getField() + "] does not use a [" + TaskType.TEXT_EMBEDDING + "] model");
        }

        VectorData queryVector = originalQuery.queryVector();
        if (queryVector == null) {
            MlDenseEmbeddingResults textEmbeddingResults = getTextEmbeddingResults(
                new FullyQualifiedInferenceId(clusterAlias, semanticTextFieldType.getSearchInferenceId())
            );
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
            throw new IllegalStateException("Query vector is not set when querying a non-inference field");
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

    private void validateQueryVectorBuilder(boolean requireExplicitInferenceId) {
        QueryVectorBuilder queryVectorBuilder = originalQuery.queryVectorBuilder();
        if (queryVectorBuilder instanceof TextEmbeddingQueryVectorBuilder tevb && requireExplicitInferenceId) {
            // TextEmbeddingQueryVectorBuilder needs validation when an explicit inference ID is required. A non-null model text value
            // is guaranteed by its constructor.
            if (tevb.getModelId() == null) {
                throw new IllegalArgumentException("[model_id] must not be null.");
            }
        }
        // For other query vector builders, we don't validate upfront. buildVector() will throw an error if it cannot generate a vector.
    }

    private boolean hasStandaloneQueryVectorBuilder() {
        QueryVectorBuilder queryVectorBuilder = originalQuery.queryVectorBuilder();
        if (queryVectorBuilder instanceof TextEmbeddingQueryVectorBuilder tevb) {
            // TextEmbeddingQueryVectorBuilder is considered to be a standalone query vector builder if the model ID is set
            return tevb.getModelId() != null;
        }

        // All other query vector builders are assumed to be standalone
        return queryVectorBuilder != null;
    }
}
