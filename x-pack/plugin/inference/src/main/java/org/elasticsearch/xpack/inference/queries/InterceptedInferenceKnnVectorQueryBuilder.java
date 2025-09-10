/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.inference.queries;

import org.apache.lucene.search.join.ScoreMode;
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
import org.elasticsearch.search.vectors.KnnVectorQueryBuilder;
import org.elasticsearch.search.vectors.QueryVectorBuilder;
import org.elasticsearch.search.vectors.VectorData;
import org.elasticsearch.xpack.core.ml.inference.results.MlTextEmbeddingResults;
import org.elasticsearch.xpack.core.ml.vectors.TextEmbeddingQueryVectorBuilder;
import org.elasticsearch.xpack.inference.mapper.SemanticTextField;
import org.elasticsearch.xpack.inference.mapper.SemanticTextFieldMapper;

import java.io.IOException;
import java.util.Collection;
import java.util.Map;

public class InterceptedInferenceKnnVectorQueryBuilder extends InterceptedInferenceQueryBuilder<KnnVectorQueryBuilder> {
    public static final String NAME = "intercepted_inference_knn";

    public InterceptedInferenceKnnVectorQueryBuilder(KnnVectorQueryBuilder originalQuery) {
        super(originalQuery);
    }

    public InterceptedInferenceKnnVectorQueryBuilder(StreamInput in) throws IOException {
        super(in);
    }

    public InterceptedInferenceKnnVectorQueryBuilder(
        InterceptedInferenceQueryBuilder<KnnVectorQueryBuilder> other,
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
        String query = null;
        QueryVectorBuilder queryVectorBuilder = originalQuery.queryVectorBuilder();
        if (queryVectorBuilder instanceof TextEmbeddingQueryVectorBuilder textEmbeddingQueryVectorBuilder) {
            query = textEmbeddingQueryVectorBuilder.getModelText();
        }

        return query;
    }

    @Override
    protected String getInferenceIdOverride() {
        return getQueryVectorBuilderModelId();
    }

    @Override
    protected void coordinatorNodeValidate(ResolvedIndices resolvedIndices) {
        // Check if we are querying any non-inference fields
        Collection<IndexMetadata> indexMetadataCollection = resolvedIndices.getConcreteLocalIndicesMetadata().values();
        for (IndexMetadata indexMetadata : indexMetadataCollection) {
            InferenceFieldMetadata inferenceFieldMetadata = indexMetadata.getInferenceFields().get(getField());
            if (inferenceFieldMetadata == null && originalQuery.queryVector() == null && getQueryVectorBuilderModelId() == null) {
                // We are querying a non-inference field and neither a query vector nor query vector builder model ID has been provided
                throw new IllegalArgumentException("Either query vector or query vector builder model ID must be specified");
            }
        }
    }

    @Override
    protected QueryBuilder doRewriteBwC(QueryRewriteContext queryRewriteContext) {
        // TODO: Implement BwC
        return this;
    }

    @Override
    protected QueryBuilder copy(Map<String, InferenceResults> inferenceResultsMap) {
        return new InterceptedInferenceKnnVectorQueryBuilder(this, inferenceResultsMap);
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

    private String getQueryVectorBuilderModelId() {
        String modelId = null;
        QueryVectorBuilder queryVectorBuilder = originalQuery.queryVectorBuilder();
        if (queryVectorBuilder instanceof TextEmbeddingQueryVectorBuilder textEmbeddingQueryVectorBuilder) {
            modelId = textEmbeddingQueryVectorBuilder.getModelId();
        }

        return modelId;
    }

    private QueryBuilder querySemanticTextField(SemanticTextFieldMapper.SemanticTextFieldType semanticTextFieldType) {
        // TODO: Detect when querying a sparse vector semantic text field here?
        VectorData queryVector = originalQuery.queryVector();
        if (queryVector == null) {
            String inferenceId = getQueryVectorBuilderModelId();
            if (inferenceId == null) {
                inferenceId = semanticTextFieldType.getSearchInferenceId();
            }

            MlTextEmbeddingResults textEmbeddingResults = getTextEmbeddingResults(inferenceId);
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
            String modelId = getQueryVectorBuilderModelId();
            if (modelId == null) {
                throw new IllegalArgumentException("Either query vector or query vector builder model ID must be specified");
            }

            MlTextEmbeddingResults textEmbeddingResults = getTextEmbeddingResults(modelId);
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

    private MlTextEmbeddingResults getTextEmbeddingResults(String inferenceId) {
        InferenceResults inferenceResults = inferenceResultsMap.get(inferenceId);
        if (inferenceResults == null) {
            throw new IllegalStateException("Could not find inference results from inference endpoint [" + inferenceId + "]");
        } else if (inferenceResults instanceof MlTextEmbeddingResults == false) {
            throw new IllegalArgumentException(
                "Expected query inference results to be of type ["
                    + MlTextEmbeddingResults.NAME
                    + "], got ["
                    + inferenceResults.getWriteableName()
                    + "]. Are you specifying a compatible inference endpoint? Has the inference endpoint configuration changed?"
            );
        }

        return (MlTextEmbeddingResults) inferenceResults;
    }
}
