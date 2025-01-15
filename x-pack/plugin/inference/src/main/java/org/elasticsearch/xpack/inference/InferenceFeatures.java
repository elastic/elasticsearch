/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.inference;

import org.elasticsearch.features.FeatureSpecification;
import org.elasticsearch.features.NodeFeature;
import org.elasticsearch.xpack.inference.mapper.SemanticInferenceMetadataFieldsMapper;
import org.elasticsearch.xpack.inference.mapper.SemanticTextFieldMapper;
import org.elasticsearch.xpack.inference.rank.textsimilarity.TextSimilarityRankRetrieverBuilder;

import java.util.Set;

import static org.elasticsearch.xpack.inference.mapper.SemanticTextFieldMapper.SEMANTIC_TEXT_INDEX_OPTIONS;
import static org.elasticsearch.xpack.inference.queries.SemanticKnnVectorQueryRewriteInterceptor.SEMANTIC_KNN_VECTOR_QUERY_REWRITE_INTERCEPTION_SUPPORTED;
import static org.elasticsearch.xpack.inference.queries.SemanticMatchQueryRewriteInterceptor.SEMANTIC_MATCH_QUERY_REWRITE_INTERCEPTION_SUPPORTED;
import static org.elasticsearch.xpack.inference.queries.SemanticSparseVectorQueryRewriteInterceptor.SEMANTIC_SPARSE_VECTOR_QUERY_REWRITE_INTERCEPTION_SUPPORTED;

/**
 * Provides inference features.
 */
public class InferenceFeatures implements FeatureSpecification {

    private static final NodeFeature SEMANTIC_TEXT_HIGHLIGHTER = new NodeFeature("semantic_text.highlighter");

    @Override
    public Set<NodeFeature> getTestFeatures() {
        return Set.of(
            SemanticTextFieldMapper.SEMANTIC_TEXT_IN_OBJECT_FIELD_FIX,
            SemanticTextFieldMapper.SEMANTIC_TEXT_SINGLE_FIELD_UPDATE_FIX,
            SemanticTextFieldMapper.SEMANTIC_TEXT_DELETE_FIX,
            SemanticTextFieldMapper.SEMANTIC_TEXT_ZERO_SIZE_FIX,
            SemanticTextFieldMapper.SEMANTIC_TEXT_ALWAYS_EMIT_INFERENCE_ID_FIX,
            SEMANTIC_TEXT_HIGHLIGHTER,
            SEMANTIC_MATCH_QUERY_REWRITE_INTERCEPTION_SUPPORTED,
            SEMANTIC_SPARSE_VECTOR_QUERY_REWRITE_INTERCEPTION_SUPPORTED,
            SemanticInferenceMetadataFieldsMapper.EXPLICIT_NULL_FIXES,
            SEMANTIC_KNN_VECTOR_QUERY_REWRITE_INTERCEPTION_SUPPORTED,
            TextSimilarityRankRetrieverBuilder.TEXT_SIMILARITY_RERANKER_ALIAS_HANDLING_FIX,
            SEMANTIC_TEXT_INDEX_OPTIONS
        );
    }
}
