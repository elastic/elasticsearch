/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.inference.queries;

import org.elasticsearch.action.ResolvedIndices;
import org.elasticsearch.cluster.metadata.IndexMetadata;
import org.elasticsearch.cluster.metadata.InferenceFieldMetadata;
import org.elasticsearch.features.NodeFeature;
import org.elasticsearch.index.mapper.IndexFieldMapper;
import org.elasticsearch.index.query.BoolQueryBuilder;
import org.elasticsearch.index.query.MultiMatchQueryBuilder;
import org.elasticsearch.index.query.QueryBuilder;
import org.elasticsearch.index.query.QueryRewriteContext;
import org.elasticsearch.index.query.TermsQueryBuilder;
import org.elasticsearch.plugins.internal.rewriter.QueryRewriteInterceptor;

import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;

public class SemanticMultiMatchQueryRewriteInterceptor implements QueryRewriteInterceptor {

    public static final NodeFeature SEMANTIC_MULTI_MATCH_QUERY_REWRITE_INTERCEPTION_SUPPORTED = new NodeFeature(
        "search.semantic_multi_match_query_rewrite_interception_supported"
    );

    public SemanticMultiMatchQueryRewriteInterceptor() {}

    @Override
    public QueryBuilder interceptAndRewrite(QueryRewriteContext context, QueryBuilder queryBuilder) {
        assert (queryBuilder instanceof MultiMatchQueryBuilder);
        MultiMatchQueryBuilder multiMatchQueryBuilder = (MultiMatchQueryBuilder) queryBuilder;

        ResolvedIndices resolvedIndices = context.getResolvedIndices();
        if (resolvedIndices == null) {
            // No resolved indices, so return the original query.
            return queryBuilder;
        }

        Map<String, Float> fields = multiMatchQueryBuilder.fields();
        if (fields == null || fields.isEmpty()) {
            // No fields specified, return original query
            return queryBuilder;
        }

        MultiFieldInferenceInfo inferenceInfo = resolveInferenceInfoForFields(fields.keySet(), resolvedIndices);

        if (inferenceInfo.getInferenceFields().isEmpty()) {
            // No inference fields were identified, so return the original query.
            return queryBuilder;
        } else if (inferenceInfo.hasNonInferenceFields()) {
            // Combined case where some fields are semantic_text and others are not
            return buildCombinedInferenceAndNonInferenceQuery(multiMatchQueryBuilder, inferenceInfo);
        } else {
            // All specified fields are inference fields (semantic_text)
            return buildInferenceQuery(multiMatchQueryBuilder, inferenceInfo);
        }
    }

    @Override
    public String getQueryName() {
        return MultiMatchQueryBuilder.NAME;
    }

    private MultiFieldInferenceInfo resolveInferenceInfoForFields(Set<String> fieldNames, ResolvedIndices resolvedIndices) {
        Collection<IndexMetadata> indexMetadataCollection = resolvedIndices.getConcreteLocalIndicesMetadata().values();
        Map<String, Map<String, InferenceFieldMetadata>> inferenceFieldsPerIndex = new HashMap<>();
        List<String> nonInferenceIndices = new ArrayList<>();
        Map<String, Set<String>> inferenceFieldsByIndex = new HashMap<>();

        for (IndexMetadata indexMetadata : indexMetadataCollection) {
            String indexName = indexMetadata.getIndex().getName();
            Map<String, InferenceFieldMetadata> indexInferenceFields = new HashMap<>();
            Set<String> indexInferenceFieldNames = fieldNames.stream()
                .filter(fieldName -> indexMetadata.getInferenceFields().containsKey(fieldName))
                .collect(Collectors.toSet());

            if (indexInferenceFieldNames.isEmpty()) {
                nonInferenceIndices.add(indexName);
            } else {
                for (String fieldName : indexInferenceFieldNames) {
                    indexInferenceFields.put(fieldName, indexMetadata.getInferenceFields().get(fieldName));
                }
                inferenceFieldsPerIndex.put(indexName, indexInferenceFields);
                inferenceFieldsByIndex.put(indexName, indexInferenceFieldNames);
            }
        }

        return new MultiFieldInferenceInfo(fieldNames, inferenceFieldsPerIndex, nonInferenceIndices, inferenceFieldsByIndex);
    }

    private QueryBuilder buildInferenceQuery(MultiMatchQueryBuilder originalQuery, MultiFieldInferenceInfo inferenceInfo) {
        String queryValue = (String) originalQuery.value();
        Set<String> inferenceFields = inferenceInfo.getInferenceFields();

        if (inferenceFields.size() == 1) {
            // Single inference field - create a simple semantic query
            String fieldName = inferenceFields.iterator().next();
            SemanticQueryBuilder semanticQuery = new SemanticQueryBuilder(fieldName, queryValue, false);
            semanticQuery.boost(originalQuery.boost());
            semanticQuery.queryName(originalQuery.queryName());
            return semanticQuery;
        } else {
            // Multiple inference fields - create a boolean query with semantic subqueries
            BoolQueryBuilder boolQuery = new BoolQueryBuilder();
            for (String fieldName : inferenceFields) {
                SemanticQueryBuilder semanticQuery = new SemanticQueryBuilder(fieldName, queryValue, false);
                boolQuery.should(semanticQuery);
            }
            boolQuery.boost(originalQuery.boost());
            boolQuery.queryName(originalQuery.queryName());
            return boolQuery;
        }
    }

    private QueryBuilder buildCombinedInferenceAndNonInferenceQuery(
        MultiMatchQueryBuilder originalQuery,
        MultiFieldInferenceInfo inferenceInfo
    ) {
        BoolQueryBuilder combinedQuery = new BoolQueryBuilder();
        String queryValue = (String) originalQuery.value();

        // Add semantic queries for inference fields per index
        Map<String, Map<String, InferenceFieldMetadata>> inferenceFieldsPerIndex = inferenceInfo.getInferenceFieldsPerIndex();
        for (Map.Entry<String, Map<String, InferenceFieldMetadata>> entry : inferenceFieldsPerIndex.entrySet()) {
            String indexName = entry.getKey();
            Map<String, InferenceFieldMetadata> indexInferenceFields = entry.getValue();

            for (String fieldName : indexInferenceFields.keySet()) {
                BoolQueryBuilder indexSpecificQuery = new BoolQueryBuilder();
                indexSpecificQuery.must(new SemanticQueryBuilder(fieldName, queryValue, true));
                indexSpecificQuery.filter(new TermsQueryBuilder(IndexFieldMapper.NAME, List.of(indexName)));
                combinedQuery.should(indexSpecificQuery);
            }
        }

        // Add non-inference query for indices without semantic_text fields
        if (inferenceInfo.getNonInferenceIndices().isEmpty() == false) {
            MultiMatchQueryBuilder nonInferenceQuery = copyMultiMatchQueryBuilder(originalQuery);
            BoolQueryBuilder indexFilteredQuery = new BoolQueryBuilder();
            indexFilteredQuery.must(nonInferenceQuery);
            indexFilteredQuery.filter(new TermsQueryBuilder(IndexFieldMapper.NAME, inferenceInfo.getNonInferenceIndices()));
            combinedQuery.should(indexFilteredQuery);
        }

        combinedQuery.boost(originalQuery.boost());
        combinedQuery.queryName(originalQuery.queryName());
        return combinedQuery;
    }

    private MultiMatchQueryBuilder copyMultiMatchQueryBuilder(MultiMatchQueryBuilder original) {
        MultiMatchQueryBuilder copy = new MultiMatchQueryBuilder(original.value());
        copy.fields(original.fields());
        copy.type(original.type());
        copy.operator(original.operator());
        copy.slop(original.slop());
        copy.analyzer(original.analyzer());
        copy.minimumShouldMatch(original.minimumShouldMatch());
        copy.fuzzyRewrite(original.fuzzyRewrite());
        copy.prefixLength(original.prefixLength());
        copy.maxExpansions(original.maxExpansions());
        copy.fuzzyTranspositions(original.fuzzyTranspositions());
        copy.lenient(original.lenient());
        copy.zeroTermsQuery(original.zeroTermsQuery());
        copy.autoGenerateSynonymsPhraseQuery(original.autoGenerateSynonymsPhraseQuery());
        copy.tieBreaker(original.tieBreaker());

        if (original.fuzziness() != null) {
            copy.fuzziness(original.fuzziness());
        }

        return copy;
    }

    /**
     * Represents the inference information for multiple fields across indices.
     */
    public static class MultiFieldInferenceInfo {
        private final Set<String> originalFields;
        private final Map<String, Map<String, InferenceFieldMetadata>> inferenceFieldsPerIndex;
        private final List<String> nonInferenceIndices;
        private final Map<String, Set<String>> inferenceFieldsByIndex;

        public MultiFieldInferenceInfo(
            Set<String> originalFields,
            Map<String, Map<String, InferenceFieldMetadata>> inferenceFieldsPerIndex,
            List<String> nonInferenceIndices,
            Map<String, Set<String>> inferenceFieldsByIndex
        ) {
            this.originalFields = originalFields;
            this.inferenceFieldsPerIndex = inferenceFieldsPerIndex;
            this.nonInferenceIndices = nonInferenceIndices;
            this.inferenceFieldsByIndex = inferenceFieldsByIndex;
        }

        public Set<String> getInferenceFields() {
            return inferenceFieldsPerIndex.values().stream()
                .flatMap(fields -> fields.keySet().stream())
                .collect(Collectors.toSet());
        }

        public Map<String, Map<String, InferenceFieldMetadata>> getInferenceFieldsPerIndex() {
            return inferenceFieldsPerIndex;
        }

        public List<String> getNonInferenceIndices() {
            return nonInferenceIndices;
        }

        public boolean hasNonInferenceFields() {
            return !nonInferenceIndices.isEmpty();
        }

        public Map<String, Set<String>> getInferenceFieldsByIndex() {
            return inferenceFieldsByIndex;
        }
    }
}
