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
import org.elasticsearch.index.mapper.IndexFieldMapper;
import org.elasticsearch.index.query.BoolQueryBuilder;
import org.elasticsearch.index.query.MatchQueryBuilder;
import org.elasticsearch.index.query.QueryBuilder;
import org.elasticsearch.index.query.QueryRewriteContext;
import org.elasticsearch.index.query.TermsQueryBuilder;
import org.elasticsearch.plugins.internal.rewriter.QueryRewriteInterceptor;

import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

/**
 * Intercepts and adapts a query to be rewritten to work seamlessly on a semantic_text field.
 */
public abstract class SemanticQueryRewriteInterceptor implements QueryRewriteInterceptor {

    public SemanticQueryRewriteInterceptor() {}

    @Override
    public QueryBuilder interceptAndRewrite(QueryRewriteContext context, QueryBuilder queryBuilder) {
        Map<String, Float> fieldNamesWithWeights = getFieldNamesWithWeights(queryBuilder);
        ResolvedIndices resolvedIndices = context.getResolvedIndices();

        if (resolvedIndices == null) {
            // No resolved indices, so return the original query.
            return queryBuilder;
        }

        if (fieldNamesWithWeights.size() > 1) {
            // Multi-field query, so return the original query.
            return handleMultiFieldQuery(queryBuilder, fieldNamesWithWeights, resolvedIndices);
        }

        String fieldName = fieldNamesWithWeights.keySet().iterator().next();
        Float weight = fieldNamesWithWeights.get(fieldName);
        InferenceIndexInformationForField indexInformation = resolveIndicesForField(fieldName, resolvedIndices);
        if (indexInformation.getInferenceIndices().isEmpty()) {
            // No inference fields were identified, so return the original query.
            return queryBuilder;
        } else if (indexInformation.nonInferenceIndices().isEmpty() == false) {
            // Combined case where the field name requested by this query contains both
            // semantic_text and non-inference fields, so we have to combine queries per index
            // containing each field type.
            return buildCombinedInferenceAndNonInferenceQuery(queryBuilder, indexInformation, weight);
        } else {
            // The only fields we've identified are inference fields (e.g. semantic_text),
            // so rewrite the entire query to work on a semantic_text field.
            return buildInferenceQuery(queryBuilder, indexInformation, weight);
        }
    }

    /**
     * Handle multi-field queries (new logic)
     */
    private QueryBuilder handleMultiFieldQuery(
        QueryBuilder queryBuilder,
        Map<String, Float> fieldNamesWithWeights,
        ResolvedIndices resolvedIndices
    ) {
        BoolQueryBuilder finalQueryBuilder = new BoolQueryBuilder();
        boolean hasAnySemanticFields = false;

        for (Map.Entry<String, Float> fieldEntry : fieldNamesWithWeights.entrySet()) {
            String fieldName = fieldEntry.getKey();
            Float fieldWeight = fieldEntry.getValue();
            InferenceIndexInformationForField indexInformation = resolveIndicesForField(fieldName, resolvedIndices);

            if (indexInformation.getInferenceIndices().isEmpty()) {
                // Pure non-semantic field - create individual match query
                QueryBuilder nonSemanticQuery = createMatchSubQuery(
                    indexInformation.nonInferenceIndices(),
                    fieldName,
                    getQuery(queryBuilder));
                finalQueryBuilder.should(nonSemanticQuery);
            } else if (indexInformation.nonInferenceIndices().isEmpty() == false) {
                // Mixed semantic/non-semantic field - use combined approach
                QueryBuilder combinedQuery = buildCombinedInferenceAndNonInferenceQuery(queryBuilder, indexInformation, fieldWeight);
                finalQueryBuilder.should(combinedQuery);
                hasAnySemanticFields = true;
            } else {
                // Pure semantic field - create semantic query
                QueryBuilder semanticQuery = buildInferenceQuery(queryBuilder, indexInformation, fieldWeight);
                finalQueryBuilder.should(semanticQuery);
                hasAnySemanticFields = true;
            }
        }

        // If no semantic fields were found, return original query
        if (hasAnySemanticFields == false) {
            return queryBuilder;
        }

//        finalQueryBuilder.minimumShouldMatch(1);
        return finalQueryBuilder;
    }

    /**
     * @param queryBuilder {@link QueryBuilder}
     * @return Map of field names with their weights for multi-field queries.
     *         For single-field queries, return a map with one entry.
     */
    protected abstract Map<String, Float> getFieldNamesWithWeights(QueryBuilder queryBuilder);

    /**
     * @param queryBuilder {@link QueryBuilder}
     * @return The text/query string requested by the provided query builder.
     */
    protected abstract String getQuery(QueryBuilder queryBuilder);

    /**
     * Builds the inference query
     *
     * @param queryBuilder {@link QueryBuilder}
     * @param indexInformation {@link InferenceIndexInformationForField}
     * @param weight {@link Float}
     * @return {@link QueryBuilder}
     */
    protected abstract QueryBuilder buildInferenceQuery(
        QueryBuilder queryBuilder,
        InferenceIndexInformationForField indexInformation,
        Float weight
    );

    /**
     * Builds a combined inference and non-inference query,
     * which separates the different queries into appropriate indices based on field type.
     * @param queryBuilder {@link QueryBuilder}
     * @param indexInformation {@link InferenceIndexInformationForField}
     * @return {@link QueryBuilder}
     */
    // protected abstract QueryBuilder buildCombinedInferenceAndNonInferenceQuery(
    // QueryBuilder queryBuilder,
    // InferenceIndexInformationForField indexInformation
    // );

    protected abstract QueryBuilder buildCombinedInferenceAndNonInferenceQuery(
        QueryBuilder queryBuilder,
        InferenceIndexInformationForField indexInformation,
        Float fieldWeight
    );

    private InferenceIndexInformationForField resolveIndicesForField(String fieldName, ResolvedIndices resolvedIndices) {
        Collection<IndexMetadata> indexMetadataCollection = resolvedIndices.getConcreteLocalIndicesMetadata().values();
        Map<String, InferenceFieldMetadata> inferenceIndicesMetadata = new HashMap<>();
        List<String> nonInferenceIndices = new ArrayList<>();
        for (IndexMetadata indexMetadata : indexMetadataCollection) {
            String indexName = indexMetadata.getIndex().getName();
            InferenceFieldMetadata inferenceFieldMetadata = indexMetadata.getInferenceFields().get(fieldName);
            if (inferenceFieldMetadata != null) {
                inferenceIndicesMetadata.put(indexName, inferenceFieldMetadata);
            } else {
                nonInferenceIndices.add(indexName);
            }
        }

        return new InferenceIndexInformationForField(fieldName, inferenceIndicesMetadata, nonInferenceIndices);
    }

    /**
     * Build a non-semantic field query (for multi-field scenarios)
     */
    protected QueryBuilder buildNonSemanticFieldQuery(QueryBuilder queryBuilder, String fieldName, Float fieldWeight) {
        // Default implementation - subclasses can override for specific query types
        String query = getQuery(queryBuilder);
        MatchQueryBuilder matchQueryBuilder = new MatchQueryBuilder(fieldName, query);
        matchQueryBuilder.boost(fieldWeight);
        return matchQueryBuilder;
    }

    protected QueryBuilder createMatchSubQuery(Collection<String> indices, String fieldName, String queryText) {
        BoolQueryBuilder boolQueryBuilder = new BoolQueryBuilder();
        MatchQueryBuilder matchQuery = new MatchQueryBuilder(fieldName, queryText);
//        if (fieldWeight != null && !fieldWeight.equals(1.0f)) {
//            matchQuery.boost(fieldWeight);
//        }
        boolQueryBuilder.must(matchQuery);
        boolQueryBuilder.filter(new TermsQueryBuilder(IndexFieldMapper.NAME, indices));
        return boolQueryBuilder;
    }

    protected QueryBuilder createSubQueryForIndices(Collection<String> indices, QueryBuilder queryBuilder) {
        BoolQueryBuilder boolQueryBuilder = new BoolQueryBuilder();
        boolQueryBuilder.must(queryBuilder);
        boolQueryBuilder.filter(new TermsQueryBuilder(IndexFieldMapper.NAME, indices));
        return boolQueryBuilder;
    }

    protected QueryBuilder createSemanticSubQuery(Collection<String> indices, String fieldName, String value) {
        BoolQueryBuilder boolQueryBuilder = new BoolQueryBuilder();
        boolQueryBuilder.must(new SemanticQueryBuilder(fieldName, value, true));
        boolQueryBuilder.filter(new TermsQueryBuilder(IndexFieldMapper.NAME, indices));
        return boolQueryBuilder;
    }

    /**
     * Represents the indices and associated inference information for a field.
     */
    public record InferenceIndexInformationForField(
        String fieldName,
        Map<String, InferenceFieldMetadata> inferenceIndicesMetadata,
        List<String> nonInferenceIndices
    ) {

        public Collection<String> getInferenceIndices() {
            return inferenceIndicesMetadata.keySet();
        }

        public Map<String, List<String>> getInferenceIdsIndices() {
            return inferenceIndicesMetadata.entrySet()
                .stream()
                .collect(
                    Collectors.groupingBy(
                        entry -> entry.getValue().getSearchInferenceId(),
                        Collectors.mapping(Map.Entry::getKey, Collectors.toList())
                    )
                );
        }
    }
}
