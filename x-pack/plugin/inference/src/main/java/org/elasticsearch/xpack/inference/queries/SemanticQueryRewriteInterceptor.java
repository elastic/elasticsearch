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
import org.elasticsearch.index.query.MatchAllQueryBuilder;
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

/**
 * Intercepts and adapts a query to be rewritten to work seamlessly on a semantic_text field.
 */
public abstract class SemanticQueryRewriteInterceptor implements QueryRewriteInterceptor {

    public SemanticQueryRewriteInterceptor() {}

    @Override
    public QueryBuilder interceptAndRewrite(QueryRewriteContext context, QueryBuilder queryBuilder) {
        String fieldName = getFieldName(queryBuilder);
        ResolvedIndices resolvedIndices = context.getResolvedIndices();

        if (resolvedIndices == null) {
            // No resolved indices, so return the original query.
            return queryBuilder;
        }

        if (fieldName == null && getQueryName().equals(MatchAllQueryBuilder.NAME)) {

            if (getInferenceFieldsFromResolveIndices(resolvedIndices).isEmpty()) {
                // No inference fields were identified, so return the original query.
                return queryBuilder;
            }

            List<String> fieldList = new ArrayList<>(getFieldsFromResolveIndices(resolvedIndices));
            BoolQueryBuilder boolQueryBuilder = new BoolQueryBuilder();
            for (String field : fieldList) {
                InferenceIndexInformationForField indexInformation = resolveIndicesForField(field, resolvedIndices);
                if (indexInformation.nonInferenceIndices().isEmpty() == false) {
                    // Combined case where the field name requested by this query contains both
                    // semantic_text and non-inference fields, so we have to combine queries per index
                    // containing each field type.
                    boolQueryBuilder.should(buildCombinedInferenceAndNonInferenceQuery(queryBuilder, indexInformation));
                } else {
                    // The only fields we've identified are inference fields (e.g. semantic_text),
                    // so rewrite the entire query to work on a semantic_text field.
                    boolQueryBuilder.should(buildInferenceQuery(queryBuilder, indexInformation));
                }
            }
            return boolQueryBuilder;
        }

        InferenceIndexInformationForField indexInformation = resolveIndicesForField(fieldName, resolvedIndices);
        if (indexInformation.getInferenceIndices().isEmpty()) {
            // No inference fields were identified, so return the original query.
            return queryBuilder;
        } else if (indexInformation.nonInferenceIndices().isEmpty() == false) {
            // Combined case where the field name requested by this query contains both
            // semantic_text and non-inference fields, so we have to combine queries per index
            // containing each field type.
            return buildCombinedInferenceAndNonInferenceQuery(queryBuilder, indexInformation);
        } else {
            // The only fields we've identified are inference fields (e.g. semantic_text),
            // so rewrite the entire query to work on a semantic_text field.
            return buildInferenceQuery(queryBuilder, indexInformation);
        }
    }

    /**
     * @param queryBuilder {@link QueryBuilder}
     * @return The singular field name requested by the provided query builder.
     */
    protected abstract String getFieldName(QueryBuilder queryBuilder);

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
     * @return {@link QueryBuilder}
     */
    protected abstract QueryBuilder buildInferenceQuery(QueryBuilder queryBuilder, InferenceIndexInformationForField indexInformation);

    /**
     * Builds a combined inference and non-inference query,
     * which separates the different queries into appropriate indices based on field type.
     * @param queryBuilder {@link QueryBuilder}
     * @param indexInformation {@link InferenceIndexInformationForField}
     * @return {@link QueryBuilder}
     */
    protected abstract QueryBuilder buildCombinedInferenceAndNonInferenceQuery(
        QueryBuilder queryBuilder,
        InferenceIndexInformationForField indexInformation
    );

    private List<String> getFieldsFromResolveIndices(ResolvedIndices resolvedIndices) {
        Collection<IndexMetadata> indexMetadataCollection = resolvedIndices.getConcreteLocalIndicesMetadata().values();
        List<String> fields = new ArrayList<>();
        for (IndexMetadata indexMetadata : indexMetadataCollection) {
            if (indexMetadata.mapping() == null || indexMetadata.mapping().sourceAsMap() == null) {
                // No mapping, so no fields.
                continue;
            }
            Collection<Object> mappingSource = indexMetadata.mapping().getSourceAsMap().values();
            @SuppressWarnings("unchecked")
            Set<String> fieldNames = mappingSource.stream()
                .filter(obj -> obj instanceof Map<?, ?>)
                .map(obj -> (Map<String, Object>) obj)
                .flatMap(map -> map.keySet().stream())
                .collect(Collectors.toSet());
            fields.addAll(fieldNames);
        }
        return fields;
    }

    private List<InferenceFieldMetadata> getInferenceFieldsFromResolveIndices(ResolvedIndices resolvedIndices) {
        Collection<IndexMetadata> indexMetadataCollection = resolvedIndices.getConcreteLocalIndicesMetadata().values();
        List<InferenceFieldMetadata> inferenceIndicesMetadata = new ArrayList<>();
        for (IndexMetadata indexMetadata : indexMetadataCollection) {
            String indexName = indexMetadata.getIndex().getName();
            inferenceIndicesMetadata.addAll(indexMetadata.getInferenceFields().values());
        }

        return inferenceIndicesMetadata;
    }

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
