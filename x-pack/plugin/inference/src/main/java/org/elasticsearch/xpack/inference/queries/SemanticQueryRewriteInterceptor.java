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
import org.elasticsearch.common.regex.Regex;
import org.elasticsearch.index.mapper.IndexFieldMapper;
import org.elasticsearch.index.query.AbstractQueryBuilder;
import org.elasticsearch.index.query.BoolQueryBuilder;
import org.elasticsearch.index.query.QueryBuilder;
import org.elasticsearch.index.query.QueryRewriteContext;
import org.elasticsearch.index.query.TermsQueryBuilder;
import org.elasticsearch.plugins.internal.rewriter.QueryRewriteInterceptor;

import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.HashSet;
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
        ResolvedIndices resolvedIndices = context.getResolvedIndices();

        if (resolvedIndices == null) {
            // No resolved indices, so return the original query.
            return queryBuilder;
        }

        InferenceIndexInformationForField indexInformation = resolveIndicesForFields(queryBuilder, resolvedIndices);
        if (indexInformation.hasInferenceFields() == false) {
            // No inference fields were identified, so return the original query.
            return queryBuilder;
        } else if (indexInformation.hasNonInferenceFields()) {
            // Combined case where the field name(s) requested by this query contain both
            // semantic_text and non-inference fields, so we have to combine queries per index
            // containing each field type.
            return buildCombinedInferenceAndNonInferenceQuery(queryBuilder, indexInformation);
        } else {
            // The only fields we've identified are inference fields (e.g. semantic_text),
            // so rewrite the entire query to work on semantic_text field(s).
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
     * @return The field names with their weights requested by the provided query builder.
     */
    protected Map<String, Float> getFieldsWithWeights(QueryBuilder queryBuilder) {
        // Default implementation for single-field queries
        String fieldName = getFieldName(queryBuilder);
        return Map.of(fieldName, 1.0f);
    }

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

    private InferenceIndexInformationForField resolveIndicesForFields(QueryBuilder queryBuilder, ResolvedIndices resolvedIndices) {
        Map<String, Float> fieldsWithWeights = getFieldsWithWeights(queryBuilder);
        Collection<IndexMetadata> indexMetadataCollection = resolvedIndices.getConcreteLocalIndicesMetadata().values();

        Map<String, Map<String, InferenceFieldMetadata>> inferenceFieldsPerIndex = new HashMap<>();
        Map<String, Set<String>> nonInferenceFieldsPerIndex = new HashMap<>();
        Map<String, Float> fieldBoosts = new HashMap<>();

        for (IndexMetadata indexMetadata : indexMetadataCollection) {
            String indexName = indexMetadata.getIndex().getName();
            Map<String, InferenceFieldMetadata> indexInferenceFields = new HashMap<>();
            Map<String, InferenceFieldMetadata> indexInferenceMetadata = indexMetadata.getInferenceFields();

            // Resolve wildcards for inference fields and store boosts
            for (Map.Entry<String, Float> entry : fieldsWithWeights.entrySet()) {
                String field = entry.getKey();
                Float boost = entry.getValue();

                if (Regex.isMatchAllPattern(field)) {
                    indexInferenceMetadata.keySet().forEach(f -> {
                        indexInferenceFields.put(f, indexInferenceMetadata.get(f));
                        fieldBoosts.put(f, boost);
                    });
                } else if (Regex.isSimpleMatchPattern(field)) {
                    indexInferenceMetadata.keySet().stream().filter(f -> Regex.simpleMatch(field, f)).forEach(f -> {
                        indexInferenceFields.put(f, indexInferenceMetadata.get(f));
                        fieldBoosts.put(f, boost);
                    });
                } else if (indexInferenceMetadata.containsKey(field)) {
                    indexInferenceFields.put(field, indexInferenceMetadata.get(field));
                    fieldBoosts.put(field, boost);
                }
            }

            // Non-inference fields: original fields minus resolved inference fields
            Set<String> indexNonInferenceFields = new HashSet<>(fieldsWithWeights.keySet());
            indexNonInferenceFields.removeAll(indexInferenceFields.keySet());

            // Store boosts for non-inference fields in global fieldBoosts map
            for (String nonInferenceField : indexNonInferenceFields) {
                fieldBoosts.put(nonInferenceField, fieldsWithWeights.get(nonInferenceField));
            }

            if (indexInferenceFields.isEmpty() == false) {
                inferenceFieldsPerIndex.put(indexName, indexInferenceFields);
            }

            if (indexNonInferenceFields.isEmpty() == false) {
                nonInferenceFieldsPerIndex.put(indexName, indexNonInferenceFields);
            }
        }

        return new InferenceIndexInformationForField(inferenceFieldsPerIndex, nonInferenceFieldsPerIndex, fieldBoosts);
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
     * Represents the indices and associated inference information for fields.
     */
    public record InferenceIndexInformationForField(
        // Map: IndexName -> (FieldName -> InferenceFieldMetadata)
        Map<String, Map<String, InferenceFieldMetadata>> inferenceFieldsPerIndex,
        // Map: IndexName -> Set<FieldName> - non-inference fields per index (boosts stored in fieldBoosts)
        Map<String, Set<String>> nonInferenceFieldsPerIndex,
        // Map: FieldName -> Boost - stores boosts for all fields (both inference and non-inference)
        Map<String, Float> fieldBoosts
    ) {

        public Set<String> getAllInferenceFields() {
            return inferenceFieldsPerIndex.values().stream().flatMap(fields -> fields.keySet().stream()).collect(Collectors.toSet());
        }

        public boolean hasInferenceFields() {
            return inferenceFieldsPerIndex.isEmpty() == false;
        }

        public boolean hasNonInferenceFields() {
            return nonInferenceFieldsPerIndex.isEmpty() == false;
        }

        public Collection<String> getInferenceIndices() {
            return inferenceFieldsPerIndex.keySet();
        }

        public List<String> nonInferenceIndices() {
            return new ArrayList<>(nonInferenceFieldsPerIndex.keySet());
        }

        public Map<String, List<String>> getInferenceIdsIndices() {
            Map<String, List<String>> result = new HashMap<>();
            for (Map.Entry<String, Map<String, InferenceFieldMetadata>> indexEntry : inferenceFieldsPerIndex.entrySet()) {
                String indexName = indexEntry.getKey();
                for (InferenceFieldMetadata metadata : indexEntry.getValue().values()) {
                    String inferenceId = metadata.getSearchInferenceId();
                    result.computeIfAbsent(inferenceId, k -> new ArrayList<>()).add(indexName);
                }
            }
            return result;
        }

        /**
         * Returns the set of indices where the given field is a semantic field (has inference metadata).
         */
        public Set<String> getInferenceIndicesForField(String fieldName) {
            Set<String> indices = new HashSet<>();
            for (Map.Entry<String, Map<String, InferenceFieldMetadata>> entry : inferenceFieldsPerIndex.entrySet()) {
                if (entry.getValue().containsKey(fieldName)) {
                    indices.add(entry.getKey());
                }
            }
            return indices;
        }

        /**
         * @param fieldName the field name
         * @return the resolved boost for the field
         */
        public float getFieldBoost(String fieldName) {
            return fieldBoosts.getOrDefault(fieldName, AbstractQueryBuilder.DEFAULT_BOOST);
        }

    }
}
