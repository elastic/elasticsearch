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
        Map<String, Map<String, Float>> nonInferenceFieldsPerIndex = new HashMap<>();
        Map<String, Float> globalResolvedInferenceFieldBoosts = new HashMap<>();

        for (IndexMetadata indexMetadata : indexMetadataCollection) {
            String indexName = indexMetadata.getIndex().getName();
            Map<String, InferenceFieldMetadata> indexInferenceFields = new HashMap<>();
            Map<String, Float> indexNonInferenceFields = new HashMap<>();

            Map<String, InferenceFieldMetadata> indexInferenceMetadata = indexMetadata.getInferenceFields();

            // Expand wildcards for inference fields only (following RRF pattern)
            Map<String, Float> resolvedInferenceFields = new HashMap<>();
            for (Map.Entry<String, Float> entry : fieldsWithWeights.entrySet()) {
                String field = entry.getKey();
                Float weight = entry.getValue();

                if (Regex.isMatchAllPattern(field)) {
                    // Handle "*" - match all inference fields
                    indexInferenceMetadata.keySet().forEach(f ->
                        addToInferenceFieldsMap(resolvedInferenceFields, f, weight));
                } else if (Regex.isSimpleMatchPattern(field)) {
                    // Handle wildcards like "text*", "*field", etc.
                    indexInferenceMetadata.keySet()
                        .stream()
                        .filter(f -> Regex.simpleMatch(field, f))
                        .forEach(f -> addToInferenceFieldsMap(resolvedInferenceFields, f, weight));
                } else {
                    // No wildcards in field name - exact match
                    if (indexInferenceMetadata.containsKey(field)) {
                        addToInferenceFieldsMap(resolvedInferenceFields, field, weight);
                    }
                }
            }

            // Copy resolved inference fields to metadata map and aggregate global boosts
            for (String fieldName : resolvedInferenceFields.keySet()) {
                indexInferenceFields.put(fieldName, indexInferenceMetadata.get(fieldName));
                // Store the resolved boost globally (same field should have same boost across indices)
                globalResolvedInferenceFieldBoosts.put(fieldName, resolvedInferenceFields.get(fieldName));
            }

            // Non-inference fields: start with all original patterns, remove only resolved inference field names
            // This preserves wildcard patterns that MultiMatchQueryBuilder will expand itself
            indexNonInferenceFields = new HashMap<>(fieldsWithWeights);
            indexNonInferenceFields.keySet().removeAll(resolvedInferenceFields.keySet());

            // Store inference fields if any exist
            if (indexInferenceFields.isEmpty() == false) {
                inferenceFieldsPerIndex.put(indexName, indexInferenceFields);
            }

            // Store non-inference fields if any exist
            if (indexNonInferenceFields.isEmpty() == false) {
                nonInferenceFieldsPerIndex.put(indexName, indexNonInferenceFields);
            }
        }

        return new InferenceIndexInformationForField(inferenceFieldsPerIndex, nonInferenceFieldsPerIndex, globalResolvedInferenceFieldBoosts);
    }

    /**
     * Helper method to add inference fields with weight handling like in RRF
     */
    private void addToInferenceFieldsMap(Map<String, Float> inferenceFields, String fieldName, Float weight) {
        inferenceFields.compute(fieldName, (k, v) -> v == null ? weight : v * weight);
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
     * Represents the indices and associated inference information for fields.
     */
    public record InferenceIndexInformationForField(
        // Map: IndexName -> (FieldName -> InferenceFieldMetadata)
        Map<String, Map<String, InferenceFieldMetadata>> inferenceFieldsPerIndex,
        // Map: IndexName -> (FieldName -> Boost)
        Map<String, Map<String, Float>> nonInferenceFieldsPerIndex,
        // Map: FieldName -> ResolvedBoost - stores resolved wildcard boosts for inference fields
        Map<String, Float> resolvedInferenceFieldBoosts
    ) {

        // Backward compatibility for single-field queries
        public InferenceIndexInformationForField(
            String fieldName,
            Map<String, InferenceFieldMetadata> inferenceIndicesMetadata,
            List<String> nonInferenceIndices
        ) {
            this(
                // Convert single field metadata to multi-field structure
                inferenceIndicesMetadata.entrySet()
                    .stream()
                    .collect(Collectors.toMap(Map.Entry::getKey, entry -> Map.of(fieldName, entry.getValue()))),
                // Convert non-inference indices to multi-field structure with default boost
                nonInferenceIndices.stream().collect(Collectors.toMap(indexName -> indexName, indexName -> Map.of(fieldName, 1.0f))),
                // Default boost for single-field (no wildcards)
                Map.of(fieldName, 1.0f)
            );
        }

        public Set<String> getAllInferenceFields() {
            return inferenceFieldsPerIndex.values().stream().flatMap(fields -> fields.keySet().stream()).collect(Collectors.toSet());
        }

        public boolean hasInferenceFields() {
            return inferenceFieldsPerIndex.isEmpty() == false;
        }

        public boolean hasNonInferenceFields() {
            return nonInferenceFieldsPerIndex.isEmpty() == false;
        }

        // Backward compatibility methods
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
         * Returns the resolved boost for an inference field.
         * This accounts for wildcard expansion boosts.
         *
         * @param fieldName the field name
         * @return the resolved boost for the field
         */
        public float getInferenceFieldBoost(String fieldName) {
            return resolvedInferenceFieldBoosts.getOrDefault(fieldName, AbstractQueryBuilder.DEFAULT_BOOST);
        }

    }
}
