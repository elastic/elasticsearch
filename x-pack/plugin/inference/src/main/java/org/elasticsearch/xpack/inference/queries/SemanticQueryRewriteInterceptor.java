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
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.index.mapper.IndexFieldMapper;
import org.elasticsearch.index.query.AbstractQueryBuilder;
import org.elasticsearch.index.query.BoolQueryBuilder;
import org.elasticsearch.index.query.QueryBuilder;
import org.elasticsearch.index.query.QueryRewriteContext;
import org.elasticsearch.index.query.TermsQueryBuilder;
import org.elasticsearch.index.search.QueryParserHelper;
import org.elasticsearch.plugins.internal.rewriter.QueryRewriteInterceptor;

import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;

import static org.elasticsearch.index.IndexSettings.DEFAULT_FIELD_SETTING;

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

        boolean resolveInferenceFieldWildcards = shouldResolveInferenceFieldWildcards(queryBuilder);
        InferenceIndexInformationForField indexInformation = resolveIndicesForFields(
            queryBuilder,
            resolvedIndices,
            resolveInferenceFieldWildcards
        );
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
     * Determines if inference field wildcards should be resolved.
     * This is typically used to expand wildcard queries to all inference fields.
     *
     * @param queryBuilder {@link QueryBuilder}
     * @return true if inference field wildcards should be resolved, false otherwise.
     */
    protected abstract boolean shouldResolveInferenceFieldWildcards(QueryBuilder queryBuilder);

    /**
     * Determines if this query type should use default fields when no fields are specified.
     * This is typically only needed for multi_match queries.
     * Default implementation returns false for most query types.
     *
     * @return true if default fields should be used when no fields are specified, false otherwise.
     */
    protected boolean shouldUseDefaultFields() {
        return false;
    }

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

    private static void addToFieldBoostsMap(Map<String, Float> fieldBoosts, String field, Float boost) {
        fieldBoosts.compute(field, (k, v) -> v == null ? boost : v * boost);
    }

    protected InferenceIndexInformationForField resolveIndicesForFields(
        QueryBuilder queryBuilder,
        ResolvedIndices resolvedIndices,
        boolean resolveInferenceFieldWildcards
    ) {
        Map<String, Float> fieldsWithWeights = getFieldsWithWeights(queryBuilder);
        Collection<IndexMetadata> indexMetadataCollection = resolvedIndices.getConcreteLocalIndicesMetadata().values();

        // Simple implementation: only handle explicit inference fields (no wildcards)
        Map<String, Map<String, InferenceFieldMetadata>> inferenceFieldsPerIndex = new HashMap<>();
        Map<String, Set<String>> nonInferenceFieldsPerIndex = new HashMap<>();
        Map<String, Float> allFieldBoosts = new HashMap<>();

        for (IndexMetadata indexMetadata : indexMetadataCollection) {
            String indexName = indexMetadata.getIndex().getName();
            Map<String, InferenceFieldMetadata> indexInferenceFields = new HashMap<>();
            Map<String, InferenceFieldMetadata> indexInferenceMetadata = indexMetadata.getInferenceFields();

            // Handle default fields per index when no fields are specified (only for multi_match queries)
            Map<String, Float> fieldsToProcess = fieldsWithWeights;
            if (fieldsToProcess.isEmpty() && shouldUseDefaultFields()) {
                Settings settings = indexMetadata.getSettings();
                List<String> defaultFields = settings.getAsList(DEFAULT_FIELD_SETTING.getKey(), DEFAULT_FIELD_SETTING.getDefault(settings));
                fieldsToProcess = QueryParserHelper.parseFieldsAndWeights(defaultFields);
            }

            // Collect resolved inference fields for this index
            Set<String> resolvedInferenceFields = new HashSet<>();

            // Handle explicit inference fields only
            for (Map.Entry<String, Float> entry : fieldsToProcess.entrySet()) {
                String field = entry.getKey();
                Float boost = entry.getValue();

                if (indexInferenceMetadata.containsKey(field)) {
                    indexInferenceFields.put(field, indexInferenceMetadata.get(field));
                    resolvedInferenceFields.add(field);
                    addToFieldBoostsMap(allFieldBoosts, field, boost);
                }
            }

            // Non-inference fields
            Set<String> indexNonInferenceFields = new HashSet<>(fieldsToProcess.keySet());
            indexNonInferenceFields.removeAll(resolvedInferenceFields);

            // Store boosts for non-inference field patterns
            for (String nonInferenceField : indexNonInferenceFields) {
                addToFieldBoostsMap(allFieldBoosts, nonInferenceField, fieldsToProcess.get(nonInferenceField));
            }

            if (indexInferenceFields.isEmpty() == false) {
                inferenceFieldsPerIndex.put(indexName, indexInferenceFields);
            }

            if (indexNonInferenceFields.isEmpty() == false) {
                nonInferenceFieldsPerIndex.put(indexName, indexNonInferenceFields);
            }
        }

        return new InferenceIndexInformationForField(inferenceFieldsPerIndex, nonInferenceFieldsPerIndex, allFieldBoosts);
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
