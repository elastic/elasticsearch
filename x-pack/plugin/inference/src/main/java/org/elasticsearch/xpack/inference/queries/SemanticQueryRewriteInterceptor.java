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
        Set<String> fieldNames = fieldsWithWeights.keySet();
        Collection<IndexMetadata> indexMetadataCollection = resolvedIndices.getConcreteLocalIndicesMetadata().values();

        Map<String, Map<String, InferenceFieldMetadata>> inferenceFieldsPerIndex = new HashMap<>();
        Map<String, Set<String>> nonInferenceFieldsPerIndex = new HashMap<>();

        for (IndexMetadata indexMetadata : indexMetadataCollection) {
            String indexName = indexMetadata.getIndex().getName();
            Map<String, InferenceFieldMetadata> indexInferenceFields = new HashMap<>();
            Set<String> indexNonInferenceFields = new HashSet<>();

            // Classify each field as inference or non-inference
            for (String fieldName : fieldNames) {
                if (indexMetadata.getInferenceFields().containsKey(fieldName)) {
                    indexInferenceFields.put(fieldName, indexMetadata.getInferenceFields().get(fieldName));
                } else {
                    indexNonInferenceFields.add(fieldName);
                }
            }

            // Store inference fields if any exist
            if (indexInferenceFields.isEmpty() == false) {
                inferenceFieldsPerIndex.put(indexName, indexInferenceFields);
            }

            // Store non-inference fields if any exist
            if (indexNonInferenceFields.isEmpty() == false) {
                nonInferenceFieldsPerIndex.put(indexName, indexNonInferenceFields);
            }
        }

        return new InferenceIndexInformationForField(inferenceFieldsPerIndex, nonInferenceFieldsPerIndex);
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
        // Map: IndexName -> Set of non-inference field names
        Map<String, Set<String>> nonInferenceFieldsPerIndex
    ) {

        // Backward compatibility for single-field queries
        public InferenceIndexInformationForField(String fieldName, Map<String, InferenceFieldMetadata> inferenceIndicesMetadata, List<String> nonInferenceIndices) {
            this(
                // Convert single field metadata to multi-field structure
                inferenceIndicesMetadata.entrySet().stream()
                    .collect(Collectors.toMap(
                        Map.Entry::getKey,
                        entry -> Map.of(fieldName, entry.getValue())
                    )),
                // Convert non-inference indices to multi-field structure
                nonInferenceIndices.stream()
                    .collect(Collectors.toMap(
                        indexName -> indexName,
                        indexName -> Set.of(fieldName)
                    ))
            );
        }

        public Set<String> getAllInferenceFields() {
            return inferenceFieldsPerIndex.values()
                .stream()
                .flatMap(fields -> fields.keySet().stream())
                .collect(Collectors.toSet());
        }

        public Set<String> getAllNonInferenceFields() {
            return nonInferenceFieldsPerIndex.values()
                .stream()
                .flatMap(Set::stream)
                .collect(Collectors.toSet());
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
    }
}
