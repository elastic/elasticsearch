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
import org.elasticsearch.common.logging.HeaderWarning;
import org.elasticsearch.features.NodeFeature;
import org.elasticsearch.index.mapper.IndexFieldMapper;
import org.elasticsearch.index.query.BoolQueryBuilder;
import org.elasticsearch.index.query.DisMaxQueryBuilder;
import org.elasticsearch.index.query.MultiMatchQueryBuilder;
import org.elasticsearch.index.query.QueryBuilder;
import org.elasticsearch.index.query.QueryBuilders;
import org.elasticsearch.index.query.QueryRewriteContext;
import org.elasticsearch.index.query.TermsQueryBuilder;
import org.elasticsearch.inference.MinimalServiceSettings;
import org.elasticsearch.inference.TaskType;
import org.elasticsearch.plugins.internal.rewriter.QueryRewriteInterceptor;
import org.elasticsearch.xpack.inference.registry.ModelRegistry;

import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.function.Supplier;
import java.util.stream.Collectors;

public class SemanticMultiMatchQueryRewriteInterceptor implements QueryRewriteInterceptor {

    public static final NodeFeature SEMANTIC_MULTI_MATCH_QUERY_REWRITE_INTERCEPTION_SUPPORTED = new NodeFeature(
        "search.semantic_multi_match_query_rewrite_interception_supported"
    );

    private final Supplier<ModelRegistry> modelRegistrySupplier;
    private final float DEFAULT_BOOST_FIELD = 1.0f;


    public SemanticMultiMatchQueryRewriteInterceptor(Supplier<ModelRegistry> modelRegistrySupplier) {
        this.modelRegistrySupplier = modelRegistrySupplier;
    }

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
            return buildCombinedInferenceAndNonInferenceQuery(multiMatchQueryBuilder, inferenceInfo, fields);
        } else {
            // All specified fields are inference fields (semantic_text)
            return buildInferenceQuery(multiMatchQueryBuilder, inferenceInfo, fields);
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
            }
        }

        MultiFieldInferenceInfo inferenceInfo = new MultiFieldInferenceInfo(
            fieldNames,
            inferenceFieldsPerIndex,
            nonInferenceIndices
        );

        // Perform early detection of score range mismatches and emit warning if needed
        detectAndWarnScoreRangeMismatch(inferenceInfo);

        return inferenceInfo;
    }

    private QueryBuilder buildInferenceQuery(MultiMatchQueryBuilder originalQuery, MultiFieldInferenceInfo inferenceInfo, Map<String, Float> fieldsBoosts) {
        String queryValue = (String) originalQuery.value();
        Set<String> inferenceFields = inferenceInfo.getInferenceFields();

        if (inferenceFields.size() == 1) {
            // Single inference field - all multi_match types work the same (like original Elasticsearch)
            // No validation needed since single field queries don't require type-specific combination logic
            String fieldName = inferenceFields.iterator().next();
            SemanticQueryBuilder semanticQuery = new SemanticQueryBuilder(fieldName, queryValue, false);

            // Apply per-field boost
            float fieldBoost = fieldsBoosts.getOrDefault(fieldName, DEFAULT_BOOST_FIELD);
            semanticQuery.boost(fieldBoost);

            // Apply top-level query boost and name
            semanticQuery.boost(semanticQuery.boost() * originalQuery.boost());
            semanticQuery.queryName(originalQuery.queryName());
            return semanticQuery;
        } else {
            // Multiple inference fields - handle based on multi-match query type (validation happens here)
            return buildMultiFieldSemanticQuery(originalQuery, fieldsBoosts, inferenceFields, queryValue);
        }
    }

    private QueryBuilder buildMultiFieldSemanticQuery(
        MultiMatchQueryBuilder originalQuery,
        Map<String, Float> fieldsBoosts,
        Set<String> inferenceFields,
        String queryValue
    ) {
        return switch (originalQuery.type()) {
            case BEST_FIELDS -> buildBestFieldsSemanticQuery(originalQuery, fieldsBoosts, inferenceFields, queryValue);
            case MOST_FIELDS -> buildMostFieldsSemanticQuery(originalQuery, fieldsBoosts, inferenceFields, queryValue);
            default ->
                // Fallback to best_fields behavior for unknown types
                    buildBestFieldsSemanticQuery(originalQuery, fieldsBoosts, inferenceFields, queryValue);
        };
    }

    private QueryBuilder buildCombinedInferenceAndNonInferenceQuery(
        MultiMatchQueryBuilder originalQuery,
        MultiFieldInferenceInfo inferenceInfo,
        Map<String, Float> fieldsBoosts
    ) {
        validateQueryTypeSupported(originalQuery.type());

        String queryValue = (String) originalQuery.value();

        return switch (originalQuery.type()) {
            case BEST_FIELDS -> buildBestFieldsCombinedQuery(originalQuery, fieldsBoosts, inferenceInfo, queryValue);
            case MOST_FIELDS -> buildMostFieldsCombinedQuery(originalQuery, fieldsBoosts, inferenceInfo, queryValue);
            default ->
                // Fallback to best_fields behavior
                    buildBestFieldsCombinedQuery(originalQuery, fieldsBoosts, inferenceInfo, queryValue);
        };
    }

    /**
     * Validates that the multi_match query type is supported for semantic_text fields.
     * Throws IllegalArgumentException for unsupported types.
     */
    private void validateQueryTypeSupported(MultiMatchQueryBuilder.Type queryType) {
        switch (queryType) {
            case CROSS_FIELDS:
                throw new IllegalArgumentException(
                    "multi_match query with type [cross_fields] is not supported for semantic_text fields. " +
                    "Use [best_fields] or [most_fields] instead."
                );
            case PHRASE:
                throw new IllegalArgumentException(
                    "multi_match query with type [phrase] is not supported for semantic_text fields. " +
                    "Use [best_fields] instead."
                );
            case PHRASE_PREFIX:
                throw new IllegalArgumentException(
                    "multi_match query with type [phrase_prefix] is not supported for semantic_text fields. " +
                    "Use [best_fields] instead."
                );
            case BOOL_PREFIX:
                throw new IllegalArgumentException(
                    "multi_match query with type [bool_prefix] is not supported for semantic_text fields. " +
                    "Use [best_fields] or [most_fields] instead."
                );
        }
    }

    /**
     * Builds a best_fields query for pure semantic fields using DisMaxQueryBuilder.
     */
    private QueryBuilder buildBestFieldsSemanticQuery(
        MultiMatchQueryBuilder originalQuery,
        Map<String, Float> fieldsBoosts,
        Set<String> inferenceFields,
        String queryValue
    ) {
        DisMaxQueryBuilder disMaxQuery = QueryBuilders.disMaxQuery();
        for (String fieldName : inferenceFields) {
            SemanticQueryBuilder semanticQuery = new SemanticQueryBuilder(fieldName, queryValue, false);

            // Apply per-field boost if specified
            float fieldBoost = fieldsBoosts.getOrDefault(fieldName, DEFAULT_BOOST_FIELD);
            semanticQuery.boost(fieldBoost);

            disMaxQuery.add(semanticQuery);
        }
        // Apply tie_breaker if specified
        if (originalQuery.tieBreaker() != null) {
            disMaxQuery.tieBreaker(originalQuery.tieBreaker());
        }
        disMaxQuery.boost(originalQuery.boost());
        disMaxQuery.queryName(originalQuery.queryName());
        return disMaxQuery;
    }

    /**
     * Builds a most_fields query for pure semantic fields using BoolQueryBuilder.
     */
    private QueryBuilder buildMostFieldsSemanticQuery(
        MultiMatchQueryBuilder originalQuery,
        Map<String, Float> fieldsBoosts,
        Set<String> inferenceFields,
        String queryValue
    ) {
        BoolQueryBuilder boolQuery = new BoolQueryBuilder();
        for (String fieldName : inferenceFields) {
            SemanticQueryBuilder semanticQuery = new SemanticQueryBuilder(fieldName, queryValue, false);

            // Apply per-field boost if specified
            float fieldBoost = fieldsBoosts.getOrDefault(fieldName, DEFAULT_BOOST_FIELD);
            semanticQuery.boost(fieldBoost);

            boolQuery.should(semanticQuery);
        }
        boolQuery.minimumShouldMatch("1");
        boolQuery.boost(originalQuery.boost());
        boolQuery.queryName(originalQuery.queryName());
        return boolQuery;
    }

    private QueryBuilder buildBestFieldsCombinedQuery(
        MultiMatchQueryBuilder originalQuery,
        Map<String, Float> fieldsBoosts,
        MultiFieldInferenceInfo inferenceInfo,
        String queryValue
    ) {
        // For best_fields, use dis_max to find the single best matching field across all field types
        DisMaxQueryBuilder disMaxQuery = new DisMaxQueryBuilder();

        // Add semantic queries for inference fields per index
        Map<String, Map<String, InferenceFieldMetadata>> inferenceFieldsPerIndex = inferenceInfo.getInferenceFieldsPerIndex();
        for (Map.Entry<String, Map<String, InferenceFieldMetadata>> entry : inferenceFieldsPerIndex.entrySet()) {
            String indexName = entry.getKey();
            Map<String, InferenceFieldMetadata> indexInferenceFields = entry.getValue();

            for (String fieldName : indexInferenceFields.keySet()) {
                SemanticQueryBuilder semanticQuery = new SemanticQueryBuilder(fieldName, queryValue, true);

                // Apply per-field boost
                float fieldBoost = fieldsBoosts.getOrDefault(fieldName, DEFAULT_BOOST_FIELD);
                semanticQuery.boost(fieldBoost);

                BoolQueryBuilder indexSpecificQuery = new BoolQueryBuilder();
                indexSpecificQuery.must(semanticQuery);
                indexSpecificQuery.filter(new TermsQueryBuilder(IndexFieldMapper.NAME, List.of(indexName)));
                disMaxQuery.add(indexSpecificQuery);
            }
        }

        // Add non-inference query for indices without semantic_text fields
        // Note: Field boosts are preserved in the copied MultiMatchQueryBuilder
        if (inferenceInfo.getNonInferenceIndices().isEmpty() == false) {
            MultiMatchQueryBuilder nonInferenceQuery = copyMultiMatchQueryBuilder(originalQuery);
            BoolQueryBuilder indexFilteredQuery = new BoolQueryBuilder();
            indexFilteredQuery.must(nonInferenceQuery);
            indexFilteredQuery.filter(new TermsQueryBuilder(IndexFieldMapper.NAME, inferenceInfo.getNonInferenceIndices()));
            disMaxQuery.add(indexFilteredQuery);
        }

        // Apply tie_breaker if specified
        if (originalQuery.tieBreaker() != null) {
            disMaxQuery.tieBreaker(originalQuery.tieBreaker());
        }
        disMaxQuery.boost(originalQuery.boost());
        disMaxQuery.queryName(originalQuery.queryName());
        return disMaxQuery;
    }

    private QueryBuilder buildMostFieldsCombinedQuery(
        MultiMatchQueryBuilder originalQuery,
        Map<String, Float> fieldsBoosts,
        MultiFieldInferenceInfo inferenceInfo,
        String queryValue
    ) {
        // For most_fields, use bool should to score across all fields
        BoolQueryBuilder boolQuery = new BoolQueryBuilder();

        // Add semantic queries for inference fields per index
        Map<String, Map<String, InferenceFieldMetadata>> inferenceFieldsPerIndex = inferenceInfo.getInferenceFieldsPerIndex();
        for (Map.Entry<String, Map<String, InferenceFieldMetadata>> entry : inferenceFieldsPerIndex.entrySet()) {
            String indexName = entry.getKey();
            Map<String, InferenceFieldMetadata> indexInferenceFields = entry.getValue();

            for (String fieldName : indexInferenceFields.keySet()) {
                SemanticQueryBuilder semanticQuery = new SemanticQueryBuilder(fieldName, queryValue, true);

                // Apply per-field boost
                float fieldBoost = fieldsBoosts.getOrDefault(fieldName, DEFAULT_BOOST_FIELD);
                semanticQuery.boost(fieldBoost);

                BoolQueryBuilder indexSpecificQuery = new BoolQueryBuilder();
                indexSpecificQuery.must(semanticQuery);
                indexSpecificQuery.filter(new TermsQueryBuilder(IndexFieldMapper.NAME, List.of(indexName)));
                boolQuery.should(indexSpecificQuery);
            }
        }

        // Add non-inference query for indices without semantic_text fields
        // Note: Field boosts are preserved in the copied MultiMatchQueryBuilder
        if (inferenceInfo.getNonInferenceIndices().isEmpty() == false) {
            MultiMatchQueryBuilder nonInferenceQuery = copyMultiMatchQueryBuilder(originalQuery);
            BoolQueryBuilder indexFilteredQuery = new BoolQueryBuilder();
            indexFilteredQuery.must(nonInferenceQuery);
            indexFilteredQuery.filter(new TermsQueryBuilder(IndexFieldMapper.NAME, inferenceInfo.getNonInferenceIndices()));
            boolQuery.should(indexFilteredQuery);
        }

        boolQuery.minimumShouldMatch("1");
        boolQuery.boost(originalQuery.boost());
        boolQuery.queryName(originalQuery.queryName());
        return boolQuery;
    }

    /**
     * Detects and warns about score range mismatches when a multi_match query has at least one dense vector model (TEXT_EMBEDDING)
     * mixed with sparse vector models (SPARSE_EMBEDDING) or non-inference fields.
     * Dense vector models typically produce bounded scores (0-1) while sparse vector models and
     * non-inference fields produce unbounded scores, causing score range mismatches.
     */
    private void detectAndWarnScoreRangeMismatch(MultiFieldInferenceInfo inferenceInfo) {
        ModelRegistry modelRegistry = modelRegistrySupplier.get();
        // TODO: validate if we need to check if modelRegistry is null or not
        if (modelRegistry == null) {
            // Fallback: warn for any mixed semantic_text + non-inference combination
            // since we can't determine the exact task types
            if (inferenceInfo.hasNonInferenceFields() && inferenceInfo.getInferenceFields().isEmpty() == false) {
                HeaderWarning.addWarning(
                    "Query spans both semantic_text and non-inference fields. " +
                    "Dense vector models (TEXT_EMBEDDING) produce bounded scores (0-1) while sparse vector models " +
                    "(SPARSE_EMBEDDING) and non-inference fields produce unbounded scores, which may cause score " +
                    "range mismatches and affect result ranking. Consider using Linear or RRF retrievers."
                );
            }
            return;
        }

        // Check if we have any dense vector models mixed with sparse vector models or non-inference fields
        boolean hasDenseVectorModel = false;
        boolean hasSparseVectorModel = false;
        boolean hasNonInferenceFields = inferenceInfo.hasNonInferenceFields();

        // Collect all inference IDs from all fields
        Set<String> allInferenceIds = new HashSet<>();
        for (Map<String, InferenceFieldMetadata> indexFields : inferenceInfo.getInferenceFieldsPerIndex().values()) {
            for (InferenceFieldMetadata fieldMetadata : indexFields.values()) {
                allInferenceIds.add(fieldMetadata.getSearchInferenceId());
            }
        }

        // Check task types for each inference ID
        for (String inferenceId : allInferenceIds) {
            try {
                MinimalServiceSettings settings = modelRegistry.getMinimalServiceSettings(inferenceId);
                if (settings != null) {
                    TaskType taskType = settings.taskType();
                    if (taskType == TaskType.TEXT_EMBEDDING) {
                        hasDenseVectorModel = true;
                    } else if (taskType == TaskType.SPARSE_EMBEDDING) {
                        hasSparseVectorModel = true;
                    }
                }
            } catch (Exception e) {
                // TODO: validate If we can't get model info, skip this inference ID or throw an error
            }
        }

        // Emit warning only if we have dense vector model mixed with sparse vector or non-inference fields
        if (hasDenseVectorModel && (hasSparseVectorModel || hasNonInferenceFields)) {
            HeaderWarning.addWarning(
                "Query contains dense vector model (TEXT_EMBEDDING) with bounded scores (0-1) mixed with " +
                (hasSparseVectorModel ? "sparse vector model (SPARSE_EMBEDDING) and/or " : "") +
                (hasNonInferenceFields ? "non-inference fields " : "") +
                "that produce unbounded scores. This may cause score range mismatches and affect result ranking. " +
                "Consider using Linear or RRF retrievers."
            );
        }
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
        // TODO: Remove originalFields if not needed
        private final Set<String> originalFields;
        private final Map<String, Map<String, InferenceFieldMetadata>> inferenceFieldsPerIndex;
        private final List<String> nonInferenceIndices;

        public MultiFieldInferenceInfo(
            Set<String> originalFields,
            Map<String, Map<String, InferenceFieldMetadata>> inferenceFieldsPerIndex,
            List<String> nonInferenceIndices
        ) {
            this.originalFields = originalFields;
            this.inferenceFieldsPerIndex = inferenceFieldsPerIndex;
            this.nonInferenceIndices = nonInferenceIndices;
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
            return nonInferenceIndices.isEmpty() == false;
        }

    }
}
