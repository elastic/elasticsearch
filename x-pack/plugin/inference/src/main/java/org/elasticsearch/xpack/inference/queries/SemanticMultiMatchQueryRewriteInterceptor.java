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

        MultiFieldInferenceInfo inferenceInfo = new MultiFieldInferenceInfo(
            fieldNames,
            inferenceFieldsPerIndex,
            nonInferenceIndices,
            inferenceFieldsByIndex
        );

        // Perform early detection of score range mismatches and emit warning if needed
        detectAndWarnScoreRangeMismatch(inferenceInfo);

        return inferenceInfo;
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
            // Multiple inference fields - handle based on multi-match query type
            return buildMultiFieldSemanticQuery(originalQuery, inferenceFields, queryValue);
        }
    }

    private QueryBuilder buildMultiFieldSemanticQuery(
        MultiMatchQueryBuilder originalQuery,
        Set<String> inferenceFields,
        String queryValue
    ) {
        switch (originalQuery.type()) {
            case BEST_FIELDS:
                // For best_fields, use dis_max to find the single best matching field
                // This mimics the behavior of multi_match best_fields which wraps match queries in dis_max
                DisMaxQueryBuilder disMaxQuery = new DisMaxQueryBuilder();
                for (String fieldName : inferenceFields) {
                    SemanticQueryBuilder semanticQuery = new SemanticQueryBuilder(fieldName, queryValue, false);
                    disMaxQuery.add(semanticQuery);
                }
                // Apply tie_breaker if specified
                if (originalQuery.tieBreaker() != null) {
                    disMaxQuery.tieBreaker(originalQuery.tieBreaker());
                }
                disMaxQuery.boost(originalQuery.boost());
                disMaxQuery.queryName(originalQuery.queryName());
                return disMaxQuery;

            case MOST_FIELDS:
                // For most_fields, we want to score across all fields and sum the scores
                // This can be reasonably approximated with semantic queries
                BoolQueryBuilder boolQuery = new BoolQueryBuilder();
                for (String fieldName : inferenceFields) {
                    SemanticQueryBuilder semanticQuery = new SemanticQueryBuilder(fieldName, queryValue, false);
                    boolQuery.should(semanticQuery);
                }
                boolQuery.minimumShouldMatch("1");
                boolQuery.boost(originalQuery.boost());
                boolQuery.queryName(originalQuery.queryName());
                return boolQuery;

            case CROSS_FIELDS:
                // Cross-fields requires term-level analysis across fields which doesn't translate
                // meaningfully to semantic queries that work with dense vectors
                throw new IllegalArgumentException(
                    "multi_match query with type [cross_fields] is not supported for semantic_text fields. " +
                    "Use [best_fields] or [most_fields] instead."
                );

            case PHRASE:
                // Phrase queries require positional information which semantic queries don't have
                throw new IllegalArgumentException(
                    "multi_match query with type [phrase] is not supported for semantic_text fields. " +
                    "Use [best_fields] instead."
                );

            case PHRASE_PREFIX:
                // Phrase prefix queries require positional and prefix information
                throw new IllegalArgumentException(
                    "multi_match query with type [phrase_prefix] is not supported for semantic_text fields. " +
                    "Use [best_fields] instead."
                );

            case BOOL_PREFIX:
                // Bool prefix requires term-level prefix analysis
                throw new IllegalArgumentException(
                    "multi_match query with type [bool_prefix] is not supported for semantic_text fields. " +
                    "Use [best_fields] or [most_fields] instead."
                );

            default:
                // Fallback to best_fields behavior for unknown types
                DisMaxQueryBuilder defaultDisMaxQuery = new DisMaxQueryBuilder();
                for (String fieldName : inferenceFields) {
                    SemanticQueryBuilder semanticQuery = new SemanticQueryBuilder(fieldName, queryValue, false);
                    defaultDisMaxQuery.add(semanticQuery);
                }
                if (originalQuery.tieBreaker() != null) {
                    defaultDisMaxQuery.tieBreaker(originalQuery.tieBreaker());
                }
                defaultDisMaxQuery.boost(originalQuery.boost());
                defaultDisMaxQuery.queryName(originalQuery.queryName());
                return defaultDisMaxQuery;
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

    /**
     * Detects and warns about score range mismatches when a multi_match query has at least one dense vector model (TEXT_EMBEDDING)
     * mixed with sparse vector models (SPARSE_EMBEDDING) or non-inference fields.
     * Dense vector models typically produce bounded scores (0-1) while sparse vector models and
     * non-inference fields produce unbounded scores, causing score range mismatches.
     */
    private void detectAndWarnScoreRangeMismatch(MultiFieldInferenceInfo inferenceInfo) {
        ModelRegistry modelRegistry = modelRegistrySupplier.get();
        if (modelRegistry == null) {
            // Fallback: warn for any mixed semantic_text + non-inference combination
            // since we can't determine the exact task types
            if (inferenceInfo.hasNonInferenceFields() && inferenceInfo.getInferenceFields().isEmpty() == false) {
                HeaderWarning.addWarning(
                    "Query spans both semantic_text and non-inference fields. " +
                    "Dense vector models (TEXT_EMBEDDING) produce bounded scores (0-1) while sparse vector models " +
                    "(SPARSE_EMBEDDING) and non-inference fields produce unbounded scores, which may cause score " +
                    "range mismatches and affect result ranking. Consider using separate queries or score normalization."
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
                // If we can't get model info, skip this inference ID
                // Or maybe we can throw an error
            }
        }

        // Emit warning only if we have dense vector model mixed with sparse vector or non-inference fields
        if (hasDenseVectorModel && (hasSparseVectorModel || hasNonInferenceFields)) {
            HeaderWarning.addWarning(
                "Query contains dense vector model (TEXT_EMBEDDING) with bounded scores (0-1) mixed with " +
                (hasSparseVectorModel ? "sparse vector model (SPARSE_EMBEDDING) and/or " : "") +
                (hasNonInferenceFields ? "non-inference fields " : "") +
                "that produce unbounded scores. This may cause score range mismatches and affect result ranking. " +
                "Consider using separate queries or score normalization for optimal results."
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
            return nonInferenceIndices.isEmpty() == false;
        }

        public Map<String, Set<String>> getInferenceFieldsByIndex() {
            return inferenceFieldsByIndex;
        }
    }
}
