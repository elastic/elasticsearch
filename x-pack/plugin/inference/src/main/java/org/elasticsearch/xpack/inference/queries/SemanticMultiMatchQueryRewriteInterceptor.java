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
import org.elasticsearch.index.query.BoolQueryBuilder;
import org.elasticsearch.index.query.DisMaxQueryBuilder;
import org.elasticsearch.index.query.MultiMatchQueryBuilder;
import org.elasticsearch.index.query.QueryBuilder;
import org.elasticsearch.index.query.QueryBuilders;
import org.elasticsearch.index.query.QueryRewriteContext;
import org.elasticsearch.inference.MinimalServiceSettings;
import org.elasticsearch.inference.TaskType;
import org.elasticsearch.plugins.internal.rewriter.QueryRewriteInterceptor;
import org.elasticsearch.xpack.inference.registry.ModelRegistry;

import java.io.IOException;
import java.util.Collection;
import java.util.HashMap;
import java.util.Map;
import java.util.Objects;
import java.util.function.Supplier;

public class SemanticMultiMatchQueryRewriteInterceptor implements QueryRewriteInterceptor {

    private static final String SCORE_MISMATCH_WARNING = "multi_match query is targeting a mixture of semantic_text fields with dense "
        + "and sparse models, or a mixture of semantic_text and non-inference fields. Score ranges will not be comparable.";

    private final Supplier<ModelRegistry> modelRegistrySupplier;

    public SemanticMultiMatchQueryRewriteInterceptor(Supplier<ModelRegistry> modelRegistrySupplier) {
        this.modelRegistrySupplier = Objects.requireNonNull(modelRegistrySupplier);
    }

    @Override
    public QueryBuilder interceptAndRewrite(QueryRewriteContext context, QueryBuilder queryBuilder) {
        if (queryBuilder instanceof MultiMatchQueryBuilder == false) {
            return queryBuilder;
        }

        MultiMatchQueryBuilder multiMatchBuilder = (MultiMatchQueryBuilder) queryBuilder;
        ResolvedIndices resolvedIndices = context.getResolvedIndices();
        if (resolvedIndices == null) {
            return queryBuilder;
        }

        Map<String, Float> semanticFields = new HashMap<>();
        Map<String, Float> otherFields = new HashMap<>();
        Collection<IndexMetadata> allIndicesMetadata = resolvedIndices.getConcreteLocalIndicesMetadata().values();

        boolean hasDenseSemanticField = false;
        boolean hasSparseSemanticField = false;

        ModelRegistry modelRegistry = modelRegistrySupplier.get();
        if (modelRegistry == null) {
            // Should not happen in a sane lifecycle, but protect against it
            return queryBuilder;
        }

        for (Map.Entry<String, Float> fieldEntry : multiMatchBuilder.fields().entrySet()) {
            String fieldName = fieldEntry.getKey();
            InferenceFieldMetadata inferenceMetadata = findInferenceMetadata(fieldName, allIndicesMetadata);

            if (inferenceMetadata != null) {
                semanticFields.put(fieldName, fieldEntry.getValue());
                MinimalServiceSettings settings = modelRegistry.getMinimalServiceSettings(inferenceMetadata.getSearchInferenceId());
                if (settings != null) {
                    if (settings.taskType() == TaskType.TEXT_EMBEDDING) {
                        hasDenseSemanticField = true;
                    } else if (settings.taskType() == TaskType.SPARSE_EMBEDDING) {
                        hasSparseSemanticField = true;
                    }
                }
            } else {
                otherFields.put(fieldName, fieldEntry.getValue());
            }
        }

        if (semanticFields.isEmpty()) {
            return queryBuilder;
        }

        if (hasDenseSemanticField && (hasSparseSemanticField || otherFields.isEmpty() == false)) {
            HeaderWarning.addWarning(SCORE_MISMATCH_WARNING);
        }

        MultiMatchQueryBuilder.Type type = multiMatchBuilder.type();
        if (type == MultiMatchQueryBuilder.Type.CROSS_FIELDS ||
            type == MultiMatchQueryBuilder.Type.PHRASE ||
            type == MultiMatchQueryBuilder.Type.PHRASE_PREFIX) {
            throw new IllegalArgumentException("Query type [" + type.parseField().getPreferredName() + "] is not supported with semantic_text fields");
        }

        QueryBuilder rewrittenQuery;
        switch (type) {
            case BEST_FIELDS:
                DisMaxQueryBuilder disMaxQuery = QueryBuilders.disMaxQuery();
                if (otherFields.isEmpty() == false) {
                    disMaxQuery.add(createLexicalQuery(multiMatchBuilder, otherFields));
                }
                for (Map.Entry<String, Float> fieldEntry : semanticFields.entrySet()) {
                    disMaxQuery.add(createSemanticQuery(multiMatchBuilder.value().toString(), fieldEntry));
                }
                Float tieBreaker = multiMatchBuilder.tieBreaker();
                if (tieBreaker != null) {
                    disMaxQuery.tieBreaker(tieBreaker);
                }
                rewrittenQuery = disMaxQuery;
                break;
            case MOST_FIELDS:
            case BOOL_PREFIX:
            default:
                BoolQueryBuilder boolQuery = new BoolQueryBuilder();
                if (otherFields.isEmpty() == false) {
                    boolQuery.should(createLexicalQuery(multiMatchBuilder, otherFields));
                }
                if (semanticFields.isEmpty() == false) {
                    boolQuery.should(createSemanticQuery(multiMatchBuilder.value().toString(), semanticFields));
                }
                rewrittenQuery = boolQuery;
                break;
        }

        rewrittenQuery.boost(multiMatchBuilder.boost());
        rewrittenQuery.queryName(multiMatchBuilder.queryName());
        return rewrittenQuery;
    }

    @Override
    public String getQueryName() {
        return MultiMatchQueryBuilder.NAME;
    }

    private InferenceFieldMetadata findInferenceMetadata(String fieldName, Collection<IndexMetadata> allIndicesMetadata) {
        for (IndexMetadata indexMetadata : allIndicesMetadata) {
            InferenceFieldMetadata inferenceMetadata = indexMetadata.getInferenceFields().get(fieldName);
            if (inferenceMetadata != null) {
                return inferenceMetadata;
            }
        }
        return null;
    }

    private QueryBuilder createLexicalQuery(MultiMatchQueryBuilder original, Map<String, Float> lexicalFields) {
        MultiMatchQueryBuilder lexicalPart = new MultiMatchQueryBuilder(original.value());
        lexicalPart.fields(lexicalFields);
        lexicalPart.type(original.type());
        lexicalPart.operator(original.operator());
        lexicalPart.analyzer(original.analyzer());
        lexicalPart.slop(original.slop());
        if (original.fuzziness() != null) {
            lexicalPart.fuzziness(original.fuzziness());
        }
        lexicalPart.prefixLength(original.prefixLength());
        lexicalPart.maxExpansions(original.maxExpansions());
        lexicalPart.minimumShouldMatch(original.minimumShouldMatch());
        lexicalPart.fuzzyRewrite(original.fuzzyRewrite());
        if (original.tieBreaker() != null) {
            lexicalPart.tieBreaker(original.tieBreaker());
        }
        lexicalPart.lenient(original.lenient());
        lexicalPart.zeroTermsQuery(original.zeroTermsQuery());
        lexicalPart.autoGenerateSynonymsPhraseQuery(original.autoGenerateSynonymsPhraseQuery());
        lexicalPart.fuzzyTranspositions(original.fuzzyTranspositions());
        return lexicalPart;
    }

    private QueryBuilder createSemanticQuery(String queryText, Map<String, Float> semanticFields) {
        if (semanticFields.size() == 1) {
            return createSemanticQuery(queryText, semanticFields.entrySet().iterator().next());
        }

        BoolQueryBuilder boolQuery = new BoolQueryBuilder();
        for (Map.Entry<String, Float> fieldEntry : semanticFields.entrySet()) {
            boolQuery.should(createSemanticQuery(queryText, fieldEntry));
        }
        return boolQuery;
    }

    private QueryBuilder createSemanticQuery(String queryText, Map.Entry<String, Float> fieldEntry) {
        SemanticQueryBuilder semanticQuery = new SemanticQueryBuilder(fieldEntry.getKey(), queryText, true);
        if (fieldEntry.getValue() != 1.0f) {
            semanticQuery.boost(fieldEntry.getValue());
        }
        return semanticQuery;
    }
}
