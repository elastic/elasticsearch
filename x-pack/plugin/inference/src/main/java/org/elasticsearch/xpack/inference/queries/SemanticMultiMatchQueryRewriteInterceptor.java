/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.inference.queries;

import org.elasticsearch.action.ResolvedIndices;
import org.elasticsearch.cluster.metadata.IndexMetadata;
import org.elasticsearch.index.query.BoolQueryBuilder;
import org.elasticsearch.index.query.DisMaxQueryBuilder;
import org.elasticsearch.index.query.MultiMatchQueryBuilder;
import org.elasticsearch.index.query.QueryBuilder;
import org.elasticsearch.index.query.QueryBuilders;
import org.elasticsearch.index.query.QueryRewriteContext;
import org.elasticsearch.plugins.internal.rewriter.QueryRewriteInterceptor;

import java.io.IOException;
import java.util.Collection;
import java.util.HashMap;
import java.util.Map;

public class SemanticMultiMatchQueryRewriteInterceptor implements QueryRewriteInterceptor {

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

        for (Map.Entry<String, Float> fieldEntry : multiMatchBuilder.fields().entrySet()) {
            String fieldName = fieldEntry.getKey();
            boolean isSemanticInAnyIndex = allIndicesMetadata.stream()
                .anyMatch(indexMetadata -> indexMetadata.getInferenceFields().containsKey(fieldName));
            if (isSemanticInAnyIndex) {
                semanticFields.put(fieldName, fieldEntry.getValue());
            } else {
                otherFields.put(fieldName, fieldEntry.getValue());
            }
        }

        if (semanticFields.isEmpty()) {
            return queryBuilder;
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

    @Override
    public String getQueryName() {
        return MultiMatchQueryBuilder.NAME;
    }
}
