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
import org.elasticsearch.index.query.MultiMatchQueryBuilder;
import org.elasticsearch.index.query.QueryBuilder;
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
            boolean isSemanticInAnyIndex = false;
            for (IndexMetadata indexMetadata : allIndicesMetadata) {
                if (indexMetadata.getInferenceFields().containsKey(fieldName)) {
                    isSemanticInAnyIndex = true;
                    break;
                }
            }
            if (isSemanticInAnyIndex) {
                semanticFields.put(fieldName, fieldEntry.getValue());
            } else {
                otherFields.put(fieldName, fieldEntry.getValue());
            }
        }

        if (semanticFields.isEmpty()) {
            return queryBuilder;
        }

        BoolQueryBuilder rewrittenQuery = new BoolQueryBuilder();
        if (otherFields.isEmpty() == false) {
            MultiMatchQueryBuilder lexicalPart = new MultiMatchQueryBuilder(multiMatchBuilder.value());
            lexicalPart.fields(otherFields);
            lexicalPart.type(multiMatchBuilder.type());
            lexicalPart.operator(multiMatchBuilder.operator());
            lexicalPart.analyzer(multiMatchBuilder.analyzer());
            lexicalPart.slop(multiMatchBuilder.slop());
            if (multiMatchBuilder.fuzziness() != null) {
                lexicalPart.fuzziness(multiMatchBuilder.fuzziness());
            }
            lexicalPart.prefixLength(multiMatchBuilder.prefixLength());
            lexicalPart.maxExpansions(multiMatchBuilder.maxExpansions());
            lexicalPart.minimumShouldMatch(multiMatchBuilder.minimumShouldMatch());
            lexicalPart.fuzzyRewrite(multiMatchBuilder.fuzzyRewrite());
            if (multiMatchBuilder.tieBreaker() != null) {
                lexicalPart.tieBreaker(multiMatchBuilder.tieBreaker());
            }
            lexicalPart.lenient(multiMatchBuilder.lenient());
            lexicalPart.zeroTermsQuery(multiMatchBuilder.zeroTermsQuery());
            lexicalPart.autoGenerateSynonymsPhraseQuery(multiMatchBuilder.autoGenerateSynonymsPhraseQuery());
            lexicalPart.fuzzyTranspositions(multiMatchBuilder.fuzzyTranspositions());
            rewrittenQuery.should(lexicalPart);
        }

        if (semanticFields.isEmpty() == false) {
            BoolQueryBuilder semanticPart = new BoolQueryBuilder();
            for (Map.Entry<String, Float> fieldEntry : semanticFields.entrySet()) {
                SemanticQueryBuilder semanticQueryBuilder = new SemanticQueryBuilder(fieldEntry.getKey(), multiMatchBuilder.value().toString(), true);
                if (fieldEntry.getValue() != 1.0f) {
                    semanticQueryBuilder.boost(fieldEntry.getValue());
                }
                semanticPart.should(semanticQueryBuilder);
            }
            rewrittenQuery.should(semanticPart);
        }

        rewrittenQuery.boost(multiMatchBuilder.boost());
        rewrittenQuery.queryName(multiMatchBuilder.queryName());

        return rewrittenQuery;
    }

    @Override
    public String getQueryName() {
        return MultiMatchQueryBuilder.NAME;
    }
}
