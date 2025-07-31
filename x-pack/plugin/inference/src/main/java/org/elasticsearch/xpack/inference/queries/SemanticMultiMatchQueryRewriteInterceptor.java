/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.inference.queries;

import org.elasticsearch.index.query.BoolQueryBuilder;
import org.elasticsearch.index.query.MultiMatchQueryBuilder;
import org.elasticsearch.index.query.QueryBuilder;
import org.elasticsearch.index.query.QueryBuilders;

import java.util.Map;

public class SemanticMultiMatchQueryRewriteInterceptor extends SemanticQueryRewriteInterceptor {
    @Override
    protected Map<String, Float> getFieldNamesWithBoosts(QueryBuilder queryBuilder) {
        assert (queryBuilder instanceof MultiMatchQueryBuilder);
        MultiMatchQueryBuilder multiMatchQueryBuilder = (MultiMatchQueryBuilder) queryBuilder;
        return multiMatchQueryBuilder.fields();
    }

    @Override
    protected String getQuery(QueryBuilder queryBuilder) {
        assert (queryBuilder instanceof MultiMatchQueryBuilder);
        MultiMatchQueryBuilder multiMatchQueryBuilder = (MultiMatchQueryBuilder) queryBuilder;
        return (String) multiMatchQueryBuilder.value();
    }

    @Override
    protected QueryBuilder buildInferenceQuery(
        QueryBuilder queryBuilder,
        InferenceIndexInformationForField indexInformation,
        Float fieldBoost
    ) {
        SemanticQueryBuilder semanticQueryBuilder = new SemanticQueryBuilder(indexInformation.fieldName(), getQuery(queryBuilder), false);
        semanticQueryBuilder.boost(queryBuilder.boost() * fieldBoost);
        semanticQueryBuilder.queryName(queryBuilder.queryName());
        return semanticQueryBuilder;
    }

    @Override
    protected QueryBuilder buildCombinedInferenceAndNonInferenceQuery(
        QueryBuilder queryBuilder,
        InferenceIndexInformationForField indexInformation,
        Float fieldBoost
    ) {
        BoolQueryBuilder boolQueryBuilder = new BoolQueryBuilder();

        // Add the semantic part for inference indices
        boolQueryBuilder.should(
            createSemanticSubQuery(indexInformation.getInferenceIndices(), indexInformation.fieldName(), getQuery(queryBuilder))
        );

        // Add the non-semantic part for non-inference indices
        boolQueryBuilder.should(
            createSubQueryForIndices(
                indexInformation.nonInferenceIndices(),
                QueryBuilders.matchQuery(indexInformation.fieldName(), getQuery(queryBuilder))
            )
        );

        // Apply the field boost
        boolQueryBuilder.boost(queryBuilder.boost() * fieldBoost);
        boolQueryBuilder.queryName(queryBuilder.queryName());

        return boolQueryBuilder;
    }

    @Override
    public String getQueryName() {
        return MultiMatchQueryBuilder.NAME;
    }

    public static void copyMultiMatchConfiguration(MultiMatchQueryBuilder source, MultiMatchQueryBuilder target) {
        target.type(source.type());
        target.operator(source.operator());
        if (source.analyzer() != null) {
            target.analyzer(source.analyzer());
        }
        target.slop(source.slop());
        if (source.fuzziness() != null) {
            target.fuzziness(source.fuzziness());
        }
        target.prefixLength(source.prefixLength());
        target.maxExpansions(source.maxExpansions());
        if (source.minimumShouldMatch() != null) {
            target.minimumShouldMatch(source.minimumShouldMatch());
        }
        if (source.fuzzyRewrite() != null) {
            target.fuzzyRewrite(source.fuzzyRewrite());
        }
        if (source.tieBreaker() != null) {
            target.tieBreaker(source.tieBreaker());
        }
        target.lenient(source.lenient());
        target.zeroTermsQuery(source.zeroTermsQuery());
        target.autoGenerateSynonymsPhraseQuery(source.autoGenerateSynonymsPhraseQuery());
        target.fuzzyTranspositions(source.fuzzyTranspositions());
    }

}
