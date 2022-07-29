/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.ml.utils;

import org.elasticsearch.common.Strings;
import org.elasticsearch.common.regex.Regex;
import org.elasticsearch.index.query.BoolQueryBuilder;
import org.elasticsearch.index.query.QueryBuilder;
import org.elasticsearch.index.query.TermsQueryBuilder;
import org.elasticsearch.index.query.WildcardQueryBuilder;

import java.util.ArrayList;
import java.util.List;
import java.util.Optional;

public final class QueryBuilderHelper {

    private QueryBuilderHelper() {}

    /**
     * Helper function for adding OR type queries for a given identity field.
     *
     * The filter consists of should clauses (i.e. "or" boolean queries).
     *
     * - When a token is a wildcard token, a wildcard query is added
     * - When a token is NOT a wildcard, a term query is added
     *
     * @param identityField The field to query for the tokens
     * @param tokens A non-null collection of tokens. Can include wildcards
     * @return An optional boolean query builder filled with "should" queries for the supplied tokens and identify field
     */
    public static Optional<QueryBuilder> buildTokenFilterQuery(String identityField, String[] tokens) {
        if (Strings.isAllOrWildcard(tokens)) {
            return Optional.empty();
        }

        BoolQueryBuilder shouldQueries = new BoolQueryBuilder();
        List<String> terms = new ArrayList<>();
        for (String token : tokens) {
            if (Regex.isSimpleMatchPattern(token)) {
                shouldQueries.should(new WildcardQueryBuilder(identityField, token));
            } else {
                terms.add(token);
            }
        }

        if (terms.isEmpty() == false) {
            shouldQueries.should(new TermsQueryBuilder(identityField, terms));
        }

        if (shouldQueries.should().isEmpty()) {
            return Optional.empty();
        }
        return Optional.of(shouldQueries);
    }

}
