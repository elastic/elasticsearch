/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.enterprisesearch.search;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.elasticsearch.index.query.BoolQueryBuilder;
import org.elasticsearch.index.query.MultiMatchQueryBuilder;
import org.elasticsearch.index.query.QueryBuilder;
import org.elasticsearch.index.query.QueryBuilders;
import org.elasticsearch.xpack.enterprisesearch.action.EntSearchRequest;

// It would be interesting if this implemented AbstractQueryBuilder here
public final class EntSearchQueryBuilder {

    private static final Logger logger = LogManager.getLogger(EntSearchQueryBuilder.class);

    private EntSearchQueryBuilder() {
        throw new AssertionError("Class not meant for instantiation");
    }

    public static QueryBuilder getQueryBuilder(EntSearchRequest entSearchRequest) {
        MultiMatchQueryBuilder multiMatchQueryBuilder = QueryBuilders.multiMatchQuery(entSearchRequest.getQuery());
        for (String searchField : entSearchRequest.getSearchFields()) {
            multiMatchQueryBuilder.field(searchField, entSearchRequest.getBoostForField(searchField));
        }
        multiMatchQueryBuilder
            .minimumShouldMatch(entSearchRequest.getMinimumShouldMatch())
            .type(MultiMatchQueryBuilder.Type.CROSS_FIELDS);

        final BoolQueryBuilder queryBuilder = QueryBuilders.boolQuery().should(multiMatchQueryBuilder);

        logger.info(queryBuilder.toString());

        return queryBuilder;
    }
}
