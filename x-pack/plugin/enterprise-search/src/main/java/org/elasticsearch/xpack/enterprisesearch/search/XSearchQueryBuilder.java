/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.enterprisesearch.search;

import org.elasticsearch.index.query.QueryBuilder;
import org.elasticsearch.index.query.QueryBuilders;

// It would be interesting if this implemented AbstractQueryBuilder here
public class XSearchQueryBuilder {

    public static QueryBuilder getQueryBuilder(XSearchQueryOptions queryOptions) {
        QueryBuilder queryBuilder = QueryBuilders.termsQuery("my_field", queryOptions.queryString);
        return queryBuilder;
    }
}
