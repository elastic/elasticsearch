/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.inference.queries;

import org.elasticsearch.index.query.MatchQueryBuilder;
import org.elasticsearch.index.query.QueryBuilder;
import org.elasticsearch.plugins.internal.rewriter.SimpleQueryRewriter;

public class SimpleSemanticQueryRewriter implements SimpleQueryRewriter {

    public SimpleSemanticQueryRewriter() {

    }

    public String getName() {
        return MatchQueryBuilder.NAME;
    }

    @Override
    public QueryBuilder rewrite(QueryBuilder queryBuilder) {

        if (queryBuilder instanceof MatchQueryBuilder == false) {
            // no-op
            return queryBuilder;
        }

        MatchQueryBuilder matchQueryBuilder = (MatchQueryBuilder) queryBuilder;
        return new SemanticQueryBuilder(matchQueryBuilder.fieldName(), matchQueryBuilder.value().toString());
    }
}
