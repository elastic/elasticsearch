/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */
package org.elasticsearch.xpack.esql.core.querydsl.query;

import org.elasticsearch.index.query.QueryBuilder;
import org.elasticsearch.xpack.esql.core.tree.Source;

import static org.elasticsearch.index.query.QueryBuilders.matchAllQuery;

public class MatchAll extends Query {
    public MatchAll(Source source) {
        super(source);
    }

    @Override
    protected QueryBuilder asBuilder() {
        return matchAllQuery();
    }

    @Override
    protected String innerToString() {
        return "";
    }
}
