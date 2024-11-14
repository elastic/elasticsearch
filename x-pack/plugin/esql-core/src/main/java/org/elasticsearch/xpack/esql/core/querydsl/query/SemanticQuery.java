/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */
package org.elasticsearch.xpack.esql.core.querydsl.query;

import org.elasticsearch.index.query.QueryBuilder;
import org.elasticsearch.inference.InferenceResults;
import org.elasticsearch.xpack.esql.core.tree.Source;
import org.elasticsearch.xpack.inference.queries.SemanticQueryBuilder;

public class SemanticQuery extends Query {
    private final String name;
    private final String text;
    private final InferenceResults inferenceResults;

    public SemanticQuery(Source source, String name, String text, InferenceResults inferenceResults) {
        super(source);
        this.name = name;
        this.text = text;
        this.inferenceResults = inferenceResults;
    }

    @Override
    public QueryBuilder asBuilder() {
        SemanticQueryBuilder queryBuilder = new SemanticQueryBuilder(name, text);
        return new SemanticQueryBuilder(queryBuilder, null, inferenceResults, false);
    }

    @Override
    protected String innerToString() {
        return null;
    }
}
