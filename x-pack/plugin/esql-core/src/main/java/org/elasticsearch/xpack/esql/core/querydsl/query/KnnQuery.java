/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.core.querydsl.query;

import org.elasticsearch.index.query.QueryBuilder;
import org.elasticsearch.search.vectors.KnnVectorQueryBuilder;
import org.elasticsearch.xpack.esql.core.expression.Expression;
import org.elasticsearch.xpack.esql.core.tree.Source;

import java.util.Map;

public class KnnQuery extends Query {

    private final Expression field;
    private final float[] vectorData;
    private final Map<String, Object> options;

    public KnnQuery(Source source, Expression field, float[] vectorData, Map<String, Object> options) {
        super(source);
        this.field = field;
        this.vectorData = vectorData;
        this.options = options;
    }

    @Override
    public QueryBuilder asBuilder() {
        String numCandidates = options.getOrDefault("num_candidates", 10).toString();
        Integer numCands = Integer.valueOf(numCandidates);
        String vectorSimilarityString = options.getOrDefault("vector_similarity", 1f).toString();
        Float vectorSimilarity = Float.valueOf(vectorSimilarityString);
        return new KnnVectorQueryBuilder(field.sourceText(), vectorData, numCands, vectorSimilarity);
    }

    @Override
    protected String innerToString() {
        return asBuilder().toString();
    }
}
