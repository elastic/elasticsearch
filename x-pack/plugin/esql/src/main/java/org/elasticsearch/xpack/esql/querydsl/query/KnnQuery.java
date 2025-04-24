/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.querydsl.query;

import org.elasticsearch.index.query.QueryBuilder;
import org.elasticsearch.search.vectors.KnnVectorQueryBuilder;
import org.elasticsearch.xpack.esql.core.querydsl.query.Query;
import org.elasticsearch.xpack.esql.core.tree.Source;

import java.util.Arrays;
import java.util.Objects;

public class KnnQuery extends Query {

    private final String field;
    private final float[] query;

    public KnnQuery(Source source, String field, float[] query) {
        super(source);
        this.field = field;
        this.query = query;
    }

    @Override
    protected QueryBuilder asBuilder() {
        return new KnnVectorQueryBuilder(field, query, null, null, null, null);
    }

    @Override
    protected String innerToString() {
        return "knn(" + field + ", " + Arrays.toString(query) + ")";
    }

    @Override
    public boolean equals(Object o) {
        if (!(o instanceof KnnQuery knnQuery)) return false;
        if (super.equals(o) == false) return false;
        return Objects.equals(field, knnQuery.field) && Objects.deepEquals(query, knnQuery.query);
    }

    @Override
    public int hashCode() {
        return Objects.hash(super.hashCode(), field, Arrays.hashCode(query));
    }

    @Override
    public boolean scorable() {
        return true;
    }
}
