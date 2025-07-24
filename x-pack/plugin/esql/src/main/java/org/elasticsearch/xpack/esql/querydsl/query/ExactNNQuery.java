/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.querydsl.query;

import org.elasticsearch.index.query.QueryBuilder;
import org.elasticsearch.search.vectors.ExactKnnQueryBuilder;
import org.elasticsearch.search.vectors.VectorData;
import org.elasticsearch.xpack.esql.core.querydsl.query.Query;
import org.elasticsearch.xpack.esql.core.tree.Source;

import java.util.Arrays;
import java.util.Objects;

public class ExactNNQuery extends Query {

    private final String field;
    private final float[] query;
    private final Float minimumSimilarity;

    public static final String RESCORE_OVERSAMPLE_FIELD = "rescore_oversample";

    public ExactNNQuery(Source source, String field, float[] query, Float minimumSimilarity) {
        super(source);
        this.field = field;
        this.query = query;
        this.minimumSimilarity = minimumSimilarity;
    }

    @Override
    protected QueryBuilder asBuilder() {
        return new ExactKnnQueryBuilder(VectorData.fromFloats(query), field, minimumSimilarity);
    }

    @Override
    protected String innerToString() {
        return "exactNN(" + field + ", " + Arrays.toString(query) + " minimumSimilarity=" + minimumSimilarity + ")";
    }

    @Override
    public boolean equals(Object o) {
        if (super.equals(o) == false) return false;

        if (o == null || getClass() != o.getClass()) return false;
        ExactNNQuery query = (ExactNNQuery) o;
        return Objects.equals(field, query.field)
            && Objects.deepEquals(this.query, query.query)
            && Objects.equals(minimumSimilarity, query.minimumSimilarity);
    }

    @Override
    public int hashCode() {
        return Objects.hash(super.hashCode(), field, Arrays.hashCode(query), minimumSimilarity);
    }

    @Override
    public boolean scorable() {
        return true;
    }

    @Override
    public boolean containsPlan() {
        return false;
    }
}
