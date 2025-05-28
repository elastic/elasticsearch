/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.querydsl.query;

import org.elasticsearch.index.query.QueryBuilder;
import org.elasticsearch.search.vectors.KnnVectorQueryBuilder;
import org.elasticsearch.search.vectors.RescoreVectorBuilder;
import org.elasticsearch.xpack.esql.core.querydsl.query.Query;
import org.elasticsearch.xpack.esql.core.tree.Source;

import java.util.Arrays;
import java.util.Map;
import java.util.Objects;

import static org.elasticsearch.search.vectors.KnnVectorQueryBuilder.K_FIELD;
import static org.elasticsearch.search.vectors.KnnVectorQueryBuilder.NUM_CANDS_FIELD;
import static org.elasticsearch.search.vectors.KnnVectorQueryBuilder.VECTOR_SIMILARITY_FIELD;

public class KnnQuery extends Query {

    private final String field;
    private final float[] query;
    private final Map<String, Object> options;

    public KnnQuery(Source source, String field, float[] query, Map<String, Object> options) {
        super(source);
        assert options != null;
        this.field = field;
        this.query = query;
        this.options = options;
    }

    @Override
    protected QueryBuilder asBuilder() {
        Integer k = (Integer) options.get(K_FIELD.getPreferredName());
        Integer numCands = (Integer) options.get(NUM_CANDS_FIELD.getPreferredName());
        RescoreVectorBuilder rescoreVectorBuilder = null;
        Float oversample = (Float) options.get(RescoreVectorBuilder.OVERSAMPLE_FIELD.getPreferredName());
        if (oversample != null) {
            rescoreVectorBuilder = new RescoreVectorBuilder(oversample);
        }
        Float vectorSimilarity = (Float) options.get(VECTOR_SIMILARITY_FIELD.getPreferredName());

        return new KnnVectorQueryBuilder(field, query, k, numCands, rescoreVectorBuilder, vectorSimilarity);
    }

    @Override
    protected String innerToString() {
        return "knn(" + field + ", " + Arrays.toString(query) + " options={" + options + "}))";
    }

    @Override
    public boolean equals(Object o) {
        if (!(o instanceof KnnQuery knnQuery)) return false;
        if (super.equals(o) == false) return false;
        return Objects.equals(field, knnQuery.field)
            && Objects.deepEquals(query, knnQuery.query)
            && Objects.equals(options, knnQuery.options);
    }

    @Override
    public int hashCode() {
        return Objects.hash(super.hashCode(), field, Arrays.hashCode(query), options);
    }

    @Override
    public boolean scorable() {
        return true;
    }
}
