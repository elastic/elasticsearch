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
import org.elasticsearch.xpack.esql.expression.function.vector.Knn;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.Objects;

import static org.elasticsearch.index.query.AbstractQueryBuilder.BOOST_FIELD;
import static org.elasticsearch.search.vectors.KnnVectorQueryBuilder.VECTOR_SIMILARITY_FIELD;
import static org.elasticsearch.search.vectors.KnnVectorQueryBuilder.VISIT_PERCENTAGE_FIELD;

public class KnnQuery extends Query {

    private final String field;
    private final float[] query;
    private final Map<String, Object> options;
    private final List<QueryBuilder> filterQueries;

    public static final String RESCORE_OVERSAMPLE_FIELD = "rescore_oversample";
    private final Integer k;

    public KnnQuery(Source source, String field, float[] query, Integer k, Map<String, Object> options, List<QueryBuilder> filterQueries) {
        super(source);
        assert k != null && k > 0 : "k must be a positive integer, but was: " + k;
        this.k = k;
        assert options != null;
        this.field = field;
        this.query = query;
        this.options = options;
        this.filterQueries = new ArrayList<>(filterQueries);
    }

    @Override
    protected QueryBuilder asBuilder() {
        RescoreVectorBuilder rescoreVectorBuilder = null;
        Float oversample = (Float) options.get(RESCORE_OVERSAMPLE_FIELD);
        if (oversample != null) {
            rescoreVectorBuilder = new RescoreVectorBuilder(oversample);
        }
        Float vectorSimilarity = (Float) options.get(VECTOR_SIMILARITY_FIELD.getPreferredName());
        Integer minCandidates = (Integer) options.get(Knn.MIN_CANDIDATES_OPTION);
        Float visitPercentage = (Float) options.get(VISIT_PERCENTAGE_FIELD.getPreferredName());
        minCandidates = minCandidates == null ? null : Math.max(minCandidates, k);

        // TODO: expose visit_percentage in ESQL
        KnnVectorQueryBuilder queryBuilder = new KnnVectorQueryBuilder(
            field,
            query,
            k,
            minCandidates,
            visitPercentage,
            rescoreVectorBuilder,
            vectorSimilarity
        );
        for (QueryBuilder filter : filterQueries) {
            queryBuilder.addFilterQuery(filter);
        }
        Number boost = (Number) options.get(BOOST_FIELD.getPreferredName());
        if (boost != null) {
            queryBuilder.boost(boost.floatValue());
        }
        return queryBuilder;
    }

    @Override
    protected String innerToString() {
        return "knn(" + field + ", " + Arrays.toString(query) + " options={" + options + "}))";
    }

    @Override
    public boolean equals(Object o) {
        if (super.equals(o) == false) return false;

        if (o == null || getClass() != o.getClass()) return false;
        KnnQuery knnQuery = (KnnQuery) o;
        return Objects.equals(field, knnQuery.field)
            && Objects.deepEquals(query, knnQuery.query)
            && Objects.equals(options, knnQuery.options)
            && Objects.equals(filterQueries, knnQuery.filterQueries);
    }

    @Override
    public int hashCode() {
        return Objects.hash(super.hashCode(), field, Arrays.hashCode(query), options, filterQueries);
    }

    @Override
    public boolean scorable() {
        return true;
    }

    @Override
    public boolean containsPlan() {
        return false;
    }

    public List<QueryBuilder> filterQueries() {
        return filterQueries;
    }
}
