/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.search.aggregations.support;

import org.apache.lucene.search.BooleanClause;
import org.apache.lucene.search.BooleanQuery;
import org.apache.lucene.search.Query;
import org.elasticsearch.index.query.QueryBuilder;
import org.elasticsearch.search.aggregations.bucket.sampler.random.RandomSamplingQuery;

import java.io.IOException;
import java.util.Optional;

/**
 * This provides information around the current sampling context for aggregations
 */
public record SamplingContext(double probability, int seed) {
    public static SamplingContext NONE = new SamplingContext(1.0, 0);

    public boolean isSampled() {
        return probability < 1.0;
    }

    /**
     * Scales the given value according to the configured sampling probability.
     *
     * If the sampling context is NONE, then no scaling takes place.
     * @param value the value to scale
     * @return the scaled value, or the passed value if no sampling is configured
     */
    public long scaleDown(long value) {
        if (isSampled()) {
            return Math.round(value * probability);
        }
        return value;
    }

    /**
     * This scales the given value according to the inverse of the configured sampling probability
     *
     * The value is rounded to the nearest whole value
     * @param value the value to inversely scale
     * @return the scaled value, or the passed value if no sampling has been configured
     */
    public long scaleUp(long value) {
        if (isSampled()) {
            return Math.round(value * (1.0 / probability));
        }
        return value;
    }

    /**
     * Scales the given value according to the configured sampling probability.
     *
     * If the sampling context is NONE, then no scaling takes place.
     * @param value the value to scale
     * @return the scaled value, or the passed value if no sampling is configured
     */
    public double scaleDown(double value) {
        if (isSampled()) {
            return value * probability;
        }
        return value;
    }

    /**
     * This scales the given value according to the inverse of the configured sampling probability
     *
     * @param value the value to inversely scale
     * @return the scaled value, or the passed value if no sampling has been configured
     */
    public double scaleUp(double value) {
        if (isSampled()) {
            return value / probability;
        }
        return value;
    }

    /**
     * Builds the provided query builder into a Lucene query object. The returned query takes sampling into account.
     *
     * @param builder The filter query to build along with the random sampling query
     * @param context The current aggregation context
     * @return A lucene query that takes sampling into account if necessary
     * @throws IOException on query build failure
     */
    public Query buildQueryWithSampler(QueryBuilder builder, AggregationContext context) throws IOException {
        Query rewritten = context.buildQuery(builder);
        if (isSampled() == false) {
            return rewritten;
        }
        BooleanQuery.Builder queryBuilder = new BooleanQuery.Builder();
        queryBuilder.add(rewritten, BooleanClause.Occur.FILTER);
        queryBuilder.add(new RandomSamplingQuery(probability(), seed(), context.shardRandomSeed()), BooleanClause.Occur.FILTER);
        return queryBuilder.build();
    }

    /**
     * @param context The current aggregation context
     * @return the sampling query if the sampling context indicates that sampling is required
     * @throws IOException thrown on query build failure
     */
    public Optional<Query> buildSamplingQueryIfNecessary(AggregationContext context) throws IOException {
        if (isSampled() == false) {
            return Optional.empty();
        }
        return Optional.of(new RandomSamplingQuery(probability(), seed(), context.shardRandomSeed()));
    }

}
