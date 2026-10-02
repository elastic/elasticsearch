/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */
package org.elasticsearch.xpack.sql.expression.function.aggregate;

/**
 * Marker for an {@link org.elasticsearch.xpack.ql.expression.function.aggregate.AggregateFunction} that is an identity
 * over a single-value input: applied to a group of exactly one row, it returns that row's value unchanged. Summing,
 * averaging, or taking the min, max or any percentile of one value all yield that same value.
 * <p>
 * This is what makes {@code HAVING F(a)} / {@code ORDER BY F(a)} over an aggregate alias {@code a} meaningful: at that
 * point a group has already been reduced to one row, so the outer aggregate is redundant and
 * {@code Optimizer.CollapseAggregateOverAggregate} drops it. Aggregates <i>not</i> marked here are rejected in that
 * position by {@code Verifier.checkNestedAggregateFunctions}, since there is nothing sensible to reduce them to -
 * {@code COUNT} of one value is 1 rather than the value, {@code STDDEV_POP}/{@code MAD} are 0, {@code VAR_SAMP} and
 * {@code SKEWNESS} are undefined, and {@code PERCENTILE_RANK} compares its own argument against the one-row
 * distribution.
 * <p>
 * {@code FIRST}/{@code LAST} are identities over a single value too, but are deliberately left out: their optional
 * sort argument would need the same treatment, which no query has asked for so far.
 */
public interface SingleValueIdentityAgg {

}
