/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.search.aggregations.metrics;

import org.elasticsearch.search.aggregations.InternalAggregation;

import static org.hamcrest.Matchers.equalTo;

public abstract class InternalPercentilesRanksTestCase<T extends InternalAggregation & PercentileRanks> extends AbstractPercentilesTestCase<
    T> {

    @Override
    protected void assertPercentile(T agg, Double value) {
        assertThat(agg.percent(value), equalTo(Double.NaN));
        assertThat(agg.percentAsString(value), equalTo("NaN"));
    }
}
