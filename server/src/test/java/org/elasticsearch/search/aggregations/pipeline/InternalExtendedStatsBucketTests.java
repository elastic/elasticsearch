/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.search.aggregations.pipeline;

import org.elasticsearch.search.DocValueFormat;
import org.elasticsearch.search.aggregations.metrics.InternalExtendedStats;
import org.elasticsearch.search.aggregations.metrics.InternalExtendedStatsTests;

import java.util.List;
import java.util.Map;

public class InternalExtendedStatsBucketTests extends InternalExtendedStatsTests {

    @Override
    protected InternalExtendedStatsBucket createInstance(
        String name,
        long count,
        double sum,
        double min,
        double max,
        double sumOfSqrs,
        double sigma,
        DocValueFormat formatter,
        Map<String, Object> metadata
    ) {
        return new InternalExtendedStatsBucket(name, count, sum, min, max, sumOfSqrs, sigma, formatter, metadata);
    }

    @Override
    public void testReduceRandom() {
        expectThrows(UnsupportedOperationException.class, () -> createTestInstance("name", null).getReducer(null, 0));
    }

    @Override
    protected void assertReduced(InternalExtendedStats reduced, List<InternalExtendedStats> inputs) {
        // no test since reduce operation is unsupported
    }
}
