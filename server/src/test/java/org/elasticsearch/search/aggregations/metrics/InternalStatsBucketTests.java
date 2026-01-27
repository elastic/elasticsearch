/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.search.aggregations.metrics;

import org.elasticsearch.search.DocValueFormat;
import org.elasticsearch.search.aggregations.pipeline.InternalStatsBucket;

import java.util.List;
import java.util.Map;

public class InternalStatsBucketTests extends InternalStatsTests {

    @Override
    protected InternalStatsBucket createInstance(
        String name,
        long count,
        double sum,
        double min,
        double max,
        DocValueFormat formatter,
        Map<String, Object> metadata
    ) {
        return new InternalStatsBucket(name, count, sum, min, max, formatter, metadata);
    }

    @Override
    public void testReduceRandom() {
        expectThrows(UnsupportedOperationException.class, () -> createTestInstance("name", null).getReducer(null, 0));
    }

    @Override
    protected void assertReduced(InternalStats reduced, List<InternalStats> inputs) {
        // no test since reduce operation is unsupported
    }
}
