/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.aggregations.bucket.timeseries;

import org.elasticsearch.aggregations.bucket.AggregationBuilderTestCase;

public class TimeSeriesAggregationBuilderTests extends AggregationBuilderTestCase<TimeSeriesAggregationBuilder> {

    @Override
    protected TimeSeriesAggregationBuilder createTestAggregatorBuilder() {
        // Size set large enough tests not intending to hit the size limit shouldn't see it.
        return new TimeSeriesAggregationBuilder(randomAlphaOfLength(10), randomBoolean(), randomIntBetween(1000, 100_000));
    }

}
