/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.search.aggregations.metrics;

public class MedianAbsoluteDeviationTests extends AbstractNumericMetricTestCase<MedianAbsoluteDeviationAggregationBuilder> {

    @Override
    protected MedianAbsoluteDeviationAggregationBuilder doCreateTestAggregatorFactory() {
        MedianAbsoluteDeviationAggregationBuilder builder = new MedianAbsoluteDeviationAggregationBuilder(
            randomAlphaOfLengthBetween(1, 20)
        );

        if (randomBoolean()) {
            builder.compression(randomDoubleBetween(0, 1000.0, false));
        }
        if (randomBoolean()) {
            builder.parseExecutionHint(TDigestExecutionHint.HIGH_ACCURACY.toString());
        }

        if (randomBoolean()) {
            builder.missing("MISSING");
        }

        if (randomBoolean()) {
            builder.format("###.00");
        }

        return builder;
    }
}
