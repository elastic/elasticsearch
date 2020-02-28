/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.xpack.analytics;

import org.elasticsearch.xpack.analytics.randomsampling.RandomSamplingQueryBuilder;

public class AnalyticsQueryBuilders {

    /**
     * Creates a random sampler query
     *
     * @param probability The probability that a document is randomly sampled
     * @param seed  A seed to use with the random generator.  Null if you do not wish to provide a seed
     */
    public static RandomSamplingQueryBuilder randomSampleQuery(double probability, Integer seed) {
        return new RandomSamplingQueryBuilder(probability, seed);
    }
}
