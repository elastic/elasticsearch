/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */
package org.elasticsearch.xpack.analytics;

import org.elasticsearch.xpack.analytics.cumulativecardinality.CumulativeCardinalityPipelineAggregationBuilder;
import org.elasticsearch.xpack.analytics.stringstats.StringStatsAggregationBuilder;

public class AnalyticsAggregationBuilders {

    public static CumulativeCardinalityPipelineAggregationBuilder cumulativeCardinality(String name, String bucketsPath) {
        return new CumulativeCardinalityPipelineAggregationBuilder(name, bucketsPath);
    }

    public static StringStatsAggregationBuilder stringStats(String name) {
        return new StringStatsAggregationBuilder(name);
    }
}
