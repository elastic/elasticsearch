/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */
package org.elasticsearch.analytics;

import org.elasticsearch.analytics.cumulativecardinality.CumulativeCardinalityPipelineAggregationBuilder;
import org.elasticsearch.analytics.stringstats.StringStatsAggregationBuilder;

public class AnalyticsAggregationBuilders {

    public static CumulativeCardinalityPipelineAggregationBuilder cumulativeCardinality(String name, String bucketsPath) {
        return new CumulativeCardinalityPipelineAggregationBuilder(name, bucketsPath);
    }

    public static StringStatsAggregationBuilder stringStats(String name) {
        return new StringStatsAggregationBuilder(name);
    }
}
