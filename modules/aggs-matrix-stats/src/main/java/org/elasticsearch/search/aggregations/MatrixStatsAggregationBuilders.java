/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */
package org.elasticsearch.search.aggregations;

import org.elasticsearch.search.aggregations.matrix.stats.MatrixStats;
import org.elasticsearch.search.aggregations.matrix.stats.MatrixStatsAggregationBuilder;

public class MatrixStatsAggregationBuilders {
    /**
     * Create a new {@link MatrixStats} aggregation with the given name.
     */
    public static MatrixStatsAggregationBuilder matrixStats(String name) {
        return new MatrixStatsAggregationBuilder(name);
    }
}
