/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.search.aggregations.matrix;

import org.elasticsearch.plugins.Plugin;
import org.elasticsearch.plugins.SearchPlugin;
import org.elasticsearch.search.aggregations.matrix.stats.InternalMatrixStats;
import org.elasticsearch.search.aggregations.matrix.stats.MatrixStatsAggregationBuilder;
import org.elasticsearch.search.aggregations.matrix.stats.MatrixStatsParser;

import java.util.List;

import static java.util.Collections.singletonList;

public class MatrixAggregationPlugin extends Plugin implements SearchPlugin {
    @Override
    public List<AggregationSpec> getAggregations() {
        return singletonList(new AggregationSpec(MatrixStatsAggregationBuilder.NAME, MatrixStatsAggregationBuilder::new,
                new MatrixStatsParser()).addResultReader(InternalMatrixStats::new));
    }
}
