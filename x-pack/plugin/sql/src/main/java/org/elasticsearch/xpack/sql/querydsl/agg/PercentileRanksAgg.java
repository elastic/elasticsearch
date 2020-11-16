/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.xpack.sql.querydsl.agg;

import org.elasticsearch.search.aggregations.AggregationBuilders;
import org.elasticsearch.search.aggregations.metrics.PercentilesConfig;
import org.elasticsearch.search.aggregations.support.ValuesSourceAggregationBuilder;

import java.util.List;
import java.util.function.Function;

public class PercentileRanksAgg extends DefaultAggSourceLeafAgg {

    private final List<Double> values;
    private final PercentilesConfig percentilesConfig;

    public static PercentilesConfig DEFAULT_PERCENTILES_CONFIG =
        AggregationBuilders.percentileRanks("not_important", new double[1]).percentilesConfig();

    public PercentileRanksAgg(String id, AggSource source, List<Double> values, PercentilesConfig percentilesConfig) {
        super(id, source);
        this.values = values;
        this.percentilesConfig = percentilesConfig;
    }

    @Override
    Function<String, ValuesSourceAggregationBuilder<?>> builder() {
        return s -> AggregationBuilders.percentileRanks(s, values.stream().mapToDouble(Double::doubleValue).toArray())
            .percentilesConfig(percentilesConfig);
    }
}
