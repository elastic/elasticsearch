/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.xpack.sql.querydsl.agg;

import org.elasticsearch.search.aggregations.support.ValuesSourceAggregationBuilder;

import java.util.List;
import java.util.function.Function;

import static org.elasticsearch.search.aggregations.AggregationBuilders.percentileRanks;

public class PercentileRanksAgg extends DefaultAggSourceLeafAgg {

    private final List<Double> values;

    public PercentileRanksAgg(String id, AggSource source, List<Double> values) {
        super(id, source);
        this.values = values;
    }

    @Override
    Function<String, ValuesSourceAggregationBuilder<?>> builder() {
        return s -> percentileRanks(s, values.stream().mapToDouble(Double::doubleValue).toArray());
    }
}
