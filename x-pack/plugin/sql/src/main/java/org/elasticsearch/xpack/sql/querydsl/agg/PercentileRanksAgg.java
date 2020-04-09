/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.xpack.sql.querydsl.agg;

import org.elasticsearch.search.aggregations.AggregationBuilder;

import java.util.List;

import static org.elasticsearch.search.aggregations.AggregationBuilders.percentileRanks;

public class PercentileRanksAgg extends LeafAgg {

    private final List<Double> values;

    public PercentileRanksAgg(String id, Object fieldOrScript, List<Double> values) {
        super(id, fieldOrScript);
        this.values = values;
    }

    @Override
    AggregationBuilder toBuilder() {
        return addFieldOrScript(percentileRanks(id(), values.stream().mapToDouble(Double::doubleValue).toArray()));
    }
}
