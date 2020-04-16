/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.xpack.sql.querydsl.agg;

import org.elasticsearch.search.aggregations.AggregationBuilder;
import org.elasticsearch.search.aggregations.metrics.PercentilesAggregationBuilder;

import java.util.List;

import static org.elasticsearch.search.aggregations.AggregationBuilders.percentiles;

public class PercentilesAgg extends LeafAgg {

    private final List<Double> percents;

    public PercentilesAgg(String id, AggSource source, List<Double> percents) {
        super(id, source);
        this.percents = percents;
    }

    @Override
    AggregationBuilder toBuilder() {
        // TODO: look at keyed
        PercentilesAggregationBuilder builder = (PercentilesAggregationBuilder) addAggSource(percentiles(id()));
        return builder.percentiles(percents.stream().mapToDouble(Double::doubleValue).toArray());
    }
}
