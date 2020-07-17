/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.xpack.sql.querydsl.agg;

import org.elasticsearch.search.aggregations.AggregationBuilder;
import org.elasticsearch.search.aggregations.AggregationBuilders;
import org.elasticsearch.search.aggregations.metrics.PercentilesAggregationBuilder;
import org.elasticsearch.search.aggregations.support.ValuesSourceAggregationBuilder;

import java.util.List;
import java.util.function.Function;

public class PercentilesAgg extends DefaultAggSourceLeafAgg {

    private final List<Double> percents;

    public PercentilesAgg(String id, AggSource source, List<Double> percents) {
        super(id, source);
        this.percents = percents;
    }
    
    @Override
    AggregationBuilder toBuilder() {
        // TODO: look at keyed
        PercentilesAggregationBuilder builder = (PercentilesAggregationBuilder) super.toBuilder();
        return builder.percentiles(percents.stream().mapToDouble(Double::doubleValue).toArray());
    }

    @Override
    Function<String, ValuesSourceAggregationBuilder<?>> builder() {
        return AggregationBuilders::percentiles;
    }
}