/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.xpack.sql.querydsl.agg;

import org.elasticsearch.search.aggregations.AggregationBuilder;

import java.util.List;

import static org.elasticsearch.search.aggregations.AggregationBuilders.percentiles;

public class PercentilesAgg extends LeafAgg {

    private final List<Double> percents;

    public PercentilesAgg(String id, String fieldName, List<Double> percents) {
        super(id, fieldName);
        this.percents = percents;
    }

    public List<Double> percents() {
        return percents;
    }

    @Override
    AggregationBuilder toBuilder() {
        // TODO: look at keyed
        return percentiles(id())
                .field(fieldName())
                .percentiles(percents.stream().mapToDouble(Double::doubleValue).toArray());
    }
}
