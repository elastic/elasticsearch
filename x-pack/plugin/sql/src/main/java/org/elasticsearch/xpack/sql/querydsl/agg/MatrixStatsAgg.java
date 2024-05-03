/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */
package org.elasticsearch.xpack.sql.querydsl.agg;

import org.elasticsearch.aggregations.metric.MatrixStatsAggregationBuilder;
import org.elasticsearch.search.aggregations.AggregationBuilder;

import java.util.List;

public class MatrixStatsAgg extends LeafAgg {

    private final List<String> fields;

    public MatrixStatsAgg(String id, List<String> fields) {
        super(id, AggSource.of("<multi-field>"));
        this.fields = fields;
    }

    @Override
    AggregationBuilder toBuilder() {
        return new MatrixStatsAggregationBuilder(id()).fields(fields);
    }
}
