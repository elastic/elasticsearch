/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.xpack.sql.querydsl.agg;

import java.util.List;

import org.elasticsearch.search.aggregations.AggregationBuilder;

import static org.elasticsearch.search.aggregations.MatrixStatsAggregationBuilders.matrixStats;

public class MatrixStatsAgg extends LeafAgg {

    private final List<String> fields;

    public MatrixStatsAgg(String id, String propertyPath, List<String> fields) {
        super(id, propertyPath, "<multi-field>");
        this.fields = fields;
    }

    @Override
    AggregationBuilder toBuilder() {
        return matrixStats(id()).fields(fields);
    }
}
