/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.xpack.sql.querydsl.agg;

import org.elasticsearch.search.aggregations.AggregationBuilder;

import static org.elasticsearch.search.aggregations.AggregationBuilders.min;

public class MinAgg extends LeafAgg {

    public MinAgg(String id, AggSource source) {
        super(id, source);
    }

    @Override
    AggregationBuilder toBuilder() {
        return addAggSource(min(id()));
    }
}
