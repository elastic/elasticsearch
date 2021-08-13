/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.sql.querydsl.agg;

import org.elasticsearch.search.aggregations.AggregationBuilder;
import org.elasticsearch.search.aggregations.support.ValuesSourceAggregationBuilder;

import java.util.function.Function;

abstract class DefaultAggSourceLeafAgg extends LeafAgg {

    DefaultAggSourceLeafAgg(String id, AggSource source) {
        super(id, source);
    }

    @Override
    AggregationBuilder toBuilder() {
        return source().with(builder().apply(id()));
    }

    abstract Function<String, ValuesSourceAggregationBuilder<?>> builder();
}
