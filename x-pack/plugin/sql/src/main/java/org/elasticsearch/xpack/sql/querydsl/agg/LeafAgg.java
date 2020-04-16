/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.xpack.sql.querydsl.agg;

import org.elasticsearch.search.aggregations.AggregationBuilder;
import org.elasticsearch.search.aggregations.support.ValuesSourceAggregationBuilder;

public abstract class LeafAgg extends Agg {

    LeafAgg(String id, AggSource source) {
        super(id, source);
    }

    abstract AggregationBuilder toBuilder();

    @SuppressWarnings("rawtypes")
    protected ValuesSourceAggregationBuilder addAggSource(ValuesSourceAggregationBuilder builder) {
        return source().with(builder);
    }
}
