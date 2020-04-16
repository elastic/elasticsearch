/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.xpack.sql.querydsl.agg;

import org.elasticsearch.search.aggregations.AggregationBuilder;
import org.elasticsearch.search.aggregations.support.ValuesSourceAggregationBuilder;

public abstract class LeafAgg extends Agg {

    LeafAgg(String id, AggTarget target) {
        super(id, target);
    }

    abstract AggregationBuilder toBuilder();

    @SuppressWarnings("rawtypes")
    protected ValuesSourceAggregationBuilder addFieldOrScript(ValuesSourceAggregationBuilder builder) {
        return target().with(builder);
    }
}
