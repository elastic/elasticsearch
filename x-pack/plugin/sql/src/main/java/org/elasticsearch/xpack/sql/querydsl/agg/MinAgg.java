/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.xpack.sql.querydsl.agg;

import org.elasticsearch.search.aggregations.AggregationBuilders;
import org.elasticsearch.search.aggregations.support.ValuesSourceAggregationBuilder;

import java.util.function.Function;

public class MinAgg extends DefaultAggSourceLeafAgg {

    public MinAgg(String id, AggSource source) {
        super(id, source);
    }

    @Override
    Function<String, ValuesSourceAggregationBuilder<?>> builder() {
        return AggregationBuilders::min;
    }
}
