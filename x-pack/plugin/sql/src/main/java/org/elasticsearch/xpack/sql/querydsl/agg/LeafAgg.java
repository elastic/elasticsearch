/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.xpack.sql.querydsl.agg;

import org.elasticsearch.search.aggregations.AggregationBuilder;
import org.elasticsearch.search.aggregations.support.ValuesSourceAggregationBuilder;

public abstract class LeafAgg extends Agg {

    LeafAgg(String id, Object fieldOrScript) {
        super(id, fieldOrScript);
    }

    abstract AggregationBuilder toBuilder();

    @SuppressWarnings("rawtypes")
    protected ValuesSourceAggregationBuilder addFieldOrScript(ValuesSourceAggregationBuilder builder) {
        if (fieldName() != null) {
            return builder.field(fieldName());
        } else {
            return builder.script(scriptTemplate().toPainless());
        }
    }
}
