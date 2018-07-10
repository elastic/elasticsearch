/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.xpack.sql.querydsl.container;

import org.elasticsearch.xpack.sql.execution.search.AggRef;
import org.elasticsearch.xpack.sql.querydsl.agg.Aggs;

/**
 * Reference to a sub/nested/metric aggregation.
 * Due to how ES query works, this is _always_ a child aggregation with the grouping (composite agg) as the parent.
 */
public class MetricAggRef extends AggRef {

    private final String name;
    private final String property;
    private final String innerKey;

    public MetricAggRef(String name) {
        this(name, "value");
    }

    public MetricAggRef(String name, String property) {
        this(name, property, null);
    }

    public MetricAggRef(String name, String property, String innerKey) {
        this.name = name;
        this.property = property;
        this.innerKey = innerKey;
    }

    public String name() {
        return name;
    }

    public String property() {
        return property;
    }

    public String innerKey() {
        return innerKey;
    }

    @Override
    public String toString() {
        String i = innerKey != null ? "[" + innerKey + "]" : "";
        return Aggs.ROOT_GROUP_NAME + ">" + name + "." + property + i;
    }
}
