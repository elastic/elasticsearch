/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.xpack.sql.querydsl.agg;

import org.elasticsearch.search.aggregations.PipelineAggregationBuilder;

public abstract class PipelineAgg {

    private final String name;

    public PipelineAgg(String name) {
        this.name = name;
    }

    public String name() {
        return name;
    }

    abstract PipelineAggregationBuilder toBuilder();
}
