/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
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
