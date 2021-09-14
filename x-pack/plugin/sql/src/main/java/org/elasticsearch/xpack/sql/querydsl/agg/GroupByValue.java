/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */
package org.elasticsearch.xpack.sql.querydsl.agg;

import org.elasticsearch.search.aggregations.bucket.composite.CompositeValuesSourceBuilder;
import org.elasticsearch.search.aggregations.bucket.composite.TermsValuesSourceBuilder;
import org.elasticsearch.xpack.ql.expression.gen.script.ScriptTemplate;
import org.elasticsearch.xpack.ql.querydsl.container.Sort.Direction;

/**
 * GROUP BY key for fields or scripts.
 */
public class GroupByValue extends GroupByKey {

    public GroupByValue(String id, String fieldName) {
        this(id, AggSource.of(fieldName), null);
    }

    public GroupByValue(String id, ScriptTemplate script) {
        this(id, AggSource.of(script), null);
    }

    private GroupByValue(String id, AggSource source, Direction direction) {
        super(id, source, direction);
    }

    @Override
    protected CompositeValuesSourceBuilder<?> createSourceBuilder() {
        return new TermsValuesSourceBuilder(id());
    }

    @Override
    protected GroupByKey copy(String id, AggSource source, Direction direction) {
        return new GroupByValue(id, source(), direction);
    }
}
