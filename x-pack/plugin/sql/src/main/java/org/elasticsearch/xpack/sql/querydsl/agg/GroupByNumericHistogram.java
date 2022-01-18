/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */
package org.elasticsearch.xpack.sql.querydsl.agg;

import org.elasticsearch.search.aggregations.bucket.composite.CompositeValuesSourceBuilder;
import org.elasticsearch.search.aggregations.bucket.composite.HistogramValuesSourceBuilder;
import org.elasticsearch.xpack.ql.expression.gen.script.ScriptTemplate;
import org.elasticsearch.xpack.ql.querydsl.container.Sort.Direction;
import org.elasticsearch.xpack.ql.querydsl.container.Sort.Missing;

import java.util.Objects;

/**
 * GROUP BY key based on histograms on numeric fields.
 */
public class GroupByNumericHistogram extends GroupByKey {

    private final double interval;

    public GroupByNumericHistogram(String id, String fieldName, double interval) {
        this(id, AggSource.of(fieldName), null, null, interval);
    }

    public GroupByNumericHistogram(String id, ScriptTemplate script, double interval) {
        this(id, AggSource.of(script), null, null, interval);
    }

    private GroupByNumericHistogram(String id, AggSource aggSource, Direction direction, Missing missing, double interval) {
        super(id, aggSource, direction, missing);
        this.interval = interval;
    }

    @Override
    protected CompositeValuesSourceBuilder<?> createSourceBuilder() {
        return new HistogramValuesSourceBuilder(id()).interval(interval);
    }

    @Override
    protected GroupByKey copy(String id, AggSource source, Direction direction, Missing missing) {
        return new GroupByNumericHistogram(id, source(), direction, missing, interval);
    }

    @Override
    public int hashCode() {
        return Objects.hash(super.hashCode(), interval);
    }

    @Override
    public boolean equals(Object obj) {
        if (super.equals(obj)) {
            GroupByNumericHistogram other = (GroupByNumericHistogram) obj;
            return interval == other.interval;
        }
        return false;
    }
}
