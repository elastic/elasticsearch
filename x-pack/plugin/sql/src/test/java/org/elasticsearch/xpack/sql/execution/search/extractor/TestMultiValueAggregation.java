/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */
package org.elasticsearch.xpack.sql.execution.search.extractor;

import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.search.aggregations.AggregationReduceContext;
import org.elasticsearch.search.aggregations.AggregatorReducer;
import org.elasticsearch.search.aggregations.metrics.InternalNumericMetricsAggregation;
import org.elasticsearch.xcontent.XContentBuilder;

import java.io.IOException;
import java.util.Map;

import static java.util.Collections.emptyMap;

class TestMultiValueAggregation extends InternalNumericMetricsAggregation.MultiValue {

    private final Map<String, Double> values;

    TestMultiValueAggregation(String name, Map<String, Double> values) {
        super(name, null, emptyMap());
        this.values = values;
    }

    @Override
    public String getWriteableName() {
        throw new UnsupportedOperationException();
    }

    @Override
    public double value(String name) {
        return values.get(name);
    }

    @Override
    protected void doWriteTo(StreamOutput out) throws IOException {
        throw new UnsupportedOperationException();
    }

    @Override
    protected AggregatorReducer getLeaderReducer(AggregationReduceContext reduceContext, int size) {
        throw new UnsupportedOperationException();
    }

    @Override
    public XContentBuilder doXContentBody(XContentBuilder builder, Params params) throws IOException {
        throw new UnsupportedOperationException();
    }

    @Override
    public Iterable<String> valueNames() {
        return values.keySet();
    }
}
