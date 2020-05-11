/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.xpack.sql.execution.search.extractor;

import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.search.aggregations.InternalAggregation;

import java.io.IOException;
import java.util.List;

import static java.util.Collections.emptyMap;

public class TestSingleValueAggregation extends InternalAggregation {

    private final List<String> path;
    private final Object value;

    TestSingleValueAggregation(String name, List<String> path, Object value) {
        super(name, emptyMap());
        this.path = path;
        this.value = value;
    }

    @Override
    public String getWriteableName() {
        throw new UnsupportedOperationException();
    }

    @Override
    protected void doWriteTo(StreamOutput out) throws IOException {
        throw new UnsupportedOperationException();
    }

    @Override
    public InternalAggregation reduce(List<InternalAggregation> aggregations, ReduceContext reduceContext) {
        throw new UnsupportedOperationException();
    }

    @Override
    public Object getProperty(List<String> path) {
        if (this.path.equals(path)) {
            return value;
        }
        throw new IllegalArgumentException("unknown path " + path);
    }

    @Override
    public XContentBuilder doXContentBody(XContentBuilder builder, Params params) throws IOException {
        throw new UnsupportedOperationException();
    }

}
