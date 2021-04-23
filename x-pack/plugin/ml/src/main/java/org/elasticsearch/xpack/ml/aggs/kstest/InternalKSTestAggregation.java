/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.ml.aggs.kstest;

import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.search.aggregations.InternalAggregation;
import org.elasticsearch.search.aggregations.InvalidAggregationPathException;
import org.elasticsearch.xpack.ml.aggs.inference.InferencePipelineAggregationBuilder;

import java.io.IOException;
import java.util.List;
import java.util.Map;

public class InternalKSTestAggregation extends InternalAggregation {

    private final Map<String, Double> modeValues;

    public InternalKSTestAggregation(String name, Map<String, Object> metadata, Map<String, Double> modeValues) {
        super(name, metadata);
        this.modeValues = modeValues;
    }

    @Override
    public String getWriteableName() {
        return BucketCountKSTestAggregationBuilder.NAME.getPreferredName();
    }

    @Override
    protected void doWriteTo(StreamOutput out) throws IOException {
        out.writeMap(modeValues, StreamOutput::writeString, StreamOutput::writeDouble);
    }

    @Override
    public InternalAggregation reduce(List<InternalAggregation> aggregations, ReduceContext reduceContext) {
        throw new UnsupportedOperationException("Reducing a bucket_count_ks_test aggregation is not supported");
    }

    @Override
    protected boolean mustReduceOnSingleInternalAgg() {
        return false;
    }

    @Override
    public Object getProperty(List<String> path) {
        if (path.isEmpty()) {
            return this;
        } else if (path.size() == 1) {
            return modeValues.get(path.get(0));
        }
        throw invalidPathException(path);
    }

    private InvalidAggregationPathException invalidPathException(List<String> path) {
        return new InvalidAggregationPathException(
            "unknown property " + path + " for " + InferencePipelineAggregationBuilder.NAME + " aggregation [" + getName() + "]"
        );
    }

    @Override
    public XContentBuilder doXContentBody(XContentBuilder builder, Params params) throws IOException {
        for (Map.Entry<String, Double> kv : modeValues.entrySet()) {
            builder.field(kv.getKey(), kv.getValue());
        }
        return builder;
    }
}
