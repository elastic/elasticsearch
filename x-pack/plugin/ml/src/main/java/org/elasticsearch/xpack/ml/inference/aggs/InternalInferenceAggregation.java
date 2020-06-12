/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */

package org.elasticsearch.xpack.ml.inference.aggs;

import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.search.aggregations.InternalAggregation;
import org.elasticsearch.xpack.core.ml.inference.results.InferenceResults;

import java.io.IOException;
import java.util.List;
import java.util.Map;

public class InternalInferenceAggregation extends InternalAggregation {

    private final InferenceResults inferenceResult;

    protected InternalInferenceAggregation(String name, Map<String, Object> metadata,
                                           InferenceResults inferenceResult) {
        super(name, metadata);
        this.inferenceResult = inferenceResult;
    }

    @Override
    protected void doWriteTo(StreamOutput out) throws IOException {
        out.writeNamedWriteable(inferenceResult);
    }

    @Override
    public InternalAggregation reduce(List<InternalAggregation> aggregations, ReduceContext reduceContext) {
        throw new UnsupportedOperationException("Reducing an inference aggregation is not supported");
    }

    @Override
    public Object getProperty(List<String> path) {
        throw new UnsupportedOperationException();
    }

    @Override
    public XContentBuilder doXContentBody(XContentBuilder builder, Params params) {
        return null;
    }

    @Override
    public String getWriteableName() {
        return null;
    }
}
