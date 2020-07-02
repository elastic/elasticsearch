/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */

package org.elasticsearch.xpack.ml.inference.aggs;

import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.search.aggregations.InternalAggregation;
import org.elasticsearch.search.aggregations.InvalidAggregationPathException;
import org.elasticsearch.xpack.core.ml.inference.results.InferenceResults;

import java.io.IOException;
import java.util.List;
import java.util.Map;
import java.util.Objects;

public class InternalInferenceAggregation extends InternalAggregation {

    private final InferenceResults inferenceResult;

    protected InternalInferenceAggregation(String name, Map<String, Object> metadata,
                                           InferenceResults inferenceResult) {
        super(name, metadata);
        this.inferenceResult = inferenceResult;
    }

    public InternalInferenceAggregation(StreamInput in) throws IOException {
        super(in);
        inferenceResult = in.readNamedWriteable(InferenceResults.class);
    }

    public InferenceResults getInferenceResult() {
        return inferenceResult;
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
        Object propertyValue;

        if (path.isEmpty()) {
            propertyValue = this;
        } else if (path.size() == 1) {
            if (CommonFields.VALUE.getPreferredName().equals(path.get(0))) {
                propertyValue = inferenceResult.predictedValue();
            } else {
                throw invalidPathException(path);
            }
        } else {
            throw invalidPathException(path);
        }

        return propertyValue;
    }

    private InvalidAggregationPathException invalidPathException(List<String> path) {
        return new InvalidAggregationPathException("unknown property " +  path + " for " +
            InferencePipelineAggregationBuilder.NAME + " aggregation [" + getName() + "]");
    }

    @Override
    public XContentBuilder doXContentBody(XContentBuilder builder, Params params) throws IOException {
        return inferenceResult.toXContent(builder, params);
    }

    @Override
    public String getWriteableName() {
        return "inference";
    }

    @Override
    public int hashCode() {
        return Objects.hash(super.hashCode(), inferenceResult);
    }

    @Override
    public boolean equals(Object obj) {
        if (this == obj) return true;
        if (obj == null || getClass() != obj.getClass()) return false;
        if (super.equals(obj) == false) return false;
        InternalInferenceAggregation other = (InternalInferenceAggregation) obj;
        return Objects.equals(inferenceResult, other.inferenceResult);
    }
}
