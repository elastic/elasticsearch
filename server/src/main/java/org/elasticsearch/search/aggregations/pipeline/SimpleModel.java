/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.search.aggregations.pipeline;

import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.core.Nullable;

import java.io.IOException;
import java.text.ParseException;
import java.util.Arrays;
import java.util.Collection;
import java.util.Map;

/**
 * Calculate a simple unweighted (arithmetic) moving average
 */
public class SimpleModel extends MovAvgModel {
    public static final String NAME = "simple";

    public SimpleModel() {}

    /**
     * Read from a stream.
     */
    public SimpleModel(StreamInput in) throws IOException {}

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        // Nothing to write
    }

    @Override
    public String getWriteableName() {
        return NAME;
    }

    @Override
    public boolean canBeMinimized() {
        return false;
    }

    @Override
    public MovAvgModel neighboringModel() {
        return new SimpleModel();
    }

    @Override
    public MovAvgModel clone() {
        return new SimpleModel();
    }

    @Override
    protected double[] doPredict(Collection<Double> values, int numPredictions) {
        double[] predictions = new double[numPredictions];

        // Simple just emits the same final prediction repeatedly.
        Arrays.fill(predictions, next(values));

        return predictions;
    }

    @Override
    public double next(Collection<Double> values) {
        return MovingFunctions.unweightedAvg(values.stream().mapToDouble(Double::doubleValue).toArray());
    }

    @Override
    public XContentBuilder toXContent(XContentBuilder builder, Params params) throws IOException {
        builder.field(MovAvgPipelineAggregationBuilder.MODEL.getPreferredName(), NAME);
        return builder;
    }

    public static final AbstractModelParser PARSER = new AbstractModelParser() {
        @Override
        public MovAvgModel parse(@Nullable Map<String, Object> settings, String pipelineName, int windowSize) throws ParseException {
            checkUnrecognizedParams(settings);
            return new SimpleModel();
        }
    };

    public static class SimpleModelBuilder implements MovAvgModelBuilder {
        @Override
        public XContentBuilder toXContent(XContentBuilder builder, Params params) throws IOException {
            builder.field(MovAvgPipelineAggregationBuilder.MODEL.getPreferredName(), NAME);
            return builder;
        }

        @Override
        public MovAvgModel build() {
            return new SimpleModel();
        }
    }

    @Override
    public int hashCode() {
        return 0;
    }

    @Override
    public boolean equals(Object obj) {
        if (obj == null) {
            return false;
        }
        if (getClass() != obj.getClass()) {
            return false;
        }
        return true;
    }
}
