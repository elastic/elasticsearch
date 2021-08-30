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
import java.util.Objects;

/**
 * Calculate a exponentially weighted moving average
 */
public class EwmaModel extends MovAvgModel {
    public static final String NAME = "ewma";

    private static final double DEFAULT_ALPHA = 0.3;

    /**
     * Controls smoothing of data.  Also known as "level" value.
     * Alpha = 1 retains no memory of past values
     * (e.g. random walk), while alpha = 0 retains infinite memory of past values (e.g.
     * mean of the series).
     */
    private final double alpha;

    public EwmaModel() {
        this(DEFAULT_ALPHA);
    }

    public EwmaModel(double alpha) {
        this.alpha = alpha;
    }

    /**
     * Read from a stream.
     */
    public EwmaModel(StreamInput in) throws IOException {
        alpha = in.readDouble();
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        out.writeDouble(alpha);
    }

    @Override
    public String getWriteableName() {
        return NAME;
    }

    @Override
    public boolean canBeMinimized() {
        return true;
    }

    @Override
    public MovAvgModel neighboringModel() {
        double alpha = Math.random();
        return new EwmaModel(alpha);
    }

    @Override
    public MovAvgModel clone() {
        return new EwmaModel(this.alpha);
    }

    @Override
    protected double[] doPredict(Collection<Double> values, int numPredictions) {
        double[] predictions = new double[numPredictions];

        // EWMA just emits the same final prediction repeatedly.
        Arrays.fill(predictions, next(values));

        return predictions;
    }

    @Override
    public double next(Collection<Double> values) {
        return MovingFunctions.ewma(values.stream().mapToDouble(Double::doubleValue).toArray(), alpha);
    }

    @Override
    public XContentBuilder toXContent(XContentBuilder builder, Params params) throws IOException {
        builder.field(MovAvgPipelineAggregationBuilder.MODEL.getPreferredName(), NAME);
        builder.startObject(MovAvgPipelineAggregationBuilder.SETTINGS.getPreferredName());
        builder.field("alpha", alpha);
        builder.endObject();
        return builder;
    }

    public static final AbstractModelParser PARSER = new AbstractModelParser() {
        @Override
        public MovAvgModel parse(@Nullable Map<String, Object> settings, String pipelineName, int windowSize) throws ParseException {
            double alpha = parseDoubleParam(settings, "alpha", DEFAULT_ALPHA);
            checkUnrecognizedParams(settings);
            return new EwmaModel(alpha);
        }
    };

    @Override
    public int hashCode() {
        return Objects.hash(alpha);
    }

    @Override
    public boolean equals(Object obj) {
        if (obj == null) {
            return false;
        }
        if (getClass() != obj.getClass()) {
            return false;
        }
        EwmaModel other = (EwmaModel) obj;
        return Objects.equals(alpha, other.alpha);
    }

    public static class EWMAModelBuilder implements MovAvgModelBuilder {

        private double alpha = DEFAULT_ALPHA;

        /**
         * Alpha controls the smoothing of the data.  Alpha = 1 retains no memory of past values
         * (e.g. a random walk), while alpha = 0 retains infinite memory of past values (e.g.
         * the series mean).  Useful values are somewhere in between.  Defaults to 0.5.
         *
         * @param alpha A double between 0-1 inclusive, controls data smoothing
         *
         * @return The builder to continue chaining
         */
        public EWMAModelBuilder alpha(double alpha) {
            this.alpha = alpha;
            return this;
        }

        @Override
        public XContentBuilder toXContent(XContentBuilder builder, Params params) throws IOException {
            builder.field(MovAvgPipelineAggregationBuilder.MODEL.getPreferredName(), NAME);
            builder.startObject(MovAvgPipelineAggregationBuilder.SETTINGS.getPreferredName());
            builder.field("alpha", alpha);

            builder.endObject();
            return builder;
        }

        @Override
        public MovAvgModel build() {
            return new EwmaModel(alpha);
        }
    }
}
