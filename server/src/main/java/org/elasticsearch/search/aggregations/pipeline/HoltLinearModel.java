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
import java.util.Collection;
import java.util.Map;
import java.util.Objects;

/**
 * Calculate a doubly exponential weighted moving average
 */
public class HoltLinearModel extends MovAvgModel {
    public static final String NAME = "holt";

    private static final double DEFAULT_ALPHA = 0.3;
    private static final double DEFAULT_BETA = 0.1;

    /**
     * Controls smoothing of data.  Also known as "level" value.
     * Alpha = 1 retains no memory of past values
     * (e.g. random walk), while alpha = 0 retains infinite memory of past values (e.g.
     * mean of the series).
     */
    private final double alpha;

    /**
     * Controls smoothing of trend.
     * Beta = 1 retains no memory of past values
     * (e.g. random walk), while alpha = 0 retains infinite memory of past values (e.g.
     * mean of the series).
     */
    private final double beta;

    public HoltLinearModel() {
        this(DEFAULT_ALPHA, DEFAULT_BETA);
    }

    public HoltLinearModel(double alpha, double beta) {
        this.alpha = alpha;
        this.beta = beta;
    }

    /**
     * Read from a stream.
     */
    public HoltLinearModel(StreamInput in) throws IOException {
        alpha = in.readDouble();
        beta = in.readDouble();
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        out.writeDouble(alpha);
        out.writeDouble(beta);
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
        double newValue = Math.random();
        switch ((int) (Math.random() * 2)) {
            case 0:
                return new HoltLinearModel(newValue, this.beta);
            case 1:
                return new HoltLinearModel(this.alpha, newValue);
            default:
                assert (false) : "Random value fell outside of range [0-1]";
                return new HoltLinearModel(newValue, this.beta);    // This should never technically happen...
        }
    }

    @Override
    public MovAvgModel clone() {
        return new HoltLinearModel(this.alpha, this.beta);
    }

    /**
     * Predicts the next `n` values in the series, using the smoothing model to generate new values.
     * Unlike the other moving averages, Holt-Linear has forecasting/prediction built into the algorithm.
     * Prediction is more than simply adding the next prediction to the window and repeating.  Holt-Linear
     * will extrapolate into the future by applying the trend information to the smoothed data.
     *
     * @param values            Collection of numerics to movingAvg, usually windowed
     * @param numPredictions    Number of newly generated predictions to return
     * @return                  Returns an array of doubles, since most smoothing methods operate on floating points
     */
    @Override
    protected double[] doPredict(Collection<Double> values, int numPredictions) {
        return next(values, numPredictions);
    }

    @Override
    public double next(Collection<Double> values) {
        return next(values, 1)[0];
    }

    /**
     * Calculate a Holt-Linear (doubly exponential weighted) moving average
     *
     * @param values Collection of values to calculate avg for
     * @param numForecasts number of forecasts into the future to return
     *
     * @return       Returns a Double containing the moving avg for the window
     */
    public double[] next(Collection<Double> values, int numForecasts) {
        if (values.size() == 0) {
            return emptyPredictions(numForecasts);
        }
        return MovingFunctions.holtForecast(values.stream().mapToDouble(Double::doubleValue).toArray(), alpha, beta, numForecasts);
    }

    @Override
    public XContentBuilder toXContent(XContentBuilder builder, Params params) throws IOException {
        builder.field(MovAvgPipelineAggregationBuilder.MODEL.getPreferredName(), NAME);
        builder.startObject(MovAvgPipelineAggregationBuilder.SETTINGS.getPreferredName());
        builder.field("alpha", alpha);
        builder.field("beta", beta);
        builder.endObject();
        return builder;
    }

    public static final AbstractModelParser PARSER = new AbstractModelParser() {
        @Override
        public MovAvgModel parse(@Nullable Map<String, Object> settings, String pipelineName, int windowSize) throws ParseException {

            double alpha = parseDoubleParam(settings, "alpha", DEFAULT_ALPHA);
            double beta = parseDoubleParam(settings, "beta", DEFAULT_BETA);
            checkUnrecognizedParams(settings);
            return new HoltLinearModel(alpha, beta);
        }
    };

    @Override
    public int hashCode() {
        return Objects.hash(alpha, beta);
    }

    @Override
    public boolean equals(Object obj) {
        if (obj == null) {
            return false;
        }
        if (getClass() != obj.getClass()) {
            return false;
        }
        HoltLinearModel other = (HoltLinearModel) obj;
        return Objects.equals(alpha, other.alpha) && Objects.equals(beta, other.beta);
    }

    public static class HoltLinearModelBuilder implements MovAvgModelBuilder {
        private double alpha = DEFAULT_ALPHA;
        private double beta = DEFAULT_BETA;

        /**
         * Alpha controls the smoothing of the data.  Alpha = 1 retains no memory of past values
         * (e.g. a random walk), while alpha = 0 retains infinite memory of past values (e.g.
         * the series mean).  Useful values are somewhere in between.  Defaults to 0.5.
         *
         * @param alpha A double between 0-1 inclusive, controls data smoothing
         *
         * @return The builder to continue chaining
         */
        public HoltLinearModelBuilder alpha(double alpha) {
            this.alpha = alpha;
            return this;
        }

        /**
         * Equivalent to <code>alpha</code>, but controls the smoothing of the trend instead of the data
         *
         * @param beta a double between 0-1 inclusive, controls trend smoothing
         *
         * @return The builder to continue chaining
         */
        public HoltLinearModelBuilder beta(double beta) {
            this.beta = beta;
            return this;
        }

        @Override
        public XContentBuilder toXContent(XContentBuilder builder, Params params) throws IOException {
            builder.field(MovAvgPipelineAggregationBuilder.MODEL.getPreferredName(), NAME);
            builder.startObject(MovAvgPipelineAggregationBuilder.SETTINGS.getPreferredName());
            builder.field("alpha", alpha);
            builder.field("beta", beta);

            builder.endObject();
            return builder;
        }

        @Override
        public MovAvgModel build() {
            return new HoltLinearModel(alpha, beta);
        }
    }
}
