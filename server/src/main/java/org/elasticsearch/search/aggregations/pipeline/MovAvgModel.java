/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.search.aggregations.pipeline;

import org.elasticsearch.common.io.stream.NamedWriteable;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.common.xcontent.ToXContentFragment;
import org.elasticsearch.core.Nullable;

import java.io.IOException;
import java.text.ParseException;
import java.util.Arrays;
import java.util.Collection;
import java.util.Map;

public abstract class MovAvgModel implements NamedWriteable, ToXContentFragment {

    /**
     * Should this model be fit to the data via a cost minimizing algorithm by default?
     */
    public boolean minimizeByDefault() {
        return false;
    }

    /**
     * Returns if the model can be cost minimized.  Not all models have parameters
     * which can be tuned / optimized.
     */
    public abstract boolean canBeMinimized();

    /**
     * Generates a "neighboring" model, where one of the tunable parameters has been
     * randomly mutated within the allowed range.  Used for minimization
     */
    public abstract MovAvgModel neighboringModel();

    /**
     * Checks to see this model can produce a new value, without actually running the algo.
     * This can be used for models that have certain preconditions that need to be met in order
     * to short-circuit execution
     *
     * @param valuesAvailable Number of values in the current window of values
     * @return                Returns `true` if calling next() will produce a value, `false` otherwise
     */
    public boolean hasValue(int valuesAvailable) {
        // Default implementation can always provide a next() value
        return valuesAvailable > 0;
    }

    /**
     * Returns the next value in the series, according to the underlying smoothing model
     *
     * @param values    Collection of numerics to movingAvg, usually windowed
     * @return          Returns a double, since most smoothing methods operate on floating points
     */
    public abstract double next(Collection<Double> values);

    /**
     * Predicts the next `n` values in the series.
     *
     * @param values            Collection of numerics to movingAvg, usually windowed
     * @param numPredictions    Number of newly generated predictions to return
     * @return                  Returns an array of doubles, since most smoothing methods operate on floating points
     */
    public double[] predict(Collection<Double> values, int numPredictions) {
        assert (numPredictions >= 1);

        // If there are no values, we can't do anything. Return an array of NaNs.
        if (values.isEmpty()) {
            return emptyPredictions(numPredictions);
        }

        return doPredict(values, numPredictions);
    }

    /**
     * Calls to the model-specific implementation which actually generates the predictions
     *
     * @param values            Collection of numerics to movingAvg, usually windowed
     * @param numPredictions    Number of newly generated predictions to return
     * @return                  Returns an array of doubles, since most smoothing methods operate on floating points
     */
    protected abstract double[] doPredict(Collection<Double> values, int numPredictions);

    /**
     * This method allows models to validate the window size if required
     */
    protected void validate(long window, String aggregationName) {
        if (window <= 0) {
            throw new IllegalArgumentException("[window] must be a positive integer in aggregation [" + aggregationName + "]");
        }
    }

    /**
     * Returns an empty set of predictions, filled with NaNs
     * @param numPredictions Number of empty predictions to generate
     */
    protected double[] emptyPredictions(int numPredictions) {
        double[] predictions = new double[numPredictions];
        Arrays.fill(predictions, Double.NaN);
        return predictions;
    }

    /**
     * Write the model to the output stream
     *
     * @param out   Output stream
     */
    @Override
    public abstract void writeTo(StreamOutput out) throws IOException;

    /**
     * Clone the model, returning an exact copy
     */
    @Override
    public abstract MovAvgModel clone();

    @Override
    public abstract int hashCode();

    @Override
    public abstract boolean equals(Object obj);

    /**
     * Abstract class which also provides some concrete parsing functionality.
     */
    public abstract static class AbstractModelParser {
        /**
         * Parse a settings hash that is specific to this model
         *
         * @param settings           Map of settings, extracted from the request
         * @param pipelineName       Name of the parent pipeline agg
         * @param windowSize         Size of the window for this moving avg
         * @return                   A fully built moving average model
         */
        public abstract MovAvgModel parse(@Nullable Map<String, Object> settings, String pipelineName, int windowSize)
            throws ParseException;

        /**
         * Extracts a 0-1 inclusive double from the settings map, otherwise throws an exception
         *
         * @param settings      Map of settings provided to this model
         * @param name          Name of parameter we are attempting to extract
         * @param defaultValue  Default value to be used if value does not exist in map
         * @return Double value extracted from settings map
         */
        protected double parseDoubleParam(@Nullable Map<String, Object> settings, String name, double defaultValue) throws ParseException {
            if (settings == null) {
                return defaultValue;
            }

            Object value = settings.get(name);
            if (value == null) {
                return defaultValue;
            } else if (value instanceof Number) {
                double v = ((Number) value).doubleValue();
                if (v >= 0 && v <= 1) {
                    settings.remove(name);
                    return v;
                }

                throw new ParseException("Parameter [" + name + "] must be between 0-1 inclusive.  Provided" + "value was [" + v + "]", 0);
            }

            throw new ParseException(
                "Parameter [" + name + "] must be a double, type `" + value.getClass().getSimpleName() + "` provided instead",
                0
            );
        }

        /**
         * Extracts an integer from the settings map, otherwise throws an exception
         *
         * @param settings      Map of settings provided to this model
         * @param name          Name of parameter we are attempting to extract
         * @param defaultValue  Default value to be used if value does not exist in map
         * @return Integer value extracted from settings map
         */
        protected int parseIntegerParam(@Nullable Map<String, Object> settings, String name, int defaultValue) throws ParseException {
            if (settings == null) {
                return defaultValue;
            }

            Object value = settings.get(name);
            if (value == null) {
                return defaultValue;
            } else if (value instanceof Number) {
                settings.remove(name);
                return ((Number) value).intValue();
            }

            throw new ParseException(
                "Parameter [" + name + "] must be an integer, type `" + value.getClass().getSimpleName() + "` provided instead",
                0
            );
        }

        /**
         * Extracts a boolean from the settings map, otherwise throws an exception
         *
         * @param settings      Map of settings provided to this model
         * @param name          Name of parameter we are attempting to extract
         * @param defaultValue  Default value to be used if value does not exist in map
         * @return Boolean value extracted from settings map
         */
        protected boolean parseBoolParam(@Nullable Map<String, Object> settings, String name, boolean defaultValue) throws ParseException {
            if (settings == null) {
                return defaultValue;
            }

            Object value = settings.get(name);
            if (value == null) {
                return defaultValue;
            } else if (value instanceof Boolean) {
                settings.remove(name);
                return (Boolean) value;
            }

            throw new ParseException(
                "Parameter [" + name + "] must be a boolean, type `" + value.getClass().getSimpleName() + "` provided instead",
                0
            );
        }

        protected void checkUnrecognizedParams(@Nullable Map<String, Object> settings) throws ParseException {
            if (settings != null && settings.size() > 0) {
                throw new ParseException("Unrecognized parameter(s): [" + settings.keySet() + "]", 0);
            }
        }
    }

}
