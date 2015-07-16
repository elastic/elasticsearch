/*
 * Licensed to Elasticsearch under one or more contributor
 * license agreements. See the NOTICE file distributed with
 * this work for additional information regarding copyright
 * ownership. Elasticsearch licenses this file to you under
 * the Apache License, Version 2.0 (the "License"); you may
 * not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package org.elasticsearch.search.aggregations.pipeline.movavg.models;

import org.elasticsearch.common.Nullable;
import org.elasticsearch.common.ParseFieldMatcher;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.search.SearchParseException;

import java.io.IOException;
import java.text.ParseException;
import java.util.Arrays;
import java.util.Collection;
import java.util.Map;

public abstract class MovAvgModel {

    /**
     * Should this model be fit to the data via a cost minimizing algorithm by default?
     *
     * @return
     */
    public boolean minimizeByDefault() {
        return false;
    }

    /**
     * Returns if the model can be cost minimized.  Not all models have parameters
     * which can be tuned / optimized.
     *
     * @return
     */
    public abstract boolean canBeMinimized();

    /**
     * Generates a "neighboring" model, where one of the tunable parameters has been
     * randomly mutated within the allowed range.  Used for minimization
     *
     * @return
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
     * @param <T>       Type of numeric
     * @return          Returns a double, since most smoothing methods operate on floating points
     */
    public abstract <T extends Number> double next(Collection<T> values);

    /**
     * Predicts the next `n` values in the series.
     *
     * @param values            Collection of numerics to movingAvg, usually windowed
     * @param numPredictions    Number of newly generated predictions to return
     * @param <T>               Type of numeric
     * @return                  Returns an array of doubles, since most smoothing methods operate on floating points
     */
    public <T extends Number> double[] predict(Collection<T> values, int numPredictions) {
        assert(numPredictions >= 1);

        // If there are no values, we can't do anything.  Return an array of NaNs.
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
     * @param <T>               Type of numeric
     * @return                  Returns an array of doubles, since most smoothing methods operate on floating points
     */
    protected abstract <T extends Number> double[] doPredict(Collection<T> values, int numPredictions);

    /**
     * Returns an empty set of predictions, filled with NaNs
     * @param numPredictions Number of empty predictions to generate
     * @return
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
     * @throws IOException
     */
    public abstract void writeTo(StreamOutput out) throws IOException;

    /**
     * Clone the model, returning an exact copy
     *
     * @return
     */
    public abstract MovAvgModel clone();

    /**
     * Abstract class which also provides some concrete parsing functionality.
     */
    public abstract static class AbstractModelParser {

        /**
         * Returns the name of the model
         *
         * @return The model's name
         */
        public abstract String getName();

        /**
         * Parse a settings hash that is specific to this model
         *
         * @param settings           Map of settings, extracted from the request
         * @param pipelineName       Name of the parent pipeline agg
         * @param windowSize         Size of the window for this moving avg
         * @param parseFieldMatcher  Matcher for field names
         * @return                   A fully built moving average model
         */
        public abstract MovAvgModel parse(@Nullable Map<String, Object> settings, String pipelineName,
                                          int windowSize, ParseFieldMatcher parseFieldMatcher) throws ParseException;


        /**
         * Extracts a 0-1 inclusive double from the settings map, otherwise throws an exception
         *
         * @param settings      Map of settings provided to this model
         * @param name          Name of parameter we are attempting to extract
         * @param defaultValue  Default value to be used if value does not exist in map
         *
         * @throws ParseException
         *
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

                throw new ParseException("Parameter [" + name + "] must be between 0-1 inclusive.  Provided"
                        + "value was [" + v + "]", 0);
            }

            throw new ParseException("Parameter [" + name + "] must be a double, type `"
                    + value.getClass().getSimpleName() + "` provided instead", 0);
        }

        /**
         * Extracts an integer from the settings map, otherwise throws an exception
         *
         * @param settings      Map of settings provided to this model
         * @param name          Name of parameter we are attempting to extract
         * @param defaultValue  Default value to be used if value does not exist in map
         *
         * @throws ParseException
         *
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

            throw new ParseException("Parameter [" + name + "] must be an integer, type `"
                    + value.getClass().getSimpleName() + "` provided instead", 0);
        }

        /**
         * Extracts a boolean from the settings map, otherwise throws an exception
         *
         * @param settings      Map of settings provided to this model
         * @param name          Name of parameter we are attempting to extract
         * @param defaultValue  Default value to be used if value does not exist in map
         *
         * @throws SearchParseException
         *
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
                return (Boolean)value;
            }

            throw new ParseException("Parameter [" + name + "] must be a boolean, type `"
                    + value.getClass().getSimpleName() + "` provided instead", 0);
        }

        protected void checkUnrecognizedParams(@Nullable Map<String, Object> settings) throws ParseException {
            if (settings != null && settings.size() > 0) {
                throw new ParseException("Unrecognized parameter(s): [" + settings.keySet() + "]", 0);
            }
        }
    }

}




