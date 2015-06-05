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

import com.google.common.collect.EvictingQueue;

import org.elasticsearch.common.Nullable;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.search.SearchParseException;
import org.elasticsearch.search.internal.SearchContext;

import java.io.IOException;
import java.util.Arrays;
import java.util.Collection;
import java.util.Map;

public abstract class MovAvgModel {

    /**
     * Checks to see this model can produce a new value, without actually running the algo.
     * This can be used for models that have certain preconditions that need to be met in order
     * to short-circuit execution
     *
     * @param windowLength  Length of current window
     * @return              Returns `true` if calling next() will produce a value, `false` otherwise
     */
    public boolean hasValue(int windowLength) {
        // Default implementation can always provide a next() value
        return true;
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
     * Predicts the next `n` values in the series, using the smoothing model to generate new values.
     * Default prediction mode is to simply continuing calling <code>next()</code> and adding the
     * predicted value back into the windowed buffer.
     *
     * @param values            Collection of numerics to movingAvg, usually windowed
     * @param numPredictions    Number of newly generated predictions to return
     * @param <T>               Type of numeric
     * @return                  Returns an array of doubles, since most smoothing methods operate on floating points
     */
    public <T extends Number> double[] predict(Collection<T> values, int numPredictions) {
        double[] predictions = new double[numPredictions];

        // If there are no values, we can't do anything.  Return an array of NaNs.
        if (values.size() == 0) {
            return emptyPredictions(numPredictions);
        }

        // special case for one prediction, avoids allocation
        if (numPredictions < 1) {
            throw new IllegalArgumentException("numPredictions may not be less than 1.");
        } else if (numPredictions == 1){
            predictions[0] = next(values);
            return predictions;
        }

        Collection<Number> predictionBuffer = EvictingQueue.create(values.size());
        predictionBuffer.addAll(values);

        for (int i = 0; i < numPredictions; i++) {
            predictions[i] = next(predictionBuffer);

            // Add the last value to the buffer, so we can keep predicting
            predictionBuffer.add(predictions[i]);
        }

        return predictions;
    }

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
         * @param settings      Map of settings, extracted from the request
         * @param pipelineName   Name of the parent pipeline agg
         * @param context       The parser context that we are in
         * @param windowSize    Size of the window for this moving avg
         * @return              A fully built moving average model
         */
        public abstract MovAvgModel parse(@Nullable Map<String, Object> settings, String pipelineName, SearchContext context, int windowSize);


        /**
         * Extracts a 0-1 inclusive double from the settings map, otherwise throws an exception
         *
         * @param context       Search query context
         * @param settings      Map of settings provided to this model
         * @param name          Name of parameter we are attempting to extract
         * @param defaultValue  Default value to be used if value does not exist in map
         *
         * @throws SearchParseException
         *
         * @return Double value extracted from settings map
         */
        protected double parseDoubleParam(SearchContext context, @Nullable Map<String, Object> settings, String name, double defaultValue) {
            if (settings == null) {
                return defaultValue;
            }

            Object value = settings.get(name);
            if (value == null) {
                return defaultValue;
            } else if (value instanceof Double) {
                double v = (Double)value;
                if (v >= 0 && v <= 1) {
                    return v;
                }

                throw new SearchParseException(context, "Parameter [" + name + "] must be between 0-1 inclusive.  Provided"
                        + "value was [" + v + "]", null);
            }

            throw new SearchParseException(context, "Parameter [" + name + "] must be a double, type `"
                    + value.getClass().getSimpleName() + "` provided instead", null);
        }

        /**
         * Extracts an integer from the settings map, otherwise throws an exception
         *
         * @param context       Search query context
         * @param settings      Map of settings provided to this model
         * @param name          Name of parameter we are attempting to extract
         * @param defaultValue  Default value to be used if value does not exist in map
         *
         * @throws SearchParseException
         *
         * @return Integer value extracted from settings map
         */
        protected int parseIntegerParam(SearchContext context, @Nullable Map<String, Object> settings, String name, int defaultValue) {
            if (settings == null) {
                return defaultValue;
            }

            Object value = settings.get(name);
            if (value == null) {
                return defaultValue;
            } else if (value instanceof Integer) {
                return (Integer)value;
            }

            throw new SearchParseException(context, "Parameter [" + name + "] must be an integer, type `"
                    + value.getClass().getSimpleName() + "` provided instead", null);
        }

        /**
         * Extracts a boolean from the settings map, otherwise throws an exception
         *
         * @param context       Search query context
         * @param settings      Map of settings provided to this model
         * @param name          Name of parameter we are attempting to extract
         * @param defaultValue  Default value to be used if value does not exist in map
         *
         * @throws SearchParseException
         *
         * @return Boolean value extracted from settings map
         */
        protected boolean parseBoolParam(SearchContext context, @Nullable Map<String, Object> settings, String name, boolean defaultValue) {
            if (settings == null) {
                return defaultValue;
            }

            Object value = settings.get(name);
            if (value == null) {
                return defaultValue;
            } else if (value instanceof Boolean) {
                return (Boolean)value;
            }

            throw new SearchParseException(context, "Parameter [" + name + "] must be a boolean, type `"
                    + value.getClass().getSimpleName() + "` provided instead", null);
        }
    }

}




