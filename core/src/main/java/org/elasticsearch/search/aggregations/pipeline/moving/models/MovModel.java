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

package org.elasticsearch.search.aggregations.pipeline.moving.models;

import org.elasticsearch.common.Nullable;
import org.elasticsearch.common.ParseField;
import org.elasticsearch.common.io.stream.NamedWriteable;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.common.xcontent.ToXContent;

import java.io.IOException;
import java.text.ParseException;
import java.util.Collection;
import java.util.Map;

public abstract class MovModel implements NamedWriteable, ToXContent {

    public static final ParseField MODEL = new ParseField("model");
    public static final ParseField FUNCTION = new ParseField("function");
    public static final ParseField WINDOW = new ParseField("window");
    public static final ParseField SETTINGS = new ParseField("settings");

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
     * @param values    Collection of numerics to movingFn, usually windowed
     * @return          Returns a double, since most smoothing methods operate on floating points
     */
    public abstract double next(Collection<Double> values);

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
    public abstract MovModel clone();

    public int hashCode() {
        return 0;
    }

    public boolean equals(Object obj) {
        if (obj == null) {
            return false;
        }
        return getClass() == obj.getClass();
    }

    /**
     * Abstract class which also provides some concrete parsing functionality.
     */
    public abstract static class AbstractModelParser {
        /**
         * Parse a settings hash that is specific to this model
         *
         * @param settings           Map of settings, extracted from the request
         * @param pipelineName       Name of the parent pipeline agg
         * @param windowSize         Size of the window for this moving model
         * @return                   A fully built moving average model
         */
        public abstract MovModel parse(@Nullable Map<String, Object> settings, String pipelineName,
                                          int windowSize) throws ParseException;


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
