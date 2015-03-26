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

package org.elasticsearch.search.aggregations.reducers.smooth.models;

import org.elasticsearch.common.Nullable;
import org.elasticsearch.common.ParseField;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.Map;

/**
 * Calculate a doubly exponential weighted moving average
 */
public class DoubleExpModel extends SmoothingModel {

    protected static final ParseField NAME_FIELD = new ParseField("double_exp");

    /**
     * Controls smoothing of data. Alpha = 1 retains no memory of past values
     * (e.g. random walk), while alpha = 0 retains infinite memory of past values (e.g.
     * mean of the series).  Useful values are somewhere in between
     */
    private double alpha;

    /**
     * Equivalent to <code>alpha</code>, but controls the smoothing of the trend instead of the data
     */
    private double beta;

    DoubleExpModel(double alpha, double beta) {
        this.alpha = alpha;
        this.beta = beta;
    }


    @Override
    public <T extends Number> double next(Collection<T> values) {
        return next(values, 1).get(0);
    }

    /**
     * Calculate a doubly exponential weighted moving average
     *
     * @param values Collection of values to calculate avg for
     * @param numForecasts number of forecasts into the future to return
     *
     * @param <T>    Type T extending Number
     * @return       Returns a Double containing the moving avg for the window
     */
    public <T extends Number> List<Double> next(Collection<T> values, int numForecasts) {
        // Smoothed value
        double s = 0;
        double last_s = 0;

        // Trend value
        double b = 0;
        double last_b = 0;

        int counter = 0;

        //TODO bail if too few values

        T last;
        for (T v : values) {
            last = v;
            if (counter == 1) {
                s = v.doubleValue();
                b = v.doubleValue() - last.doubleValue();
            } else {
                s = alpha * v.doubleValue() + (1.0d - alpha) * (last_s + last_b);
                b = beta * (s - last_s) + (1 - beta) * last_b;
            }

            counter += 1;
            last_s = s;
            last_b = b;
        }

        List<Double> forecastValues = new ArrayList<>(numForecasts);
        for (int i = 0; i < numForecasts; i++) {
            forecastValues.add(s + (i * b));
        }

        return forecastValues;
    }

    public static final SmoothingModelStreams.Stream STREAM = new SmoothingModelStreams.Stream() {
        @Override
        public SmoothingModel readResult(StreamInput in) throws IOException {
            return new DoubleExpModel(in.readDouble(), in.readDouble());
        }

        @Override
        public String getName() {
            return NAME_FIELD.getPreferredName();
        }
    };

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        out.writeString(STREAM.getName());
        out.writeDouble(alpha);
        out.writeDouble(beta);
    }

    public static class DoubleExpModelParser implements SmoothingModelParser {

        @Override
        public String getName() {
            return NAME_FIELD.getPreferredName();
        }

        @Override
        public SmoothingModel parse(@Nullable Map<String, Object> settings) {

            Double alpha;
            Double beta;

            if (settings == null || (alpha = (Double)settings.get("alpha")) == null) {
                alpha = 0.5;
            }

            if (settings == null || (beta = (Double)settings.get("beta")) == null) {
                beta = 0.5;
            }

            return new DoubleExpModel(alpha, beta);
        }
    }
}

