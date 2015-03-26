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
import java.util.Collection;
import java.util.HashMap;
import java.util.Map;

/**
 * Calculate a exponentially weighted moving average
 */
public class SingleExpModel extends SmoothingModel {

    protected static final ParseField NAME_FIELD = new ParseField("single_exp");

    /**
     * Controls smoothing of data. Alpha = 1 retains no memory of past values
     * (e.g. random walk), while alpha = 0 retains infinite memory of past values (e.g.
     * mean of the series).  Useful values are somewhere in between
     */
    private double alpha;

    public SingleExpModel(double alpha) {
        this.alpha = alpha;
    }


    @Override
    public <T extends Number> double next(Collection<T> values) {
        double avg = 0;
        boolean first = true;

        for (T v : values) {
            if (first) {
                avg = v.doubleValue();
                first = false;
            } else {
                avg = (v.doubleValue() * alpha) + (avg * (1 - alpha));
            }
        }
        return avg;
    }

    public static final SmoothingModelStreams.Stream STREAM = new SmoothingModelStreams.Stream() {
        @Override
        public SmoothingModel readResult(StreamInput in) throws IOException {
            return new SingleExpModel(in.readDouble());
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
    }

    public static class SingleExpModelParser implements SmoothingModelParser {

        @Override
        public String getName() {
            return NAME_FIELD.getPreferredName();
        }

        @Override
        public SmoothingModel parse(@Nullable Map<String, Object> settings) {

            Double alpha;
            if (settings == null || (alpha = (Double)settings.get("alpha")) == null) {
                alpha = 0.5;
            }

            return new SingleExpModel(alpha);
        }
    }

    public static String type() {
        return NAME_FIELD.getPreferredName();
    }

    public static @Nullable Map<String, Object> settings(double alpha) {
        Map<String, Object> settings = new HashMap<>(1);
        settings.put("alpha", alpha);
        return settings;
    }
}

