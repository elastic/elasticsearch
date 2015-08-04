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
import org.elasticsearch.common.ParseField;
import org.elasticsearch.common.ParseFieldMatcher;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.search.aggregations.pipeline.movavg.MovAvgParser;

import java.io.IOException;
import java.text.ParseException;
import java.util.Arrays;
import java.util.Collection;
import java.util.Map;

/**
 * Calculate a exponentially weighted moving average
 */
public class EwmaModel extends MovAvgModel {

    protected static final ParseField NAME_FIELD = new ParseField("ewma");

    /**
     * Controls smoothing of data.  Also known as "level" value.
     * Alpha = 1 retains no memory of past values
     * (e.g. random walk), while alpha = 0 retains infinite memory of past values (e.g.
     * mean of the series).
     */
    private final double alpha;

    public EwmaModel(double alpha) {
        this.alpha = alpha;
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
    protected <T extends Number> double[] doPredict(Collection<T> values, int numPredictions) {
        double[] predictions = new double[numPredictions];

        // EWMA just emits the same final prediction repeatedly.
        Arrays.fill(predictions, next(values));

        return predictions;
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

    public static final MovAvgModelStreams.Stream STREAM = new MovAvgModelStreams.Stream() {
        @Override
        public MovAvgModel readResult(StreamInput in) throws IOException {
            return new EwmaModel(in.readDouble());
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

    public static class SingleExpModelParser extends AbstractModelParser {

        @Override
        public String getName() {
            return NAME_FIELD.getPreferredName();
        }

        @Override
        public MovAvgModel parse(@Nullable Map<String, Object> settings, String pipelineName, int windowSize,
                                 ParseFieldMatcher parseFieldMatcher) throws ParseException {

            double alpha = parseDoubleParam(settings, "alpha", 0.3);
            checkUnrecognizedParams(settings);
            return new EwmaModel(alpha);
        }

    }

    public static class EWMAModelBuilder implements MovAvgModelBuilder {

        private Double alpha;

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
            builder.field(MovAvgParser.MODEL.getPreferredName(), NAME_FIELD.getPreferredName());
            builder.startObject(MovAvgParser.SETTINGS.getPreferredName());
            if (alpha != null) {
                builder.field("alpha", alpha);
            }

            builder.endObject();
            return builder;
        }
    }
}

