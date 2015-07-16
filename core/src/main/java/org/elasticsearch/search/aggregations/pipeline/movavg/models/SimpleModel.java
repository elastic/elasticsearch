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
 * Calculate a simple unweighted (arithmetic) moving average
 */
public class SimpleModel extends MovAvgModel {

    protected static final ParseField NAME_FIELD = new ParseField("simple");


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
    protected <T extends Number> double[] doPredict(Collection<T> values, int numPredictions) {
        double[] predictions = new double[numPredictions];

        // Simple just emits the same final prediction repeatedly.
        Arrays.fill(predictions, next(values));

        return predictions;
    }

    @Override
    public <T extends Number> double next(Collection<T> values) {
        double avg = 0;
        for (T v : values) {
            avg += v.doubleValue();
        }
        return avg / values.size();
    }

    public static final MovAvgModelStreams.Stream STREAM = new MovAvgModelStreams.Stream() {
        @Override
        public MovAvgModel readResult(StreamInput in) throws IOException {
            return new SimpleModel();
        }

        @Override
        public String getName() {
            return NAME_FIELD.getPreferredName();
        }
    };

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        out.writeString(STREAM.getName());
    }

    public static class SimpleModelParser extends AbstractModelParser {

        @Override
        public String getName() {
            return NAME_FIELD.getPreferredName();
        }

        @Override
        public MovAvgModel parse(@Nullable Map<String, Object> settings, String pipelineName, int windowSize,
                                 ParseFieldMatcher parseFieldMatcher) throws ParseException {
            checkUnrecognizedParams(settings);
            return new SimpleModel();
        }
    }

    public static class SimpleModelBuilder implements MovAvgModelBuilder {
        @Override
        public XContentBuilder toXContent(XContentBuilder builder, Params params) throws IOException {
            builder.field(MovAvgParser.MODEL.getPreferredName(), NAME_FIELD.getPreferredName());
            return builder;
        }
    }
}
