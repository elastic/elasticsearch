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
import java.util.Map;

/**
 * Calculate a simple unweighted (arithmetic) moving average
 */
public class SimpleModel extends SmoothingModel {

    protected static final ParseField NAME_FIELD = new ParseField("simple");

    @Override
    public <T extends Number> double next(Collection<T> values) {
        double avg = 0;
        for (T v : values) {
            avg += v.doubleValue();
        }
        return avg / values.size();
    }

    public static final SmoothingModelStreams.Stream STREAM = new SmoothingModelStreams.Stream() {
        @Override
        public SmoothingModel readResult(StreamInput in) throws IOException {
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

    public static class SimpleModelParser implements SmoothingModelParser {

        @Override
        public String getName() {
            return NAME_FIELD.getPreferredName();
        }

        @Override
        public SmoothingModel parse(@Nullable Map<String, Object> settings) {
            return new SimpleModel();
        }
    }
}
