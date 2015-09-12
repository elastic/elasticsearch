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

package org.elasticsearch.search.aggregations.pipeline.derivative;

import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.search.aggregations.AggregationStreams;
import org.elasticsearch.search.aggregations.pipeline.InternalSimpleValue;
import org.elasticsearch.search.aggregations.pipeline.PipelineAggregator;
import org.elasticsearch.search.aggregations.support.format.ValueFormatter;

import java.io.IOException;
import java.util.List;
import java.util.Map;

public class InternalDerivative extends InternalSimpleValue implements Derivative {

    public final static Type TYPE = new Type("derivative");

    public final static AggregationStreams.Stream STREAM = new AggregationStreams.Stream() {
        @Override
        public InternalDerivative readResult(StreamInput in) throws IOException {
            InternalDerivative result = new InternalDerivative();
            result.readFrom(in);
            return result;
        }
    };

    public static void registerStreams() {
        AggregationStreams.registerStream(STREAM, TYPE.stream());
    }

    private double normalizationFactor;

    InternalDerivative() {
    }

    public InternalDerivative(String name, double value, double normalizationFactor, ValueFormatter formatter, List<PipelineAggregator> pipelineAggregators,
            Map<String, Object> metaData) {
        super(name, value, formatter, pipelineAggregators, metaData);
        this.normalizationFactor = normalizationFactor;
    }

    @Override
    public double normalizedValue() {
        return normalizationFactor > 0 ? (value() / normalizationFactor) : value();
    }

    @Override
    public Type type() {
        return TYPE;
    }

    @Override
    public Object getProperty(List<String> path) {
        if (path.isEmpty()) {
            return this;
        } else if (path.size() == 1 && "value".equals(path.get(0))) {
            return value();
        } else if (path.size() == 1 && "normalized_value".equals(path.get(0))) {
            return normalizedValue();
        } else {
            throw new IllegalArgumentException("path not supported for [" + getName() + "]: " + path);
        }
    }

    @Override
    protected void doWriteTo(StreamOutput out) throws IOException {
        super.doWriteTo(out);
        out.writeDouble(normalizationFactor);
    }

    @Override
    protected void doReadFrom(StreamInput in) throws IOException {
        super.doReadFrom(in);
        normalizationFactor = in.readDouble();
    }

    @Override
    public XContentBuilder doXContentBody(XContentBuilder builder, Params params) throws IOException {
        super.doXContentBody(builder, params);

        if (normalizationFactor > 0) {
            boolean hasValue = !(Double.isInfinite(normalizedValue()) || Double.isNaN(normalizedValue()));
            builder.field("normalized_value", hasValue ? normalizedValue() : null);
            if (hasValue && !(valueFormatter instanceof ValueFormatter.Raw)) {
                builder.field("normalized_value_as_string", valueFormatter.format(normalizedValue()));
            }
        }
        return builder;
    }
}
