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

package org.elasticsearch.search.aggregations.transformer;

import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.search.aggregations.AggregationStreams;
import org.elasticsearch.search.aggregations.InternalAggregation;
import org.elasticsearch.search.aggregations.metrics.InternalNumericMetricsAggregation.MultiValue;
import org.elasticsearch.search.aggregations.support.format.ValueFormatterStreams;

import java.io.IOException;
import java.util.Collection;
import java.util.Map;
import java.util.Map.Entry;

public class InternalSimpleMultiValue extends MultiValue implements SimpleMultiValue {

    public final static Type TYPE = new Type("simple_value");

    public final static AggregationStreams.Stream STREAM = new AggregationStreams.Stream() {
        @Override
        public InternalSimpleMultiValue readResult(StreamInput in) throws IOException {
            InternalSimpleMultiValue result = new InternalSimpleMultiValue();
            result.readFrom(in);
            return result;
        }
    };

    public static void registerStream() {
        AggregationStreams.registerStream(STREAM, TYPE.stream());
    }

    private Map<String, Double> values;

    protected InternalSimpleMultiValue() {
        super();
    }

    public InternalSimpleMultiValue(String name, Map<String, Double> values, Map<String, Object> metaData) {
        super(name, metaData);
        this.values = values;
    }

    @Override
    public Collection<String> valueNames() {
        return values.keySet();
    }

    @Override
    public double value(String name) {
        return values.get(name);
    }

    @Override
    public Type type() {
        return TYPE;
    }

    @Override
    public InternalAggregation reduce(ReduceContext reduceContext) {
        throw new UnsupportedOperationException();
    }

    @Override
    protected void doReadFrom(StreamInput in) throws IOException {
        valueFormatter = ValueFormatterStreams.readOptional(in);
        values = (Map<String, Double>) in.readGenericValue();
    }

    @Override
    protected void doWriteTo(StreamOutput out) throws IOException {
        ValueFormatterStreams.writeOptional(valueFormatter, out);
        out.writeGenericValue(values);
        ;
    }

    @Override
    public XContentBuilder doXContentBody(XContentBuilder builder, Params params) throws IOException {
        for (Entry<String, Double> entry : values.entrySet()) {
            String name = entry.getKey();
            Double value = entry.getValue();
            boolean hasValue = value != null && !Double.isInfinite(value);
            builder.field(name, hasValue ? value : null);
            if (hasValue && valueFormatter != null) {
                builder.field(name + "_as_string", valueFormatter.format(value));
            }
        }
        return builder;
    }

}
