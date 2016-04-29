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

package org.elasticsearch.search.aggregations.pipeline.bucketmetrics;

import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.search.DocValueFormat;
import org.elasticsearch.search.aggregations.AggregationStreams;
import org.elasticsearch.search.aggregations.InternalAggregation;
import org.elasticsearch.search.aggregations.metrics.InternalNumericMetricsAggregation;
import org.elasticsearch.search.aggregations.pipeline.PipelineAggregator;

import java.io.IOException;
import java.util.List;
import java.util.Map;

public class InternalBucketMetricValue extends InternalNumericMetricsAggregation.SingleValue {

    public final static Type TYPE = new Type("bucket_metric_value");

    public final static AggregationStreams.Stream STREAM = new AggregationStreams.Stream() {
        @Override
        public InternalBucketMetricValue readResult(StreamInput in) throws IOException {
            InternalBucketMetricValue result = new InternalBucketMetricValue();
            result.readFrom(in);
            return result;
        }
    };

    public static void registerStreams() {
        AggregationStreams.registerStream(STREAM, TYPE.stream());
    }

    private double value;

    private String[] keys;

    protected InternalBucketMetricValue() {
        super();
    }

    public InternalBucketMetricValue(String name, String[] keys, double value, DocValueFormat formatter,
            List<PipelineAggregator> pipelineAggregators, Map<String, Object> metaData) {
        super(name, pipelineAggregators, metaData);
        this.keys = keys;
        this.value = value;
        this.format = formatter;
    }

    @Override
    public Type type() {
        return TYPE;
    }

    @Override
    public double value() {
        return value;
    }

    public String[] keys() {
        return keys;
    }

    @Override
    public InternalAggregation doReduce(List<InternalAggregation> aggregations, ReduceContext reduceContext) {
        throw new UnsupportedOperationException("Not supported");
    }

    @Override
    public Object getProperty(List<String> path) {
        if (path.isEmpty()) {
            return this;
        } else if (path.size() == 1 && "value".equals(path.get(0))) {
            return value();
        } else if (path.size() == 1 && "keys".equals(path.get(0))) {
            return keys();
        } else {
            throw new IllegalArgumentException("path not supported for [" + getName() + "]: " + path);
        }
    }

    @Override
    protected void doReadFrom(StreamInput in) throws IOException {
        format = in.readNamedWriteable(DocValueFormat.class);
        value = in.readDouble();
        keys = in.readStringArray();
    }

    @Override
    protected void doWriteTo(StreamOutput out) throws IOException {
        out.writeNamedWriteable(format);
        out.writeDouble(value);
        out.writeStringArray(keys);
    }

    @Override
    public XContentBuilder doXContentBody(XContentBuilder builder, Params params) throws IOException {
        boolean hasValue = !Double.isInfinite(value);
        builder.field(CommonFields.VALUE, hasValue ? value : null);
        if (hasValue && format != DocValueFormat.RAW) {
            builder.field(CommonFields.VALUE_AS_STRING, format.format(value));
        }
        builder.startArray("keys");
        for (String key : keys) {
            builder.value(key);
        }
        builder.endArray();
        return builder;
    }

}
