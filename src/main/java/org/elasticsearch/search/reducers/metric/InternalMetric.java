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
package org.elasticsearch.search.reducers.metric;

import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.search.aggregations.AggregationStreams;
import org.elasticsearch.search.aggregations.InternalAggregation;
import org.elasticsearch.search.aggregations.metrics.InternalMetricsAggregation;

import java.io.IOException;
import java.util.List;

public class InternalMetric extends InternalMetricsAggregation {

    public final static Type TYPE = new Type("reducer_metric");

    public final static AggregationStreams.Stream STREAM = new AggregationStreams.Stream() {
        @Override
        public InternalMetric readResult(StreamInput in) throws IOException {
            InternalMetric result = new InternalMetric();
            result.readFrom(in);
            return result;
        }
    };

    public MetricResult getMetricResult() {
        return metricResult;
    }

    private MetricResult metricResult;

    public static void registerStreams() {
        AggregationStreams.registerStream(STREAM, TYPE.stream());
    }

    InternalMetric() {
    } // for serialization

    public InternalMetric(String name, MetricResult metricResult) {
        super();
        this.name = name;
        this.metricResult = metricResult;
    }

    @Override
    public Type type() {
        return TYPE;
    }

    @Override
    public InternalAggregation reduce(ReduceContext reduceContext) {
        throw new UnsupportedOperationException("Shard reducers not implemented yet.");
    }

    @Override
    public Object getProperty(List<String> path) {
        return metricResult.getProperty(path);
    }

    @Override
    public void doReadFrom(StreamInput in) throws IOException {
        String metricType = in.readString();
        metricResult = MetricResultFactory.getInstance(metricType);
        name = in.readString();
        metricResult.readFrom(in);
    }

    @Override
    public void doWriteTo(StreamOutput out) throws IOException {
        out.writeString(metricResult.getType());
        out.writeString(name);
        metricResult.writeTo(out);
    }

    @Override
    public XContentBuilder doXContentBody(XContentBuilder builder, Params params) throws IOException {
        return metricResult.doXContentBody(builder, params);
    }
}
