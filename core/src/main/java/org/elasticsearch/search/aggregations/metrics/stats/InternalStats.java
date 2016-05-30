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
package org.elasticsearch.search.aggregations.metrics.stats;

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

/**
*
*/
public class InternalStats extends InternalNumericMetricsAggregation.MultiValue implements Stats {

    public final static Type TYPE = new Type("stats");

    public final static AggregationStreams.Stream STREAM = new AggregationStreams.Stream() {
        @Override
        public InternalStats readResult(StreamInput in) throws IOException {
            InternalStats result = new InternalStats();
            result.readFrom(in);
            return result;
        }
    };

    public static void registerStreams() {
        AggregationStreams.registerStream(STREAM, TYPE.stream());
    }

    enum Metrics {

        count, sum, min, max, avg;

        public static Metrics resolve(String name) {
            return Metrics.valueOf(name);
        }
    }

    protected long count;
    protected double min;
    protected double max;
    protected double sum;

    protected InternalStats() {} // for serialization

    public InternalStats(String name, long count, double sum, double min, double max, DocValueFormat formatter,
            List<PipelineAggregator> pipelineAggregators,
            Map<String, Object> metaData) {
        super(name, pipelineAggregators, metaData);
        this.count = count;
        this.sum = sum;
        this.min = min;
        this.max = max;
        this.format = formatter;
    }

    @Override
    public long getCount() {
        return count;
    }

    @Override
    public double getMin() {
        return min;
    }

    @Override
    public double getMax() {
        return max;
    }

    @Override
    public double getAvg() {
        return sum / count;
    }

    @Override
    public double getSum() {
        return sum;
    }

    @Override
    public String getCountAsString() {
        return valueAsString(Metrics.count.name());
    }

    @Override
    public String getMinAsString() {
        return valueAsString(Metrics.min.name());
    }

    @Override
    public String getMaxAsString() {
        return valueAsString(Metrics.max.name());
    }

    @Override
    public String getAvgAsString() {
        return valueAsString(Metrics.avg.name());
    }

    @Override
    public String getSumAsString() {
        return valueAsString(Metrics.sum.name());
    }

    @Override
    public Type type() {
        return TYPE;
    }

    @Override
    public double value(String name) {
        Metrics metrics = Metrics.valueOf(name);
        switch (metrics) {
            case min: return this.min;
            case max: return this.max;
            case avg: return this.getAvg();
            case count: return this.count;
            case sum: return this.sum;
            default:
                throw new IllegalArgumentException("Unknown value [" + name + "] in common stats aggregation");
        }
    }

    @Override
    public InternalStats doReduce(List<InternalAggregation> aggregations, ReduceContext reduceContext) {
        long count = 0;
        double min = Double.POSITIVE_INFINITY;
        double max = Double.NEGATIVE_INFINITY;
        double sum = 0;
        for (InternalAggregation aggregation : aggregations) {
            InternalStats stats = (InternalStats) aggregation;
            count += stats.getCount();
            min = Math.min(min, stats.getMin());
            max = Math.max(max, stats.getMax());
            sum += stats.getSum();
        }
        return new InternalStats(name, count, sum, min, max, format, pipelineAggregators(), getMetaData());
    }

    @Override
    protected void doReadFrom(StreamInput in) throws IOException {
        format = in.readNamedWriteable(DocValueFormat.class);
        count = in.readVLong();
        min = in.readDouble();
        max = in.readDouble();
        sum = in.readDouble();
        readOtherStatsFrom(in);
    }

    public void readOtherStatsFrom(StreamInput in) throws IOException {
    }

    @Override
    protected void doWriteTo(StreamOutput out) throws IOException {
        out.writeNamedWriteable(format);
        out.writeVLong(count);
        out.writeDouble(min);
        out.writeDouble(max);
        out.writeDouble(sum);
        writeOtherStatsTo(out);
    }

    protected void writeOtherStatsTo(StreamOutput out) throws IOException {
    }

    static class Fields {
        public static final String COUNT = "count";
        public static final String MIN = "min";
        public static final String MIN_AS_STRING = "min_as_string";
        public static final String MAX = "max";
        public static final String MAX_AS_STRING = "max_as_string";
        public static final String AVG = "avg";
        public static final String AVG_AS_STRING = "avg_as_string";
        public static final String SUM = "sum";
        public static final String SUM_AS_STRING = "sum_as_string";
    }

    @Override
    public XContentBuilder doXContentBody(XContentBuilder builder, Params params) throws IOException {
        builder.field(Fields.COUNT, count);
        builder.field(Fields.MIN, count != 0 ? min : null);
        builder.field(Fields.MAX, count != 0 ? max : null);
        builder.field(Fields.AVG, count != 0 ? getAvg() : null);
        builder.field(Fields.SUM, count != 0 ? sum : null);
        if (count != 0 && format != DocValueFormat.RAW) {
            builder.field(Fields.MIN_AS_STRING, format.format(min));
            builder.field(Fields.MAX_AS_STRING, format.format(max));
            builder.field(Fields.AVG_AS_STRING, format.format(getAvg()));
            builder.field(Fields.SUM_AS_STRING, format.format(sum));
        }
        otherStatsToXCotent(builder, params);
        return builder;
    }

    protected XContentBuilder otherStatsToXCotent(XContentBuilder builder, Params params) throws IOException {
        return builder;
    }
}
