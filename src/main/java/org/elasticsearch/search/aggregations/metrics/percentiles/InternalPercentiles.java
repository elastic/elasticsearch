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
package org.elasticsearch.search.aggregations.metrics.percentiles;

import com.google.common.collect.UnmodifiableIterator;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.search.aggregations.AggregationStreams;
import org.elasticsearch.search.aggregations.InternalAggregation;
import org.elasticsearch.search.aggregations.metrics.MetricsAggregation;
import org.elasticsearch.search.aggregations.support.format.ValueFormatterStreams;

import java.io.IOException;
import java.util.Iterator;
import java.util.List;

/**
*
*/
public class InternalPercentiles extends MetricsAggregation.MultiValue implements Percentiles {

    public final static Type TYPE = new Type("percentiles");

    public final static AggregationStreams.Stream STREAM = new AggregationStreams.Stream() {
        @Override
        public InternalPercentiles readResult(StreamInput in) throws IOException {
            InternalPercentiles result = new InternalPercentiles();
            result.readFrom(in);
            return result;
        }
    };

    public static void registerStreams() {
        AggregationStreams.registerStream(STREAM, TYPE.stream());
    }

    private PercentilesEstimator.Result result;
    private boolean keyed;

    InternalPercentiles() {} // for serialization

    public InternalPercentiles(String name, PercentilesEstimator.Result result, boolean keyed) {
        super(name);
        this.result = result;
        this.keyed = keyed;
    }

    @Override
    public double value(String name) {
        return result.estimate(Double.valueOf(name));
    }

    @Override
    public Type type() {
        return TYPE;
    }

    @Override
    public double percentile(double percent) {
        return result.estimate(percent);
    }

    @Override
    public Iterator<Percentiles.Percentile> iterator() {
        return new Iter(result);
    }

    @Override
    public InternalPercentiles reduce(ReduceContext reduceContext) {
        List<InternalAggregation> aggregations = reduceContext.aggregations();
        InternalPercentiles first = (InternalPercentiles) aggregations.get(0);
        if (aggregations.size() == 1) {
            return first;
        }
        PercentilesEstimator.Result.Merger merger = first.result.merger(aggregations.size());
        for (InternalAggregation aggregation : aggregations) {
            merger.add(((InternalPercentiles) aggregation).result);
        }
        first.result = merger.merge();
        return first;
    }

    @Override
    public void readFrom(StreamInput in) throws IOException {
        name = in.readString();
        valueFormatter = ValueFormatterStreams.readOptional(in);
        result = PercentilesEstimator.Streams.read(in);
        keyed = in.readBoolean();
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        out.writeString(name);
        ValueFormatterStreams.writeOptional(valueFormatter, out);
        PercentilesEstimator.Streams.write(result, out);
        out.writeBoolean(keyed);
    }

    @Override
    public XContentBuilder toXContent(XContentBuilder builder, Params params) throws IOException {
        double[] percents = result.percents;
        if (keyed) {
            builder.startObject(name);
            for(int i = 0; i < percents.length; ++i) {
                String key = String.valueOf(percents[i]);
                double value = result.estimate(i);
                builder.field(key, value);
                if (valueFormatter != null) {
                    builder.field(key + "_as_string", valueFormatter.format(value));
                }
            }
            builder.endObject();
        } else {
            builder.startArray(name);
            for (int i = 0; i < percents.length; i++) {
                double value = result.estimate(i);
                builder.startObject();
                builder.field(CommonFields.KEY, percents[i]);
                builder.field(CommonFields.VALUE, value);
                if (valueFormatter != null) {
                    builder.field(CommonFields.VALUE_AS_STRING, valueFormatter.format(value));
                }
                builder.endObject();
            }
            builder.endArray();
        }
        return builder;
    }

    public static class Iter extends UnmodifiableIterator<Percentiles.Percentile> {

        private final PercentilesEstimator.Result result;
        private int i;

        public Iter(PercentilesEstimator.Result estimator) {
            this.result = estimator;
            i = 0;
        }

        @Override
        public boolean hasNext() {
            return i < result.percents.length;
        }

        @Override
        public Percentiles.Percentile next() {
            final Percentiles.Percentile next = new InnerPercentile(result.percents[i], result.estimate(i));
            ++i;
            return next;
        }
    }

    private static class InnerPercentile implements Percentiles.Percentile {

        private final double percent;
        private final double value;

        private InnerPercentile(double percent, double value) {
            this.percent = percent;
            this.value = value;
        }

        @Override
        public double getPercent() {
            return percent;
        }

        @Override
        public double getValue() {
            return value;
        }
    }
}
