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
import org.elasticsearch.ElasticsearchIllegalArgumentException;
import org.elasticsearch.Version;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.search.aggregations.AggregationStreams;
import org.elasticsearch.search.aggregations.InternalAggregation;
import org.elasticsearch.search.aggregations.metrics.MetricsAggregation;
import org.elasticsearch.search.aggregations.metrics.percentiles.tdigest.TDigestState;
import org.elasticsearch.search.aggregations.support.format.ValueFormatterStreams;

import java.io.IOException;
import java.util.Iterator;

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

    private double[] percents;
    private TDigestState state;
    private boolean keyed;

    InternalPercentiles() {} // for serialization

    public InternalPercentiles(String name, double[] percents, TDigestState state, boolean keyed) {
        super(name);
        this.percents = percents;
        this.state = state;
        this.keyed = keyed;
    }

    @Override
    public double value(String name) {
        return percentile(Double.parseDouble(name));
    }

    @Override
    public Type type() {
        return TYPE;
    }

    @Override
    public double percentile(double percent) {
        return state.quantile(percent / 100);
    }

    @Override
    public Iterator<Percentiles.Percentile> iterator() {
        return new Iter(percents, state);
    }

    @Override
    public InternalPercentiles reduce(ReduceContext reduceContext) {
        TDigestState merged = null;
        for (InternalAggregation aggregation : reduceContext.aggregations()) {
            final InternalPercentiles percentiles = (InternalPercentiles) aggregation;
            if (merged == null) {
                merged = new TDigestState(percentiles.state.compression());
            }
            merged.add(percentiles.state);
        }
        return new InternalPercentiles(getName(), percents, merged, keyed);
    }

    @Override
    public void readFrom(StreamInput in) throws IOException {
        name = in.readString();
        valueFormatter = ValueFormatterStreams.readOptional(in);
        if (in.getVersion().before(Version.V_1_2_0)) {
            final byte id = in.readByte();
            if (id != 0) {
                throw new ElasticsearchIllegalArgumentException("Unexpected percentiles aggregator id [" + id + "]");
            }
        }
        percents = new double[in.readInt()];
        for (int i = 0; i < percents.length; ++i) {
            percents[i] = in.readDouble();
        }
        state = TDigestState.read(in);
        keyed = in.readBoolean();
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        out.writeString(name);
        ValueFormatterStreams.writeOptional(valueFormatter, out);
        if (out.getVersion().before(Version.V_1_2_0)) {
            out.writeByte((byte) 0);
        }
        out.writeInt(percents.length);
        for (int i = 0 ; i < percents.length; ++i) {
            out.writeDouble(percents[i]);
        }
        TDigestState.write(state, out);
        out.writeBoolean(keyed);
    }

    @Override
    public XContentBuilder toXContent(XContentBuilder builder, Params params) throws IOException {
        builder.startObject(name);
        if (keyed) {
            builder.startObject(CommonFields.VALUES);
            for(int i = 0; i < percents.length; ++i) {
                String key = String.valueOf(percents[i]);
                double value = percentile(percents[i]);
                builder.field(key, value);
                if (valueFormatter != null) {
                    builder.field(key + "_as_string", valueFormatter.format(value));
                }
            }
            builder.endObject();
        } else {
            builder.startArray(CommonFields.VALUES);
            for (int i = 0; i < percents.length; i++) {
                double value = percentile(percents[i]);
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
        builder.endObject();
        return builder;
    }

    public static class Iter extends UnmodifiableIterator<Percentiles.Percentile> {

        private final double[] percents;
        private final TDigestState state;
        private int i;

        public Iter(double[] percents, TDigestState state) {
            this.percents = percents;
            this.state = state;
            i = 0;
        }

        @Override
        public boolean hasNext() {
            return i < percents.length;
        }

        @Override
        public Percentiles.Percentile next() {
            final Percentiles.Percentile next = new InnerPercentile(percents[i], state.quantile(percents[i] / 100));
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
