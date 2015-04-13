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

import org.elasticsearch.ElasticsearchIllegalArgumentException;
import org.elasticsearch.Version;
import org.elasticsearch.common.inject.internal.Nullable;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.search.aggregations.InternalAggregation;
import org.elasticsearch.search.aggregations.metrics.InternalNumericMetricsAggregation;
import org.elasticsearch.search.aggregations.metrics.percentiles.tdigest.TDigestState;
import org.elasticsearch.search.aggregations.support.format.ValueFormatter;
import org.elasticsearch.search.aggregations.support.format.ValueFormatterStreams;

import java.io.IOException;
import java.util.List;
import java.util.Map;

abstract class AbstractInternalPercentiles extends InternalNumericMetricsAggregation.MultiValue {

    protected double[] keys;
    protected TDigestState state;
    private boolean keyed;

    AbstractInternalPercentiles() {} // for serialization

    public AbstractInternalPercentiles(String name, double[] keys, TDigestState state, boolean keyed, @Nullable ValueFormatter formatter,
            Map<String, Object> metaData) {
        super(name, metaData);
        this.keys = keys;
        this.state = state;
        this.keyed = keyed;
        this.valueFormatter = formatter;
    }

    @Override
    public double value(String name) {
        return value(Double.parseDouble(name));
    }
    
    public abstract double value(double key);

    @Override
    public AbstractInternalPercentiles reduce(List<InternalAggregation> aggregations, ReduceContext reduceContext) {
        TDigestState merged = null;
        for (InternalAggregation aggregation : aggregations) {
            final AbstractInternalPercentiles percentiles = (AbstractInternalPercentiles) aggregation;
            if (merged == null) {
                merged = new TDigestState(percentiles.state.compression());
            }
            merged.add(percentiles.state);
        }
        return createReduced(getName(), keys, merged, keyed, getMetaData());
    }

    protected abstract AbstractInternalPercentiles createReduced(String name, double[] keys, TDigestState merged, boolean keyed, Map<String, Object> metaData);

    @Override
    protected void doReadFrom(StreamInput in) throws IOException {
        valueFormatter = ValueFormatterStreams.readOptional(in);
        if (in.getVersion().before(Version.V_1_2_0)) {
            final byte id = in.readByte();
            if (id != 0) {
                throw new ElasticsearchIllegalArgumentException("Unexpected percentiles aggregator id [" + id + "]");
            }
        }
        keys = new double[in.readInt()];
        for (int i = 0; i < keys.length; ++i) {
            keys[i] = in.readDouble();
        }
        state = TDigestState.read(in);
        keyed = in.readBoolean();
    }

    @Override
    protected void doWriteTo(StreamOutput out) throws IOException {
        ValueFormatterStreams.writeOptional(valueFormatter, out);
        if (out.getVersion().before(Version.V_1_2_0)) {
            out.writeByte((byte) 0);
        }
        out.writeInt(keys.length);
        for (int i = 0 ; i < keys.length; ++i) {
            out.writeDouble(keys[i]);
        }
        TDigestState.write(state, out);
        out.writeBoolean(keyed);
    }

    @Override
    public XContentBuilder doXContentBody(XContentBuilder builder, Params params) throws IOException {
        if (keyed) {
            builder.startObject(CommonFields.VALUES);
            for(int i = 0; i < keys.length; ++i) {
                String key = String.valueOf(keys[i]);
                double value = value(keys[i]);
                builder.field(key, value);
                if (valueFormatter != null && !(valueFormatter instanceof ValueFormatter.Raw)) {
                    builder.field(key + "_as_string", valueFormatter.format(value));
                }
            }
            builder.endObject();
        } else {
            builder.startArray(CommonFields.VALUES);
            for (int i = 0; i < keys.length; i++) {
                double value = value(keys[i]);
                builder.startObject();
                builder.field(CommonFields.KEY, keys[i]);
                builder.field(CommonFields.VALUE, value);
                if (valueFormatter != null && !(valueFormatter instanceof ValueFormatter.Raw)) {
                    builder.field(CommonFields.VALUE_AS_STRING, valueFormatter.format(value));
                }
                builder.endObject();
            }
            builder.endArray();
        }
        return builder;
    }
}
