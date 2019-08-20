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

package org.elasticsearch.search.aggregations.metrics;

import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.search.DocValueFormat;
import org.elasticsearch.search.aggregations.InternalAggregation;
import org.elasticsearch.search.aggregations.pipeline.PipelineAggregator;

import java.io.IOException;
import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.Objects;

abstract class AbstractInternalTDigestPercentiles extends InternalNumericMetricsAggregation.MultiValue {

    protected final double[] keys;
    protected final TDigestState state;
    final boolean keyed;

    AbstractInternalTDigestPercentiles(String name, double[] keys, TDigestState state, boolean keyed, DocValueFormat formatter,
            List<PipelineAggregator> pipelineAggregators,
            Map<String, Object> metaData) {
        super(name, pipelineAggregators, metaData);
        this.keys = keys;
        this.state = state;
        this.keyed = keyed;
        this.format = formatter;
    }

    /**
     * Read from a stream.
     */
    protected AbstractInternalTDigestPercentiles(StreamInput in) throws IOException {
        super(in);
        format = in.readNamedWriteable(DocValueFormat.class);
        keys = in.readDoubleArray();
        state = TDigestState.read(in);
        keyed = in.readBoolean();
    }

    @Override
    protected void doWriteTo(StreamOutput out) throws IOException {
        out.writeNamedWriteable(format);
        out.writeDoubleArray(keys);
        TDigestState.write(state, out);
        out.writeBoolean(keyed);
    }

    @Override
    public double value(String name) {
        return value(Double.parseDouble(name));
    }

    public abstract double value(double key);

    DocValueFormat formatter() {
        return format;
    }

    public long getEstimatedMemoryFootprint() {
        return state.byteSize();
    }

    TDigestState getState() {
        return state;
    }

    @Override
    public AbstractInternalTDigestPercentiles reduce(List<InternalAggregation> aggregations, ReduceContext reduceContext) {
        TDigestState merged = null;
        for (InternalAggregation aggregation : aggregations) {
            final AbstractInternalTDigestPercentiles percentiles = (AbstractInternalTDigestPercentiles) aggregation;
            if (merged == null) {
                merged = new TDigestState(percentiles.state.compression());
            }
            merged.add(percentiles.state);
        }
        return createReduced(getName(), keys, merged, keyed, pipelineAggregators(), getMetaData());
    }

    protected abstract AbstractInternalTDigestPercentiles createReduced(String name, double[] keys, TDigestState merged, boolean keyed,
            List<PipelineAggregator> pipelineAggregators, Map<String, Object> metaData);

    @Override
    public XContentBuilder doXContentBody(XContentBuilder builder, Params params) throws IOException {
        if (keyed) {
            builder.startObject(CommonFields.VALUES.getPreferredName());
            for(int i = 0; i < keys.length; ++i) {
                String key = String.valueOf(keys[i]);
                double value = value(keys[i]);
                builder.field(key, state.size() == 0 ? null : value);
                if (format != DocValueFormat.RAW && state.size() > 0) {
                    builder.field(key + "_as_string", format.format(value).toString());
                }
            }
            builder.endObject();
        } else {
            builder.startArray(CommonFields.VALUES.getPreferredName());
            for (int i = 0; i < keys.length; i++) {
                double value = value(keys[i]);
                builder.startObject();
                builder.field(CommonFields.KEY.getPreferredName(), keys[i]);
                builder.field(CommonFields.VALUE.getPreferredName(), state.size() == 0 ? null : value);
                if (format != DocValueFormat.RAW && state.size() > 0) {
                    builder.field(CommonFields.VALUE_AS_STRING.getPreferredName(), format.format(value).toString());
                }
                builder.endObject();
            }
            builder.endArray();
        }
        return builder;
    }

    @Override
    public boolean equals(Object obj) {
        if (this == obj) return true;
        if (obj == null || getClass() != obj.getClass()) return false;
        if (super.equals(obj) == false) return false;

        AbstractInternalTDigestPercentiles that = (AbstractInternalTDigestPercentiles) obj;
        return keyed == that.keyed
                && Arrays.equals(keys, that.keys)
                && Objects.equals(state, that.state);
    }

    @Override
    public int hashCode() {
        return Objects.hash(super.hashCode(), keyed, Arrays.hashCode(keys), state);
    }
}
