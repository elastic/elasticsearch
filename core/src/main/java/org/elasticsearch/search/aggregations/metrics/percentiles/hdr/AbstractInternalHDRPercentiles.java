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

package org.elasticsearch.search.aggregations.metrics.percentiles.hdr;

import org.HdrHistogram.DoubleHistogram;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.search.DocValueFormat;
import org.elasticsearch.search.aggregations.InternalAggregation;
import org.elasticsearch.search.aggregations.metrics.InternalNumericMetricsAggregation;
import org.elasticsearch.search.aggregations.pipeline.PipelineAggregator;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.List;
import java.util.Map;
import java.util.zip.DataFormatException;

abstract class AbstractInternalHDRPercentiles extends InternalNumericMetricsAggregation.MultiValue {

    protected double[] keys;
    protected DoubleHistogram state;
    private boolean keyed;

    AbstractInternalHDRPercentiles() {} // for serialization

    public AbstractInternalHDRPercentiles(String name, double[] keys, DoubleHistogram state, boolean keyed, DocValueFormat format,
            List<PipelineAggregator> pipelineAggregators,
            Map<String, Object> metaData) {
        super(name, pipelineAggregators, metaData);
        this.keys = keys;
        this.state = state;
        this.keyed = keyed;
        this.format = format;
    }

    @Override
    public double value(String name) {
        return value(Double.parseDouble(name));
    }

    public abstract double value(double key);

    public long getEstimatedMemoryFootprint() {
        return state.getEstimatedFootprintInBytes();
    }

    @Override
    public AbstractInternalHDRPercentiles doReduce(List<InternalAggregation> aggregations, ReduceContext reduceContext) {
        DoubleHistogram merged = null;
        for (InternalAggregation aggregation : aggregations) {
            final AbstractInternalHDRPercentiles percentiles = (AbstractInternalHDRPercentiles) aggregation;
            if (merged == null) {
                merged = new DoubleHistogram(percentiles.state);
                merged.setAutoResize(true);
            }
            merged.add(percentiles.state);
        }
        return createReduced(getName(), keys, merged, keyed, pipelineAggregators(), getMetaData());
    }

    protected abstract AbstractInternalHDRPercentiles createReduced(String name, double[] keys, DoubleHistogram merged, boolean keyed,
            List<PipelineAggregator> pipelineAggregators, Map<String, Object> metaData);

    @Override
    protected void doReadFrom(StreamInput in) throws IOException {
        format = in.readNamedWriteable(DocValueFormat.class);
        keys = new double[in.readInt()];
        for (int i = 0; i < keys.length; ++i) {
            keys[i] = in.readDouble();
        }
        long minBarForHighestToLowestValueRatio = in.readLong();
        final int serializedLen = in.readVInt();
        byte[] bytes = new byte[serializedLen];
        in.readBytes(bytes, 0, serializedLen);
        ByteBuffer stateBuffer = ByteBuffer.wrap(bytes);
        try {
            state = DoubleHistogram.decodeFromCompressedByteBuffer(stateBuffer, minBarForHighestToLowestValueRatio);
        } catch (DataFormatException e) {
            throw new IOException("Failed to decode DoubleHistogram for aggregation [" + name + "]", e);
        }
        keyed = in.readBoolean();
    }

    @Override
    protected void doWriteTo(StreamOutput out) throws IOException {
        out.writeNamedWriteable(format);
        out.writeInt(keys.length);
        for (int i = 0 ; i < keys.length; ++i) {
            out.writeDouble(keys[i]);
        }
        out.writeLong(state.getHighestToLowestValueRatio());
        ByteBuffer stateBuffer = ByteBuffer.allocate(state.getNeededByteBufferCapacity());
        final int serializedLen = state.encodeIntoCompressedByteBuffer(stateBuffer);
        out.writeVInt(serializedLen);
        out.writeBytes(stateBuffer.array(), 0, serializedLen);
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
                if (format != DocValueFormat.RAW) {
                    builder.field(key + "_as_string", format.format(value));
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
                if (format != DocValueFormat.RAW) {
                    builder.field(CommonFields.VALUE_AS_STRING, format.format(value));
                }
                builder.endObject();
            }
            builder.endArray();
        }
        return builder;
    }
}
