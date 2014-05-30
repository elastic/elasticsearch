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

package org.elasticsearch.index.fielddata;

import com.carrotsearch.hppc.ObjectLongOpenHashMap;
import org.elasticsearch.Version;
import org.elasticsearch.common.Nullable;
import org.elasticsearch.common.Strings;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.common.io.stream.Streamable;
import org.elasticsearch.common.metrics.MeterMetric;
import org.elasticsearch.common.unit.ByteSizeValue;
import org.elasticsearch.common.xcontent.ToXContent;
import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.common.xcontent.XContentBuilderString;

import java.io.IOException;

/**
 */
public class FieldDataStats implements Streamable, ToXContent {

    long memorySize;

    private long evictions;
    private double evictionsOneMinuteRate;
    private double evictionsFiveMinuteRate;
    private double evictionsFifteenMinuteRate;
    @Nullable
    ObjectLongOpenHashMap<String> fields;

    public FieldDataStats() {

    }

    public FieldDataStats(long memorySize, MeterMetric evictionMeter, @Nullable ObjectLongOpenHashMap<String> fields) {
        this.memorySize = memorySize;
        this.fields = fields;
        this.evictions = evictionMeter.count();
        this.evictionsOneMinuteRate = evictionMeter.oneMinuteRate();
        this.evictionsFiveMinuteRate = evictionMeter.fiveMinuteRate();
        this.evictionsFifteenMinuteRate = evictionMeter.fifteenMinuteRate();
    }

    public void add(FieldDataStats stats) {
        this.memorySize += stats.memorySize;
        this.evictions += stats.evictions;
        this.evictionsOneMinuteRate += stats.evictionsOneMinuteRate;
        this.evictionsFiveMinuteRate += stats.evictionsFiveMinuteRate;
        this.evictionsFifteenMinuteRate += stats.evictionsFifteenMinuteRate;
        if (stats.fields != null) {
            if (fields == null) fields = new ObjectLongOpenHashMap<>();
            final boolean[] states = stats.fields.allocated;
            final Object[] keys = stats.fields.keys;
            final long[] values = stats.fields.values;
            for (int i = 0; i < states.length; i++) {
                if (states[i]) {
                    fields.addTo((String) keys[i], values[i]);
                }
            }
        }
    }

    public long getMemorySizeInBytes() {
        return this.memorySize;
    }

    public ByteSizeValue getMemorySize() {
        return new ByteSizeValue(memorySize);
    }

    public long getEvictions() {
        return this.evictions;
    }

    public double getEvictionsOneMinuteRate() {
        return this.evictionsOneMinuteRate;
    }

    public double getEvictionsFiveMinuteRate() {
        return this.evictionsFiveMinuteRate;
    }

    public double getEvictionsFifteenMinuteRate() {
        return this.evictionsFifteenMinuteRate;
    }

    @Nullable
    public ObjectLongOpenHashMap<String> getFields() {
        return fields;
    }

    public static FieldDataStats readFieldDataStats(StreamInput in) throws IOException {
        FieldDataStats stats = new FieldDataStats();
        stats.readFrom(in);
        return stats;
    }

    @Override
    public void readFrom(StreamInput in) throws IOException {
        memorySize = in.readVLong();
        evictions = in.readVLong();
        if (in.getVersion().onOrAfter(Version.V_1_3_0)) {
            evictionsOneMinuteRate = in.readDouble();
            evictionsFiveMinuteRate = in.readDouble();
            evictionsFifteenMinuteRate = in.readDouble();
        } else {
            evictionsOneMinuteRate = -1;
            evictionsFiveMinuteRate = -1;
            evictionsFifteenMinuteRate = -1;
        }
        if (in.readBoolean()) {
            int size = in.readVInt();
            fields = new ObjectLongOpenHashMap<>(size);
            for (int i = 0; i < size; i++) {
                fields.put(in.readString(), in.readVLong());
            }
        }
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        out.writeVLong(memorySize);
        out.writeVLong(evictions);
        if (out.getVersion().onOrAfter(Version.V_1_3_0)) {
            out.writeDouble(evictionsOneMinuteRate);
            out.writeDouble(evictionsFiveMinuteRate);
            out.writeDouble(evictionsFifteenMinuteRate);
        }
        if (fields == null) {
            out.writeBoolean(false);
        } else {
            out.writeBoolean(true);
            out.writeVInt(fields.size());
            final boolean[] states = fields.allocated;
            final Object[] keys = fields.keys;
            final long[] values = fields.values;
            for (int i = 0; i < states.length; i++) {
                if (states[i]) {
                    out.writeString((String) keys[i]);
                    out.writeVLong(values[i]);
                }
            }
        }
    }

    @Override
    public XContentBuilder toXContent(XContentBuilder builder, Params params) throws IOException {
        builder.startObject(Fields.FIELDDATA);
        builder.byteSizeField(Fields.MEMORY_SIZE_IN_BYTES, Fields.MEMORY_SIZE, memorySize);
        builder.field(Fields.EVICTIONS, getEvictions());

        builder.startObject(Fields.RATES);
        builder.field(Fields.ONE_MIN, Double.valueOf(Strings.format1Decimals(getEvictionsOneMinuteRate(),"")));
        builder.field(Fields.FIVE_MIN, Double.valueOf(Strings.format1Decimals(getEvictionsFiveMinuteRate(),"")));
        builder.field(Fields.FIFTEEN_MIN, Double.valueOf(Strings.format1Decimals(getEvictionsFifteenMinuteRate(),"")));
        builder.endObject();

        if (fields != null) {
            builder.startObject(Fields.FIELDS);
            final boolean[] states = fields.allocated;
            final Object[] keys = fields.keys;
            final long[] values = fields.values;
            for (int i = 0; i < states.length; i++) {
                if (states[i]) {
                    builder.startObject((String) keys[i], XContentBuilder.FieldCaseConversion.NONE);
                    builder.byteSizeField(Fields.MEMORY_SIZE_IN_BYTES, Fields.MEMORY_SIZE, values[i]);
                    builder.endObject();
                }
            }
            builder.endObject();
        }
        builder.endObject();
        return builder;
    }

    static final class Fields {
        static final XContentBuilderString FIELDDATA = new XContentBuilderString("fielddata");
        static final XContentBuilderString MEMORY_SIZE = new XContentBuilderString("memory_size");
        static final XContentBuilderString MEMORY_SIZE_IN_BYTES = new XContentBuilderString("memory_size_in_bytes");
        static final XContentBuilderString EVICTIONS = new XContentBuilderString("evictions");
        static final XContentBuilderString FIELDS = new XContentBuilderString("fields");
        static final XContentBuilderString RATES = new XContentBuilderString("evictions_per_sec");
        static final XContentBuilderString ONE_MIN = new XContentBuilderString("1m");
        static final XContentBuilderString FIVE_MIN = new XContentBuilderString("5m");
        static final XContentBuilderString FIFTEEN_MIN = new XContentBuilderString("15m");
    }
}
