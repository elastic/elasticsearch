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
import org.elasticsearch.common.Nullable;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.common.io.stream.Streamable;
import org.elasticsearch.common.unit.ByteSizeValue;
import org.elasticsearch.common.xcontent.ToXContent;
import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.common.xcontent.XContentBuilderString;

import java.io.IOException;

/**
 */
public class FieldDataStats implements Streamable, ToXContent {

    long memorySize;
    long evictions;
    @Nullable
    ObjectLongOpenHashMap<String> fields;

    public FieldDataStats() {

    }

    public FieldDataStats(long memorySize, long evictions, @Nullable ObjectLongOpenHashMap<String> fields) {
        this.memorySize = memorySize;
        this.evictions = evictions;
        this.fields = fields;
    }

    public void add(FieldDataStats stats) {
        this.memorySize += stats.memorySize;
        this.evictions += stats.evictions;
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
    }
}
