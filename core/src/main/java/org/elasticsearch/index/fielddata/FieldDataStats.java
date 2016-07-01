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

import com.carrotsearch.hppc.ObjectLongHashMap;
import org.elasticsearch.common.Nullable;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.common.io.stream.Streamable;
import org.elasticsearch.common.unit.ByteSizeValue;
import org.elasticsearch.common.xcontent.ToXContent;
import org.elasticsearch.common.xcontent.XContentBuilder;

import java.io.IOException;

/**
 */
public class FieldDataStats implements Streamable, ToXContent {

    long memorySize;
    long evictions;
    @Nullable
    ObjectLongHashMap<String> fields;

    public FieldDataStats() {

    }

    public FieldDataStats(long memorySize, long evictions, @Nullable ObjectLongHashMap<String> fields) {
        this.memorySize = memorySize;
        this.evictions = evictions;
        this.fields = fields;
    }

    public void add(FieldDataStats stats) {
        this.memorySize += stats.memorySize;
        this.evictions += stats.evictions;
        if (stats.fields != null) {
            if (fields == null) {
                fields = stats.fields.clone();
            } else {
                assert !stats.fields.containsKey(null);
                final Object[] keys = stats.fields.keys;
                final long[] values = stats.fields.values;
                for (int i = 0; i < keys.length; i++) {
                    if (keys[i] != null) {
                        fields.addTo((String) keys[i], values[i]);
                    }
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
    public ObjectLongHashMap<String> getFields() {
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
            fields = new ObjectLongHashMap<>(size);
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
            assert !fields.containsKey(null);
            final Object[] keys = fields.keys;
            final long[] values = fields.values;
            for (int i = 0; i < keys.length; i++) {
                if (keys[i] != null) {
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
            assert !fields.containsKey(null);
            final Object[] keys = fields.keys;
            final long[] values = fields.values;
            for (int i = 0; i < keys.length; i++) {
                if (keys[i] != null) {
                    builder.startObject((String) keys[i]);
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
        static final String FIELDDATA = "fielddata";
        static final String MEMORY_SIZE = "memory_size";
        static final String MEMORY_SIZE_IN_BYTES = "memory_size_in_bytes";
        static final String EVICTIONS = "evictions";
        static final String FIELDS = "fields";
    }
}
