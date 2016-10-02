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
package org.elasticsearch.search.suggest.completion;

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
 *
 */
public class CompletionStats implements Streamable, ToXContent {

    private long sizeInBytes;

    @Nullable
    private ObjectLongHashMap<String> fields;

    public CompletionStats() {
    }

    public CompletionStats(long size, @Nullable ObjectLongHashMap<String> fields) {
        this.sizeInBytes = size;
        this.fields = fields;
    }

    public long getSizeInBytes() {
        return sizeInBytes;
    }

    public ByteSizeValue getSize() {
        return new ByteSizeValue(sizeInBytes);
    }

    public ObjectLongHashMap<String> getFields() {
        return fields;
    }

    @Override
    public void readFrom(StreamInput in) throws IOException {
        sizeInBytes = in.readVLong();
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
        out.writeVLong(sizeInBytes);
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
        builder.startObject(Fields.COMPLETION);
        builder.byteSizeField(Fields.SIZE_IN_BYTES, Fields.SIZE, sizeInBytes);
        if (fields != null) {
            builder.startObject(Fields.FIELDS);

            assert !fields.containsKey(null);
            final Object[] keys = fields.keys;
            final long[] values = fields.values;
            for (int i = 0; i < keys.length; i++) {
                if (keys[i] != null) {
                    builder.startObject((String) keys[i]);
                    builder.byteSizeField(Fields.SIZE_IN_BYTES, Fields.SIZE, values[i]);
                    builder.endObject();
                }
            }
            builder.endObject();
        }
        builder.endObject();
        return builder;
    }

    public static CompletionStats readCompletionStats(StreamInput in) throws IOException {
        CompletionStats stats = new CompletionStats();
        stats.readFrom(in);
        return stats;
    }

    static final class Fields {
        static final String COMPLETION = "completion";
        static final String SIZE_IN_BYTES = "size_in_bytes";
        static final String SIZE = "size";
        static final String FIELDS = "fields";
    }

    public void add(CompletionStats completion) {
        if (completion == null) {
            return;
        }

        sizeInBytes += completion.getSizeInBytes();

        if (completion.fields != null) {
            if (fields == null) {
                fields = completion.fields.clone();
            } else {
                assert !completion.fields.containsKey(null);
                final Object[] keys = completion.fields.keys;
                final long[] values = completion.fields.values;
                for (int i = 0; i < keys.length; i++) {
                    if (keys[i] != null) {
                        fields.addTo((String) keys[i], values[i]);
                    }
                }
            }
        }
    }
}
