/*
 * Licensed to ElasticSearch and Shay Banon under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership. ElasticSearch licenses this
 * file to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
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

import gnu.trove.iterator.TObjectLongIterator;
import gnu.trove.map.hash.TObjectLongHashMap;
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
 *
 */
public class CompletionStats implements Streamable, ToXContent {

    private long sizeInBytes;

    @Nullable
    private TObjectLongHashMap<String> fields;

    public CompletionStats() {
    }

    public CompletionStats(long size, @Nullable TObjectLongHashMap<String> fields) {
        this.sizeInBytes = size;
        this.fields = fields;
    }

    public long getSizeInBytes() {
        return sizeInBytes;
    }

    public ByteSizeValue getSize() {
        return new ByteSizeValue(sizeInBytes);
    }

    public TObjectLongHashMap<String> getFields() {
        return fields;
    }

    @Override
    public void readFrom(StreamInput in) throws IOException {
        sizeInBytes = in.readVLong();
        if (in.readBoolean()) {
            int size = in.readVInt();
            fields = new TObjectLongHashMap<String>(size);
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
            for (TObjectLongIterator<String> it = fields.iterator(); it.hasNext(); ) {
                it.advance();
                out.writeString(it.key());
                out.writeVLong(it.value());
            }
        }
    }

    @Override
    public XContentBuilder toXContent(XContentBuilder builder, Params params) throws IOException {
        builder.startObject(Fields.COMPLETION);
        builder.byteSizeField(Fields.SIZE_IN_BYTES, Fields.SIZE, sizeInBytes);
        if (fields != null) {
            builder.startObject(Fields.FIELDS);
            for (TObjectLongIterator<String> it = fields.iterator(); it.hasNext(); ) {
                it.advance();
                builder.startObject(it.key(), XContentBuilder.FieldCaseConversion.NONE);
                builder.byteSizeField(Fields.SIZE_IN_BYTES, Fields.SIZE, it.value());
                builder.endObject();
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
        static final XContentBuilderString COMPLETION = new XContentBuilderString("completion");
        static final XContentBuilderString SIZE_IN_BYTES = new XContentBuilderString("size_in_bytes");
        static final XContentBuilderString SIZE = new XContentBuilderString("size");
        static final XContentBuilderString FIELDS = new XContentBuilderString("fields");
    }

    public void add(CompletionStats completion) {
        if (completion == null) {
            return;
        }

        sizeInBytes += completion.getSizeInBytes();

        if (completion.fields != null) {
            if (fields == null) fields = new TObjectLongHashMap<String>();
            for (TObjectLongIterator<String> it = completion.fields.iterator(); it.hasNext(); ) {
                it.advance();
                fields.adjustOrPutValue(it.key(), it.value(), it.value());
            }
        }
    }
}
