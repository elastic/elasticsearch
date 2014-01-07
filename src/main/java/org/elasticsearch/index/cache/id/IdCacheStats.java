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

package org.elasticsearch.index.cache.id;

import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.common.io.stream.Streamable;
import org.elasticsearch.common.unit.ByteSizeValue;
import org.elasticsearch.common.xcontent.ToXContent;
import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.common.xcontent.XContentBuilderString;

import java.io.IOException;

/**
 * @deprecated Id cache has been removed in favor for {@link org.elasticsearch.index.fielddata.plain.ParentChildIndexFieldData}
 * this class now reports field data memory usage for _parent field.
 */
@Deprecated
public class IdCacheStats implements Streamable, ToXContent {

    long memorySize;

    public IdCacheStats() {
    }

    public IdCacheStats(long memorySize) {
        this.memorySize = memorySize;
    }

    public void add(IdCacheStats stats) {
        this.memorySize += stats.memorySize;
    }

    public long getMemorySizeInBytes() {
        return this.memorySize;
    }

    public ByteSizeValue getMemorySize() {
        return new ByteSizeValue(memorySize);
    }

    public static IdCacheStats readIdCacheStats(StreamInput in) throws IOException {
        IdCacheStats stats = new IdCacheStats();
        stats.readFrom(in);
        return stats;
    }

    @Override
    public void readFrom(StreamInput in) throws IOException {
        memorySize = in.readVLong();
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        out.writeVLong(memorySize);
    }

    @Override
    public XContentBuilder toXContent(XContentBuilder builder, Params params) throws IOException {
        builder.startObject(Fields.ID_CACHE);
        builder.byteSizeField(Fields.MEMORY_SIZE_IN_BYTES, Fields.MEMORY_SIZE, memorySize);
        builder.endObject();
        return builder;
    }

    static final class Fields {
        static final XContentBuilderString ID_CACHE = new XContentBuilderString("id_cache");
        static final XContentBuilderString MEMORY_SIZE = new XContentBuilderString("memory_size");
        static final XContentBuilderString MEMORY_SIZE_IN_BYTES = new XContentBuilderString("memory_size_in_bytes");
    }
}
