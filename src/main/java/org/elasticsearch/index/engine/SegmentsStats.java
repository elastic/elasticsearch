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

package org.elasticsearch.index.engine;

import org.elasticsearch.Version;
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
public class SegmentsStats implements Streamable, ToXContent {

    private long count;
    private long memoryInBytes;
    private long indexWriterMemoryInBytes;
    private long versionMapMemoryInBytes;

    public SegmentsStats() {

    }

    public void add(long count, long memoryInBytes) {
        this.count += count;
        this.memoryInBytes += memoryInBytes;
    }

    public void addIndexWriterMemoryInBytes(long indexWriterMemoryInBytes) {
        this.indexWriterMemoryInBytes += indexWriterMemoryInBytes;
    }

    public void addVersionMapMemoryInBytes(long versionMapMemoryInBytes) {
        this.versionMapMemoryInBytes += versionMapMemoryInBytes;
    }

    public void add(SegmentsStats mergeStats) {
        if (mergeStats == null) {
            return;
        }
        add(mergeStats.count, mergeStats.memoryInBytes);
        addIndexWriterMemoryInBytes(mergeStats.indexWriterMemoryInBytes);
        addVersionMapMemoryInBytes(mergeStats.versionMapMemoryInBytes);
    }

    /**
     * The the segments count.
     */
    public long getCount() {
        return this.count;
    }

    /**
     * Estimation of the memory usage used by a segment.
     */
    public long getMemoryInBytes() {
        return this.memoryInBytes;
    }

    public ByteSizeValue getMemory() {
        return new ByteSizeValue(memoryInBytes);
    }

    /**
     * Estimation of the memory usage by index writer
     */
    public long getIndexWriterMemoryInBytes() {
        return this.indexWriterMemoryInBytes;
    }

    public ByteSizeValue getIndexWriterMemory() {
        return new ByteSizeValue(indexWriterMemoryInBytes);
    }

    /**
     * Estimation of the memory usage by version map
     */
    public long getVersionMapMemoryInBytes() {
        return this.versionMapMemoryInBytes;
    }

    public ByteSizeValue getVersionMapMemory() {
        return new ByteSizeValue(versionMapMemoryInBytes);
    }

    public static SegmentsStats readSegmentsStats(StreamInput in) throws IOException {
        SegmentsStats stats = new SegmentsStats();
        stats.readFrom(in);
        return stats;
    }

    @Override
    public XContentBuilder toXContent(XContentBuilder builder, Params params) throws IOException {
        builder.startObject(Fields.SEGMENTS);
        builder.field(Fields.COUNT, count);
        builder.byteSizeField(Fields.MEMORY_IN_BYTES, Fields.MEMORY, memoryInBytes);
        builder.byteSizeField(Fields.INDEX_WRITER_MEMORY_IN_BYTES, Fields.INDEX_WRITER_MEMORY, indexWriterMemoryInBytes);
        builder.byteSizeField(Fields.VERSION_MAP_MEMORY_IN_BYTES, Fields.VERSION_MAP_MEMORY, versionMapMemoryInBytes);
        builder.endObject();
        return builder;
    }

    static final class Fields {
        static final XContentBuilderString SEGMENTS = new XContentBuilderString("segments");
        static final XContentBuilderString COUNT = new XContentBuilderString("count");
        static final XContentBuilderString MEMORY = new XContentBuilderString("memory");
        static final XContentBuilderString MEMORY_IN_BYTES = new XContentBuilderString("memory_in_bytes");
        static final XContentBuilderString INDEX_WRITER_MEMORY = new XContentBuilderString("index_writer_memory");
        static final XContentBuilderString INDEX_WRITER_MEMORY_IN_BYTES = new XContentBuilderString("index_writer_memory_in_bytes");
        static final XContentBuilderString VERSION_MAP_MEMORY = new XContentBuilderString("version_map_memory");
        static final XContentBuilderString VERSION_MAP_MEMORY_IN_BYTES = new XContentBuilderString("version_map_memory_in_bytes");
    }

    @Override
    public void readFrom(StreamInput in) throws IOException {
        count = in.readVLong();
        memoryInBytes = in.readLong();
        if (in.getVersion().onOrAfter(Version.V_1_3_0)) {
            indexWriterMemoryInBytes = in.readLong();
            versionMapMemoryInBytes = in.readLong();
        }
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        out.writeVLong(count);
        out.writeLong(memoryInBytes);
        if (out.getVersion().onOrAfter(Version.V_1_3_0)) {
            out.writeLong(indexWriterMemoryInBytes);
            out.writeLong(versionMapMemoryInBytes);
        }
    }
}