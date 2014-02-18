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

    public SegmentsStats() {

    }

    public void add(long count, long memoryInBytes) {
        this.count += count;
        this.memoryInBytes += memoryInBytes;
    }

    public void add(SegmentsStats mergeStats) {
        if (mergeStats == null) {
            return;
        }
        add(mergeStats.count, mergeStats.memoryInBytes);
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
        builder.endObject();
        return builder;
    }

    static final class Fields {
        static final XContentBuilderString SEGMENTS = new XContentBuilderString("segments");
        static final XContentBuilderString COUNT = new XContentBuilderString("count");
        static final XContentBuilderString MEMORY = new XContentBuilderString("memory");
        static final XContentBuilderString MEMORY_IN_BYTES = new XContentBuilderString("memory_in_bytes");
    }

    @Override
    public void readFrom(StreamInput in) throws IOException {
        count = in.readVLong();
        memoryInBytes = in.readLong();
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        out.writeVLong(count);
        out.writeLong(memoryInBytes);
    }
}