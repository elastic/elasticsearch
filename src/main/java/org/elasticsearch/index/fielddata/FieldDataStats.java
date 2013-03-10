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

package org.elasticsearch.index.fielddata;

import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.common.io.stream.Streamable;
import org.elasticsearch.common.metrics.CounterMetric;
import org.elasticsearch.common.unit.ByteSizeValue;
import org.elasticsearch.common.xcontent.ToXContent;
import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.common.xcontent.XContentBuilderString;

import java.io.IOException;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.ConcurrentMap;

/**
 */
public class FieldDataStats implements Streamable, ToXContent {

    long memorySize;
    long evictions;
	Map<String,Long> fieldDataSizes;

    public FieldDataStats() {
        this.fieldDataSizes = new HashMap<String, Long>();
    }

    public FieldDataStats(long memorySize, ConcurrentMap<String, CounterMetric> fieldDataSizes, long evictions) {
        this.memorySize = memorySize;
		this.evictions = evictions;
        this.fieldDataSizes = new HashMap<String, Long>(fieldDataSizes.size());
        for (Map.Entry<String, CounterMetric> entry : fieldDataSizes.entrySet()) {
            this.fieldDataSizes.put(entry.getKey(), entry.getValue().count());
        }
    }

    public void add(FieldDataStats stats) {
        this.memorySize += stats.memorySize;
		this.evictions += stats.evictions;
        for (Map.Entry<String, Long> entry : stats.fieldDataSizes.entrySet()) {
            Long val = fieldDataSizes.get(entry.getKey());
            if (val == null) {
                val = (long)0;
            }
            fieldDataSizes.put(entry.getKey(), val + entry.getValue());
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

    public static FieldDataStats readFieldDataStats(StreamInput in) throws IOException {
        FieldDataStats stats = new FieldDataStats();
        stats.readFrom(in);
        return stats;
    }

    @Override
    public void readFrom(StreamInput in) throws IOException {
        memorySize = in.readVLong();
		evictions = in.readVLong();
        int fields = in.readVInt();
        fieldDataSizes = new HashMap<String, Long>(fields);
        if (fields > 0) {
            for (int i = 0; i < fields; i++) {
                fieldDataSizes.put(in.readString(), in.readVLong());
            }
        }
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        out.writeVLong(memorySize);
		out.writeVLong(evictions);
        if (fieldDataSizes == null) {
            out.writeVInt(0);
            return;
        }

        out.writeVInt(fieldDataSizes.size());
        for (Map.Entry<String, Long> entry : fieldDataSizes.entrySet()) {
            out.writeString(entry.getKey());
            out.writeVLong(entry.getValue());
        }
    }

    @Override
    public XContentBuilder toXContent(XContentBuilder builder, Params params) throws IOException {
        builder.startObject(Fields.FIELDDATA);
        builder.field(Fields.MEMORY_SIZE, getMemorySize().toString());
        builder.field(Fields.MEMORY_SIZE_IN_BYTES, memorySize);
        builder.field(Fields.EVICTIONS, getEvictions());
        builder.endObject();
        return builder;
    }

    public Map<String, Long> getFieldDataSizes() {
        return fieldDataSizes;
    }

    static final class Fields {
        static final XContentBuilderString FIELDDATA = new XContentBuilderString("fielddata");
        static final XContentBuilderString MEMORY_SIZE = new XContentBuilderString("memory_size");
        static final XContentBuilderString MEMORY_SIZE_IN_BYTES = new XContentBuilderString("memory_size_in_bytes");
        static final XContentBuilderString EVICTIONS = new XContentBuilderString("evictions");
    }
}
