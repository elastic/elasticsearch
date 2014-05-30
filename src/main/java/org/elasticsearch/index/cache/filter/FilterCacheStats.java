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

package org.elasticsearch.index.cache.filter;

import org.elasticsearch.Version;
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
public class FilterCacheStats implements Streamable, ToXContent {

    long memorySize;
    private long evictions;
    private double evictionsOneMinuteRate;
    private double evictionsFiveMinuteRate;
    private double evictionsFifteenMinuteRate;

    public FilterCacheStats() {
    }

    public FilterCacheStats(long memorySize, MeterMetric evictionMeter) {
        this.memorySize = memorySize;
        this.evictions = evictionMeter.count();
        this.evictionsOneMinuteRate = evictionMeter.oneMinuteRate();
        this.evictionsFiveMinuteRate = evictionMeter.fiveMinuteRate();
        this.evictionsFifteenMinuteRate = evictionMeter.fifteenMinuteRate();
    }

    public void add(FilterCacheStats stats) {
        this.memorySize += stats.memorySize;
        this.evictions += stats.evictions;
        this.evictionsOneMinuteRate += stats.evictionsOneMinuteRate;
        this.evictionsFiveMinuteRate += stats.evictionsFiveMinuteRate;
        this.evictionsFifteenMinuteRate += stats.evictionsFifteenMinuteRate;
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

    public static FilterCacheStats readFilterCacheStats(StreamInput in) throws IOException {
        FilterCacheStats stats = new FilterCacheStats();
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

    }

    @Override
    public XContentBuilder toXContent(XContentBuilder builder, ToXContent.Params params) throws IOException {
        builder.startObject(Fields.FILTER_CACHE);
        builder.byteSizeField(Fields.MEMORY_SIZE_IN_BYTES, Fields.MEMORY_SIZE, memorySize);
        builder.field(Fields.EVICTIONS, getEvictions());

        builder.startObject(Fields.RATES);
        builder.field(Fields.ONE_MIN, Double.valueOf(Strings.format1Decimals(getEvictionsOneMinuteRate(), "")));
        builder.field(Fields.FIVE_MIN, Double.valueOf(Strings.format1Decimals(getEvictionsFiveMinuteRate(), "")));
        builder.field(Fields.FIFTEEN_MIN, Double.valueOf(Strings.format1Decimals(getEvictionsFifteenMinuteRate(), "")));
        builder.endObject();

        builder.endObject();
        return builder;
    }

    static final class Fields {
        static final XContentBuilderString FILTER_CACHE = new XContentBuilderString("filter_cache");
        static final XContentBuilderString MEMORY_SIZE = new XContentBuilderString("memory_size");
        static final XContentBuilderString MEMORY_SIZE_IN_BYTES = new XContentBuilderString("memory_size_in_bytes");
        static final XContentBuilderString EVICTIONS = new XContentBuilderString("evictions");
        static final XContentBuilderString RATES = new XContentBuilderString("evictions_per_sec");
        static final XContentBuilderString ONE_MIN = new XContentBuilderString("1m");
        static final XContentBuilderString FIVE_MIN = new XContentBuilderString("5m");
        static final XContentBuilderString FIFTEEN_MIN = new XContentBuilderString("15m");
    }
}
