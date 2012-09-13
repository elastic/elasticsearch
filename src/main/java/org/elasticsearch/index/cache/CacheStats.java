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

package org.elasticsearch.index.cache;

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
public class CacheStats implements Streamable, ToXContent {

    long fieldEvictions;
    long filterEvictions;
    long filterCount;
    long fieldSize;
    long filterSize;
    long bloomSize;
    long idCacheSize;

    public CacheStats() {
    }

    public CacheStats(long fieldEvictions, long filterEvictions, long fieldSize, long filterSize, long filterCount, long bloomSize, long idCacheSize) {
        this.fieldEvictions = fieldEvictions;
        this.filterEvictions = filterEvictions;
        this.fieldSize = fieldSize;
        this.filterSize = filterSize;
        this.filterCount = filterCount;
        this.bloomSize = bloomSize;
        this.idCacheSize = idCacheSize;
    }

    public void add(CacheStats stats) {
        this.fieldEvictions += stats.fieldEvictions;
        this.filterEvictions += stats.filterEvictions;
        this.fieldSize += stats.fieldSize;
        this.filterSize += stats.filterSize;
        this.filterCount += stats.filterCount;
        this.bloomSize += stats.bloomSize;
        this.idCacheSize += stats.idCacheSize;
    }

    public long fieldEvictions() {
        return this.fieldEvictions;
    }

    public long getFieldEvictions() {
        return this.fieldEvictions();
    }

    public long filterEvictions() {
        return this.filterEvictions;
    }

    public long getFilterEvictions() {
        return this.filterEvictions;
    }

    public long filterMemEvictions() {
        return this.filterEvictions;
    }

    public long getFilterMemEvictions() {
        return this.filterEvictions;
    }

    public long filterCount() {
        return this.filterCount;
    }

    public long getFilterCount() {
        return filterCount;
    }

    public long fieldSizeInBytes() {
        return this.fieldSize;
    }

    public long getFieldSizeInBytes() {
        return fieldSizeInBytes();
    }

    public ByteSizeValue fieldSize() {
        return new ByteSizeValue(fieldSize);
    }

    public ByteSizeValue getFieldSize() {
        return this.fieldSize();
    }

    public long filterSizeInBytes() {
        return this.filterSize;
    }

    public long getFilterSizeInBytes() {
        return this.filterSizeInBytes();
    }

    public ByteSizeValue filterSize() {
        return new ByteSizeValue(filterSize);
    }

    public ByteSizeValue getFilterSize() {
        return filterSize();
    }

    public long bloomSizeInBytes() {
        return this.bloomSize;
    }

    public long getBloomSizeInBytes() {
        return this.bloomSize;
    }

    public ByteSizeValue bloomSize() {
        return new ByteSizeValue(bloomSize);
    }

    public ByteSizeValue getBloomSize() {
        return bloomSize();
    }

    public long idCacheSizeInBytes() {
        return idCacheSize;
    }

    public long getIdCacheSizeInBytes() {
        return idCacheSizeInBytes();
    }

    public ByteSizeValue idCacheSize() {
        return new ByteSizeValue(idCacheSize);
    }

    public ByteSizeValue getIdCacheSize() {
        return bloomSize();
    }

    @Override
    public XContentBuilder toXContent(XContentBuilder builder, Params params) throws IOException {
        builder.startObject(Fields.CACHE);
        builder.field(Fields.FIELD_EVICTIONS, fieldEvictions);
        builder.field(Fields.FIELD_SIZE, fieldSize().toString());
        builder.field(Fields.FIELD_SIZE_IN_BYTES, fieldSize);
        builder.field(Fields.FILTER_COUNT, filterCount);
        builder.field(Fields.FILTER_EVICTIONS, filterEvictions);
        builder.field(Fields.FILTER_SIZE, filterSize().toString());
        builder.field(Fields.FILTER_SIZE_IN_BYTES, filterSize);
        builder.field(Fields.BLOOM_SIZE, bloomSize().toString());
        builder.field(Fields.BLOOM_SIZE_IN_BYTES, bloomSize);
        builder.field(Fields.ID_CACHE_SIZE, idCacheSize().toString());
        builder.field(Fields.ID_CACHE_SIZE_IN_BYTES, idCacheSize);
        builder.endObject();
        return builder;
    }

    static final class Fields {
        static final XContentBuilderString CACHE = new XContentBuilderString("cache");
        static final XContentBuilderString FIELD_SIZE = new XContentBuilderString("field_size");
        static final XContentBuilderString FIELD_SIZE_IN_BYTES = new XContentBuilderString("field_size_in_bytes");
        static final XContentBuilderString FIELD_EVICTIONS = new XContentBuilderString("field_evictions");
        static final XContentBuilderString FILTER_EVICTIONS = new XContentBuilderString("filter_evictions");
        static final XContentBuilderString FILTER_COUNT = new XContentBuilderString("filter_count");
        static final XContentBuilderString FILTER_SIZE = new XContentBuilderString("filter_size");
        static final XContentBuilderString FILTER_SIZE_IN_BYTES = new XContentBuilderString("filter_size_in_bytes");
        static final XContentBuilderString BLOOM_SIZE = new XContentBuilderString("bloom_size");
        static final XContentBuilderString BLOOM_SIZE_IN_BYTES = new XContentBuilderString("bloom_size_in_bytes");
        static final XContentBuilderString ID_CACHE_SIZE = new XContentBuilderString("id_cache_size");
        static final XContentBuilderString ID_CACHE_SIZE_IN_BYTES = new XContentBuilderString("id_cache_size_in_bytes");
    }

    public static CacheStats readCacheStats(StreamInput in) throws IOException {
        CacheStats stats = new CacheStats();
        stats.readFrom(in);
        return stats;
    }

    @Override
    public void readFrom(StreamInput in) throws IOException {
        fieldEvictions = in.readVLong();
        filterEvictions = in.readVLong();
        fieldSize = in.readVLong();
        filterSize = in.readVLong();
        filterCount = in.readVLong();
        bloomSize = in.readVLong();
        idCacheSize = in.readVLong();
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        out.writeVLong(fieldEvictions);
        out.writeVLong(filterEvictions);
        out.writeVLong(fieldSize);
        out.writeVLong(filterSize);
        out.writeVLong(filterCount);
        out.writeVLong(bloomSize);
        out.writeVLong(idCacheSize);
    }
}