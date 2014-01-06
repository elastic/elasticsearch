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

package org.elasticsearch.index.store;

import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.common.io.stream.Streamable;
import org.elasticsearch.common.unit.ByteSizeValue;
import org.elasticsearch.common.unit.TimeValue;
import org.elasticsearch.common.xcontent.ToXContent;
import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.common.xcontent.XContentBuilderString;

import java.io.IOException;

/**
 */
public class StoreStats implements Streamable, ToXContent {

    private long sizeInBytes;

    private long throttleTimeInNanos;

    public StoreStats() {

    }

    public StoreStats(long sizeInBytes, long throttleTimeInNanos) {
        this.sizeInBytes = sizeInBytes;
        this.throttleTimeInNanos = throttleTimeInNanos;
    }

    public void add(StoreStats stats) {
        if (stats == null) {
            return;
        }
        sizeInBytes += stats.sizeInBytes;
        throttleTimeInNanos += stats.throttleTimeInNanos;
    }


    public long sizeInBytes() {
        return sizeInBytes;
    }

    public long getSizeInBytes() {
        return sizeInBytes;
    }

    public ByteSizeValue size() {
        return new ByteSizeValue(sizeInBytes);
    }

    public ByteSizeValue getSize() {
        return size();
    }

    public TimeValue throttleTime() {
        return TimeValue.timeValueNanos(throttleTimeInNanos);
    }

    public TimeValue getThrottleTime() {
        return throttleTime();
    }

    public static StoreStats readStoreStats(StreamInput in) throws IOException {
        StoreStats store = new StoreStats();
        store.readFrom(in);
        return store;
    }

    @Override
    public void readFrom(StreamInput in) throws IOException {
        sizeInBytes = in.readVLong();
        throttleTimeInNanos = in.readVLong();
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        out.writeVLong(sizeInBytes);
        out.writeVLong(throttleTimeInNanos);
    }

    @Override
    public XContentBuilder toXContent(XContentBuilder builder, Params params) throws IOException {
        builder.startObject(Fields.STORE);
        builder.byteSizeField(Fields.SIZE_IN_BYTES, Fields.SIZE, sizeInBytes);
        builder.timeValueField(Fields.THROTTLE_TIME_IN_MILLIS, Fields.THROTTLE_TIME, throttleTime());
        builder.endObject();
        return builder;
    }

    static final class Fields {
        static final XContentBuilderString STORE = new XContentBuilderString("store");
        static final XContentBuilderString SIZE = new XContentBuilderString("size");
        static final XContentBuilderString SIZE_IN_BYTES = new XContentBuilderString("size_in_bytes");

        static final XContentBuilderString THROTTLE_TIME = new XContentBuilderString("throttle_time");
        static final XContentBuilderString THROTTLE_TIME_IN_MILLIS = new XContentBuilderString("throttle_time_in_millis");
    }
}
