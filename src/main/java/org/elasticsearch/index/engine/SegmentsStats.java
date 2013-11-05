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

package org.elasticsearch.index.engine;

import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.common.io.stream.Streamable;
import org.elasticsearch.common.xcontent.ToXContent;
import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.common.xcontent.XContentBuilderString;

import java.io.IOException;

/**
 *
 */
public class SegmentsStats implements Streamable, ToXContent {

    private long count;

    public SegmentsStats() {

    }

    public void add(long count) {
        this.count += count;
    }

    public void add(SegmentsStats mergeStats) {
        if (mergeStats == null) {
            return;
        }
        this.count += mergeStats.count;
    }

    /**
     * The the segments count.
     */
    public long getCount() {
        return this.count;
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
        builder.endObject();
        return builder;
    }

    static final class Fields {
        static final XContentBuilderString SEGMENTS = new XContentBuilderString("segments");
        static final XContentBuilderString COUNT = new XContentBuilderString("count");
    }

    @Override
    public void readFrom(StreamInput in) throws IOException {
        count = in.readVLong();
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        out.writeVLong(count);
    }
}