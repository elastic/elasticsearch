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

package org.elasticsearch.index.bulk.stats;

import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.common.io.stream.Writeable;
import org.elasticsearch.common.unit.TimeValue;
import org.elasticsearch.common.xcontent.ToXContent;
import org.elasticsearch.common.xcontent.ToXContentFragment;
import org.elasticsearch.common.xcontent.XContentBuilder;

import java.io.IOException;

public class BulkStats implements Writeable, ToXContentFragment {

    private long total = 0;
    private long totalTimeInMillis = 0;
    private long totalSizeInBytes = 0;

    public BulkStats() {

    }

    public BulkStats(StreamInput in) throws IOException {
        total = in.readVLong();
        totalTimeInMillis = in.readVLong();
        totalSizeInBytes = in.readVLong();
    }

    public BulkStats(long total, long totalTimeInMillis, long totalSizeInBytes) {
        this.total = total;
        this.totalTimeInMillis = totalTimeInMillis;
        this.totalSizeInBytes = totalSizeInBytes;
    }

    public void add(BulkStats bulkStats) {
        addTotals(bulkStats);
    }

    public void addTotals(BulkStats bulkStats) {
        if (bulkStats == null) {
            return;
        }
        this.total += bulkStats.total;
        this.totalTimeInMillis += bulkStats.totalTimeInMillis;
        this.totalSizeInBytes += bulkStats.totalSizeInBytes;
    }

    public long getTotalSizeInBytes() {
        return totalSizeInBytes;
    }

    public long getTotal() {
        return total;
    }

    public TimeValue getTotalTime() {
        return new TimeValue(totalTimeInMillis);
    }

    public long getTotalTimeInMillis() {
        return totalTimeInMillis;
    }

    @Override public void writeTo(StreamOutput out) throws IOException {
        out.writeVLong(total);
        out.writeVLong(totalTimeInMillis);
        out.writeVLong(totalSizeInBytes);
    }

    @Override public XContentBuilder toXContent(XContentBuilder builder, ToXContent.Params params) throws IOException {
        builder.startObject(Fields.BULK);
        builder.field(Fields.TOTAL, total);
        builder.humanReadableField(Fields.TOTAL_TIME_IN_MILLIS, Fields.TOTAL_TIME, getTotalTime());
        builder.field(Fields.TOTAL_SIZE_IN_BYTES, totalSizeInBytes);
        builder.endObject();
        return builder;
    }

    static final class Fields {
        static final String BULK = "bulk";
        static final String TOTAL = "total";
        static final String TOTAL_TIME = "total_time";
        static final String TOTAL_TIME_IN_MILLIS = "total_time_in_millis";
        static final String TOTAL_SIZE_IN_BYTES = "total_size_in_bytes";
    }
}

