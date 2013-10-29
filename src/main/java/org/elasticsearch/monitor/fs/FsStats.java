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

package org.elasticsearch.monitor.fs;

import com.google.common.collect.Iterators;
import org.elasticsearch.common.Nullable;
import org.elasticsearch.common.Strings;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.common.io.stream.Streamable;
import org.elasticsearch.common.unit.ByteSizeValue;
import org.elasticsearch.common.xcontent.ToXContent;
import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.common.xcontent.XContentBuilderString;

import java.io.IOException;
import java.util.Iterator;

/**
 */
public class FsStats implements Iterable<FsStats.Info>, Streamable, ToXContent {

    public static class Info implements Streamable {

        String path;
        @Nullable
        String mount;
        @Nullable
        String dev;
        long total = -1;
        long free = -1;
        long available = -1;
        long diskReads = -1;
        long diskWrites = -1;
        long diskReadBytes = -1;
        long diskWriteBytes = -1;
        double diskQueue = -1;
        double diskServiceTime = -1;

        @Override
        public void readFrom(StreamInput in) throws IOException {
            path = in.readString();
            mount = in.readOptionalString();
            dev = in.readOptionalString();
            total = in.readLong();
            free = in.readLong();
            available = in.readLong();
            diskReads = in.readLong();
            diskWrites = in.readLong();
            diskReadBytes = in.readLong();
            diskWriteBytes = in.readLong();
            diskQueue = in.readDouble();
            diskServiceTime = in.readDouble();
        }

        @Override
        public void writeTo(StreamOutput out) throws IOException {
            out.writeString(path);
            out.writeOptionalString(mount);
            out.writeOptionalString(dev);
            out.writeLong(total);
            out.writeLong(free);
            out.writeLong(available);
            out.writeLong(diskReads);
            out.writeLong(diskWrites);
            out.writeLong(diskReadBytes);
            out.writeLong(diskWriteBytes);
            out.writeDouble(diskQueue);
            out.writeDouble(diskServiceTime);
        }

        public String getPath() {
            return path;
        }

        public String getMount() {
            return mount;
        }

        public String getDev() {
            return dev;
        }

        public ByteSizeValue getTotal() {
            return new ByteSizeValue(total);
        }

        public ByteSizeValue getFree() {
            return new ByteSizeValue(free);
        }

        public ByteSizeValue getAvailable() {
            return new ByteSizeValue(available);
        }

        public long getDiskReads() {
            return this.diskReads;
        }

        public long getDiskWrites() {
            return this.diskWrites;
        }

        public long getDiskReadSizeInBytes() {
            return diskReadBytes;
        }

        public ByteSizeValue getDiskReadSizeSize() {
            return new ByteSizeValue(diskReadBytes);
        }

        public long getDiskWriteSizeInBytes() {
            return diskWriteBytes;
        }

        public ByteSizeValue getDiskWriteSizeSize() {
            return new ByteSizeValue(diskWriteBytes);
        }

        public double getDiskQueue() {
            return diskQueue;
        }

        public double getDiskServiceTime() {
            return diskServiceTime;
        }

    }

    long timestamp;
    Info[] infos;

    FsStats() {

    }

    FsStats(long timestamp, Info[] infos) {
        this.timestamp = timestamp;
        this.infos = infos;
    }

    public long getTimestamp() {
        return timestamp;
    }

    @Override
    public Iterator<Info> iterator() {
        return Iterators.forArray(infos);
    }

    public static FsStats readFsStats(StreamInput in) throws IOException {
        FsStats stats = new FsStats();
        stats.readFrom(in);
        return stats;
    }

    @Override
    public void readFrom(StreamInput in) throws IOException {
        timestamp = in.readVLong();
        infos = new Info[in.readVInt()];
        for (int i = 0; i < infos.length; i++) {
            infos[i] = new Info();
            infos[i].readFrom(in);
        }
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        out.writeVLong(timestamp);
        out.writeVInt(infos.length);
        for (Info info : infos) {
            info.writeTo(out);
        }
    }

    static final class Fields {
        static final XContentBuilderString FS = new XContentBuilderString("fs");
        static final XContentBuilderString TIMESTAMP = new XContentBuilderString("timestamp");
        static final XContentBuilderString DATA = new XContentBuilderString("data");
        static final XContentBuilderString PATH = new XContentBuilderString("path");
        static final XContentBuilderString MOUNT = new XContentBuilderString("mount");
        static final XContentBuilderString DEV = new XContentBuilderString("dev");
        static final XContentBuilderString TOTAL = new XContentBuilderString("total");
        static final XContentBuilderString TOTAL_IN_BYTES = new XContentBuilderString("total_in_bytes");
        static final XContentBuilderString FREE = new XContentBuilderString("free");
        static final XContentBuilderString FREE_IN_BYTES = new XContentBuilderString("free_in_bytes");
        static final XContentBuilderString AVAILABLE = new XContentBuilderString("available");
        static final XContentBuilderString AVAILABLE_IN_BYTES = new XContentBuilderString("available_in_bytes");
        static final XContentBuilderString DISK_READS = new XContentBuilderString("disk_reads");
        static final XContentBuilderString DISK_WRITES = new XContentBuilderString("disk_writes");
        static final XContentBuilderString DISK_READ_SIZE = new XContentBuilderString("disk_read_size");
        static final XContentBuilderString DISK_READ_SIZE_IN_BYTES = new XContentBuilderString("disk_read_size_in_bytes");
        static final XContentBuilderString DISK_WRITE_SIZE = new XContentBuilderString("disk_write_size");
        static final XContentBuilderString DISK_WRITE_SIZE_IN_BYTES = new XContentBuilderString("disk_write_size_in_bytes");
        static final XContentBuilderString DISK_QUEUE = new XContentBuilderString("disk_queue");
        static final XContentBuilderString DISK_SERVICE_TIME = new XContentBuilderString("disk_service_time");
    }

    @Override
    public XContentBuilder toXContent(XContentBuilder builder, Params params) throws IOException {
        builder.startObject(Fields.FS);
        builder.field(Fields.TIMESTAMP, timestamp);
        builder.startArray(Fields.DATA);
        for (Info info : infos) {
            builder.startObject();
            builder.field(Fields.PATH, info.path, XContentBuilder.FieldCaseConversion.NONE);
            if (info.mount != null) {
                builder.field(Fields.MOUNT, info.mount, XContentBuilder.FieldCaseConversion.NONE);
            }
            if (info.dev != null) {
                builder.field(Fields.DEV, info.dev, XContentBuilder.FieldCaseConversion.NONE);
            }

            if (info.total != -1) {
                builder.byteSizeField(Fields.TOTAL_IN_BYTES, Fields.TOTAL, info.total);
            }
            if (info.free != -1) {
                builder.byteSizeField(Fields.FREE_IN_BYTES, Fields.FREE, info.free);
            }
            if (info.available != -1) {
                builder.byteSizeField(Fields.AVAILABLE_IN_BYTES, Fields.AVAILABLE, info.available);
            }

            if (info.diskReads != -1) {
                builder.field(Fields.DISK_READS, info.diskReads);
            }
            if (info.diskWrites != -1) {
                builder.field(Fields.DISK_WRITES, info.diskWrites);
            }

            if (info.diskReadBytes != -1) {
                builder.byteSizeField(Fields.DISK_READ_SIZE_IN_BYTES, Fields.DISK_READ_SIZE, info.getDiskReadSizeInBytes());
            }
            if (info.diskWriteBytes != -1) {
                builder.byteSizeField(Fields.DISK_WRITE_SIZE_IN_BYTES, Fields.DISK_WRITE_SIZE, info.getDiskWriteSizeInBytes());
            }

            if (info.diskQueue != -1) {
                builder.field(Fields.DISK_QUEUE, Strings.format1Decimals(info.diskQueue, ""));
            }
            if (info.diskServiceTime != -1) {
                builder.field(Fields.DISK_SERVICE_TIME, Strings.format1Decimals(info.diskServiceTime, ""));
            }

            builder.endObject();
        }
        builder.endArray();
        builder.endObject();
        return builder;
    }
}
