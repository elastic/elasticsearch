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
            path = in.readUTF();
            mount = in.readOptionalUTF();
            dev = in.readOptionalUTF();
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
            out.writeUTF(path);
            out.writeOptionalUTF(mount);
            out.writeOptionalUTF(dev);
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

        public ByteSizeValue total() {
            return new ByteSizeValue(total);
        }

        public ByteSizeValue getTotal() {
            return total();
        }

        public ByteSizeValue free() {
            return new ByteSizeValue(free);
        }

        public ByteSizeValue getFree() {
            return free();
        }

        public ByteSizeValue available() {
            return new ByteSizeValue(available);
        }

        public ByteSizeValue getAvailable() {
            return available();
        }

        public long diskReads() {
            return this.diskReads;
        }

        public long getDiskReads() {
            return this.diskReads;
        }

        public long diskWrites() {
            return this.diskWrites;
        }

        public long getDiskWrites() {
            return this.diskWrites;
        }

        public long diskReadSizeInBytes() {
            return diskReadBytes;
        }

        public long getDiskReadSizeInBytes() {
            return diskReadBytes;
        }

        public ByteSizeValue diskReadSizeSize() {
            return new ByteSizeValue(diskReadBytes);
        }

        public ByteSizeValue getDiskReadSizeSize() {
            return new ByteSizeValue(diskReadBytes);
        }

        public long diskWriteSizeInBytes() {
            return diskWriteBytes;
        }

        public long getDiskWriteSizeInBytes() {
            return diskWriteBytes;
        }

        public ByteSizeValue diskWriteSizeSize() {
            return new ByteSizeValue(diskWriteBytes);
        }

        public ByteSizeValue getDiskWriteSizeSize() {
            return new ByteSizeValue(diskWriteBytes);
        }

        public double diskQueue() {
            return diskQueue;
        }

        public double getDiskQueue() {
            return diskQueue;
        }

        public double diskServiceTime() {
            return diskServiceTime;
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

    public long timestamp() {
        return timestamp;
    }

    public long getTimestamp() {
        return timestamp();
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

    @Override
    public XContentBuilder toXContent(XContentBuilder builder, Params params) throws IOException {
        builder.startObject("fs");
        builder.field("timestamp", timestamp);
        builder.startArray("data");
        for (Info info : infos) {
            builder.startObject();
            builder.field("path", info.path);
            if (info.mount != null) {
                builder.field("mount", info.mount);
            }
            if (info.dev != null) {
                builder.field("dev", info.dev);
            }

            if (info.total != -1) {
                builder.field("total", info.total().toString());
                builder.field("total_in_bytes", info.total);
            }
            if (info.free != -1) {
                builder.field("free", info.free().toString());
                builder.field("free_in_bytes", info.free);
            }
            if (info.available != -1) {
                builder.field("available", info.available().toString());
                builder.field("available_in_bytes", info.available);
            }

            if (info.diskReads != -1) {
                builder.field("disk_reads", info.diskReads);
            }
            if (info.diskWrites != -1) {
                builder.field("disk_writes", info.diskWrites);
            }

            if (info.diskReadBytes != -1) {
                builder.field("disk_read_size", info.diskReadSizeSize().toString());
                builder.field("disk_read_size_bytes", info.diskReadSizeInBytes());
            }
            if (info.diskWriteBytes != -1) {
                builder.field("disk_write_size", info.diskWriteSizeSize().toString());
                builder.field("disk_write_size_bytes", info.diskWriteSizeInBytes());
            }

            if (info.diskQueue != -1) {
                builder.field("disk_queue", Strings.format1Decimals(info.diskQueue, ""));
            }
            if (info.diskServiceTime != -1) {
                builder.field("disk_service_time", Strings.format1Decimals(info.diskServiceTime, ""));
            }

            builder.endObject();
        }
        builder.endArray();
        builder.endObject();
        return builder;
    }
}
