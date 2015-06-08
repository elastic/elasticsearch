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
import java.util.HashSet;
import java.util.Iterator;
import java.util.Set;

/**
 */
public class FsStats implements Iterable<FsStats.Info>, Streamable, ToXContent {

    public static class Info implements Streamable, ToXContent {

        String path;
        @Nullable
        String mount;
        @Nullable
        String dev;
        /** File system type from {@code java.nio.file.FileStore type()}, if available. */
        @Nullable
        String type;
        long total = -1;
        long free = -1;
        long available = -1;
        long diskReads = -1;
        long diskWrites = -1;
        long diskReadBytes = -1;
        long diskWriteBytes = -1;
        double diskQueue = -1;
        double diskServiceTime = -1;
        /** Uses Lucene's {@code IOUtils.spins} method to try to determine if the device backed by spinning media.
         *  This is null if we could not determine it, true if it possibly spins, else false. */
        Boolean spins = null;

        public Info() {
        }

        public Info(String path, @Nullable String mount, @Nullable String dev, long total, long free, long available, long diskReads,
                    long diskWrites, long diskReadBytes, long diskWriteBytes, double diskQueue, double diskServiceTime) {
            this.path = path;
            this.mount = mount;
            this.dev = dev;
            this.total = total;
            this.free = free;
            this.available = available;
            this.diskReads = diskReads;
            this.diskWrites = diskWrites;
            this.diskReadBytes = diskReadBytes;
            this.diskWriteBytes = diskWriteBytes;
            this.diskQueue = diskQueue;
            this.diskServiceTime = diskServiceTime;
        }

        static public Info readInfoFrom(StreamInput in) throws IOException {
            Info i = new Info();
            i.readFrom(in);
            return i;
        }

        @Override
        public void readFrom(StreamInput in) throws IOException {
            path = in.readOptionalString();
            mount = in.readOptionalString();
            dev = in.readOptionalString();
            type = in.readOptionalString();
            total = in.readLong();
            free = in.readLong();
            available = in.readLong();
            diskReads = in.readLong();
            diskWrites = in.readLong();
            diskReadBytes = in.readLong();
            diskWriteBytes = in.readLong();
            diskQueue = in.readDouble();
            diskServiceTime = in.readDouble();
            spins = in.readOptionalBoolean();
        }

        @Override
        public void writeTo(StreamOutput out) throws IOException {
            out.writeOptionalString(path); // total aggregates do not have a path
            out.writeOptionalString(mount);
            out.writeOptionalString(dev);
            out.writeOptionalString(type);
            out.writeLong(total);
            out.writeLong(free);
            out.writeLong(available);
            out.writeLong(diskReads);
            out.writeLong(diskWrites);
            out.writeLong(diskReadBytes);
            out.writeLong(diskWriteBytes);
            out.writeDouble(diskQueue);
            out.writeDouble(diskServiceTime);
            out.writeOptionalBoolean(spins);
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

        public String getType() {
            return type;
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

        public Boolean getSpins() {
            return spins;
        }

        private long addLong(long current, long other) {
            if (other == -1) {
                return current;
            }
            if (current == -1) {
                return other;
            }
            return current + other;
        }

        private double addDouble(double current, double other) {
            if (other == -1) {
                return current;
            }
            if (current == -1) {
                return other;
            }
            return current + other;
        }

        public void add(Info info) {
            total = addLong(total, info.total);
            free = addLong(free, info.free);
            available = addLong(available, info.available);
            diskReads = addLong(diskReads, info.diskReads);
            diskWrites = addLong(diskWrites, info.diskWrites);
            diskReadBytes = addLong(diskReadBytes, info.diskReadBytes);
            diskWriteBytes = addLong(diskWriteBytes, info.diskWriteBytes);
            diskQueue = addDouble(diskQueue, info.diskQueue);
            diskServiceTime = addDouble(diskServiceTime, info.diskServiceTime);
            if (info.spins != null && info.spins.booleanValue()) {
                // Spinning is contagious!
                spins = Boolean.TRUE;
            }
        }

        static final class Fields {
            static final XContentBuilderString PATH = new XContentBuilderString("path");
            static final XContentBuilderString MOUNT = new XContentBuilderString("mount");
            static final XContentBuilderString DEV = new XContentBuilderString("dev");
            static final XContentBuilderString TYPE = new XContentBuilderString("type");
            static final XContentBuilderString TOTAL = new XContentBuilderString("total");
            static final XContentBuilderString TOTAL_IN_BYTES = new XContentBuilderString("total_in_bytes");
            static final XContentBuilderString FREE = new XContentBuilderString("free");
            static final XContentBuilderString FREE_IN_BYTES = new XContentBuilderString("free_in_bytes");
            static final XContentBuilderString AVAILABLE = new XContentBuilderString("available");
            static final XContentBuilderString AVAILABLE_IN_BYTES = new XContentBuilderString("available_in_bytes");
            static final XContentBuilderString DISK_READS = new XContentBuilderString("disk_reads");
            static final XContentBuilderString DISK_WRITES = new XContentBuilderString("disk_writes");
            static final XContentBuilderString DISK_IO_OP = new XContentBuilderString("disk_io_op");
            static final XContentBuilderString DISK_READ_SIZE = new XContentBuilderString("disk_read_size");
            static final XContentBuilderString DISK_READ_SIZE_IN_BYTES = new XContentBuilderString("disk_read_size_in_bytes");
            static final XContentBuilderString DISK_WRITE_SIZE = new XContentBuilderString("disk_write_size");
            static final XContentBuilderString DISK_WRITE_SIZE_IN_BYTES = new XContentBuilderString("disk_write_size_in_bytes");
            static final XContentBuilderString DISK_IO_SIZE = new XContentBuilderString("disk_io_size");
            static final XContentBuilderString DISK_IO_IN_BYTES = new XContentBuilderString("disk_io_size_in_bytes");
            static final XContentBuilderString DISK_QUEUE = new XContentBuilderString("disk_queue");
            static final XContentBuilderString DISK_SERVICE_TIME = new XContentBuilderString("disk_service_time");
            static final XContentBuilderString SPINS = new XContentBuilderString("spins");
        }

        @Override
        public XContentBuilder toXContent(XContentBuilder builder, Params params) throws IOException {
            builder.startObject();
            if (path != null) {
                builder.field(Fields.PATH, path, XContentBuilder.FieldCaseConversion.NONE);
            }
            if (mount != null) {
                builder.field(Fields.MOUNT, mount, XContentBuilder.FieldCaseConversion.NONE);
            }
            if (dev != null) {
                builder.field(Fields.DEV, dev, XContentBuilder.FieldCaseConversion.NONE);
            }
            if (type != null) {
                builder.field(Fields.TYPE, type, XContentBuilder.FieldCaseConversion.NONE);
            }

            if (total != -1) {
                builder.byteSizeField(Fields.TOTAL_IN_BYTES, Fields.TOTAL, total);
            }
            if (free != -1) {
                builder.byteSizeField(Fields.FREE_IN_BYTES, Fields.FREE, free);
            }
            if (available != -1) {
                builder.byteSizeField(Fields.AVAILABLE_IN_BYTES, Fields.AVAILABLE, available);
            }

            long iop = -1;

            if (diskReads != -1) {
                iop = diskReads;
                builder.field(Fields.DISK_READS, diskReads);
            }
            if (diskWrites != -1) {
                if (iop != -1) {
                    iop += diskWrites;
                } else {
                    iop = diskWrites;
                }
                builder.field(Fields.DISK_WRITES, diskWrites);
            }

            if (iop != -1) {
                builder.field(Fields.DISK_IO_OP, iop);
            }

            long ioBytes = -1;

            if (diskReadBytes != -1) {
                ioBytes = diskReadBytes;
                builder.byteSizeField(Fields.DISK_READ_SIZE_IN_BYTES, Fields.DISK_READ_SIZE, diskReadBytes);
            }
            if (diskWriteBytes != -1) {
                if (ioBytes != -1) {
                    ioBytes += diskWriteBytes;
                } else {
                    ioBytes = diskWriteBytes;
                }
                builder.byteSizeField(Fields.DISK_WRITE_SIZE_IN_BYTES, Fields.DISK_WRITE_SIZE, diskWriteBytes);
            }

            if (ioBytes != -1) {
                builder.byteSizeField(Fields.DISK_IO_IN_BYTES, Fields.DISK_IO_SIZE, ioBytes);
            }

            if (diskQueue != -1) {
                builder.field(Fields.DISK_QUEUE, Strings.format1Decimals(diskQueue, ""));
            }
            if (diskServiceTime != -1) {
                builder.field(Fields.DISK_SERVICE_TIME, Strings.format1Decimals(diskServiceTime, ""));
            }
            if (spins != null) {
                builder.field(Fields.SPINS, spins.toString());
            }

            builder.endObject();
            return builder;
        }
    }

    long timestamp;
    Info total;
    Info[] infos;

    FsStats() {

    }

    public FsStats(long timestamp, Info[] infos) {
        this.timestamp = timestamp;
        this.infos = infos;
        this.total = null;
    }

    public Info getTotal() {
        return total();
    }

    public Info total() {
        if (total != null) {
            return total;
        }
        Info res = new Info();
        Set<String> seenDevices = new HashSet<>(infos.length);
        for (Info subInfo : infos) {
            if (subInfo.dev != null) {
                if (!seenDevices.add(subInfo.dev)) {
                    continue; // already added numbers for this device;
                }
            }
            res.add(subInfo);
        }
        total = res;
        return res;
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
            infos[i] = Info.readInfoFrom(in);
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
        static final XContentBuilderString TOTAL = new XContentBuilderString("total");
    }

    @Override
    public XContentBuilder toXContent(XContentBuilder builder, Params params) throws IOException {
        builder.startObject(Fields.FS);
        builder.field(Fields.TIMESTAMP, timestamp);
        builder.field(Fields.TOTAL);
        total().toXContent(builder, params);
        builder.startArray(Fields.DATA);
        for (Info info : infos) {
            info.toXContent(builder, params);
        }
        builder.endArray();
        builder.endObject();
        return builder;
    }
}
