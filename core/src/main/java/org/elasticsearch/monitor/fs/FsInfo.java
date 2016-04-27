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

import org.elasticsearch.common.Nullable;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.common.io.stream.Writeable;
import org.elasticsearch.common.unit.ByteSizeValue;
import org.elasticsearch.common.xcontent.ToXContent;
import org.elasticsearch.common.xcontent.XContentBuilder;

import java.io.IOException;
import java.util.Arrays;
import java.util.HashSet;
import java.util.Iterator;
import java.util.Set;
import java.util.concurrent.TimeUnit;

public class FsInfo implements Iterable<FsInfo.Path>, Writeable, ToXContent {

    public static class Path implements Writeable, ToXContent {

        String path;
        @Nullable
        String mount;
        /** File system type from {@code java.nio.file.FileStore type()}, if available. */
        @Nullable
        String type;
        long total = -1;
        long free = -1;
        long available = -1;

        /** Uses Lucene's {@code IOUtils.spins} method to try to determine if the device backed by spinning media.
         *  This is null if we could not determine it, true if it possibly spins, else false. */
        Boolean spins = null;

        public Path() {
        }

        public Path(String path, @Nullable String mount, long total, long free, long available) {
            this.path = path;
            this.mount = mount;
            this.total = total;
            this.free = free;
            this.available = available;
        }

        /**
         * Read from a stream.
         */
        public Path(StreamInput in) throws IOException {
            path = in.readOptionalString();
            mount = in.readOptionalString();
            type = in.readOptionalString();
            total = in.readLong();
            free = in.readLong();
            available = in.readLong();
            spins = in.readOptionalBoolean();
        }

        @Override
        public void writeTo(StreamOutput out) throws IOException {
            out.writeOptionalString(path); // total aggregates do not have a path
            out.writeOptionalString(mount);
            out.writeOptionalString(type);
            out.writeLong(total);
            out.writeLong(free);
            out.writeLong(available);
            out.writeOptionalBoolean(spins);
        }

        public String getPath() {
            return path;
        }

        public String getMount() {
            return mount;
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

        public void add(Path path) {
            total = addLong(total, path.total);
            free = addLong(free, path.free);
            available = addLong(available, path.available);
            if (path.spins != null && path.spins.booleanValue()) {
                // Spinning is contagious!
                spins = Boolean.TRUE;
            }
        }

        static final class Fields {
            static final String PATH = "path";
            static final String MOUNT = "mount";
            static final String TYPE = "type";
            static final String TOTAL = "total";
            static final String TOTAL_IN_BYTES = "total_in_bytes";
            static final String FREE = "free";
            static final String FREE_IN_BYTES = "free_in_bytes";
            static final String AVAILABLE = "available";
            static final String AVAILABLE_IN_BYTES = "available_in_bytes";
            static final String SPINS = "spins";
        }

        @Override
        public XContentBuilder toXContent(XContentBuilder builder, Params params) throws IOException {
            builder.startObject();
            if (path != null) {
                builder.field(Fields.PATH, path);
            }
            if (mount != null) {
                builder.field(Fields.MOUNT, mount);
            }
            if (type != null) {
                builder.field(Fields.TYPE, type);
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
            if (spins != null) {
                builder.field(Fields.SPINS, spins.toString());
            }

            builder.endObject();
            return builder;
        }
    }

    public static class DeviceStats implements Writeable, ToXContent {

        final int majorDeviceNumber;
        final int minorDeviceNumber;
        final String deviceName;
        final long currentReadsCompleted;
        final long previousReadsCompleted;
        final long currentSectorsRead;
        final long previousSectorsRead;
        final long currentReadMilliseconds;
        final long previousReadMilliseconds;
        final long currentWritesCompleted;
        final long previousWritesCompleted;
        final long currentSectorsWritten;
        final long previousSectorsWritten;
        final long currentWriteMilliseconds;
        final long previousWriteMilliseconds;
        final long currentWeightedMilliseconds;
        final long previousWeightedMilliseconds;
        final long currentRelativeTime;
        final long previousRelativeTime;

        public DeviceStats(
                final int majorDeviceNumber,
                final int minorDeviceNumber,
                final String deviceName,
                final long currentReadsCompleted,
                final long currentSectorsRead,
                final long currentReadMilliseconds,
                final long currentWritesCompleted,
                final long currentSectorsWritten,
                final long currentWriteMilliseconds,
                final long currentWeightedMillseconds,
                final long currentRelativeTime,
                final DeviceStats previousDeviceStats) {
            this(
                    majorDeviceNumber,
                    minorDeviceNumber,
                    deviceName,
                    currentReadsCompleted,
                    previousDeviceStats != null ? previousDeviceStats.currentReadsCompleted : -1,
                    currentSectorsWritten,
                    previousDeviceStats != null ? previousDeviceStats.currentSectorsWritten : -1,
                    currentSectorsRead,
                    previousDeviceStats != null ? previousDeviceStats.currentSectorsRead : -1,
                    currentWritesCompleted,
                    previousDeviceStats != null ? previousDeviceStats.currentWritesCompleted : -1,
                    currentReadMilliseconds,
                    previousDeviceStats != null ? previousDeviceStats.currentReadMilliseconds : -1,
                    currentWriteMilliseconds,
                    previousDeviceStats != null ? previousDeviceStats.currentWriteMilliseconds : -1,
                    currentWeightedMillseconds,
                    previousDeviceStats != null ? previousDeviceStats.currentWeightedMilliseconds : -1,
                    currentRelativeTime,
                    previousDeviceStats != null ? previousDeviceStats.currentRelativeTime : -1);
        }

        private DeviceStats(
                final int majorDeviceNumber,
                final int minorDeviceNumber,
                final String deviceName,
                final long currentReadsCompleted,
                final long previousReadsCompleted,
                final long currentSectorsWritten,
                final long previousSectorsWritten,
                final long currentSectorsRead,
                final long previousSectorsRead,
                final long currentWritesCompleted,
                final long previousWritesCompleted,
                final long currentReadMilliseconds,
                final long previousReadMilliseconds,
                final long currentWriteMilliseconds,
                final long previousWriteMilliseconds,
                final long currentWeightedMilliseconds,
                final long previousWeightedMilliseconds,
                final long currentRelativeTime,
                final long previousRelativeTime) {
            this.majorDeviceNumber = majorDeviceNumber;
            this.minorDeviceNumber = minorDeviceNumber;
            this.deviceName = deviceName;
            this.currentReadsCompleted = currentReadsCompleted;
            this.previousReadsCompleted = previousReadsCompleted;
            this.currentWritesCompleted = currentWritesCompleted;
            this.previousWritesCompleted = previousWritesCompleted;
            this.currentSectorsRead = currentSectorsRead;
            this.previousSectorsRead = previousSectorsRead;
            this.currentSectorsWritten = currentSectorsWritten;
            this.previousSectorsWritten = previousSectorsWritten;
            this.currentReadMilliseconds = currentReadMilliseconds;
            this.previousReadMilliseconds = previousReadMilliseconds;
            this.currentWriteMilliseconds = currentWriteMilliseconds;
            this.previousWriteMilliseconds = previousWriteMilliseconds;
            this.currentWeightedMilliseconds = currentWeightedMilliseconds;
            this.previousWeightedMilliseconds = previousWeightedMilliseconds;
            this.currentRelativeTime = currentRelativeTime;
            this.previousRelativeTime = previousRelativeTime;
        }

        public DeviceStats(StreamInput in) throws IOException {
            majorDeviceNumber = in.readVInt();
            minorDeviceNumber = in.readVInt();
            deviceName = in.readString();
            currentReadsCompleted = in.readLong();
            previousReadsCompleted = in.readLong();
            currentWritesCompleted = in.readLong();
            previousWritesCompleted = in.readLong();
            currentSectorsRead = in.readLong();
            previousSectorsRead = in.readLong();
            currentSectorsWritten = in.readLong();
            previousSectorsWritten = in.readLong();
            currentReadMilliseconds = in.readLong();
            previousReadMilliseconds = in.readLong();
            currentWriteMilliseconds = in.readLong();
            previousWriteMilliseconds = in.readLong();
            currentWeightedMilliseconds = in.readLong();
            previousWeightedMilliseconds = in.readLong();
            currentRelativeTime = in.readLong();
            previousRelativeTime = in.readLong();
        }

        @Override
        public void writeTo(StreamOutput out) throws IOException {
            out.writeVInt(majorDeviceNumber);
            out.writeVInt(minorDeviceNumber);
            out.writeString(deviceName);
            out.writeLong(currentReadsCompleted);
            out.writeLong(previousReadsCompleted);
            out.writeLong(currentWritesCompleted);
            out.writeLong(previousWritesCompleted);
            out.writeLong(currentSectorsRead);
            out.writeLong(previousSectorsRead);
            out.writeLong(currentSectorsWritten);
            out.writeLong(previousSectorsWritten);
            out.writeLong(currentReadMilliseconds);
            out.writeLong(previousReadMilliseconds);
            out.writeLong(currentWriteMilliseconds);
            out.writeLong(previousWriteMilliseconds);
            out.writeLong(currentWeightedMilliseconds);
            out.writeLong(previousWeightedMilliseconds);
            out.writeLong(currentRelativeTime);
            out.writeLong(previousRelativeTime);
        }

        private long totalOperationsCompleted() {
            return (currentReadsCompleted - previousReadsCompleted) + (currentWritesCompleted - previousWritesCompleted);
        }

        public float iops() {
            if (previousReadsCompleted == -1 || previousWritesCompleted == -1 || previousRelativeTime == -1) return -1;

            return rateOfChange(
                    previousReadsCompleted + previousWritesCompleted,
                    currentReadsCompleted + currentWritesCompleted,
                    previousRelativeTime,
                    currentRelativeTime);
        }

        public float readsPerSecond() {
            if (previousReadsCompleted == -1 || previousRelativeTime == -1) return -1;

            return rateOfChange(previousReadsCompleted, currentReadsCompleted, previousRelativeTime, currentRelativeTime);
        }

        public float writesPerSecond() {
            if (previousWritesCompleted == -1 || previousRelativeTime == -1) return -1;

            return rateOfChange(previousWritesCompleted, currentWritesCompleted, previousRelativeTime, currentRelativeTime);
        }

        public float readKilobytesPerSecond() {
            if (previousSectorsRead == -1 || previousRelativeTime == -1) return -1;

            return rateOfChange(previousSectorsRead, currentSectorsRead, previousRelativeTime, currentRelativeTime) / 2;
        }

        public float writeKilobytesPerSecond() {
            if (previousSectorsWritten == -1 || previousRelativeTime == -1) return -1;

            return rateOfChange(previousSectorsWritten, currentSectorsWritten, previousRelativeTime, currentRelativeTime) / 2;
        }

        public float averageRequestSizeInKilobytes() {
            if (previousReadsCompleted == -1 || previousWritesCompleted == -1 || previousSectorsRead == -1) return -1;

            final long totalOperationsCompleted = totalOperationsCompleted();
            if (totalOperationsCompleted == 0) return 0;
            return ((currentSectorsRead - previousSectorsRead) + (currentSectorsWritten - previousSectorsWritten))
                    / (float)totalOperationsCompleted / 2;
        }

        public float averageResidentRequests() {
            if (previousWeightedMilliseconds == -1 || previousRelativeTime == -1) return -1;

            return rateOfChange(
                    previousWeightedMilliseconds,
                    currentWeightedMilliseconds,
                    previousRelativeTime,
                    currentRelativeTime) / 1000;
        }

        public float averageAwaitTimeInMilliseconds() {
            if (previousReadMilliseconds == -1 || previousReadsCompleted == -1) return -1;

            final long totalOperationsCompleted = totalOperationsCompleted();
            if (totalOperationsCompleted == 0) return 0;
            return ((currentReadMilliseconds - previousReadMilliseconds) + (currentWriteMilliseconds - previousWriteMilliseconds))
                    / (float) totalOperationsCompleted;
        }

        public float averageReadAwaitTimeInMilliseconds() {
            if (previousReadMilliseconds == -1 || previousReadsCompleted == -1) return -1;

            final long readOperationsCompleted = currentReadsCompleted - previousReadsCompleted;
            if (readOperationsCompleted == 0) return 0;
            return (currentReadMilliseconds - previousReadMilliseconds) / (float)readOperationsCompleted;
        }

        public float averageWriteAwaitTimeInMilliseconds() {
            if (previousWriteMilliseconds == -1 || previousWritesCompleted == -1) return -1;

            final long writeOperationsCompleted = currentWritesCompleted - previousWritesCompleted;
            if (writeOperationsCompleted == 0) return 0;
            return (currentWriteMilliseconds - previousWriteMilliseconds) / (float)writeOperationsCompleted;

        }

        @Override
        public XContentBuilder toXContent(XContentBuilder builder, Params params) throws IOException {
            builder.field("device_name", deviceName);
            builder.field("iops", iops());
            builder.field("reads_per_second", readsPerSecond());
            builder.field("writes_per_second", writesPerSecond());
            builder.field("read_kilobytes_per_second", readKilobytesPerSecond());
            builder.field("write_kilobytes_per_second", writeKilobytesPerSecond());
            builder.field("average_request_size_in_kilobytes", averageRequestSizeInKilobytes());
            builder.field("average_resident_requests", averageResidentRequests());
            builder.field("average_await_time_in_milliseconds", averageAwaitTimeInMilliseconds());
            builder.field("average_read_await_time_in_milliseconds", averageReadAwaitTimeInMilliseconds());
            builder.field("average_write_await_time_in_milliseconds", averageWriteAwaitTimeInMilliseconds());
            return builder;
        }

    }

    public static class IoStats implements Writeable, ToXContent {
        final DeviceStats[] devicesStats;

        public IoStats(final DeviceStats[] devicesStats) {
            this.devicesStats = devicesStats;
        }

        public IoStats(StreamInput in) throws IOException {
            final int length = in.readVInt();
            final DeviceStats[] devicesStats = new DeviceStats[length];
            for (int i = 0; i < length; i++) {
                devicesStats[i] = new DeviceStats(in);
            }
            this.devicesStats = devicesStats;
        }

        @Override
        public void writeTo(StreamOutput out) throws IOException {
            out.writeVInt(devicesStats.length);
            for (int i = 0; i < devicesStats.length; i++) {
                devicesStats[i].writeTo(out);
            }
        }

        @Override
        public XContentBuilder toXContent(XContentBuilder builder, Params params) throws IOException {
            if (devicesStats.length > 0) {
                builder.startArray("devices");
                for (DeviceStats deviceStats : devicesStats) {
                    builder.startObject();
                    deviceStats.toXContent(builder, params);
                    builder.endObject();
                }
                builder.endArray();
            }
            return builder;
        }

    }

    private static float rateOfChange(final long previous, final long current, final long t1, final long t2) {
        return TimeUnit.SECONDS.toNanos(1) * (current - previous) / (float)(t2 - t1);
    }

    final long timestamp;
    final Path[] paths;
    final IoStats ioStats;
    Path total;

    public FsInfo(long timestamp, IoStats ioStats, Path[] paths) {
        this.timestamp = timestamp;
        this.ioStats = ioStats;
        this.paths = paths;
        this.total = null;
    }

    /**
     * Read from a stream.
     */
    public FsInfo(StreamInput in) throws IOException {
        timestamp = in.readVLong();
        ioStats = in.readOptionalWriteable(IoStats::new);
        paths = new Path[in.readVInt()];
        for (int i = 0; i < paths.length; i++) {
            paths[i] = new Path(in);
        }
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        out.writeVLong(timestamp);
        out.writeOptionalWriteable(ioStats);
        out.writeVInt(paths.length);
        for (Path path : paths) {
            path.writeTo(out);
        }
    }

    public Path getTotal() {
        return total();
    }

    public Path total() {
        if (total != null) {
            return total;
        }
        Path res = new Path();
        Set<String> seenDevices = new HashSet<>(paths.length);
        for (Path subPath : paths) {
            if (subPath.path != null) {
                if (!seenDevices.add(subPath.path)) {
                    continue; // already added numbers for this device;
                }
            }
            res.add(subPath);
        }
        total = res;
        return res;
    }

    public long getTimestamp() {
        return timestamp;
    }

    public IoStats getIoStats() {
        return ioStats;
    }

    @Override
    public Iterator<Path> iterator() {
        return Arrays.stream(paths).iterator();
    }

    @Override
    public XContentBuilder toXContent(XContentBuilder builder, Params params) throws IOException {
        builder.startObject(Fields.FS);
        builder.field(Fields.TIMESTAMP, timestamp);
        builder.field(Fields.TOTAL);
        total().toXContent(builder, params);
        builder.startArray(Fields.DATA);
        for (Path path : paths) {
            path.toXContent(builder, params);
        }
        builder.endArray();
        if (ioStats != null) {
            builder.startObject(Fields.IO_STATS);
            ioStats.toXContent(builder, params);
            builder.endObject();
        }
        builder.endObject();
        return builder;
    }

    static final class Fields {
        static final String FS = "fs";
        static final String TIMESTAMP = "timestamp";
        static final String DATA = "data";
        static final String TOTAL = "total";
        static final String IO_STATS = "io_stats";
    }

}
