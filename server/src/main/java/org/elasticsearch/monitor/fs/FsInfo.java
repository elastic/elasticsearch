/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.monitor.fs;

import org.elasticsearch.cluster.routing.allocation.DiskThresholdSettings;
import org.elasticsearch.common.collect.Iterators;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.common.io.stream.Writeable;
import org.elasticsearch.common.unit.ByteSizeUnit;
import org.elasticsearch.common.unit.ByteSizeValue;
import org.elasticsearch.common.util.set.Sets;
import org.elasticsearch.core.Nullable;
import org.elasticsearch.xcontent.ToXContentFragment;
import org.elasticsearch.xcontent.ToXContentObject;
import org.elasticsearch.xcontent.XContentBuilder;

import java.io.IOException;
import java.nio.file.FileStore;
import java.util.Iterator;
import java.util.Set;

public class FsInfo implements Iterable<FsInfo.Path>, Writeable, ToXContentFragment {

    public static class Path implements Writeable, ToXContentObject {

        String path;
        /** File system string from {@link FileStore#toString()}. The concrete subclasses of FileStore have meaningful toString methods. */
        String mount; // e.g. "/app (/dev/mapper/lxc-data)", "/System/Volumes/Data (/dev/disk1s2)", "Local Disk (C:)", etc.
        /** File system type from {@link FileStore#type()}. */
        String type; // e.g. "xfs", "apfs", "NTFS", etc.
        long total = -1;
        long free = -1;
        long available = -1;

        ByteSizeValue lowWatermarkFreeSpace = null;
        ByteSizeValue highWatermarkFreeSpace = null;
        ByteSizeValue floodStageWatermarkFreeSpace = null;
        ByteSizeValue frozenFloodStageWatermarkFreeSpace = null;

        public Path() {}

        public Path(String path, String mount, long total, long free, long available) {
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
            path = in.readOptionalString(); // total aggregates do not have a path, mount, or type
            mount = in.readOptionalString();
            type = in.readOptionalString();
            total = in.readLong();
            free = in.readLong();
            available = in.readLong();
        }

        @Override
        public void writeTo(StreamOutput out) throws IOException {
            out.writeOptionalString(path); // total aggregates do not have a path, mount, or type
            out.writeOptionalString(mount);
            out.writeOptionalString(type);
            out.writeLong(total);
            out.writeLong(free);
            out.writeLong(available);
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
            return ByteSizeValue.ofBytes(total);
        }

        public ByteSizeValue getFree() {
            return ByteSizeValue.ofBytes(free);
        }

        public ByteSizeValue getAvailable() {
            return ByteSizeValue.ofBytes(available);
        }

        public void setEffectiveWatermarks(final DiskThresholdSettings masterThresholdSettings, boolean isDedicatedFrozenNode) {
            lowWatermarkFreeSpace = masterThresholdSettings.getFreeBytesThresholdLowStage(ByteSizeValue.of(total, ByteSizeUnit.BYTES));
            highWatermarkFreeSpace = masterThresholdSettings.getFreeBytesThresholdHighStage(ByteSizeValue.of(total, ByteSizeUnit.BYTES));
            floodStageWatermarkFreeSpace = masterThresholdSettings.getFreeBytesThresholdFloodStage(
                ByteSizeValue.of(total, ByteSizeUnit.BYTES)
            );
            if (isDedicatedFrozenNode) {
                frozenFloodStageWatermarkFreeSpace = masterThresholdSettings.getFreeBytesThresholdFrozenFloodStage(
                    ByteSizeValue.of(total, ByteSizeUnit.BYTES)
                );
            }
        }

        public ByteSizeValue getLowWatermarkFreeSpace() {
            return lowWatermarkFreeSpace;
        }

        public ByteSizeValue getHighWatermarkFreeSpace() {
            return highWatermarkFreeSpace;
        }

        public ByteSizeValue getFloodStageWatermarkFreeSpace() {
            return floodStageWatermarkFreeSpace;
        }

        public ByteSizeValue getFrozenFloodStageWatermarkFreeSpace() {
            return frozenFloodStageWatermarkFreeSpace;
        }

        private static long addLong(long current, long other) {
            if (current == -1 && other == -1) {
                return 0;
            }
            if (other == -1) {
                return current;
            }
            if (current == -1) {
                return other;
            }
            return current + other;
        }

        public void add(Path path) {
            total = FsProbe.adjustForHugeFilesystems(addLong(total, path.total));
            free = FsProbe.adjustForHugeFilesystems(addLong(free, path.free));
            available = FsProbe.adjustForHugeFilesystems(addLong(available, path.available));
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
            static final String LOW_WATERMARK_FREE_SPACE = "low_watermark_free_space";
            static final String LOW_WATERMARK_FREE_SPACE_IN_BYTES = "low_watermark_free_space_in_bytes";
            static final String HIGH_WATERMARK_FREE_SPACE = "high_watermark_free_space";
            static final String HIGH_WATERMARK_FREE_SPACE_IN_BYTES = "high_watermark_free_space_in_bytes";
            static final String FLOOD_STAGE_FREE_SPACE = "flood_stage_free_space";
            static final String FLOOD_STAGE_FREE_SPACE_IN_BYTES = "flood_stage_free_space_in_bytes";
            static final String FROZEN_FLOOD_STAGE_FREE_SPACE = "frozen_flood_stage_free_space";
            static final String FROZEN_FLOOD_STAGE_FREE_SPACE_IN_BYTES = "frozen_flood_stage_free_space_in_bytes";
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
                builder.humanReadableField(Fields.TOTAL_IN_BYTES, Fields.TOTAL, getTotal());
            }
            if (free != -1) {
                builder.humanReadableField(Fields.FREE_IN_BYTES, Fields.FREE, getFree());
            }
            if (available != -1) {
                builder.humanReadableField(Fields.AVAILABLE_IN_BYTES, Fields.AVAILABLE, getAvailable());
            }

            if (lowWatermarkFreeSpace != null) {
                builder.humanReadableField(
                    Fields.LOW_WATERMARK_FREE_SPACE_IN_BYTES,
                    Fields.LOW_WATERMARK_FREE_SPACE,
                    getLowWatermarkFreeSpace()
                );
            }
            if (highWatermarkFreeSpace != null) {
                builder.humanReadableField(
                    Fields.HIGH_WATERMARK_FREE_SPACE_IN_BYTES,
                    Fields.HIGH_WATERMARK_FREE_SPACE,
                    getHighWatermarkFreeSpace()
                );
            }
            if (floodStageWatermarkFreeSpace != null) {
                builder.humanReadableField(
                    Fields.FLOOD_STAGE_FREE_SPACE_IN_BYTES,
                    Fields.FLOOD_STAGE_FREE_SPACE,
                    getFloodStageWatermarkFreeSpace()
                );
            }
            if (frozenFloodStageWatermarkFreeSpace != null) {
                builder.humanReadableField(
                    Fields.FROZEN_FLOOD_STAGE_FREE_SPACE_IN_BYTES,
                    Fields.FROZEN_FLOOD_STAGE_FREE_SPACE,
                    getFrozenFloodStageWatermarkFreeSpace()
                );
            }

            builder.endObject();
            return builder;
        }
    }

    public static class DeviceStats implements Writeable, ToXContentFragment {

        final int majorDeviceNumber;
        final int minorDeviceNumber;
        final String deviceName;
        final long currentReadsCompleted;
        final long previousReadsCompleted;
        final long currentSectorsRead;
        final long previousSectorsRead;
        final long currentWritesCompleted;
        final long previousWritesCompleted;
        final long currentSectorsWritten;
        final long previousSectorsWritten;
        final long currentIOTime;
        final long previousIOTime;

        public DeviceStats(
            final int majorDeviceNumber,
            final int minorDeviceNumber,
            final String deviceName,
            final long currentReadsCompleted,
            final long currentSectorsRead,
            final long currentWritesCompleted,
            final long currentSectorsWritten,
            final long currentIOTime,
            final DeviceStats previousDeviceStats
        ) {
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
                currentIOTime,
                previousDeviceStats != null ? previousDeviceStats.currentIOTime : -1
            );
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
            final long currentIOTime,
            final long previousIOTime
        ) {
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
            this.currentIOTime = currentIOTime;
            this.previousIOTime = previousIOTime;
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
            currentIOTime = in.readLong();
            previousIOTime = in.readLong();
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
            out.writeLong(currentIOTime);
            out.writeLong(previousIOTime);
        }

        public String getDeviceName() {
            return deviceName;
        }

        public long operations() {
            if (previousReadsCompleted == -1 || previousWritesCompleted == -1) return -1;

            return (currentReadsCompleted - previousReadsCompleted) + (currentWritesCompleted - previousWritesCompleted);
        }

        public long readOperations() {
            if (previousReadsCompleted == -1) return -1;

            return (currentReadsCompleted - previousReadsCompleted);
        }

        public long writeOperations() {
            if (previousWritesCompleted == -1) return -1;

            return (currentWritesCompleted - previousWritesCompleted);
        }

        public long readKilobytes() {
            if (previousSectorsRead == -1) return -1;

            return (currentSectorsRead - previousSectorsRead) / 2;
        }

        public long writeKilobytes() {
            if (previousSectorsWritten == -1) return -1;

            return (currentSectorsWritten - previousSectorsWritten) / 2;
        }

        public long ioTimeInMillis() {
            if (previousIOTime == -1) return -1;

            return (currentIOTime - previousIOTime);
        }

        @Override
        public XContentBuilder toXContent(XContentBuilder builder, Params params) throws IOException {
            builder.field("device_name", deviceName);
            builder.field(IoStats.OPERATIONS, operations());
            builder.field(IoStats.READ_OPERATIONS, readOperations());
            builder.field(IoStats.WRITE_OPERATIONS, writeOperations());
            builder.field(IoStats.READ_KILOBYTES, readKilobytes());
            builder.field(IoStats.WRITE_KILOBYTES, writeKilobytes());
            builder.field(IoStats.IO_TIMEMS, ioTimeInMillis());
            return builder;
        }

    }

    public static class IoStats implements Writeable, ToXContentFragment {

        private static final String OPERATIONS = "operations";
        private static final String READ_OPERATIONS = "read_operations";
        private static final String WRITE_OPERATIONS = "write_operations";
        private static final String READ_KILOBYTES = "read_kilobytes";
        private static final String WRITE_KILOBYTES = "write_kilobytes";
        private static final String IO_TIMEMS = "io_time_in_millis";

        final DeviceStats[] devicesStats;
        final long totalOperations;
        final long totalReadOperations;
        final long totalWriteOperations;
        final long totalReadKilobytes;
        final long totalWriteKilobytes;
        final long totalIOTimeInMillis;

        public IoStats(final DeviceStats[] devicesStats) {
            this.devicesStats = devicesStats;
            long totalOperations = 0;
            long totalReadOperations = 0;
            long totalWriteOperations = 0;
            long totalReadKilobytes = 0;
            long totalWriteKilobytes = 0;
            long totalIOTimeInMillis = 0;
            for (DeviceStats deviceStats : devicesStats) {
                totalOperations += deviceStats.operations() != -1 ? deviceStats.operations() : 0;
                totalReadOperations += deviceStats.readOperations() != -1 ? deviceStats.readOperations() : 0;
                totalWriteOperations += deviceStats.writeOperations() != -1 ? deviceStats.writeOperations() : 0;
                totalReadKilobytes += deviceStats.readKilobytes() != -1 ? deviceStats.readKilobytes() : 0;
                totalWriteKilobytes += deviceStats.writeKilobytes() != -1 ? deviceStats.writeKilobytes() : 0;
                totalIOTimeInMillis += deviceStats.ioTimeInMillis() != -1 ? deviceStats.ioTimeInMillis() : 0;
            }
            this.totalOperations = totalOperations;
            this.totalReadOperations = totalReadOperations;
            this.totalWriteOperations = totalWriteOperations;
            this.totalReadKilobytes = totalReadKilobytes;
            this.totalWriteKilobytes = totalWriteKilobytes;
            this.totalIOTimeInMillis = totalIOTimeInMillis;
        }

        public IoStats(StreamInput in) throws IOException {
            final int length = in.readVInt();
            final DeviceStats[] devicesStats = new DeviceStats[length];
            for (int i = 0; i < length; i++) {
                devicesStats[i] = new DeviceStats(in);
            }
            this.devicesStats = devicesStats;
            this.totalOperations = in.readLong();
            this.totalReadOperations = in.readLong();
            this.totalWriteOperations = in.readLong();
            this.totalReadKilobytes = in.readLong();
            this.totalWriteKilobytes = in.readLong();
            this.totalIOTimeInMillis = in.readLong();
        }

        @Override
        public void writeTo(StreamOutput out) throws IOException {
            out.writeVInt(devicesStats.length);
            for (int i = 0; i < devicesStats.length; i++) {
                devicesStats[i].writeTo(out);
            }
            out.writeLong(totalOperations);
            out.writeLong(totalReadOperations);
            out.writeLong(totalWriteOperations);
            out.writeLong(totalReadKilobytes);
            out.writeLong(totalWriteKilobytes);
            out.writeLong(totalIOTimeInMillis);
        }

        public DeviceStats[] getDevicesStats() {
            return devicesStats;
        }

        public long getTotalOperations() {
            return totalOperations;
        }

        public long getTotalReadOperations() {
            return totalReadOperations;
        }

        public long getTotalWriteOperations() {
            return totalWriteOperations;
        }

        public long getTotalReadKilobytes() {
            return totalReadKilobytes;
        }

        public long getTotalWriteKilobytes() {
            return totalWriteKilobytes;
        }

        public long getTotalIOTimeMillis() {
            return totalIOTimeInMillis;
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

                builder.startObject("total");
                builder.field(OPERATIONS, totalOperations);
                builder.field(READ_OPERATIONS, totalReadOperations);
                builder.field(WRITE_OPERATIONS, totalWriteOperations);
                builder.field(READ_KILOBYTES, totalReadKilobytes);
                builder.field(WRITE_KILOBYTES, totalWriteKilobytes);
                builder.field(IO_TIMEMS, totalIOTimeInMillis);
                builder.endObject();
            }
            return builder;
        }

    }

    private final long timestamp;
    private final Path[] paths;
    private final IoStats ioStats;
    private final Path total;

    public FsInfo(long timestamp, IoStats ioStats, Path[] paths) {
        this.timestamp = timestamp;
        this.ioStats = ioStats;
        this.paths = paths;
        this.total = total();
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
        this.total = total();
    }

    public static FsInfo setEffectiveWatermarks(
        @Nullable final FsInfo fsInfo,
        @Nullable final DiskThresholdSettings masterThresholdSettings,
        boolean isDedicatedFrozenNode
    ) {
        if (fsInfo != null && masterThresholdSettings != null) {
            for (Path path : fsInfo.paths) {
                path.setEffectiveWatermarks(masterThresholdSettings, isDedicatedFrozenNode);
            }
        }
        return fsInfo;
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        out.writeVLong(timestamp);
        out.writeOptionalWriteable(ioStats);
        out.writeArray(paths);
    }

    public Path getTotal() {
        return total;
    }

    private Path total() {
        Path res = new Path();
        Set<String> seenDevices = Sets.newHashSetWithExpectedSize(paths.length);
        for (Path subPath : paths) {
            if (subPath.mount != null) {
                if (seenDevices.add(subPath.mount) == false) {
                    continue; // already added numbers for this device;
                }
            }
            res.add(subPath);
        }
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
        return Iterators.forArray(paths);
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
