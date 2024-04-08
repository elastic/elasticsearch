/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.monitor.process;

import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.common.io.stream.Writeable;
import org.elasticsearch.common.unit.ByteSizeValue;
import org.elasticsearch.core.TimeValue;
import org.elasticsearch.xcontent.ToXContentFragment;
import org.elasticsearch.xcontent.XContentBuilder;

import java.io.IOException;

public class ProcessStats implements Writeable, ToXContentFragment {

    private final long timestamp;
    private final long openFileDescriptors;
    private final long maxFileDescriptors;
    private final Cpu cpu;
    private final Mem mem;

    public ProcessStats(long timestamp, long openFileDescriptors, long maxFileDescriptors, Cpu cpu, Mem mem) {
        this.timestamp = timestamp;
        this.openFileDescriptors = openFileDescriptors;
        this.maxFileDescriptors = maxFileDescriptors;
        this.cpu = cpu;
        this.mem = mem;
    }

    public ProcessStats(StreamInput in) throws IOException {
        timestamp = in.readVLong();
        openFileDescriptors = in.readLong();
        maxFileDescriptors = in.readLong();
        cpu = in.readOptionalWriteable(Cpu::new);
        mem = in.readOptionalWriteable(Mem::new);
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        out.writeVLong(timestamp);
        out.writeLong(openFileDescriptors);
        out.writeLong(maxFileDescriptors);
        out.writeOptionalWriteable(cpu);
        out.writeOptionalWriteable(mem);
    }

    public long getTimestamp() {
        return timestamp;
    }

    public long getOpenFileDescriptors() {
        return openFileDescriptors;
    }

    public long getMaxFileDescriptors() {
        return maxFileDescriptors;
    }

    public Cpu getCpu() {
        return cpu;
    }

    public Mem getMem() {
        return mem;
    }

    static final class Fields {
        static final String PROCESS = "process";
        static final String TIMESTAMP = "timestamp";
        static final String OPEN_FILE_DESCRIPTORS = "open_file_descriptors";
        static final String MAX_FILE_DESCRIPTORS = "max_file_descriptors";

        static final String CPU = "cpu";
        static final String PERCENT = "percent";
        static final String TOTAL = "total";
        static final String TOTAL_IN_MILLIS = "total_in_millis";

        static final String MEM = "mem";
        static final String TOTAL_VIRTUAL = "total_virtual";
        static final String TOTAL_VIRTUAL_IN_BYTES = "total_virtual_in_bytes";
    }

    @Override
    public XContentBuilder toXContent(XContentBuilder builder, Params params) throws IOException {
        builder.startObject(Fields.PROCESS);
        builder.field(Fields.TIMESTAMP, timestamp);
        builder.field(Fields.OPEN_FILE_DESCRIPTORS, openFileDescriptors);
        builder.field(Fields.MAX_FILE_DESCRIPTORS, maxFileDescriptors);
        if (cpu != null) {
            builder.startObject(Fields.CPU);
            builder.field(Fields.PERCENT, cpu.percent);
            builder.humanReadableField(Fields.TOTAL_IN_MILLIS, Fields.TOTAL, new TimeValue(cpu.total));
            builder.endObject();
        }
        if (mem != null) {
            builder.startObject(Fields.MEM);
            builder.humanReadableField(Fields.TOTAL_VIRTUAL_IN_BYTES, Fields.TOTAL_VIRTUAL, ByteSizeValue.ofBytes(mem.totalVirtual));
            builder.endObject();
        }
        builder.endObject();
        return builder;
    }

    public static class Mem implements Writeable {

        private final long totalVirtual;

        public Mem(long totalVirtual) {
            this.totalVirtual = totalVirtual;
        }

        public Mem(StreamInput in) throws IOException {
            totalVirtual = in.readLong();
        }

        @Override
        public void writeTo(StreamOutput out) throws IOException {
            out.writeLong(totalVirtual);
        }

        public ByteSizeValue getTotalVirtual() {
            return ByteSizeValue.ofBytes(totalVirtual);
        }
    }

    public static class Cpu implements Writeable {

        private final short percent;
        private final long total;

        public Cpu(short percent, long total) {
            this.percent = percent;
            this.total = total;
        }

        public Cpu(StreamInput in) throws IOException {
            percent = in.readShort();
            total = in.readLong();
        }

        @Override
        public void writeTo(StreamOutput out) throws IOException {
            out.writeShort(percent);
            out.writeLong(total);
        }

        /**
         * Get the Process cpu usage.
         * <p>
         * Supported Platforms: All.
         */
        public short getPercent() {
            return percent;
        }

        /**
         * Get the Process cpu time (sum of User and Sys).
         * <p>
         * Supported Platforms: All.
         */
        public TimeValue getTotal() {
            return new TimeValue(total);
        }
    }
}
