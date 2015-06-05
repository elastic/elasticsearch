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

package org.elasticsearch.monitor.process;

import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.common.io.stream.Streamable;
import org.elasticsearch.common.unit.ByteSizeValue;
import org.elasticsearch.common.unit.TimeValue;
import org.elasticsearch.common.xcontent.ToXContent;
import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.common.xcontent.XContentBuilderString;

import java.io.IOException;
import java.io.Serializable;

/**
 *
 */
public class ProcessStats implements Streamable, Serializable, ToXContent {

    long timestamp = -1;

    long openFileDescriptors;

    Cpu cpu = null;

    Mem mem = null;

    ProcessStats() {
    }

    public long timestamp() {
        return this.timestamp;
    }

    public long getTimestamp() {
        return timestamp();
    }

    public long openFileDescriptors() {
        return this.openFileDescriptors;
    }

    public long getOpenFileDescriptors() {
        return openFileDescriptors;
    }

    public Cpu cpu() {
        return cpu;
    }

    public Cpu getCpu() {
        return cpu();
    }

    public Mem mem() {
        return mem;
    }

    public Mem getMem() {
        return mem();
    }

    static final class Fields {
        static final XContentBuilderString PROCESS = new XContentBuilderString("process");
        static final XContentBuilderString TIMESTAMP = new XContentBuilderString("timestamp");
        static final XContentBuilderString OPEN_FILE_DESCRIPTORS = new XContentBuilderString("open_file_descriptors");

        static final XContentBuilderString CPU = new XContentBuilderString("cpu");
        static final XContentBuilderString PERCENT = new XContentBuilderString("percent");
        static final XContentBuilderString SYS = new XContentBuilderString("sys");
        static final XContentBuilderString SYS_IN_MILLIS = new XContentBuilderString("sys_in_millis");
        static final XContentBuilderString USER = new XContentBuilderString("user");
        static final XContentBuilderString USER_IN_MILLIS = new XContentBuilderString("user_in_millis");
        static final XContentBuilderString TOTAL = new XContentBuilderString("total");
        static final XContentBuilderString TOTAL_IN_MILLIS = new XContentBuilderString("total_in_millis");

        static final XContentBuilderString MEM = new XContentBuilderString("mem");
        static final XContentBuilderString RESIDENT = new XContentBuilderString("resident");
        static final XContentBuilderString RESIDENT_IN_BYTES = new XContentBuilderString("resident_in_bytes");
        static final XContentBuilderString SHARE = new XContentBuilderString("share");
        static final XContentBuilderString SHARE_IN_BYTES = new XContentBuilderString("share_in_bytes");
        static final XContentBuilderString TOTAL_VIRTUAL = new XContentBuilderString("total_virtual");
        static final XContentBuilderString TOTAL_VIRTUAL_IN_BYTES = new XContentBuilderString("total_virtual_in_bytes");
    }

    @Override
    public XContentBuilder toXContent(XContentBuilder builder, Params params) throws IOException {
        builder.startObject(Fields.PROCESS);
        builder.field(Fields.TIMESTAMP, timestamp);
        builder.field(Fields.OPEN_FILE_DESCRIPTORS, openFileDescriptors);
        if (cpu != null) {
            builder.startObject(Fields.CPU);
            builder.field(Fields.PERCENT, cpu.percent());
            builder.timeValueField(Fields.SYS_IN_MILLIS, Fields.SYS, cpu.sys);
            builder.timeValueField(Fields.USER_IN_MILLIS, Fields.USER, cpu.user);
            builder.timeValueField(Fields.TOTAL_IN_MILLIS, Fields.TOTAL, cpu.total);
            builder.endObject();
        }
        if (mem != null) {
            builder.startObject(Fields.MEM);
            builder.byteSizeField(Fields.RESIDENT_IN_BYTES, Fields.RESIDENT, mem.resident);
            builder.byteSizeField(Fields.SHARE_IN_BYTES, Fields.SHARE, mem.share);
            builder.byteSizeField(Fields.TOTAL_VIRTUAL_IN_BYTES, Fields.TOTAL_VIRTUAL, mem.totalVirtual);
            builder.endObject();
        }
        builder.endObject();
        return builder;
    }

    public static ProcessStats readProcessStats(StreamInput in) throws IOException {
        ProcessStats stats = new ProcessStats();
        stats.readFrom(in);
        return stats;
    }

    @Override
    public void readFrom(StreamInput in) throws IOException {
        timestamp = in.readVLong();
        openFileDescriptors = in.readLong();
        if (in.readBoolean()) {
            cpu = Cpu.readCpu(in);
        }
        if (in.readBoolean()) {
            mem = Mem.readMem(in);
        }
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        out.writeVLong(timestamp);
        out.writeLong(openFileDescriptors);
        if (cpu == null) {
            out.writeBoolean(false);
        } else {
            out.writeBoolean(true);
            cpu.writeTo(out);
        }
        if (mem == null) {
            out.writeBoolean(false);
        } else {
            out.writeBoolean(true);
            mem.writeTo(out);
        }
    }

    public static class Mem implements Streamable, Serializable {

        long totalVirtual = -1;
        long resident = -1;
        long share = -1;

        Mem() {
        }

        public static Mem readMem(StreamInput in) throws IOException {
            Mem mem = new Mem();
            mem.readFrom(in);
            return mem;
        }

        @Override
        public void readFrom(StreamInput in) throws IOException {
            totalVirtual = in.readLong();
            resident = in.readLong();
            share = in.readLong();
        }

        @Override
        public void writeTo(StreamOutput out) throws IOException {
            out.writeLong(totalVirtual);
            out.writeLong(resident);
            out.writeLong(share);
        }

        public ByteSizeValue totalVirtual() {
            return new ByteSizeValue(totalVirtual);
        }

        public ByteSizeValue getTotalVirtual() {
            return totalVirtual();
        }

        public ByteSizeValue resident() {
            return new ByteSizeValue(resident);
        }

        public ByteSizeValue getResident() {
            return resident();
        }

        public ByteSizeValue share() {
            return new ByteSizeValue(share);
        }

        public ByteSizeValue getShare() {
            return share();
        }
    }

    public static class Cpu implements Streamable, Serializable {

        short percent = -1;
        long sys = -1;
        long user = -1;
        long total = -1;

        Cpu() {

        }

        public static Cpu readCpu(StreamInput in) throws IOException {
            Cpu cpu = new Cpu();
            cpu.readFrom(in);
            return cpu;
        }

        @Override
        public void readFrom(StreamInput in) throws IOException {
            percent = in.readShort();
            sys = in.readLong();
            user = in.readLong();
            total = in.readLong();
        }

        @Override
        public void writeTo(StreamOutput out) throws IOException {
            out.writeShort(percent);
            out.writeLong(sys);
            out.writeLong(user);
            out.writeLong(total);
        }

        /**
         * Get the Process cpu usage.
         * <p/>
         * <p>Supported Platforms: All.
         */
        public short percent() {
            return percent;
        }

        /**
         * Get the Process cpu usage.
         * <p/>
         * <p>Supported Platforms: All.
         */
        public short getPercent() {
            return percent();
        }

        /**
         * Get the Process cpu kernel time.
         * <p/>
         * <p>Supported Platforms: All.
         */
        public TimeValue sys() {
            return new TimeValue(sys);
        }

        /**
         * Get the Process cpu kernel time.
         * <p/>
         * <p>Supported Platforms: All.
         */
        public TimeValue getSys() {
            return sys();
        }

        /**
         * Get the Process cpu user time.
         * <p/>
         * <p>Supported Platforms: All.
         */
        public TimeValue user() {
            return new TimeValue(user);
        }

        /**
         * Get the Process cpu time (sum of User and Sys).
         * <p/>
         * Supported Platforms: All.
         */
        public TimeValue total() {
            return new TimeValue(total);
        }

        /**
         * Get the Process cpu time (sum of User and Sys).
         * <p/>
         * Supported Platforms: All.
         */
        public TimeValue getTotal() {
            return total();
        }

        /**
         * Get the Process cpu user time.
         * <p/>
         * <p>Supported Platforms: All.
         */
        public TimeValue getUser() {
            return user();
        }

    }
}
