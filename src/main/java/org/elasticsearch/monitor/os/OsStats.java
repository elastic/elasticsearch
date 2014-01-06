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

package org.elasticsearch.monitor.os;

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
import java.util.concurrent.TimeUnit;

/**
 *
 */
public class OsStats implements Streamable, Serializable, ToXContent {

    public static final double[] EMPTY_LOAD = new double[0];


    long timestamp;

    double[] loadAverage = EMPTY_LOAD;

    long uptime = -1;

    Cpu cpu = null;

    Mem mem = null;

    Swap swap = null;

    OsStats() {
    }

    public long timestamp() {
        return timestamp;
    }

    public long getTimestamp() {
        return timestamp();
    }

    public double[] loadAverage() {
        return loadAverage;
    }

    public double[] getLoadAverage() {
        return loadAverage();
    }

    public TimeValue uptime() {
        return new TimeValue(uptime, TimeUnit.SECONDS);
    }

    public TimeValue getUptime() {
        return uptime();
    }

    public Cpu cpu() {
        return this.cpu;
    }

    public Cpu getCpu() {
        return cpu();
    }

    public Mem mem() {
        return this.mem;
    }

    public Mem getMem() {
        return mem();
    }

    public Swap swap() {
        return this.swap;
    }

    public Swap getSwap() {
        return swap();
    }

    static final class Fields {
        static final XContentBuilderString OS = new XContentBuilderString("os");
        static final XContentBuilderString TIMESTAMP = new XContentBuilderString("timestamp");
        static final XContentBuilderString UPTIME = new XContentBuilderString("uptime");
        static final XContentBuilderString UPTIME_IN_MILLIS = new XContentBuilderString("uptime_in_millis");
        static final XContentBuilderString LOAD_AVERAGE = new XContentBuilderString("load_average");
        static final XContentBuilderString LOAD_AVERAGE_1m = new XContentBuilderString("1m");
        static final XContentBuilderString LOAD_AVERAGE_5m = new XContentBuilderString("5m");
        static final XContentBuilderString LOAD_AVERAGE_15m = new XContentBuilderString("15m");

        static final XContentBuilderString CPU = new XContentBuilderString("cpu");
        static final XContentBuilderString SYS = new XContentBuilderString("sys");
        static final XContentBuilderString USER = new XContentBuilderString("user");
        static final XContentBuilderString USAGE = new XContentBuilderString("usage");
        static final XContentBuilderString IDLE = new XContentBuilderString("idle");
        static final XContentBuilderString STOLEN = new XContentBuilderString("stolen");

        static final XContentBuilderString MEM = new XContentBuilderString("mem");
        static final XContentBuilderString SWAP = new XContentBuilderString("swap");
        static final XContentBuilderString FREE = new XContentBuilderString("free");
        static final XContentBuilderString FREE_IN_BYTES = new XContentBuilderString("free_in_bytes");
        static final XContentBuilderString USED = new XContentBuilderString("used");
        static final XContentBuilderString USED_IN_BYTES = new XContentBuilderString("used_in_bytes");

        static final XContentBuilderString FREE_PERCENT = new XContentBuilderString("free_percent");
        static final XContentBuilderString USED_PERCENT = new XContentBuilderString("used_percent");

        static final XContentBuilderString ACTUAL_FREE = new XContentBuilderString("actual_free");
        static final XContentBuilderString ACTUAL_FREE_IN_BYTES = new XContentBuilderString("actual_free_in_bytes");
        static final XContentBuilderString ACTUAL_USED = new XContentBuilderString("actual_used");
        static final XContentBuilderString ACTUAL_USED_IN_BYTES = new XContentBuilderString("actual_used_in_bytes");
    }

    @Override
    public XContentBuilder toXContent(XContentBuilder builder, Params params) throws IOException {
        builder.startObject(Fields.OS);
        builder.field(Fields.TIMESTAMP, timestamp);

        if (uptime != -1) {
            builder.timeValueField(Fields.UPTIME_IN_MILLIS, Fields.UPTIME, uptime);
        }

        if (loadAverage.length > 0) {
            if (params.param("load_average_format", "array").equals("hash")) {
                builder.startObject(Fields.LOAD_AVERAGE);
                builder.field(Fields.LOAD_AVERAGE_1m, loadAverage[0]);
                builder.field(Fields.LOAD_AVERAGE_5m, loadAverage[1]);
                builder.field(Fields.LOAD_AVERAGE_15m, loadAverage[2]);
                builder.endObject();
            } else {
                builder.startArray(Fields.LOAD_AVERAGE);
                for (double value : loadAverage) {
                    builder.value(value);
                }
                builder.endArray();
            }
        }

        if (cpu != null) {
            builder.startObject(Fields.CPU);
            builder.field(Fields.SYS, cpu.sys());
            builder.field(Fields.USER, cpu.user());
            builder.field(Fields.IDLE, cpu.idle());
            builder.field(Fields.USAGE, cpu.user() + cpu.sys());
            builder.field(Fields.STOLEN, cpu.stolen());
            builder.endObject();
        }

        if (mem != null) {
            builder.startObject(Fields.MEM);
            builder.byteSizeField(Fields.FREE_IN_BYTES, Fields.FREE, mem.free);
            builder.byteSizeField(Fields.USED_IN_BYTES, Fields.USED, mem.used);

            builder.field(Fields.FREE_PERCENT, mem.freePercent());
            builder.field(Fields.USED_PERCENT, mem.usedPercent());

            builder.byteSizeField(Fields.ACTUAL_FREE_IN_BYTES, Fields.ACTUAL_FREE, mem.actualFree);
            builder.byteSizeField(Fields.ACTUAL_USED_IN_BYTES, Fields.ACTUAL_USED, mem.actualUsed);

            builder.endObject();
        }

        if (swap != null) {
            builder.startObject(Fields.SWAP);
            builder.byteSizeField(Fields.USED_IN_BYTES, Fields.USED, swap.used);
            builder.byteSizeField(Fields.FREE_IN_BYTES, Fields.FREE, swap.free);
            builder.endObject();
        }

        builder.endObject();
        return builder;
    }

    public static OsStats readOsStats(StreamInput in) throws IOException {
        OsStats stats = new OsStats();
        stats.readFrom(in);
        return stats;
    }

    @Override
    public void readFrom(StreamInput in) throws IOException {
        timestamp = in.readVLong();
        loadAverage = new double[in.readVInt()];
        for (int i = 0; i < loadAverage.length; i++) {
            loadAverage[i] = in.readDouble();
        }
        uptime = in.readLong();
        if (in.readBoolean()) {
            cpu = Cpu.readCpu(in);
        }
        if (in.readBoolean()) {
            mem = Mem.readMem(in);
        }
        if (in.readBoolean()) {
            swap = Swap.readSwap(in);
        }
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        out.writeVLong(timestamp);
        out.writeVInt(loadAverage.length);
        for (double val : loadAverage) {
            out.writeDouble(val);
        }
        out.writeLong(uptime);
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
        if (swap == null) {
            out.writeBoolean(false);
        } else {
            out.writeBoolean(true);
            swap.writeTo(out);
        }
    }

    public static class Swap implements Streamable, Serializable {

        long free = -1;
        long used = -1;

        public ByteSizeValue free() {
            return new ByteSizeValue(free);
        }

        public ByteSizeValue getFree() {
            return free();
        }

        public ByteSizeValue used() {
            return new ByteSizeValue(used);
        }

        public ByteSizeValue getUsed() {
            return used();
        }

        public static Swap readSwap(StreamInput in) throws IOException {
            Swap swap = new Swap();
            swap.readFrom(in);
            return swap;
        }

        @Override
        public void readFrom(StreamInput in) throws IOException {
            free = in.readLong();
            used = in.readLong();
        }

        @Override
        public void writeTo(StreamOutput out) throws IOException {
            out.writeLong(free);
            out.writeLong(used);
        }
    }

    public static class Mem implements Streamable, Serializable {

        long free = -1;
        short freePercent = -1;
        long used = -1;
        short usedPercent = -1;
        long actualFree = -1;
        long actualUsed = -1;

        public static Mem readMem(StreamInput in) throws IOException {
            Mem mem = new Mem();
            mem.readFrom(in);
            return mem;
        }

        @Override
        public void readFrom(StreamInput in) throws IOException {
            free = in.readLong();
            freePercent = in.readShort();
            used = in.readLong();
            usedPercent = in.readShort();
            actualFree = in.readLong();
            actualUsed = in.readLong();
        }

        @Override
        public void writeTo(StreamOutput out) throws IOException {
            out.writeLong(free);
            out.writeShort(freePercent);
            out.writeLong(used);
            out.writeShort(usedPercent);
            out.writeLong(actualFree);
            out.writeLong(actualUsed);
        }

        public ByteSizeValue used() {
            return new ByteSizeValue(used);
        }

        public ByteSizeValue getUsed() {
            return used();
        }

        public short usedPercent() {
            return usedPercent;
        }

        public short getUsedPercent() {
            return usedPercent();
        }

        public ByteSizeValue free() {
            return new ByteSizeValue(free);
        }

        public ByteSizeValue getFree() {
            return free();
        }

        public short freePercent() {
            return freePercent;
        }

        public short getFreePercent() {
            return freePercent();
        }

        public ByteSizeValue actualFree() {
            return new ByteSizeValue(actualFree);
        }

        public ByteSizeValue getActualFree() {
            return actualFree();
        }

        public ByteSizeValue actualUsed() {
            return new ByteSizeValue(actualUsed);
        }

        public ByteSizeValue getActualUsed() {
            return actualUsed();
        }
    }

    public static class Cpu implements Streamable, Serializable {

        short sys = -1;
        short user = -1;
        short idle = -1;
        short stolen = -1;

        Cpu() {

        }

        public static Cpu readCpu(StreamInput in) throws IOException {
            Cpu cpu = new Cpu();
            cpu.readFrom(in);
            return cpu;
        }

        @Override
        public void readFrom(StreamInput in) throws IOException {
            sys = in.readShort();
            user = in.readShort();
            idle = in.readShort();
            stolen = in.readShort();
        }

        @Override
        public void writeTo(StreamOutput out) throws IOException {
            out.writeShort(sys);
            out.writeShort(user);
            out.writeShort(idle);
            out.writeShort(stolen);
        }

        public short sys() {
            return sys;
        }

        public short getSys() {
            return sys();
        }

        public short user() {
            return user;
        }

        public short getUser() {
            return user();
        }

        public short idle() {
            return idle;
        }

        public short getIdle() {
            return idle();
        }

        public short stolen() {
            return stolen;
        }

        public short getStolen() {
            return stolen();
        }
    }
}
