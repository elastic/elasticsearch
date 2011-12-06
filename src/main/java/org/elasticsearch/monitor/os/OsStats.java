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

package org.elasticsearch.monitor.os;

import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.common.io.stream.Streamable;
import org.elasticsearch.common.unit.ByteSizeValue;
import org.elasticsearch.common.unit.TimeValue;
import org.elasticsearch.common.xcontent.ToXContent;
import org.elasticsearch.common.xcontent.XContentBuilder;

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

    @Override
    public XContentBuilder toXContent(XContentBuilder builder, Params params) throws IOException {
        builder.startObject("os");
        builder.field("timestamp", timestamp);

        builder.field("uptime", uptime().format());
        builder.field("uptime_in_millis", uptime().millis());

        builder.startArray("load_average");
        for (double value : loadAverage) {
            builder.value(value);
        }
        builder.endArray();

        if (cpu != null) {
            builder.startObject("cpu");
            builder.field("sys", cpu.sys());
            builder.field("user", cpu.user());
            builder.field("idle", cpu.idle());
            builder.endObject();
        }

        if (mem != null) {
            builder.startObject("mem");
            builder.field("free", mem.free().toString());
            builder.field("free_in_bytes", mem.free().bytes());
            builder.field("used", mem.used().toString());
            builder.field("used_in_bytes", mem.used().bytes());

            builder.field("free_percent", mem.freePercent());
            builder.field("used_percent", mem.usedPercent());

            builder.field("actual_free", mem.actualFree().toString());
            builder.field("actual_free_in_bytes", mem.actualFree().bytes());
            builder.field("actual_used", mem.actualUsed().toString());
            builder.field("actual_used_in_bytes", mem.actualUsed().bytes());

            builder.endObject();
        }

        if (swap != null) {
            builder.startObject("swap");
            builder.field("used", swap.used().toString());
            builder.field("used_in_bytes", swap.used().bytes());
            builder.field("free", swap.free().toString());
            builder.field("free_in_bytes", swap.free().bytes());
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
        }

        @Override
        public void writeTo(StreamOutput out) throws IOException {
            out.writeShort(sys);
            out.writeShort(user);
            out.writeShort(idle);
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
    }
}
