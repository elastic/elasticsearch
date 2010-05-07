/*
 * Licensed to Elastic Search and Shay Banon under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership. Elastic Search licenses this
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

import org.elasticsearch.util.Percent;
import org.elasticsearch.util.SizeValue;
import org.elasticsearch.util.TimeValue;
import org.elasticsearch.util.io.stream.StreamInput;
import org.elasticsearch.util.io.stream.StreamOutput;
import org.elasticsearch.util.io.stream.Streamable;

import java.io.IOException;
import java.io.Serializable;
import java.util.concurrent.TimeUnit;

/**
 * @author kimchy (shay.banon)
 */
public class OsStats implements Streamable, Serializable {

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

    public static OsStats readOsStats(StreamInput in) throws IOException {
        OsStats stats = new OsStats();
        stats.readFrom(in);
        return stats;
    }

    @Override public void readFrom(StreamInput in) throws IOException {
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

    @Override public void writeTo(StreamOutput out) throws IOException {
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

        public static Swap readSwap(StreamInput in) throws IOException {
            Swap swap = new Swap();
            swap.readFrom(in);
            return swap;
        }

        @Override public void readFrom(StreamInput in) throws IOException {
            free = in.readLong();
            used = in.readLong();
        }

        @Override public void writeTo(StreamOutput out) throws IOException {
            out.writeLong(free);
            out.writeLong(used);
        }
    }

    public static class Mem implements Streamable, Serializable {

        long free = -1;
        double freePercent = -1;
        long used = -1;
        double usedPercent = -1;
        long actualFree = -1;
        long actualUsed = -1;

        public static Mem readMem(StreamInput in) throws IOException {
            Mem mem = new Mem();
            mem.readFrom(in);
            return mem;
        }

        @Override public void readFrom(StreamInput in) throws IOException {
            free = in.readLong();
            freePercent = in.readDouble();
            used = in.readLong();
            usedPercent = in.readDouble();
            actualFree = in.readLong();
            actualUsed = in.readLong();
        }

        @Override public void writeTo(StreamOutput out) throws IOException {
            out.writeLong(free);
            out.writeDouble(freePercent);
            out.writeLong(used);
            out.writeDouble(usedPercent);
            out.writeLong(actualFree);
            out.writeLong(actualUsed);
        }

        public SizeValue used() {
            return new SizeValue(used);
        }

        public SizeValue getUsed() {
            return used();
        }

        public Percent usedPercent() {
            return new Percent(usedPercent);
        }

        public Percent getUsedPercent() {
            return usedPercent();
        }

        public SizeValue free() {
            return new SizeValue(free);
        }

        public SizeValue getFree() {
            return free();
        }

        public Percent freePercent() {
            return new Percent(freePercent);
        }

        public Percent getFreePercent() {
            return freePercent();
        }

        public SizeValue actualFree() {
            return new SizeValue(actualFree);
        }

        public SizeValue getActualFree() {
            return actualFree();
        }

        public SizeValue actualUsed() {
            return new SizeValue(actualUsed);
        }

        public SizeValue getActualUsed() {
            return actualUsed();
        }
    }

    public static class Cpu implements Streamable, Serializable {

        double sys = -1;
        double user = -1;
        double idle = -1;

        Cpu() {

        }

        public static Cpu readCpu(StreamInput in) throws IOException {
            Cpu cpu = new Cpu();
            cpu.readFrom(in);
            return cpu;
        }

        @Override public void readFrom(StreamInput in) throws IOException {
            sys = in.readDouble();
            user = in.readDouble();
            idle = in.readDouble();
        }

        @Override public void writeTo(StreamOutput out) throws IOException {
            out.writeDouble(sys);
            out.writeDouble(user);
            out.writeDouble(idle);
        }

        public Percent sys() {
            return new Percent(sys);
        }

        public Percent getSys() {
            return sys();
        }

        public Percent user() {
            return new Percent(user);
        }

        public Percent getUser() {
            return user();
        }

        public Percent idle() {
            return new Percent(idle);
        }

        public Percent getIdle() {
            return idle();
        }
    }
}
