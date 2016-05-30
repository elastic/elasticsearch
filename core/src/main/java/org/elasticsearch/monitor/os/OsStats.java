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
import org.elasticsearch.common.xcontent.ToXContent;
import org.elasticsearch.common.xcontent.XContentBuilder;

import java.io.IOException;
import java.util.Arrays;

/**
 *
 */
public class OsStats implements Streamable, ToXContent {

    long timestamp;

    Cpu cpu = null;

    Mem mem = null;

    Swap swap = null;

    OsStats() {
    }

    public long getTimestamp() {
        return timestamp;
    }

    public Cpu getCpu() { return cpu; }

    public Mem getMem() {
        return mem;
    }

    public Swap getSwap() {
        return swap;
    }

    static final class Fields {
        static final String OS = "os";
        static final String TIMESTAMP = "timestamp";
        static final String CPU = "cpu";
        static final String PERCENT = "percent";
        static final String LOAD_AVERAGE = "load_average";
        static final String LOAD_AVERAGE_1M = new String("1m");
        static final String LOAD_AVERAGE_5M = new String("5m");
        static final String LOAD_AVERAGE_15M = new String("15m");

        static final String MEM = "mem";
        static final String SWAP = "swap";
        static final String FREE = "free";
        static final String FREE_IN_BYTES = "free_in_bytes";
        static final String USED = "used";
        static final String USED_IN_BYTES = "used_in_bytes";
        static final String TOTAL = "total";
        static final String TOTAL_IN_BYTES = "total_in_bytes";

        static final String FREE_PERCENT = "free_percent";
        static final String USED_PERCENT = "used_percent";
    }

    @Override
    public XContentBuilder toXContent(XContentBuilder builder, Params params) throws IOException {
        builder.startObject(Fields.OS);
        builder.field(Fields.TIMESTAMP, getTimestamp());
        if (cpu != null) {
            builder.startObject(Fields.CPU);
            builder.field(Fields.PERCENT, cpu.getPercent());
            if (cpu.getLoadAverage() != null && Arrays.stream(cpu.getLoadAverage()).anyMatch(load -> load != -1)) {
                builder.startObject(Fields.LOAD_AVERAGE);
                if (cpu.getLoadAverage()[0] != -1) {
                    builder.field(Fields.LOAD_AVERAGE_1M, cpu.getLoadAverage()[0]);
                }
                if (cpu.getLoadAverage()[1] != -1) {
                    builder.field(Fields.LOAD_AVERAGE_5M, cpu.getLoadAverage()[1]);
                }
                if (cpu.getLoadAverage()[2] != -1) {
                    builder.field(Fields.LOAD_AVERAGE_15M, cpu.getLoadAverage()[2]);
                }
                builder.endObject();
            }
            builder.endObject();
        }

        if (mem != null) {
            builder.startObject(Fields.MEM);
            builder.byteSizeField(Fields.TOTAL_IN_BYTES, Fields.TOTAL, mem.getTotal());
            builder.byteSizeField(Fields.FREE_IN_BYTES, Fields.FREE, mem.getFree());
            builder.byteSizeField(Fields.USED_IN_BYTES, Fields.USED, mem.getUsed());

            builder.field(Fields.FREE_PERCENT, mem.getFreePercent());
            builder.field(Fields.USED_PERCENT, mem.getUsedPercent());

            builder.endObject();
        }

        if (swap != null) {
            builder.startObject(Fields.SWAP);
            builder.byteSizeField(Fields.TOTAL_IN_BYTES, Fields.TOTAL, swap.getTotal());
            builder.byteSizeField(Fields.FREE_IN_BYTES, Fields.FREE, swap.getFree());
            builder.byteSizeField(Fields.USED_IN_BYTES, Fields.USED, swap.getUsed());
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
        cpu = in.readOptionalStreamable(Cpu::new);
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
        out.writeOptionalStreamable(cpu);
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

    public static class Cpu implements Streamable {

        short percent = -1;
        double[] loadAverage = null;

        Cpu() {}

        public static Cpu readCpu(StreamInput in) throws IOException {
            Cpu cpu = new Cpu();
            cpu.readFrom(in);
            return cpu;
        }

        @Override
        public void readFrom(StreamInput in) throws IOException {
            percent = in.readShort();
            if (in.readBoolean()) {
                loadAverage = in.readDoubleArray();
            } else {
                loadAverage = null;
            }
        }

        @Override
        public void writeTo(StreamOutput out) throws IOException {
            out.writeShort(percent);
            if (loadAverage == null) {
                out.writeBoolean(false);
            } else {
                out.writeBoolean(true);
                out.writeDoubleArray(loadAverage);
            }
        }

        public short getPercent() {
            return percent;
        }

        public double[] getLoadAverage() {
            return loadAverage;
        }
    }

    public static class Swap implements Streamable {

        long total = -1;
        long free = -1;

        public ByteSizeValue getFree() {
            return new ByteSizeValue(free);
        }

        public ByteSizeValue getUsed() {
            return new ByteSizeValue(total - free);
        }

        public ByteSizeValue getTotal() {
            return new ByteSizeValue(total);
        }

        public static Swap readSwap(StreamInput in) throws IOException {
            Swap swap = new Swap();
            swap.readFrom(in);
            return swap;
        }

        @Override
        public void readFrom(StreamInput in) throws IOException {
            total = in.readLong();
            free = in.readLong();
        }

        @Override
        public void writeTo(StreamOutput out) throws IOException {
            out.writeLong(total);
            out.writeLong(free);
        }
    }

    public static class Mem implements Streamable {

        long total = -1;
        long free = -1;

        public static Mem readMem(StreamInput in) throws IOException {
            Mem mem = new Mem();
            mem.readFrom(in);
            return mem;
        }

        @Override
        public void readFrom(StreamInput in) throws IOException {
            total = in.readLong();
            free = in.readLong();
        }

        @Override
        public void writeTo(StreamOutput out) throws IOException {
            out.writeLong(total);
            out.writeLong(free);
        }

        public ByteSizeValue getTotal() {
            return new ByteSizeValue(total);
        }

        public ByteSizeValue getUsed() {
            return new ByteSizeValue(total - free);
        }

        public short getUsedPercent() {
            return calculatePercentage(getUsed().bytes(), getTotal().bytes());
        }

        public ByteSizeValue getFree() {
            return new ByteSizeValue(free);
        }

        public short getFreePercent() {
            return calculatePercentage(getFree().bytes(), getTotal().bytes());
        }
    }

    private static short calculatePercentage(long used, long max) {
        return max <= 0 ? 0 : (short) (Math.round((100d * used) / max));
    }
}
