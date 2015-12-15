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
import org.elasticsearch.common.xcontent.XContentBuilderString;

import java.io.IOException;

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
        static final XContentBuilderString OS = new XContentBuilderString("os");
        static final XContentBuilderString TIMESTAMP = new XContentBuilderString("timestamp");
        static final XContentBuilderString CPU = new XContentBuilderString("cpu");
        static final XContentBuilderString PERCENT = new XContentBuilderString("percent");
        static final XContentBuilderString LOAD_AVERAGE = new XContentBuilderString("load_average");

        static final XContentBuilderString MEM = new XContentBuilderString("mem");
        static final XContentBuilderString SWAP = new XContentBuilderString("swap");
        static final XContentBuilderString FREE = new XContentBuilderString("free");
        static final XContentBuilderString FREE_IN_BYTES = new XContentBuilderString("free_in_bytes");
        static final XContentBuilderString USED = new XContentBuilderString("used");
        static final XContentBuilderString USED_IN_BYTES = new XContentBuilderString("used_in_bytes");
        static final XContentBuilderString TOTAL = new XContentBuilderString("total");
        static final XContentBuilderString TOTAL_IN_BYTES = new XContentBuilderString("total_in_bytes");

        static final XContentBuilderString FREE_PERCENT = new XContentBuilderString("free_percent");
        static final XContentBuilderString USED_PERCENT = new XContentBuilderString("used_percent");

    }

    @Override
    public XContentBuilder toXContent(XContentBuilder builder, Params params) throws IOException {
        builder.startObject(Fields.OS);
        builder.field(Fields.TIMESTAMP, getTimestamp());
        if (cpu != null) {
            builder.startObject(Fields.CPU);
            builder.field(Fields.PERCENT, cpu.getPercent());
            builder.field(Fields.LOAD_AVERAGE, cpu.getLoadAverage());
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
        double loadAverage = -1;

        Cpu() {}

        public static Cpu readCpu(StreamInput in) throws IOException {
            Cpu cpu = new Cpu();
            cpu.readFrom(in);
            return cpu;
        }

        @Override
        public void readFrom(StreamInput in) throws IOException {
            percent = in.readShort();
            loadAverage = in.readDouble();
        }

        @Override
        public void writeTo(StreamOutput out) throws IOException {
            out.writeShort(percent);
            out.writeDouble(loadAverage);
        }

        public short getPercent() {
            return percent;
        }

        public double getLoadAverage() {
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
