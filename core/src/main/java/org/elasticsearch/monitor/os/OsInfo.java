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
import java.io.Serializable;

/**
 *
 */
public class OsInfo implements Streamable, Serializable, ToXContent {

    long refreshInterval;

    int availableProcessors;

    Cpu cpu = null;

    Mem mem = null;

    Swap swap = null;

    OsInfo() {
    }

    public long refreshInterval() {
        return this.refreshInterval;
    }

    public long getRefreshInterval() {
        return this.refreshInterval;
    }

    public int availableProcessors() {
        return this.availableProcessors;
    }

    public int getAvailableProcessors() {
        return this.availableProcessors;
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
        static final XContentBuilderString REFRESH_INTERVAL = new XContentBuilderString("refresh_interval");
        static final XContentBuilderString REFRESH_INTERVAL_IN_MILLIS = new XContentBuilderString("refresh_interval_in_millis");
        static final XContentBuilderString AVAILABLE_PROCESSORS = new XContentBuilderString("available_processors");
        static final XContentBuilderString CPU = new XContentBuilderString("cpu");
        static final XContentBuilderString VENDOR = new XContentBuilderString("vendor");
        static final XContentBuilderString MODEL = new XContentBuilderString("model");
        static final XContentBuilderString MHZ = new XContentBuilderString("mhz");
        static final XContentBuilderString TOTAL_CORES = new XContentBuilderString("total_cores");
        static final XContentBuilderString TOTAL_SOCKETS = new XContentBuilderString("total_sockets");
        static final XContentBuilderString CORES_PER_SOCKET = new XContentBuilderString("cores_per_socket");
        static final XContentBuilderString CACHE_SIZE = new XContentBuilderString("cache_size");
        static final XContentBuilderString CACHE_SIZE_IN_BYTES = new XContentBuilderString("cache_size_in_bytes");

        static final XContentBuilderString MEM = new XContentBuilderString("mem");
        static final XContentBuilderString SWAP = new XContentBuilderString("swap");
        static final XContentBuilderString TOTAL = new XContentBuilderString("total");
        static final XContentBuilderString TOTAL_IN_BYTES = new XContentBuilderString("total_in_bytes");
    }

    @Override
    public XContentBuilder toXContent(XContentBuilder builder, Params params) throws IOException {
        builder.startObject(Fields.OS);
        builder.timeValueField(Fields.REFRESH_INTERVAL_IN_MILLIS, Fields.REFRESH_INTERVAL, refreshInterval);
        builder.field(Fields.AVAILABLE_PROCESSORS, availableProcessors);
        if (cpu != null) {
            builder.startObject(Fields.CPU);
            cpu.toXContent(builder, params);
            builder.endObject();
        }
        if (mem != null) {
            builder.startObject(Fields.MEM);
            builder.byteSizeField(Fields.TOTAL_IN_BYTES, Fields.TOTAL, mem.total);
            builder.endObject();
        }
        if (swap != null) {
            builder.startObject(Fields.SWAP);
            builder.byteSizeField(Fields.TOTAL_IN_BYTES, Fields.TOTAL, swap.total);
            builder.endObject();
        }
        builder.endObject();
        return builder;
    }

    public static OsInfo readOsInfo(StreamInput in) throws IOException {
        OsInfo info = new OsInfo();
        info.readFrom(in);
        return info;
    }

    @Override
    public void readFrom(StreamInput in) throws IOException {
        refreshInterval = in.readLong();
        availableProcessors = in.readInt();
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
        out.writeLong(refreshInterval);
        out.writeInt(availableProcessors);
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

        long total = -1;

        Swap() {

        }

        public static Swap readSwap(StreamInput in) throws IOException {
            Swap swap = new Swap();
            swap.readFrom(in);
            return swap;
        }

        @Override
        public void readFrom(StreamInput in) throws IOException {
            total = in.readLong();
        }

        @Override
        public void writeTo(StreamOutput out) throws IOException {
            out.writeLong(total);
        }

        public ByteSizeValue total() {
            return new ByteSizeValue(total);
        }

        public ByteSizeValue getTotal() {
            return total();
        }

    }

    public static class Mem implements Streamable, Serializable {

        long total = -1;

        Mem() {

        }

        public static Mem readMem(StreamInput in) throws IOException {
            Mem mem = new Mem();
            mem.readFrom(in);
            return mem;
        }

        @Override
        public void readFrom(StreamInput in) throws IOException {
            total = in.readLong();
        }

        @Override
        public void writeTo(StreamOutput out) throws IOException {
            out.writeLong(total);
        }

        public ByteSizeValue total() {
            return new ByteSizeValue(total);
        }

        public ByteSizeValue getTotal() {
            return total();
        }

    }

    public static class Cpu implements Streamable, Serializable, ToXContent {

        String vendor = "";
        String model = "";
        int mhz = -1;
        int totalCores = -1;
        int totalSockets = -1;
        int coresPerSocket = -1;
        long cacheSize = -1;

        Cpu() {

        }

        public String vendor() {
            return this.vendor;
        }

        public String getVendor() {
            return vendor();
        }

        public String model() {
            return model;
        }

        public String getModel() {
            return model;
        }

        public int mhz() {
            return mhz;
        }

        public int getMhz() {
            return mhz;
        }

        public int totalCores() {
            return totalCores;
        }

        public int getTotalCores() {
            return totalCores();
        }

        public int totalSockets() {
            return totalSockets;
        }

        public int getTotalSockets() {
            return totalSockets();
        }

        public int coresPerSocket() {
            return coresPerSocket;
        }

        public int getCoresPerSocket() {
            return coresPerSocket();
        }

        public ByteSizeValue cacheSize() {
            return new ByteSizeValue(cacheSize);
        }

        public ByteSizeValue getCacheSize() {
            return cacheSize();
        }

        public static Cpu readCpu(StreamInput in) throws IOException {
            Cpu cpu = new Cpu();
            cpu.readFrom(in);
            return cpu;
        }

        @Override
        public void readFrom(StreamInput in) throws IOException {
            vendor = in.readString();
            model = in.readString();
            mhz = in.readInt();
            totalCores = in.readInt();
            totalSockets = in.readInt();
            coresPerSocket = in.readInt();
            cacheSize = in.readLong();
        }

        @Override
        public void writeTo(StreamOutput out) throws IOException {
            out.writeString(vendor);
            out.writeString(model);
            out.writeInt(mhz);
            out.writeInt(totalCores);
            out.writeInt(totalSockets);
            out.writeInt(coresPerSocket);
            out.writeLong(cacheSize);
        }

        @Override
        public boolean equals(Object o) {
            if (this == o) {
                return true;
            }
            if (o == null || getClass() != o.getClass()) {
                return false;
            }

            Cpu cpu = (Cpu) o;

            return model.equals(cpu.model) && vendor.equals(cpu.vendor);
        }

        @Override
        public int hashCode() {
            return model.hashCode();
        }

        @Override
        public XContentBuilder toXContent(XContentBuilder builder, Params params) throws IOException {
            builder.field(Fields.VENDOR, vendor);
            builder.field(Fields.MODEL, model);
            builder.field(Fields.MHZ, mhz);
            builder.field(Fields.TOTAL_CORES, totalCores);
            builder.field(Fields.TOTAL_SOCKETS, totalSockets);
            builder.field(Fields.CORES_PER_SOCKET, coresPerSocket);
            builder.byteSizeField(Fields.CACHE_SIZE_IN_BYTES, Fields.CACHE_SIZE, cacheSize);
            return builder;
        }
    }
}
