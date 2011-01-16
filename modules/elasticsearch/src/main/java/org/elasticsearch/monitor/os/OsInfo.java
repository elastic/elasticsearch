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

import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.common.io.stream.Streamable;
import org.elasticsearch.common.unit.ByteSizeValue;
import org.elasticsearch.common.xcontent.ToXContent;
import org.elasticsearch.common.xcontent.XContentBuilder;

import java.io.IOException;
import java.io.Serializable;

/**
 * @author kimchy (shay.banon)
 */
public class OsInfo implements Streamable, Serializable, ToXContent {

    long refreshInterval;

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

    @Override public XContentBuilder toXContent(XContentBuilder builder, Params params) throws IOException {
        builder.startObject("os");
        builder.field("refresh_interval", refreshInterval);
        if (cpu != null) {
            builder.startObject("cpu");
            builder.field("vendor", cpu.vendor());
            builder.field("model", cpu.model());
            builder.field("mhz", cpu.mhz());
            builder.field("total_cores", cpu.totalCores());
            builder.field("total_sockets", cpu.totalSockets());
            builder.field("cores_per_socket", cpu.coresPerSocket());
            builder.field("cache_size", cpu.cacheSize().toString());
            builder.field("cache_size_in_bytes", cpu.cacheSize().bytes());
            builder.endObject();
        }
        if (mem != null) {
            builder.startObject("mem");
            builder.field("total", mem.total().toString());
            builder.field("total_in_bytes", mem.total().bytes());
            builder.endObject();
        }
        if (swap != null) {
            builder.startObject("swap");
            builder.field("total", swap.total().toString());
            builder.field("total_in_bytes", swap.total().bytes());
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

    @Override public void readFrom(StreamInput in) throws IOException {
        refreshInterval = in.readLong();
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
        out.writeLong(refreshInterval);
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

        @Override public void readFrom(StreamInput in) throws IOException {
            total = in.readLong();
        }

        @Override public void writeTo(StreamOutput out) throws IOException {
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

        @Override public void readFrom(StreamInput in) throws IOException {
            total = in.readLong();
        }

        @Override public void writeTo(StreamOutput out) throws IOException {
            out.writeLong(total);
        }

        public ByteSizeValue total() {
            return new ByteSizeValue(total);
        }

        public ByteSizeValue getTotal() {
            return total();
        }

    }

    public static class Cpu implements Streamable, Serializable {

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

        @Override public void readFrom(StreamInput in) throws IOException {
            vendor = in.readUTF();
            model = in.readUTF();
            mhz = in.readInt();
            totalCores = in.readInt();
            totalSockets = in.readInt();
            coresPerSocket = in.readInt();
            cacheSize = in.readLong();
        }

        @Override public void writeTo(StreamOutput out) throws IOException {
            out.writeUTF(vendor);
            out.writeUTF(model);
            out.writeInt(mhz);
            out.writeInt(totalCores);
            out.writeInt(totalSockets);
            out.writeInt(coresPerSocket);
            out.writeLong(cacheSize);
        }
    }
}
