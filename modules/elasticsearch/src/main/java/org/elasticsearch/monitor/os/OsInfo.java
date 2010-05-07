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

import org.elasticsearch.util.SizeValue;
import org.elasticsearch.util.io.stream.StreamInput;
import org.elasticsearch.util.io.stream.StreamOutput;
import org.elasticsearch.util.io.stream.Streamable;

import java.io.IOException;
import java.io.Serializable;

/**
 * @author kimchy (shay.banon)
 */
public class OsInfo implements Streamable, Serializable {

    Cpu cpu = null;

    Mem mem = null;

    Swap swap = null;

    OsInfo() {
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

    public static OsInfo readOsInfo(StreamInput in) throws IOException {
        OsInfo info = new OsInfo();
        info.readFrom(in);
        return info;
    }

    @Override public void readFrom(StreamInput in) throws IOException {
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

        public SizeValue total() {
            return new SizeValue(total);
        }

        public SizeValue getTotal() {
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

        public SizeValue total() {
            return new SizeValue(total);
        }

        public SizeValue getTotal() {
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
