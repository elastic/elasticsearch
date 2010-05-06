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

    String cpuVendor = "";

    String cpuModel = "";

    int cpuMhz = -1;

    int cpuTotalCores = -1;

    int cpuTotalSockets = -1;

    int cpuCoresPerSocket = -1;

    long cpuCacheSize = -1;

    long memTotal = -1;

    long swapTotal = -1;

    OsInfo() {
    }

    public String cpuVendor() {
        return cpuVendor;
    }

    public String getCpuVendor() {
        return cpuVendor();
    }

    public String cpuModel() {
        return cpuModel;
    }

    public String getCpuModel() {
        return cpuModel();
    }

    public int cpuMhz() {
        return cpuMhz;
    }

    public int getCpuMhz() {
        return cpuMhz();
    }

    public int cpuTotalCores() {
        return cpuTotalCores;
    }

    public int getCpuTotalCores() {
        return cpuTotalCores();
    }

    public int cpuTotalSockets() {
        return cpuTotalSockets;
    }

    public int getCpuTotalSockets() {
        return cpuTotalSockets();
    }

    public int cpuCoresPerSocket() {
        return cpuCoresPerSocket;
    }

    public int getCpuCoresPerSocket() {
        return cpuCoresPerSocket();
    }

    public SizeValue cpuCacheSize() {
        return new SizeValue(cpuCacheSize);
    }

    public SizeValue getCpuCacheSize() {
        return cpuCacheSize();
    }

    public SizeValue memTotal() {
        return new SizeValue(memTotal);
    }

    public SizeValue getMemTotal() {
        return memTotal();
    }

    public SizeValue swapTotal() {
        return new SizeValue(swapTotal);
    }

    public SizeValue getSwapTotal() {
        return swapTotal();
    }

    public static OsInfo readOsInfo(StreamInput in) throws IOException {
        OsInfo info = new OsInfo();
        info.readFrom(in);
        return info;
    }

    @Override public void readFrom(StreamInput in) throws IOException {
        cpuVendor = in.readUTF();
        cpuModel = in.readUTF();
        cpuMhz = in.readInt();
        cpuTotalCores = in.readInt();
        cpuTotalSockets = in.readInt();
        cpuCoresPerSocket = in.readInt();
        cpuCacheSize = in.readLong();
        memTotal = in.readLong();
        swapTotal = in.readLong();
    }

    @Override public void writeTo(StreamOutput out) throws IOException {
        out.writeUTF(cpuVendor);
        out.writeUTF(cpuModel);
        out.writeInt(cpuMhz);
        out.writeInt(cpuTotalCores);
        out.writeInt(cpuTotalSockets);
        out.writeInt(cpuCoresPerSocket);
        out.writeLong(cpuCacheSize);
        out.writeLong(memTotal);
        out.writeLong(swapTotal);
    }
}
