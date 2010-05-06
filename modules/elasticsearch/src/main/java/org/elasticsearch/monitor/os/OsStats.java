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

    double cpuSys = -1;

    double cpuUser = -1;

    double cpuIdle = -1;

    long memFree = -1;

    double memFreePercent = -1;

    long memUsed = -1;

    double memUsedPercent = -1;

    long memActualFree = -1;

    long memActualUsed = -1;

    long swapFree = -1;

    long swapUsed = -1;

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

    public Percent cpuSys() {
        return new Percent(cpuSys);
    }

    public Percent getCpuSys() {
        return cpuSys();
    }

    public Percent cpuUser() {
        return new Percent(cpuUser);
    }

    public Percent getCpuUser() {
        return cpuUser();
    }

    public Percent cpuIdle() {
        return new Percent(cpuIdle);
    }

    public Percent getCpuIdle() {
        return cpuIdle();
    }

    public SizeValue memUsed() {
        return new SizeValue(memUsed);
    }

    public SizeValue getMemUsed() {
        return memUsed();
    }

    public Percent memUsedPercent() {
        return new Percent(memUsedPercent);
    }

    public Percent getMemUsedPercent() {
        return memUsedPercent();
    }

    public SizeValue memFree() {
        return new SizeValue(memFree);
    }

    public SizeValue getMemFree() {
        return memFree();
    }

    public Percent memFreePercent() {
        return new Percent(memFreePercent);
    }

    public Percent getMemFreePercent() {
        return memFreePercent();
    }

    public SizeValue memActualFree() {
        return new SizeValue(memActualFree);
    }

    public SizeValue getMemActualFree() {
        return memActualFree();
    }

    public SizeValue memActualUsed() {
        return new SizeValue(memActualUsed);
    }

    public SizeValue getMemActualUsed() {
        return memActualUsed();
    }

    public SizeValue swapUsed() {
        return new SizeValue(swapUsed);
    }

    public SizeValue getSwapUsed() {
        return swapUsed();
    }

    public SizeValue swapFree() {
        return new SizeValue(swapFree);
    }

    public SizeValue getSwapFree() {
        return swapFree();
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
        cpuSys = in.readDouble();
        cpuUser = in.readDouble();
        cpuIdle = in.readDouble();
        memFree = in.readLong();
        memFreePercent = in.readDouble();
        memUsed = in.readLong();
        memUsedPercent = in.readDouble();
        memActualFree = in.readLong();
        memActualUsed = in.readLong();
        swapFree = in.readLong();
        swapUsed = in.readLong();
    }

    @Override public void writeTo(StreamOutput out) throws IOException {
        out.writeVLong(timestamp);
        out.writeVInt(loadAverage.length);
        for (double val : loadAverage) {
            out.writeDouble(val);
        }
        out.writeLong(uptime);
        out.writeDouble(cpuSys);
        out.writeDouble(cpuUser);
        out.writeDouble(cpuIdle);
        out.writeLong(memFree);
        out.writeDouble(memFreePercent);
        out.writeLong(memUsed);
        out.writeDouble(memUsedPercent);
        out.writeLong(memActualFree);
        out.writeLong(memActualUsed);
        out.writeLong(swapFree);
        out.writeLong(swapFree);
    }
}
