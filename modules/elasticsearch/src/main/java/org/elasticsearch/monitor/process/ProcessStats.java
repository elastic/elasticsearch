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

package org.elasticsearch.monitor.process;

import org.elasticsearch.util.Percent;
import org.elasticsearch.util.SizeValue;
import org.elasticsearch.util.TimeValue;

/**
 * @author kimchy (shay.banon)
 */
public class ProcessStats {

    double cpuPercent = -1;

    long cpuSys = -1;

    long cpuUser = -1;

    long cpuTotal = -1;

    long memTotalVirtual = -1;

    long memResident = -1;

    long memShare = -1;

    long fd;

    /**
     * Get the Process cpu usage.
     *
     * <p>Supported Platforms: All.
     */
    public Percent cpuPercent() {
        return new Percent(cpuPercent);
    }

    /**
     * Get the Process cpu usage.
     *
     * <p>Supported Platforms: All.
     */
    public Percent getCpuPercent() {
        return cpuPercent();
    }

    /**
     * Get the Process cpu kernel time.
     *
     * <p>Supported Platforms: All.
     */
    public TimeValue cpuSys() {
        return new TimeValue(cpuSys);
    }

    /**
     * Get the Process cpu kernel time.
     *
     * <p>Supported Platforms: All.
     */
    public TimeValue getCpuSys() {
        return cpuSys();
    }

    /**
     * Get the Process cpu user time.
     *
     * <p>Supported Platforms: All.
     */
    public TimeValue cpuUser() {
        return new TimeValue(cpuUser);
    }

    /**
     * Get the Process cpu time (sum of User and Sys).
     *
     * Supported Platforms: All.
     */
    public TimeValue cpuTotal() {
        return new TimeValue(cpuTotal);
    }

    /**
     * Get the Process cpu time (sum of User and Sys).
     *
     * Supported Platforms: All.
     */
    public TimeValue getCpuTotal() {
        return cpuTotal();
    }

    /**
     * Get the Process cpu user time.
     *
     * <p>Supported Platforms: All.
     */
    public TimeValue getCpuUser() {
        return cpuUser();
    }

    public SizeValue memTotalVirtual() {
        return new SizeValue(memTotalVirtual);
    }

    public SizeValue getMemTotalVirtual() {
        return memTotalVirtual();
    }

    public SizeValue memResident() {
        return new SizeValue(memResident);
    }

    public SizeValue getMemResident() {
        return memResident();
    }

    public SizeValue memShare() {
        return new SizeValue(memShare);
    }

    public SizeValue getMemShare() {
        return memShare();
    }

    /**
     * Get the Total number of open file descriptors.
     *
     * <p>Supported Platforms: AIX, HPUX, Linux, Solaris, Win32.
     */
    public long fd() {
        return fd;
    }

    public long getFd() {
        return fd();
    }
}
