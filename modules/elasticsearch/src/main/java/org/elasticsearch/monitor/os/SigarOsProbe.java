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

import org.elasticsearch.monitor.sigar.SigarService;
import org.elasticsearch.util.component.AbstractComponent;
import org.elasticsearch.util.guice.inject.Inject;
import org.elasticsearch.util.settings.Settings;
import org.hyperic.sigar.*;

/**
 * @author kimchy (shay.banon)
 */
public class SigarOsProbe extends AbstractComponent implements OsProbe {

    private final SigarService sigarService;

    @Inject public SigarOsProbe(Settings settings, SigarService sigarService) {
        super(settings);
        this.sigarService = sigarService;
    }

    @Override public OsInfo osInfo() {
        Sigar sigar = sigarService.sigar();
        OsInfo info = new OsInfo();
        try {
            CpuInfo[] infos = sigar.getCpuInfoList();
            info.cpuVendor = infos[0].getVendor();
            info.cpuModel = infos[0].getModel();
            info.cpuMhz = infos[0].getMhz();
            info.cpuTotalCores = infos[0].getTotalCores();
            info.cpuTotalSockets = infos[0].getTotalSockets();
            info.cpuCoresPerSocket = infos[0].getCoresPerSocket();
            if (infos[0].getCacheSize() != Sigar.FIELD_NOTIMPL) {
                info.cpuCacheSize = infos[0].getCacheSize();
            }
        } catch (SigarException e) {
            // ignore
        }

        try {
            Mem mem = sigar.getMem();
            info.memTotal = mem.getTotal();
        } catch (SigarException e) {
            // ignore
        }

        try {
            Swap swap = sigar.getSwap();
            info.swapTotal = swap.getTotal();
        } catch (SigarException e) {
            // ignore
        }


        return info;
    }

    @Override public OsStats osStats() {
        Sigar sigar = sigarService.sigar();
        OsStats stats = new OsStats();
        stats.timestamp = System.currentTimeMillis();
        try {
            stats.loadAverage = sigar.getLoadAverage();
        } catch (SigarException e) {
            // ignore
        }

        try {
            stats.uptime = (long) sigar.getUptime().getUptime();
        } catch (SigarException e) {
            // ignore
        }

        try {
            CpuPerc cpuPerc = sigar.getCpuPerc();
            stats.cpuSys = cpuPerc.getSys();
            stats.cpuUser = cpuPerc.getUser();
            stats.cpuIdle = cpuPerc.getIdle();
        } catch (SigarException e) {
            // ignore
        }

        try {
            Mem mem = sigar.getMem();
            stats.memFree = mem.getFree();
            stats.memFreePercent = mem.getFreePercent() / 100;
            stats.memUsed = mem.getUsed();
            stats.memUsedPercent = mem.getUsedPercent() / 100;
            stats.memActualFree = mem.getActualFree();
            stats.memActualUsed = mem.getActualUsed();
        } catch (SigarException e) {
            // ignore
        }

        try {
            Swap swap = sigar.getSwap();
            stats.swapFree = swap.getFree();
            stats.swapUsed = swap.getUsed();
        } catch (SigarException e) {
            // ignore
        }

        return stats;
    }
}
