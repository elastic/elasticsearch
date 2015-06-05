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

import org.elasticsearch.common.component.AbstractComponent;
import org.elasticsearch.common.inject.Inject;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.monitor.sigar.SigarService;
import org.hyperic.sigar.*;

/**
 *
 */
public class SigarOsProbe extends AbstractComponent implements OsProbe {

    private final SigarService sigarService;

    @Inject
    public SigarOsProbe(Settings settings, SigarService sigarService) {
        super(settings);
        this.sigarService = sigarService;
    }

    @Override
    public OsInfo osInfo() {
        Sigar sigar = sigarService.sigar();
        OsInfo info = new OsInfo();
        try {
            CpuInfo[] infos = sigar.getCpuInfoList();
            info.cpu = new OsInfo.Cpu();
            info.cpu.vendor = infos[0].getVendor();
            info.cpu.model = infos[0].getModel();
            info.cpu.mhz = infos[0].getMhz();
            info.cpu.totalCores = infos[0].getTotalCores();
            info.cpu.totalSockets = infos[0].getTotalSockets();
            info.cpu.coresPerSocket = infos[0].getCoresPerSocket();
            if (infos[0].getCacheSize() != Sigar.FIELD_NOTIMPL) {
                info.cpu.cacheSize = infos[0].getCacheSize();
            }
        } catch (SigarException e) {
            // ignore
        }

        try {
            Mem mem = sigar.getMem();
            info.mem = new OsInfo.Mem();
            info.mem.total = mem.getTotal();
        } catch (SigarException e) {
            // ignore
        }

        try {
            Swap swap = sigar.getSwap();
            info.swap = new OsInfo.Swap();
            info.swap.total = swap.getTotal();
        } catch (SigarException e) {
            // ignore
        }


        return info;
    }

    @Override
    public OsStats osStats() {
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
            stats.cpu = new OsStats.Cpu();
            stats.cpu.sys = (short) (cpuPerc.getSys() * 100);
            stats.cpu.user = (short) (cpuPerc.getUser() * 100);
            stats.cpu.idle = (short) (cpuPerc.getIdle() * 100);
            stats.cpu.stolen = (short) (cpuPerc.getStolen() * 100);
        } catch (SigarException e) {
            // ignore
        }

        try {
            Mem mem = sigar.getMem();
            stats.mem = new OsStats.Mem();
            stats.mem.free = mem.getFree();
            stats.mem.freePercent = (short) mem.getFreePercent();
            stats.mem.used = mem.getUsed();
            stats.mem.usedPercent = (short) mem.getUsedPercent();
            stats.mem.actualFree = mem.getActualFree();
            stats.mem.actualUsed = mem.getActualUsed();
        } catch (SigarException e) {
            // ignore
        }

        try {
            Swap swap = sigar.getSwap();
            stats.swap = new OsStats.Swap();
            stats.swap.free = swap.getFree();
            stats.swap.used = swap.getUsed();
        } catch (SigarException e) {
            // ignore
        }

        return stats;
    }
}
