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

package org.elasticsearch.monitor.process;

import org.elasticsearch.common.component.AbstractComponent;
import org.elasticsearch.common.inject.Inject;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.monitor.sigar.SigarService;
import org.hyperic.sigar.ProcCpu;
import org.hyperic.sigar.ProcMem;
import org.hyperic.sigar.Sigar;
import org.hyperic.sigar.SigarException;

/**
 *
 */
public class SigarProcessProbe extends AbstractComponent implements ProcessProbe {

    private final SigarService sigarService;

    @Inject
    public SigarProcessProbe(Settings settings, SigarService sigarService) {
        super(settings);
        this.sigarService = sigarService;
    }

    @Override
    public synchronized ProcessInfo processInfo() {
        return new ProcessInfo(sigarService.sigar().getPid(), JmxProcessProbe.getMaxFileDescriptorCount());
    }

    @Override
    public synchronized ProcessStats processStats() {
        Sigar sigar = sigarService.sigar();
        ProcessStats stats = new ProcessStats();
        stats.timestamp = System.currentTimeMillis();
        stats.openFileDescriptors = JmxProcessProbe.getOpenFileDescriptorCount();
        try {
            if (stats.openFileDescriptors == -1) {
                stats.openFileDescriptors = sigar.getProcFd(sigar.getPid()).getTotal();
            }
            ProcCpu cpu = sigar.getProcCpu(sigar.getPid());
            stats.cpu = new ProcessStats.Cpu();
            stats.cpu.percent = (short) (cpu.getPercent() * 100);
            stats.cpu.sys = cpu.getSys();
            stats.cpu.user = cpu.getUser();
            stats.cpu.total = cpu.getTotal();
        } catch (SigarException e) {
            // ignore
        }

        try {
            ProcMem mem = sigar.getProcMem(sigar.getPid());
            stats.mem = new ProcessStats.Mem();
            stats.mem.totalVirtual = mem.getSize();
            stats.mem.resident = mem.getResident();
            stats.mem.share = mem.getShare();
        } catch (SigarException e) {
            // ignore
        }

        return stats;
    }
}
