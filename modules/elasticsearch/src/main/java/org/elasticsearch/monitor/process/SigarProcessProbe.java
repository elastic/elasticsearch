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

import org.elasticsearch.monitor.sigar.SigarService;
import org.elasticsearch.util.component.AbstractComponent;
import org.elasticsearch.util.guice.inject.Inject;
import org.elasticsearch.util.settings.Settings;
import org.hyperic.sigar.*;

/**
 * @author kimchy (shay.banon)
 */
public class SigarProcessProbe extends AbstractComponent implements ProcessProbe {

    private final SigarService sigarService;

    @Inject public SigarProcessProbe(Settings settings, SigarService sigarService) {
        super(settings);
        this.sigarService = sigarService;
    }

    @Override public ProcessInfo processInfo() {
        return new ProcessInfo(sigarService.sigar().getPid());
    }

    @Override public ProcessStats processStats() {
        Sigar sigar = sigarService.sigar();
        ProcessStats stats = new ProcessStats();
        
        try {
            ProcCpu cpu = sigar.getProcCpu(sigar.getPid());
            stats.cpuPercent = cpu.getPercent();
            stats.cpuSys = cpu.getSys();
            stats.cpuUser = cpu.getUser();
        } catch (SigarException e) {
            // ignore
        }

        try {
            ProcMem mem = sigar.getProcMem(sigar.getPid());
            stats.memTotalVirtual = mem.getSize();
            stats.memResident = mem.getResident();
            stats.memShare = mem.getShare();
        } catch (SigarException e) {
            // ignore
        }

        try {
            ProcFd fd = sigar.getProcFd(sigar.getPid());
            stats.fd = fd.getTotal();
        } catch (SigarException e) {
            // ignore
        }

        return stats;
    }
}
