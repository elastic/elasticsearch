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
import org.elasticsearch.plugin.probe.sigar.SigarService;
import org.hyperic.sigar.ProcCpu;
import org.hyperic.sigar.ProcMem;
import org.hyperic.sigar.Sigar;
import org.hyperic.sigar.SigarException;

public class SigarProcessProbe extends AbstractComponent implements ProcessProbe {

    public static final String TYPE = "sigar";

    private final Sigar sigar;

    @Inject
    public SigarProcessProbe(Settings settings, SigarService sigarService) {
        super(settings);
        if (sigarService.sigarAvailable()) {
            this.sigar = sigarService.sigar();
        } else {
            this.sigar = null;
        }
    }

    @Override
    public Long pid() {
        if (sigar != null) {
            return sigar.getPid();
        }
        return null;
    }

    @Override
    public Long maxFileDescriptor() {
        throw new UnsupportedOperationException("SigarProcessProbe does not support max file descriptor");
    }

    @Override
    public Long openFileDescriptor() {
        if (sigar != null) {
            try {
                return sigar.getProcFd(sigar.getPid()).getTotal();
            } catch (SigarException e) {
                logger.warn("unable to get open file descriptors");
            }
        }
        return null;
    }

    @Override
    public Short processCpuLoad() {
        if (sigar != null) {
            try {
                ProcCpu cpu = sigar.getProcCpu(sigar.getPid());
                return  (short) (cpu.getPercent() * 100);
            } catch (SigarException e) {
                logger.warn("unable to get process cpu load");
            }
        }
        return null;
    }

    @Override
    public Long processCpuTime() {
        if (sigar != null) {
            try {
                ProcCpu cpu = sigar.getProcCpu(sigar.getPid());
                return  cpu.getTotal();
            } catch (SigarException e) {
                logger.warn("unable to get process cpu time");
            }
        }
        return null;
    }

    @Override
    public Long processSystemTime() {
        if (sigar != null) {
            try {
                ProcCpu cpu = sigar.getProcCpu(sigar.getPid());
                return  cpu.getSys();
            } catch (SigarException e) {
                logger.warn("unable to get process cpu time");
            }
        }
        return null;
    }

    @Override
    public Long processUserTime() {
        if (sigar != null) {
            try {
                ProcCpu cpu = sigar.getProcCpu(sigar.getPid());
                return  cpu.getUser();
            } catch (SigarException e) {
                logger.warn("unable to get process cpu time");
            }
        }
        return null;
    }

    @Override
    public Long totalVirtualMemorySize() {
        if (sigar != null) {
            try {
                ProcMem mem = sigar.getProcMem(sigar.getPid());
                return  mem.getSize();
            } catch (SigarException e) {
                logger.warn("unable to get total virtual memory size");
            }
        }
        return null;
    }

    @Override
    public Long residentMemorySize() {
        if (sigar != null) {
            try {
                ProcMem mem = sigar.getProcMem(sigar.getPid());
                return  mem.getResident();
            } catch (SigarException e) {
                logger.warn("unable to get commited virtual memory size");
            }
        }
        return null;
    }

    @Override
    public Long sharedMemorySize() {
        if (sigar != null) {
            try {
                ProcMem mem = sigar.getProcMem(sigar.getPid());
                return  mem.getShare();
            } catch (SigarException e) {
                logger.warn("unable to get commited virtual memory size");
            }
        }
        return null;
    }
}
