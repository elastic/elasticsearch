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

import org.elasticsearch.bootstrap.Bootstrap;
import org.elasticsearch.common.component.AbstractComponent;
import org.elasticsearch.common.inject.Inject;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.unit.TimeValue;
import org.elasticsearch.common.util.SingleObjectCache;
import org.elasticsearch.monitor.probe.ProbeUtils;

/**
 *
 */
public final class ProcessService extends AbstractComponent {

    private final ProcessProbe probe;
    private final ProcessInfo info;
    private final ProcessStatsCache processStatsCache;

    @Inject
    public ProcessService(Settings settings, ProcessProbeRegistry registry) {
        super(settings);
        this.probe = registry.probe();

        final TimeValue refreshInterval = settings.getAsTime("monitor.process.refresh_interval", TimeValue.timeValueSeconds(1));
        this.info = new ProcessInfo(refreshInterval.millis(), probe.pid(), probe.maxFileDescriptor(), Bootstrap.isMemoryLocked());

        processStatsCache = new ProcessStatsCache(refreshInterval, new ProcessStats());
        processStatsCache.refresh();
        logger.debug("Using probe {} with refresh_interval [{}]", registry.types(), refreshInterval);
        if (logger.isTraceEnabled()) {
            ProbeUtils.logProbesResults(logger, registry);
        }
    }

    public ProcessInfo info() {
        return this.info;
    }

    public ProcessStats stats() {
        return processStatsCache.getOrRefresh();
    }

    private class ProcessStatsCache extends SingleObjectCache<ProcessStats> {
        public ProcessStatsCache(TimeValue interval, ProcessStats initValue) {
            super(interval, initValue);
        }

        @Override
        protected ProcessStats refresh() {
            ProcessStats processStats = new ProcessStats();
            processStats.timestamp = System.currentTimeMillis();
            processStats.openFileDescriptors = probe.openFileDescriptor();

            ProcessStats.Cpu cpu = new ProcessStats.Cpu();
            cpu.percent = probe.processCpuLoad();
            cpu.total = probe.processCpuTime();
            cpu.sys = probe.processSystemTime();
            cpu.user = probe.processUserTime();
            processStats.cpu = cpu;

            ProcessStats.Mem mem = new ProcessStats.Mem();
            mem.totalVirtual = probe.totalVirtualMemorySize();
            mem.resident = probe.residentMemorySize();
            mem.share = probe.sharedMemorySize();
            processStats.mem = mem;

            return processStats;
        }
    }
}
