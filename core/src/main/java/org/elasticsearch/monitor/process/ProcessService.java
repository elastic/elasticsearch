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
import org.elasticsearch.common.unit.TimeValue;
import org.elasticsearch.common.util.SingleObjectCache;

/**
 *
 */
public final class ProcessService extends AbstractComponent {

    private final ProcessProbe probe;
    private final ProcessInfo info;
    private final SingleObjectCache<ProcessStats> processStatsCache;

    @Inject
    public ProcessService(Settings settings, ProcessProbe probe) {
        super(settings);
        this.probe = probe;

        final TimeValue refreshInterval = settings.getAsTime("monitor.process.refresh_interval", TimeValue.timeValueSeconds(1));
        processStatsCache = new ProcessStatsCache(refreshInterval, probe.processStats());
        this.info = probe.processInfo();
        this.info.refreshInterval = refreshInterval.millis();
        logger.debug("Using probe [{}] with refresh_interval [{}]", probe, refreshInterval);
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
            return probe.processStats();
        }
    }
}
