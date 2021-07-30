/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.monitor.process;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.elasticsearch.common.settings.Setting;
import org.elasticsearch.common.settings.Setting.Property;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.core.TimeValue;
import org.elasticsearch.common.util.SingleObjectCache;
import org.elasticsearch.node.ReportingService;

public final class ProcessService implements ReportingService<ProcessInfo> {

    private static final Logger logger = LogManager.getLogger(ProcessService.class);

    private final ProcessProbe probe;
    private final ProcessInfo info;
    private final SingleObjectCache<ProcessStats> processStatsCache;

    public static final Setting<TimeValue> REFRESH_INTERVAL_SETTING =
        Setting.timeSetting("monitor.process.refresh_interval", TimeValue.timeValueSeconds(1), TimeValue.timeValueSeconds(1),
            Property.NodeScope);

    public ProcessService(Settings settings) {
        this.probe = ProcessProbe.getInstance();
        final TimeValue refreshInterval = REFRESH_INTERVAL_SETTING.get(settings);
        processStatsCache = new ProcessStatsCache(refreshInterval, probe.processStats());
        this.info = probe.processInfo(refreshInterval.millis());
        logger.debug("using refresh_interval [{}]", refreshInterval);
    }

    @Override
    public ProcessInfo info() {
        return this.info;
    }

    public ProcessStats stats() {
        return processStatsCache.getOrRefresh();
    }

    private class ProcessStatsCache extends SingleObjectCache<ProcessStats> {
        ProcessStatsCache(TimeValue interval, ProcessStats initValue) {
            super(interval, initValue);
        }

        @Override
        protected ProcessStats refresh() {
            return probe.processStats();
        }
    }
}
