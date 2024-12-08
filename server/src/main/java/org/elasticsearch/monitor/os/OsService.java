/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.monitor.os;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.elasticsearch.common.settings.Setting;
import org.elasticsearch.common.settings.Setting.Property;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.util.SingleObjectCache;
import org.elasticsearch.common.util.concurrent.EsExecutors;
import org.elasticsearch.core.TimeValue;
import org.elasticsearch.node.ReportingService;

import java.io.IOException;

public class OsService implements ReportingService<OsInfo> {

    private static final Logger logger = LogManager.getLogger(OsService.class);

    private final OsInfo info;
    private final SingleObjectCache<OsStats> osStatsCache;

    public static final Setting<TimeValue> REFRESH_INTERVAL_SETTING = Setting.timeSetting(
        "monitor.os.refresh_interval",
        TimeValue.timeValueSeconds(1),
        TimeValue.timeValueSeconds(1),
        Property.NodeScope
    );

    public OsService(Settings settings) throws IOException {
        TimeValue refreshInterval = REFRESH_INTERVAL_SETTING.get(settings);
        this.info = OsProbe.getInstance().osInfo(refreshInterval.millis(), EsExecutors.nodeProcessors(settings));
        this.osStatsCache = new OsStatsCache(refreshInterval);
        logger.debug("using refresh_interval [{}]", refreshInterval);
    }

    @Override
    public OsInfo info() {
        return this.info;
    }

    public OsStats stats() {
        return osStatsCache.getOrRefresh();
    }

    private static class OsStatsCache extends SingleObjectCache<OsStats> {

        private static final OsStats MISSING = new OsStats(
            0L,
            new OsStats.Cpu((short) 0, new double[0]),
            new OsStats.Mem(0, 0, 0),
            new OsStats.Swap(0, 0),
            null
        );

        OsStatsCache(TimeValue interval) {
            super(interval, MISSING);
        }

        @Override
        protected OsStats refresh() {
            return OsProbe.getInstance().osStats();
        }

        @Override
        protected boolean needsRefresh() {
            return getNoRefresh() == MISSING || super.needsRefresh();
        }
    }
}
