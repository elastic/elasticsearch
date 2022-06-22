/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.monitor.jvm;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.elasticsearch.common.settings.Setting;
import org.elasticsearch.common.settings.Setting.Property;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.util.SingleObjectCache;
import org.elasticsearch.core.TimeValue;
import org.elasticsearch.node.ReportingService;

public class JvmService implements ReportingService<JvmInfo> {

    private static final Logger logger = LogManager.getLogger(JvmService.class);

    private final JvmInfo jvmInfo;

    private final SingleObjectCache<JvmStats> jvmStatsCache;

    public static final Setting<TimeValue> REFRESH_INTERVAL_SETTING = Setting.timeSetting(
        "monitor.jvm.refresh_interval",
        TimeValue.timeValueSeconds(1),
        TimeValue.timeValueSeconds(1),
        Property.NodeScope
    );

    public JvmService(Settings settings) {
        this.jvmInfo = JvmInfo.jvmInfo();
        TimeValue refreshInterval = REFRESH_INTERVAL_SETTING.get(settings);
        jvmStatsCache = new JvmStatsCache(refreshInterval, JvmStats.jvmStats());
        logger.debug("using refresh_interval [{}]", refreshInterval);
    }

    @Override
    public JvmInfo info() {
        return this.jvmInfo;
    }

    public JvmStats stats() {
        return jvmStatsCache.getOrRefresh();
    }

    private class JvmStatsCache extends SingleObjectCache<JvmStats> {
        JvmStatsCache(TimeValue interval, JvmStats initValue) {
            super(interval, initValue);
        }

        @Override
        protected JvmStats refresh() {
            return JvmStats.jvmStats();
        }
    }
}
