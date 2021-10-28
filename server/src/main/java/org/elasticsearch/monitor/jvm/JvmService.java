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
import org.elasticsearch.core.TimeValue;
import org.elasticsearch.node.ReportingService;

public class JvmService implements ReportingService<JvmInfo> {

    private static final Logger logger = LogManager.getLogger(JvmService.class);

    private final JvmInfo jvmInfo;

    private final TimeValue refreshInterval;

    private JvmStats jvmStats;

    public static final Setting<TimeValue> REFRESH_INTERVAL_SETTING = Setting.timeSetting(
        "monitor.jvm.refresh_interval",
        TimeValue.timeValueSeconds(1),
        TimeValue.timeValueSeconds(1),
        Property.NodeScope
    );

    public JvmService(Settings settings) {
        this.jvmInfo = JvmInfo.jvmInfo();
        this.jvmStats = JvmStats.jvmStats();

        this.refreshInterval = REFRESH_INTERVAL_SETTING.get(settings);

        logger.debug("using refresh_interval [{}]", refreshInterval);
    }

    @Override
    public JvmInfo info() {
        return this.jvmInfo;
    }

    public synchronized JvmStats stats() {
        if ((System.currentTimeMillis() - jvmStats.getTimestamp()) > refreshInterval.millis()) {
            jvmStats = JvmStats.jvmStats();
        }
        return jvmStats;
    }
}
