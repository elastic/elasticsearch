/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.core.logging;

import org.elasticsearch.action.ActionListener;
import org.elasticsearch.action.support.ActionFilters;
import org.elasticsearch.cluster.ClusterState;
import org.elasticsearch.cluster.service.ClusterService;
import org.elasticsearch.common.settings.ClusterSettings;
import org.elasticsearch.common.settings.Setting;
import org.elasticsearch.core.TimeValue;
import org.elasticsearch.injection.guice.Inject;
import org.elasticsearch.protocol.xpack.XPackUsageRequest;
import org.elasticsearch.tasks.Task;
import org.elasticsearch.threadpool.ThreadPool;
import org.elasticsearch.transport.TransportService;
import org.elasticsearch.xpack.core.action.XPackUsageFeatureAction;
import org.elasticsearch.xpack.core.action.XPackUsageFeatureResponse;
import org.elasticsearch.xpack.core.action.XPackUsageFeatureTransportAction;
import org.elasticsearch.xpack.core.logging.LoggingFeatureSetUsage.EsqlLoggingConfig;
import org.elasticsearch.xpack.core.logging.LoggingFeatureSetUsage.LoggingConfig;
import org.elasticsearch.xpack.core.logging.LoggingFeatureSetUsage.QueryLoggingConfig;

import java.util.HashMap;
import java.util.Map;

public class LoggingUsageTransportAction extends XPackUsageFeatureTransportAction {
    // Accessible for testing
    static final String[] LOG_LEVELS = { "trace", "debug", "info", "warn" };

    @Inject
    public LoggingUsageTransportAction(
        TransportService transportService,
        ClusterService clusterService,
        ThreadPool threadPool,
        ActionFilters actionFilters
    ) {
        super(XPackUsageFeatureAction.LOGGING.name(), transportService, clusterService, threadPool, actionFilters);
    }

    @Override
    protected void localClusterStateOperation(
        Task task,
        XPackUsageRequest request,
        ClusterState state,
        ActionListener<XPackUsageFeatureResponse> listener
    ) throws Exception {
        var settings = clusterService.getClusterSettings();
        var usage = new LoggingFeatureSetUsage(getQueryLoggingConfig(settings), getEsqlLoggingConfig(settings));
        listener.onResponse(new XPackUsageFeatureResponse(usage));
    }

    @SuppressWarnings("unchecked")
    private boolean getBooleanSetting(ClusterSettings clusterSettings, String setting) {
        var settingValue = (Setting<Boolean>) clusterSettings.get(setting);
        return settingValue != null && clusterSettings.get(settingValue);
    }

    @SuppressWarnings("unchecked")
    private EsqlLoggingConfig getEsqlLoggingConfig(ClusterSettings clusterSettings) {
        Map<String, String> thresholds = new HashMap<>();
        for (String logLevel : LOG_LEVELS) {
            Setting<TimeValue> value = (Setting<TimeValue>) clusterSettings.get("esql.querylog.threshold." + logLevel);
            if (value != null && clusterSettings.get(value).nanos() >= 0) {
                thresholds.put(logLevel, clusterSettings.get(value).getStringRep());
            }
        }
        boolean includeUserEnabled = getBooleanSetting(clusterSettings, "esql.querylog.include.user");
        return thresholds.isEmpty()
            ? new EsqlLoggingConfig(new LoggingConfig(false, includeUserEnabled), Map.of())
            : new EsqlLoggingConfig(new LoggingConfig(true, includeUserEnabled), thresholds);
    }

    @SuppressWarnings("unchecked")
    private QueryLoggingConfig getQueryLoggingConfig(ClusterSettings clusterSettings) {
        Setting<TimeValue> value = (Setting<TimeValue>) clusterSettings.get("elasticsearch.querylog.threshold");
        String threshold = null;
        if (value != null && clusterSettings.get(value).nanos() > 0) {
            threshold = clusterSettings.get(value).getStringRep();
        }
        return new QueryLoggingConfig(
            new LoggingConfig(
                getBooleanSetting(clusterSettings, "elasticsearch.querylog.enabled"),
                getBooleanSetting(clusterSettings, "elasticsearch.querylog.include.user")
            ),
            getBooleanSetting(clusterSettings, "elasticsearch.querylog.include.system_indices"),
            threshold
        );
    }
}
