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
import org.elasticsearch.common.logging.activity.QueryLogger;
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

public class LoggingUsageTransportAction extends XPackUsageFeatureTransportAction {
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
        final boolean querylogEnabled = clusterService.getClusterSettings().get(QueryLogger.QUERY_LOGGER_ENABLED);
        var usage = new LoggingFeatureSetUsage(querylogEnabled, isEsqlLogEnabled(clusterService.getClusterSettings()));
        listener.onResponse(new XPackUsageFeatureResponse(usage));
    }

    @SuppressWarnings("unchecked")
    private boolean isEsqlLogEnabled(ClusterSettings clusterSettings) {
        String[] logLevels = { "trace", "debug", "info", "warn" };

        for (String logLevel : logLevels) {
            Setting<TimeValue> setting = (Setting<TimeValue>) clusterSettings.get("esql.querylog.threshold." + logLevel);
            if (setting != null && clusterSettings.get(setting).nanos() >= 0) {
                return true;
            }
        }
        return false;
    }
}
