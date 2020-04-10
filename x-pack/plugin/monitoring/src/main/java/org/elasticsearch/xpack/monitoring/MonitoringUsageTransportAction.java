/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.xpack.monitoring;

import org.elasticsearch.action.ActionListener;
import org.elasticsearch.action.support.ActionFilters;
import org.elasticsearch.cluster.ClusterState;
import org.elasticsearch.cluster.metadata.IndexNameExpressionResolver;
import org.elasticsearch.cluster.service.ClusterService;
import org.elasticsearch.common.inject.Inject;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.license.XPackLicenseState;
import org.elasticsearch.protocol.xpack.XPackUsageRequest;
import org.elasticsearch.tasks.Task;
import org.elasticsearch.threadpool.ThreadPool;
import org.elasticsearch.transport.TransportService;
import org.elasticsearch.xpack.core.XPackSettings;
import org.elasticsearch.xpack.core.action.XPackUsageFeatureAction;
import org.elasticsearch.xpack.core.action.XPackUsageFeatureResponse;
import org.elasticsearch.xpack.core.action.XPackUsageFeatureTransportAction;
import org.elasticsearch.xpack.core.monitoring.MonitoringFeatureSetUsage;
import org.elasticsearch.xpack.monitoring.exporter.Exporter;
import org.elasticsearch.xpack.monitoring.exporter.Exporters;

import java.util.HashMap;
import java.util.Map;

public class MonitoringUsageTransportAction extends XPackUsageFeatureTransportAction {
    private final boolean enabled;
    private final MonitoringService monitoringService;
    private final XPackLicenseState licenseState;
    private final Exporters exporters;

    @Inject
    public MonitoringUsageTransportAction(TransportService transportService, ClusterService clusterService, ThreadPool threadPool,
                                          ActionFilters actionFilters, IndexNameExpressionResolver indexNameExpressionResolver,
                                          Settings settings, XPackLicenseState licenseState, MonitoringUsageServices monitoringServices) {
        super(XPackUsageFeatureAction.MONITORING.name(), transportService, clusterService, threadPool,
            actionFilters, indexNameExpressionResolver);
        this.enabled = XPackSettings.MONITORING_ENABLED.get(settings);
        this.licenseState = licenseState;
        this.monitoringService = monitoringServices.monitoringService;
        this.exporters = monitoringServices.exporters;
    }

    @Override
    protected void masterOperation(Task task, XPackUsageRequest request, ClusterState state,
                                   ActionListener<XPackUsageFeatureResponse> listener) {
        final boolean collectionEnabled = monitoringService != null && monitoringService.isMonitoringActive();
        var usage =
            new MonitoringFeatureSetUsage(licenseState.isMonitoringAllowed(), enabled, collectionEnabled, exportersUsage(exporters));
        listener.onResponse(new XPackUsageFeatureResponse(usage));
    }

    static Map<String, Object> exportersUsage(Exporters exporters) {
        if (exporters == null) {
            return null;
        }
        Map<String, Object> usage = new HashMap<>();
        for (Exporter exporter : exporters.getEnabledExporters()) {
            if (exporter.config().enabled()) {
                String type = exporter.config().type();
                int count = (Integer) usage.getOrDefault(type, 0);
                usage.put(type, count + 1);
            }
        }
        return usage;
    }
}
