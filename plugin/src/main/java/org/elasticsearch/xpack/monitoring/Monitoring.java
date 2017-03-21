/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.xpack.monitoring;

import org.elasticsearch.action.ActionRequest;
import org.elasticsearch.action.ActionResponse;
import org.elasticsearch.cluster.metadata.IndexNameExpressionResolver;
import org.elasticsearch.cluster.node.DiscoveryNodes;
import org.elasticsearch.cluster.service.ClusterService;
import org.elasticsearch.common.inject.Module;
import org.elasticsearch.common.inject.util.Providers;
import org.elasticsearch.common.settings.ClusterSettings;
import org.elasticsearch.common.settings.IndexScopedSettings;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.settings.SettingsFilter;
import org.elasticsearch.license.LicenseService;
import org.elasticsearch.license.XPackLicenseState;
import org.elasticsearch.plugins.ActionPlugin;
import org.elasticsearch.rest.RestController;
import org.elasticsearch.rest.RestHandler;
import org.elasticsearch.threadpool.ThreadPool;
import org.elasticsearch.xpack.XPackPlugin;
import org.elasticsearch.xpack.XPackSettings;
import org.elasticsearch.xpack.monitoring.action.MonitoringBulkAction;
import org.elasticsearch.xpack.monitoring.action.TransportMonitoringBulkAction;
import org.elasticsearch.xpack.monitoring.cleaner.CleanerService;
import org.elasticsearch.xpack.monitoring.collector.Collector;
import org.elasticsearch.xpack.monitoring.collector.cluster.ClusterStateCollector;
import org.elasticsearch.xpack.monitoring.collector.cluster.ClusterStatsCollector;
import org.elasticsearch.xpack.monitoring.collector.indices.IndexRecoveryCollector;
import org.elasticsearch.xpack.monitoring.collector.indices.IndexStatsCollector;
import org.elasticsearch.xpack.monitoring.collector.indices.IndicesStatsCollector;
import org.elasticsearch.xpack.monitoring.collector.node.NodeStatsCollector;
import org.elasticsearch.xpack.monitoring.collector.shards.ShardsCollector;
import org.elasticsearch.xpack.monitoring.exporter.Exporter;
import org.elasticsearch.xpack.monitoring.exporter.Exporters;
import org.elasticsearch.xpack.monitoring.exporter.http.HttpExporter;
import org.elasticsearch.xpack.monitoring.exporter.local.LocalExporter;
import org.elasticsearch.xpack.monitoring.rest.action.RestMonitoringBulkAction;
import org.elasticsearch.xpack.security.InternalClient;
import org.elasticsearch.xpack.ssl.SSLService;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.function.Supplier;

import static java.util.Collections.emptyList;
import static java.util.Collections.singletonList;

/**
 * This class activates/deactivates the monitoring modules depending if we're running a node client, transport client or tribe client:
 * - node clients: all modules are binded
 * - transport clients: only action/transport actions are binded
 * - tribe clients: everything is disables by default but can be enabled per tribe cluster
 */
public class Monitoring implements ActionPlugin {

    public static final String NAME = "monitoring";

    private final Settings settings;
    private final XPackLicenseState licenseState;
    private final boolean enabled;
    private final boolean transportClientMode;
    private final boolean tribeNode;

    public Monitoring(Settings settings, XPackLicenseState licenseState) {
        this.settings = settings;
        this.licenseState = licenseState;
        this.enabled = XPackSettings.MONITORING_ENABLED.get(settings);
        this.transportClientMode = XPackPlugin.transportClientMode(settings);
        this.tribeNode = XPackPlugin.isTribeNode(settings);
    }

    boolean isEnabled() {
        return enabled;
    }

    boolean isTransportClient() {
        return transportClientMode;
    }

    public Collection<Module> nodeModules() {
        List<Module> modules = new ArrayList<>();
        modules.add(b -> {
            XPackPlugin.bindFeatureSet(b, MonitoringFeatureSet.class);
            if (transportClientMode || enabled == false || tribeNode) {
                b.bind(Exporters.class).toProvider(Providers.of(null));
            }
        });
        return modules;
    }

    public Collection<Object> createComponents(InternalClient client, ThreadPool threadPool, ClusterService clusterService,
                                               LicenseService licenseService, SSLService sslService) {
        if (enabled == false || tribeNode) {
            return Collections.emptyList();
        }

        final ClusterSettings clusterSettings = clusterService.getClusterSettings();
        final MonitoringSettings monitoringSettings = new MonitoringSettings(settings, clusterSettings);
        final CleanerService cleanerService = new CleanerService(settings, clusterSettings, threadPool, licenseState);

        // TODO: https://github.com/elastic/x-plugins/issues/3117 (remove dynamic need with static exporters)
        final SSLService dynamicSSLService = sslService.createDynamicSSLService();
        Map<String, Exporter.Factory> exporterFactories = new HashMap<>();
        exporterFactories.put(HttpExporter.TYPE, config -> new HttpExporter(config, dynamicSSLService, threadPool.getThreadContext()));
        exporterFactories.put(LocalExporter.TYPE, config -> new LocalExporter(config, client, clusterService, cleanerService));
        final Exporters exporters = new Exporters(settings, exporterFactories, clusterService, threadPool.getThreadContext());

        Set<Collector> collectors = new HashSet<>();
        collectors.add(new IndicesStatsCollector(settings, clusterService, monitoringSettings, licenseState, client));
        collectors.add(new IndexStatsCollector(settings, clusterService, monitoringSettings, licenseState, client));
        collectors.add(new ClusterStatsCollector(settings, clusterService, monitoringSettings, licenseState, client, licenseService));
        collectors.add(new ClusterStateCollector(settings, clusterService, monitoringSettings, licenseState, client));
        collectors.add(new ShardsCollector(settings, clusterService, monitoringSettings, licenseState));
        collectors.add(new NodeStatsCollector(settings, clusterService, monitoringSettings, licenseState, client));
        collectors.add(new IndexRecoveryCollector(settings, clusterService, monitoringSettings, licenseState, client));
        final MonitoringService monitoringService =
                new MonitoringService(settings, clusterSettings, threadPool, collectors, exporters);

        return Arrays.asList(monitoringService, monitoringSettings, exporters, cleanerService);
    }

    @Override
    public List<ActionHandler<? extends ActionRequest, ? extends ActionResponse>> getActions() {
        if (false == enabled || tribeNode) {
            return emptyList();
        }
        return singletonList(new ActionHandler<>(MonitoringBulkAction.INSTANCE, TransportMonitoringBulkAction.class));
    }

    @Override
    public List<RestHandler> getRestHandlers(Settings settings, RestController restController, ClusterSettings clusterSettings,
            IndexScopedSettings indexScopedSettings, SettingsFilter settingsFilter, IndexNameExpressionResolver indexNameExpressionResolver,
            Supplier<DiscoveryNodes> nodesInCluster) {
        if (false == enabled || tribeNode) {
            return emptyList();
        }
        return singletonList(new RestMonitoringBulkAction(settings, restController));
    }
}
