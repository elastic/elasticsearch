/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.xpack.monitoring;

import org.elasticsearch.action.ActionRequest;
import org.elasticsearch.action.ActionResponse;
import org.elasticsearch.client.Client;
import org.elasticsearch.cluster.metadata.IndexNameExpressionResolver;
import org.elasticsearch.cluster.node.DiscoveryNodes;
import org.elasticsearch.cluster.service.ClusterService;
import org.elasticsearch.common.io.stream.NamedWriteableRegistry;
import org.elasticsearch.common.settings.ClusterSettings;
import org.elasticsearch.common.settings.IndexScopedSettings;
import org.elasticsearch.common.settings.Setting;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.settings.SettingsFilter;
import org.elasticsearch.common.xcontent.NamedXContentRegistry;
import org.elasticsearch.env.Environment;
import org.elasticsearch.env.NodeEnvironment;
import org.elasticsearch.license.LicenseService;
import org.elasticsearch.license.XPackLicenseState;
import org.elasticsearch.plugins.ActionPlugin;
import org.elasticsearch.plugins.Plugin;
import org.elasticsearch.plugins.ReloadablePlugin;
import org.elasticsearch.repositories.RepositoriesService;
import org.elasticsearch.rest.RestController;
import org.elasticsearch.rest.RestHandler;
import org.elasticsearch.script.ScriptService;
import org.elasticsearch.threadpool.ThreadPool;
import org.elasticsearch.watcher.ResourceWatcherService;
import org.elasticsearch.xpack.core.XPackPlugin;
import org.elasticsearch.xpack.core.action.XPackInfoFeatureAction;
import org.elasticsearch.xpack.core.action.XPackUsageFeatureAction;
import org.elasticsearch.xpack.core.monitoring.MonitoringField;
import org.elasticsearch.xpack.core.monitoring.action.MonitoringBulkAction;
import org.elasticsearch.xpack.core.ssl.SSLService;
import org.elasticsearch.xpack.monitoring.action.TransportMonitoringBulkAction;
import org.elasticsearch.xpack.monitoring.cleaner.CleanerService;
import org.elasticsearch.xpack.monitoring.collector.Collector;
import org.elasticsearch.xpack.monitoring.collector.ccr.StatsCollector;
import org.elasticsearch.xpack.monitoring.collector.cluster.ClusterStatsCollector;
import org.elasticsearch.xpack.monitoring.collector.enrich.EnrichStatsCollector;
import org.elasticsearch.xpack.monitoring.collector.indices.IndexRecoveryCollector;
import org.elasticsearch.xpack.monitoring.collector.indices.IndexStatsCollector;
import org.elasticsearch.xpack.monitoring.collector.ml.JobStatsCollector;
import org.elasticsearch.xpack.monitoring.collector.node.NodeStatsCollector;
import org.elasticsearch.xpack.monitoring.collector.shards.ShardsCollector;
import org.elasticsearch.xpack.monitoring.exporter.Exporter;
import org.elasticsearch.xpack.monitoring.exporter.Exporters;
import org.elasticsearch.xpack.monitoring.exporter.http.HttpExporter;
import org.elasticsearch.xpack.monitoring.exporter.local.LocalExporter;
import org.elasticsearch.xpack.monitoring.rest.action.RestMonitoringBulkAction;

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

import static java.util.Collections.singletonList;
import static org.elasticsearch.common.settings.Setting.boolSetting;

public class Monitoring extends Plugin implements ActionPlugin, ReloadablePlugin {

    /**
     * The ability to automatically cleanup ".watcher_history*" indices while also cleaning up Monitoring indices.
     */
    @Deprecated
    public static final Setting<Boolean> CLEAN_WATCHER_HISTORY = boolSetting("xpack.watcher.history.cleaner_service.enabled",
        true, Setting.Property.Dynamic, Setting.Property.NodeScope, Setting.Property.Deprecated);

    protected final Settings settings;

    private Exporters exporters;

    public Monitoring(Settings settings) {
        this.settings = settings;
    }

    // overridable by tests
    protected SSLService getSslService() { return XPackPlugin.getSharedSslService(); }
    protected XPackLicenseState getLicenseState() { return XPackPlugin.getSharedLicenseState(); }
    protected LicenseService getLicenseService() { return XPackPlugin.getSharedLicenseService(); }

    @Override
    public Collection<Object> createComponents(Client client, ClusterService clusterService, ThreadPool threadPool,
                                               ResourceWatcherService resourceWatcherService, ScriptService scriptService,
                                               NamedXContentRegistry xContentRegistry, Environment environment,
                                               NodeEnvironment nodeEnvironment, NamedWriteableRegistry namedWriteableRegistry,
                                               IndexNameExpressionResolver expressionResolver,
                                               Supplier<RepositoriesService> repositoriesServiceSupplier) {
        final ClusterSettings clusterSettings = clusterService.getClusterSettings();
        final CleanerService cleanerService = new CleanerService(settings, clusterSettings, threadPool, getLicenseState());
        final SSLService dynamicSSLService = getSslService().createDynamicSSLService();

        Map<String, Exporter.Factory> exporterFactories = new HashMap<>();
        exporterFactories.put(HttpExporter.TYPE, config -> new HttpExporter(config, dynamicSSLService, threadPool.getThreadContext()));
        exporterFactories.put(LocalExporter.TYPE, config -> new LocalExporter(config, client, cleanerService));
        exporters = new Exporters(settings, exporterFactories, clusterService, getLicenseState(), threadPool.getThreadContext(),
            dynamicSSLService);

        Set<Collector> collectors = new HashSet<>();
        collectors.add(new IndexStatsCollector(clusterService, getLicenseState(), client));
        collectors.add(
            new ClusterStatsCollector(settings, clusterService, getLicenseState(), client, getLicenseService(), expressionResolver));
        collectors.add(new ShardsCollector(clusterService, getLicenseState()));
        collectors.add(new NodeStatsCollector(clusterService, getLicenseState(), client));
        collectors.add(new IndexRecoveryCollector(clusterService, getLicenseState(), client));
        collectors.add(new JobStatsCollector(settings, clusterService, getLicenseState(), client));
        collectors.add(new StatsCollector(settings, clusterService, getLicenseState(), client));
        collectors.add(new EnrichStatsCollector(clusterService, getLicenseState(), client));

        final MonitoringService monitoringService = new MonitoringService(settings, clusterService, threadPool, collectors, exporters);

        var usageServices = new MonitoringUsageServices(monitoringService, exporters);
        return Arrays.asList(monitoringService, exporters, cleanerService, usageServices);
    }

    @Override
    public List<ActionHandler<? extends ActionRequest, ? extends ActionResponse>> getActions() {
        var usageAction = new ActionHandler<>(XPackUsageFeatureAction.MONITORING, MonitoringUsageTransportAction.class);
        var infoAction = new ActionHandler<>(XPackInfoFeatureAction.MONITORING, MonitoringInfoTransportAction.class);
        return Arrays.asList(
            new ActionHandler<>(MonitoringBulkAction.INSTANCE, TransportMonitoringBulkAction.class),
            usageAction,
            infoAction);
    }

    @Override
    public List<RestHandler> getRestHandlers(Settings settings, RestController restController, ClusterSettings clusterSettings,
            IndexScopedSettings indexScopedSettings, SettingsFilter settingsFilter, IndexNameExpressionResolver indexNameExpressionResolver,
            Supplier<DiscoveryNodes> nodesInCluster) {
        return singletonList(new RestMonitoringBulkAction());
    }

    @Override
    public List<Setting<?>> getSettings() {
        List<Setting<?>> settings = new ArrayList<>();
        settings.add(MonitoringField.HISTORY_DURATION);
        settings.add(CLEAN_WATCHER_HISTORY);
        settings.add(MonitoringService.ENABLED);
        settings.add(MonitoringService.ELASTICSEARCH_COLLECTION_ENABLED);
        settings.add(MonitoringService.INTERVAL);
        settings.add(Collector.INDICES);
        settings.add(ClusterStatsCollector.CLUSTER_STATS_TIMEOUT);
        settings.add(IndexRecoveryCollector.INDEX_RECOVERY_TIMEOUT);
        settings.add(IndexRecoveryCollector.INDEX_RECOVERY_ACTIVE_ONLY);
        settings.add(IndexStatsCollector.INDEX_STATS_TIMEOUT);
        settings.add(JobStatsCollector.JOB_STATS_TIMEOUT);
        settings.add(StatsCollector.CCR_STATS_TIMEOUT);
        settings.add(NodeStatsCollector.NODE_STATS_TIMEOUT);
        settings.add(EnrichStatsCollector.STATS_TIMEOUT);
        settings.addAll(Exporters.getSettings());
        return Collections.unmodifiableList(settings);
    }

    @Override
    public List<String> getSettingsFilter() {
        final String exportersKey = "xpack.monitoring.exporters.";
        return List.of(exportersKey + "*.auth.*", exportersKey + "*.ssl.*");
    }

    @Override
    public void reload(Settings settings) throws Exception {
        final List<String> changedExporters = HttpExporter.loadSettings(settings);
        for (String changedExporter : changedExporters) {
            final Settings settingsForChangedExporter = settings.filter(x -> x.startsWith("xpack.monitoring.exporters." + changedExporter));
            exporters.setExportersSetting(settingsForChangedExporter);
        }
    }
}
