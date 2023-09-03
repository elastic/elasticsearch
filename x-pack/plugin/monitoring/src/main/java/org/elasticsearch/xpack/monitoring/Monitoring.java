/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */
package org.elasticsearch.xpack.monitoring;

import org.elasticsearch.action.ActionRequest;
import org.elasticsearch.action.ActionResponse;
import org.elasticsearch.client.internal.Client;
import org.elasticsearch.cluster.metadata.IndexNameExpressionResolver;
import org.elasticsearch.cluster.metadata.IndexTemplateMetadata;
import org.elasticsearch.cluster.node.DiscoveryNodes;
import org.elasticsearch.cluster.routing.allocation.AllocationService;
import org.elasticsearch.cluster.service.ClusterService;
import org.elasticsearch.common.io.stream.NamedWriteableRegistry;
import org.elasticsearch.common.settings.ClusterSettings;
import org.elasticsearch.common.settings.IndexScopedSettings;
import org.elasticsearch.common.settings.Setting;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.settings.SettingsFilter;
import org.elasticsearch.env.Environment;
import org.elasticsearch.env.NodeEnvironment;
import org.elasticsearch.indices.IndicesService;
import org.elasticsearch.license.License;
import org.elasticsearch.license.LicenseService;
import org.elasticsearch.license.LicensedFeature;
import org.elasticsearch.license.XPackLicenseState;
import org.elasticsearch.plugins.ActionPlugin;
import org.elasticsearch.plugins.Plugin;
import org.elasticsearch.plugins.ReloadablePlugin;
import org.elasticsearch.repositories.RepositoriesService;
import org.elasticsearch.rest.RestController;
import org.elasticsearch.rest.RestHandler;
import org.elasticsearch.script.ScriptService;
import org.elasticsearch.threadpool.ThreadPool;
import org.elasticsearch.tracing.Tracer;
import org.elasticsearch.watcher.ResourceWatcherService;
import org.elasticsearch.xcontent.NamedXContentRegistry;
import org.elasticsearch.xpack.core.XPackPlugin;
import org.elasticsearch.xpack.core.action.XPackInfoFeatureAction;
import org.elasticsearch.xpack.core.action.XPackUsageFeatureAction;
import org.elasticsearch.xpack.core.monitoring.MonitoringDeprecatedSettings;
import org.elasticsearch.xpack.core.monitoring.MonitoringField;
import org.elasticsearch.xpack.core.monitoring.action.MonitoringBulkAction;
import org.elasticsearch.xpack.core.monitoring.action.MonitoringMigrateAlertsAction;
import org.elasticsearch.xpack.core.ssl.SSLService;
import org.elasticsearch.xpack.monitoring.action.TransportMonitoringBulkAction;
import org.elasticsearch.xpack.monitoring.action.TransportMonitoringMigrateAlertsAction;
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
import org.elasticsearch.xpack.monitoring.exporter.MonitoringMigrationCoordinator;
import org.elasticsearch.xpack.monitoring.exporter.http.HttpExporter;
import org.elasticsearch.xpack.monitoring.exporter.local.LocalExporter;
import org.elasticsearch.xpack.monitoring.rest.action.RestMonitoringBulkAction;
import org.elasticsearch.xpack.monitoring.rest.action.RestMonitoringMigrateAlertsAction;

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
import java.util.function.UnaryOperator;

import static org.elasticsearch.common.settings.Setting.boolSetting;

public class Monitoring extends Plugin implements ActionPlugin, ReloadablePlugin {

    public static final Setting<Boolean> MIGRATION_DECOMMISSION_ALERTS = boolSetting(
        "xpack.monitoring.migration.decommission_alerts",
        false,
        Setting.Property.Dynamic,
        Setting.Property.NodeScope,
        Setting.Property.DeprecatedWarning
    );

    public static final LicensedFeature.Momentary MONITORING_CLUSTER_ALERTS_FEATURE = LicensedFeature.momentary(
        "monitoring",
        "cluster-alerts",
        License.OperationMode.STANDARD
    );

    protected final Settings settings;

    private Exporters exporters;

    public Monitoring(Settings settings) {
        this.settings = settings;
    }

    // overridable by tests
    protected SSLService getSslService() {
        return XPackPlugin.getSharedSslService();
    }

    protected XPackLicenseState getLicenseState() {
        return XPackPlugin.getSharedLicenseState();
    }

    protected LicenseService getLicenseService() {
        return XPackPlugin.getSharedLicenseService();
    }

    @Override
    public Collection<Object> createComponents(
        Client client,
        ClusterService clusterService,
        ThreadPool threadPool,
        ResourceWatcherService resourceWatcherService,
        ScriptService scriptService,
        NamedXContentRegistry xContentRegistry,
        Environment environment,
        NodeEnvironment nodeEnvironment,
        NamedWriteableRegistry namedWriteableRegistry,
        IndexNameExpressionResolver expressionResolver,
        Supplier<RepositoriesService> repositoriesServiceSupplier,
        Tracer tracer,
        AllocationService allocationService,
        IndicesService indicesService
    ) {
        final ClusterSettings clusterSettings = clusterService.getClusterSettings();
        final CleanerService cleanerService = new CleanerService(settings, clusterSettings, threadPool);
        final SSLService dynamicSSLService = getSslService().createDynamicSSLService();
        final MonitoringMigrationCoordinator migrationCoordinator = new MonitoringMigrationCoordinator();

        Map<String, Exporter.Factory> exporterFactories = new HashMap<>();
        exporterFactories.put(
            HttpExporter.TYPE,
            config -> new HttpExporter(config, dynamicSSLService, threadPool.getThreadContext(), migrationCoordinator)
        );
        exporterFactories.put(LocalExporter.TYPE, config -> new LocalExporter(config, client, migrationCoordinator, cleanerService));
        exporters = new Exporters(
            settings,
            exporterFactories,
            clusterService,
            getLicenseState(),
            threadPool.getThreadContext(),
            dynamicSSLService
        );

        Set<Collector> collectors = new HashSet<>();
        collectors.add(new IndexStatsCollector(clusterService, getLicenseState(), client));
        collectors.add(
            new ClusterStatsCollector(settings, clusterService, getLicenseState(), client, getLicenseService(), expressionResolver)
        );
        collectors.add(new ShardsCollector(clusterService, getLicenseState()));
        collectors.add(new NodeStatsCollector(clusterService, getLicenseState(), client));
        collectors.add(new IndexRecoveryCollector(clusterService, getLicenseState(), client));
        collectors.add(new JobStatsCollector(settings, clusterService, getLicenseState(), client));
        collectors.add(new StatsCollector(settings, clusterService, getLicenseState(), client));
        collectors.add(new EnrichStatsCollector(clusterService, getLicenseState(), client));

        final MonitoringService monitoringService = new MonitoringService(settings, clusterService, threadPool, collectors, exporters);

        var usageServices = new MonitoringUsageServices(monitoringService, exporters);

        MonitoringTemplateRegistry templateRegistry = new MonitoringTemplateRegistry(
            settings,
            clusterService,
            threadPool,
            client,
            xContentRegistry
        );
        templateRegistry.initialize();

        return Arrays.asList(monitoringService, exporters, migrationCoordinator, cleanerService, usageServices, templateRegistry);
    }

    @Override
    public List<ActionHandler<? extends ActionRequest, ? extends ActionResponse>> getActions() {
        var usageAction = new ActionHandler<>(XPackUsageFeatureAction.MONITORING, MonitoringUsageTransportAction.class);
        var infoAction = new ActionHandler<>(XPackInfoFeatureAction.MONITORING, MonitoringInfoTransportAction.class);
        return Arrays.asList(
            new ActionHandler<>(MonitoringBulkAction.INSTANCE, TransportMonitoringBulkAction.class),
            new ActionHandler<>(MonitoringMigrateAlertsAction.INSTANCE, TransportMonitoringMigrateAlertsAction.class),
            usageAction,
            infoAction
        );
    }

    @Override
    public List<RestHandler> getRestHandlers(
        Settings unused,
        RestController restController,
        ClusterSettings clusterSettings,
        IndexScopedSettings indexScopedSettings,
        SettingsFilter settingsFilter,
        IndexNameExpressionResolver indexNameExpressionResolver,
        Supplier<DiscoveryNodes> nodesInCluster
    ) {
        return List.of(new RestMonitoringBulkAction(), new RestMonitoringMigrateAlertsAction());
    }

    @Override
    public List<Setting<?>> getSettings() {
        List<Setting<?>> settingsList = new ArrayList<>();
        settingsList.add(MonitoringField.HISTORY_DURATION);
        settingsList.add(MonitoringService.ENABLED);
        settingsList.add(MonitoringService.ELASTICSEARCH_COLLECTION_ENABLED);
        settingsList.add(MonitoringService.INTERVAL);
        settingsList.add(MonitoringTemplateRegistry.MONITORING_TEMPLATES_ENABLED);
        settingsList.add(Collector.INDICES);
        settingsList.add(ClusterStatsCollector.CLUSTER_STATS_TIMEOUT);
        settingsList.add(IndexRecoveryCollector.INDEX_RECOVERY_TIMEOUT);
        settingsList.add(IndexRecoveryCollector.INDEX_RECOVERY_ACTIVE_ONLY);
        settingsList.add(IndexStatsCollector.INDEX_STATS_TIMEOUT);
        settingsList.add(JobStatsCollector.JOB_STATS_TIMEOUT);
        settingsList.add(StatsCollector.CCR_STATS_TIMEOUT);
        settingsList.add(NodeStatsCollector.NODE_STATS_TIMEOUT);
        settingsList.add(EnrichStatsCollector.STATS_TIMEOUT);
        settingsList.addAll(Exporters.getSettings());
        settingsList.add(Monitoring.MIGRATION_DECOMMISSION_ALERTS);
        settingsList.addAll(MonitoringDeprecatedSettings.getSettings());
        return Collections.unmodifiableList(settingsList);
    }

    @Override
    public List<String> getSettingsFilter() {
        final String exportersKey = "xpack.monitoring.exporters.";
        return List.of(exportersKey + "*.auth.*", exportersKey + "*.ssl.*");
    }

    @Override
    public void reload(Settings settingsToLoad) throws Exception {
        final List<String> changedExporters = HttpExporter.loadSettings(settingsToLoad);
        for (String changedExporter : changedExporters) {
            final Settings settingsForChangedExporter = settingsToLoad.filter(
                x -> x.startsWith("xpack.monitoring.exporters." + changedExporter)
            );
            exporters.setExportersSetting(settingsForChangedExporter);
        }
    }

    @Override
    public UnaryOperator<Map<String, IndexTemplateMetadata>> getIndexTemplateMetadataUpgrader() {
        return map -> {
            // this template was not migrated to typeless due to the possibility of the old /_monitoring/bulk API being used
            // see {@link org.elasticsearch.xpack.core.monitoring.exporter.MonitoringTemplateUtils#OLD_TEMPLATE_VERSION}
            // however the bulk API is not typed (the type field is for the docs, a field inside the docs) so it's safe to remove this
            // old template and rely on the updated, typeless, .monitoring-alerts-7 template
            map.remove(".monitoring-alerts");
            return map;
        };

    }
}
