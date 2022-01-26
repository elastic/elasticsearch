/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */
package org.elasticsearch.xpack.monitoring;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.elasticsearch.action.ActionRequest;
import org.elasticsearch.action.ActionResponse;
import org.elasticsearch.client.Client;
import org.elasticsearch.cluster.metadata.IndexNameExpressionResolver;
import org.elasticsearch.cluster.metadata.IndexTemplateMetadata;
import org.elasticsearch.cluster.node.DiscoveryNodes;
import org.elasticsearch.cluster.service.ClusterService;
import org.elasticsearch.common.collect.ImmutableOpenMap;
import org.elasticsearch.common.compress.CompressedXContent;
import org.elasticsearch.common.inject.Module;
import org.elasticsearch.common.inject.util.Providers;
import org.elasticsearch.common.io.stream.NamedWriteableRegistry;
import org.elasticsearch.common.settings.ClusterSettings;
import org.elasticsearch.common.settings.IndexScopedSettings;
import org.elasticsearch.common.settings.Setting;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.settings.SettingsFilter;
import org.elasticsearch.env.Environment;
import org.elasticsearch.env.NodeEnvironment;
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
import org.elasticsearch.watcher.ResourceWatcherService;
import org.elasticsearch.xcontent.DeprecationHandler;
import org.elasticsearch.xcontent.NamedXContentRegistry;
import org.elasticsearch.xcontent.XContentParser;
import org.elasticsearch.xcontent.XContentType;
import org.elasticsearch.xpack.core.XPackPlugin;
import org.elasticsearch.xpack.core.monitoring.MonitoringField;
import org.elasticsearch.xpack.core.monitoring.action.MonitoringBulkAction;
import org.elasticsearch.xpack.core.monitoring.action.MonitoringMigrateAlertsAction;
import org.elasticsearch.xpack.core.monitoring.exporter.MonitoringTemplateUtils;
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

import java.io.IOException;
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
import java.util.stream.Collectors;

import static org.elasticsearch.common.settings.Setting.boolSetting;
import static org.elasticsearch.xpack.core.monitoring.exporter.MonitoringTemplateUtils.LAST_UPDATED_VERSION;
import static org.elasticsearch.xpack.core.monitoring.exporter.MonitoringTemplateUtils.templateName;

/**
 * This class activates/deactivates the monitoring modules depending if we're running a node client, transport client:
 * - node clients: all modules are bound
 * - transport clients: only action/transport actions are bound
 */
public class Monitoring extends Plugin implements ActionPlugin, ReloadablePlugin {

    private static final Logger logger = LogManager.getLogger(Monitoring.class);

    /**
     * The ability to automatically cleanup ".watcher_history*" indices while also cleaning up Monitoring indices.
     */
    @Deprecated
    public static final Setting<Boolean> CLEAN_WATCHER_HISTORY = boolSetting(
        "xpack.watcher.history.cleaner_service.enabled",
        true,
        Setting.Property.Dynamic,
        Setting.Property.NodeScope,
        Setting.Property.Deprecated
    );

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
    private final boolean transportClientMode;

    private Exporters exporters;

    public Monitoring(Settings settings) {
        this.settings = settings;
        this.transportClientMode = XPackPlugin.transportClientMode(settings);
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

    boolean isTransportClient() {
        return transportClientMode;
    }

    @Override
    public Collection<Module> createGuiceModules() {
        List<Module> modules = new ArrayList<>();
        modules.add(b -> {
            XPackPlugin.bindFeatureSet(b, MonitoringFeatureSet.class);
            if (transportClientMode) {
                b.bind(MonitoringService.class).toProvider(Providers.of(null));
                b.bind(Exporters.class).toProvider(Providers.of(null));
            }
        });
        return modules;
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
        Supplier<RepositoriesService> repositoriesServiceSupplier
    ) {
        final ClusterSettings clusterSettings = clusterService.getClusterSettings();
        final CleanerService cleanerService = new CleanerService(settings, clusterSettings, threadPool, getLicenseState());
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

        return Arrays.asList(monitoringService, exporters, migrationCoordinator, cleanerService);
    }

    @Override
    public List<ActionHandler<? extends ActionRequest, ? extends ActionResponse>> getActions() {
        return Arrays.asList(
            new ActionHandler<>(MonitoringBulkAction.INSTANCE, TransportMonitoringBulkAction.class),
            new ActionHandler<>(MonitoringMigrateAlertsAction.INSTANCE, TransportMonitoringMigrateAlertsAction.class)
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
        return Arrays.asList(new RestMonitoringBulkAction(), new RestMonitoringMigrateAlertsAction());
    }

    @Override
    public List<Setting<?>> getSettings() {
        List<Setting<?>> settingList = new ArrayList<>();
        settingList.add(MonitoringField.HISTORY_DURATION);
        settingList.add(CLEAN_WATCHER_HISTORY);
        settingList.add(MonitoringService.ENABLED);
        settingList.add(MonitoringService.ELASTICSEARCH_COLLECTION_ENABLED);
        settingList.add(MonitoringService.INTERVAL);
        settingList.add(Collector.INDICES);
        settingList.add(ClusterStatsCollector.CLUSTER_STATS_TIMEOUT);
        settingList.add(IndexRecoveryCollector.INDEX_RECOVERY_TIMEOUT);
        settingList.add(IndexRecoveryCollector.INDEX_RECOVERY_ACTIVE_ONLY);
        settingList.add(IndexStatsCollector.INDEX_STATS_TIMEOUT);
        settingList.add(JobStatsCollector.JOB_STATS_TIMEOUT);
        settingList.add(StatsCollector.CCR_STATS_TIMEOUT);
        settingList.add(NodeStatsCollector.NODE_STATS_TIMEOUT);
        settingList.add(EnrichStatsCollector.STATS_TIMEOUT);
        settingList.addAll(Exporters.getSettings());
        settingList.add(Monitoring.MIGRATION_DECOMMISSION_ALERTS);
        return Collections.unmodifiableList(settingList);
    }

    @Override
    public List<String> getSettingsFilter() {
        final String exportersKey = "xpack.monitoring.exporters.";
        return Collections.unmodifiableList(Arrays.asList(exportersKey + "*.auth.*", exportersKey + "*.ssl.*"));
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
            List<IndexTemplateMetadata> monitoringTemplates = createMonitoringTemplates(getMissingMonitoringTemplateIds(map));
            for (IndexTemplateMetadata newTemplate : monitoringTemplates) {
                map.put(newTemplate.getName(), newTemplate);
            }

            map.entrySet().removeIf(Monitoring::isTypedAPMTemplate);

            // this template was not migrated to typeless due to the possibility of the old /_monitoring/bulk API being used
            // see {@link org.elasticsearch.xpack.core.monitoring.exporter.MonitoringTemplateUtils#OLD_TEMPLATE_VERSION}
            // however the bulk API is not typed (the type field is for the docs, a field inside the docs) so it's safe to remove this
            // old template and rely on the updated, typeless, .monitoring-alerts-7 template
            map.remove(".monitoring-alerts");
            return map;
        };
    }

    /**
     * Returns a list of template IDs (as defined by {@link MonitoringTemplateUtils#TEMPLATE_IDS}) that are not present in the provided
     * map or don't have at least {@link MonitoringTemplateUtils#LAST_UPDATED_VERSION} version
     */
    static List<String> getMissingMonitoringTemplateIds(Map<String, IndexTemplateMetadata> map) {
        return Arrays.stream(MonitoringTemplateUtils.TEMPLATE_IDS).filter(id -> {
            IndexTemplateMetadata templateMetadata = map.get(templateName(id));
            return templateMetadata == null || (templateMetadata.version() != null && templateMetadata.version() < LAST_UPDATED_VERSION);
        }).collect(Collectors.toList());
    }

    /**
     * Creates the monitoring templates with the provided IDs (must be some of {@link MonitoringTemplateUtils#TEMPLATE_IDS}).
     * Other ids are ignored.
     */
    static List<IndexTemplateMetadata> createMonitoringTemplates(List<String> missingTemplateIds) {
        List<IndexTemplateMetadata> createdTemplates = new ArrayList<>(missingTemplateIds.size());
        for (String templateId : missingTemplateIds) {
            try {
                final String templateName = MonitoringTemplateUtils.templateName(templateId);
                final String templateSource = MonitoringTemplateUtils.loadTemplate(templateId);
                try (
                    XContentParser parser = XContentType.JSON.xContent()
                        .createParser(NamedXContentRegistry.EMPTY, DeprecationHandler.THROW_UNSUPPORTED_OPERATION, templateSource)
                ) {
                    IndexTemplateMetadata updatedTemplate = IndexTemplateMetadata.Builder.fromXContent(parser, templateName);
                    logger.info("creating template [{}] with version [{}]", templateName, MonitoringTemplateUtils.TEMPLATE_VERSION);
                    createdTemplates.add(updatedTemplate);
                } catch (IOException e) {
                    logger.error("unable to create template [" + templateName + "]", e);
                }
                // Loading a template involves IO to some specific locations, looking for files with set names.
                // We're catching Exception here as we don't want to let anything that might fail in that process bubble up from the
                // upgrade template metadata infrastructure as that would prevent a node from starting
            } catch (Exception e) {
                logger.error("unable to create monitoring template", e);
            }
        }
        return createdTemplates;
    }

    static boolean isTypedAPMTemplate(Map.Entry<String, IndexTemplateMetadata> templateEntry) {
        String templateName = templateEntry.getKey();
        if (templateName.startsWith("apm-6.")) {
            ImmutableOpenMap<String, CompressedXContent> mappings = templateEntry.getValue().getMappings();
            if (mappings != null && mappings.get("doc") != null) {
                // this is an old APM mapping that still uses the `doc` type so let's remove it as the later 7.x APM versions
                // would've installed an APM template (versioned) that doesn't contain any type
                logger.info("removing typed legacy template [{}]", templateName);
                return true;
            }
        }
        return false;
    }
}
