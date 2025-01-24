/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.logsdb;

import org.elasticsearch.Build;
import org.elasticsearch.action.ActionRequest;
import org.elasticsearch.action.ActionResponse;
import org.elasticsearch.cluster.metadata.IndexNameExpressionResolver;
import org.elasticsearch.cluster.node.DiscoveryNode;
import org.elasticsearch.cluster.node.DiscoveryNodes;
import org.elasticsearch.common.io.stream.NamedWriteableRegistry;
import org.elasticsearch.common.settings.ClusterSettings;
import org.elasticsearch.common.settings.IndexScopedSettings;
import org.elasticsearch.common.settings.Setting;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.settings.SettingsFilter;
import org.elasticsearch.features.NodeFeature;
import org.elasticsearch.index.IndexSettingProvider;
import org.elasticsearch.index.IndexVersion;
import org.elasticsearch.license.LicenseService;
import org.elasticsearch.license.XPackLicenseState;
import org.elasticsearch.plugins.ActionPlugin;
import org.elasticsearch.plugins.Plugin;
import org.elasticsearch.rest.RestController;
import org.elasticsearch.rest.RestHandler;
import org.elasticsearch.xpack.core.XPackPlugin;
import org.elasticsearch.xpack.core.action.XPackInfoFeatureAction;
import org.elasticsearch.xpack.core.action.XPackUsageFeatureAction;
import org.elasticsearch.xpack.logsdb.seqno.RestAddRetentionLeaseAction;

import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.List;
import java.util.function.Predicate;
import java.util.function.Supplier;

import static org.elasticsearch.xpack.logsdb.SyntheticSourceLicenseService.FALLBACK_SETTING;

public class LogsDBPlugin extends Plugin implements ActionPlugin {

    private final Settings settings;
    private final SyntheticSourceLicenseService licenseService;
    public static final Setting<Boolean> CLUSTER_LOGSDB_ENABLED = Setting.boolSetting(
        "cluster.logsdb.enabled",
        false,
        Setting.Property.Dynamic,
        Setting.Property.NodeScope
    );

    private final LogsdbIndexModeSettingsProvider logsdbIndexModeSettingsProvider;

    public LogsDBPlugin(Settings settings) {
        this.settings = settings;
        this.licenseService = new SyntheticSourceLicenseService(settings);
        this.logsdbIndexModeSettingsProvider = new LogsdbIndexModeSettingsProvider(licenseService, settings);
    }

    @Override
    public Collection<?> createComponents(PluginServices services) {
        licenseService.setLicenseService(getLicenseService());
        licenseService.setLicenseState(getLicenseState());
        var clusterSettings = services.clusterService().getClusterSettings();
        // The `cluster.logsdb.enabled` setting is registered by this plugin, but its value may be updated by other plugins
        // before this plugin registers its settings update consumer below. This means we might miss updates that occurred earlier.
        // To handle this, we explicitly fetch the current `cluster.logsdb.enabled` setting value from the cluster settings
        // and update it, ensuring we capture any prior changes.
        logsdbIndexModeSettingsProvider.updateClusterIndexModeLogsdbEnabled(clusterSettings.get(CLUSTER_LOGSDB_ENABLED));
        clusterSettings.addSettingsUpdateConsumer(FALLBACK_SETTING, licenseService::setSyntheticSourceFallback);
        clusterSettings.addSettingsUpdateConsumer(
            CLUSTER_LOGSDB_ENABLED,
            logsdbIndexModeSettingsProvider::updateClusterIndexModeLogsdbEnabled
        );
        // Nothing to share here:
        return super.createComponents(services);
    }

    @Override
    public Collection<IndexSettingProvider> getAdditionalIndexSettingProviders(IndexSettingProvider.Parameters parameters) {
        logsdbIndexModeSettingsProvider.init(
            parameters.mapperServiceFactory(),
            () -> IndexVersion.min(
                IndexVersion.current(),
                parameters.clusterService().state().nodes().getMaxDataNodeCompatibleIndexVersion()
            ),
            DiscoveryNode.isStateless(settings) == false
        );
        return List.of(logsdbIndexModeSettingsProvider);
    }

    @Override
    public List<Setting<?>> getSettings() {
        return List.of(FALLBACK_SETTING, CLUSTER_LOGSDB_ENABLED);
    }

    @Override
    public List<ActionPlugin.ActionHandler<? extends ActionRequest, ? extends ActionResponse>> getActions() {
        List<ActionPlugin.ActionHandler<? extends ActionRequest, ? extends ActionResponse>> actions = new ArrayList<>();
        actions.add(new ActionPlugin.ActionHandler<>(XPackUsageFeatureAction.LOGSDB, LogsDBUsageTransportAction.class));
        actions.add(new ActionPlugin.ActionHandler<>(XPackInfoFeatureAction.LOGSDB, LogsDBInfoTransportAction.class));
        return actions;
    }

    @Override
    public Collection<RestHandler> getRestHandlers(
        Settings settings,
        NamedWriteableRegistry namedWriteableRegistry,
        RestController restController,
        ClusterSettings clusterSettings,
        IndexScopedSettings indexScopedSettings,
        SettingsFilter settingsFilter,
        IndexNameExpressionResolver indexNameExpressionResolver,
        Supplier<DiscoveryNodes> nodesInCluster,
        Predicate<NodeFeature> clusterSupportsFeature
    ) {
        if (Build.current().isSnapshot()) {
            return List.of(new RestAddRetentionLeaseAction());
        }
        return Collections.emptyList();
    }

    protected XPackLicenseState getLicenseState() {
        return XPackPlugin.getSharedLicenseState();
    }

    protected LicenseService getLicenseService() {
        return XPackPlugin.getSharedLicenseService();
    }
}
