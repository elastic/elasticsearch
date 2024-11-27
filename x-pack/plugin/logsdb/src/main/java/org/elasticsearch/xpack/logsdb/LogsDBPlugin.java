/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.logsdb;

import org.elasticsearch.action.ActionRequest;
import org.elasticsearch.action.ActionResponse;
import org.elasticsearch.cluster.node.DiscoveryNode;
import org.elasticsearch.common.settings.Setting;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.index.IndexSettingProvider;
import org.elasticsearch.license.LicenseService;
import org.elasticsearch.license.XPackLicenseState;
import org.elasticsearch.plugins.ActionPlugin;
import org.elasticsearch.plugins.Plugin;
import org.elasticsearch.xpack.core.XPackPlugin;
import org.elasticsearch.xpack.core.action.XPackInfoFeatureAction;
import org.elasticsearch.xpack.core.action.XPackUsageFeatureAction;

import java.util.ArrayList;
import java.util.Collection;
import java.util.List;

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
        this.logsdbIndexModeSettingsProvider = new LogsdbIndexModeSettingsProvider(settings);
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
        if (DiscoveryNode.isStateless(settings)) {
            return List.of(logsdbIndexModeSettingsProvider);
        }
        var syntheticSettingProvider = new SyntheticSourceIndexSettingsProvider(
            licenseService,
            parameters.mapperServiceFactory(),
            logsdbIndexModeSettingsProvider,
            () -> parameters.clusterService().state().nodes().getMinSupportedIndexVersion()
        );
        return List.of(syntheticSettingProvider, logsdbIndexModeSettingsProvider);
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

    protected XPackLicenseState getLicenseState() {
        return XPackPlugin.getSharedLicenseState();
    }

    protected LicenseService getLicenseService() {
        return XPackPlugin.getSharedLicenseService();
    }
}
