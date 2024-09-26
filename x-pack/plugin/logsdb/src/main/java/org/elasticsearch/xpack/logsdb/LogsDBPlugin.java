/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.logsdb;

import org.elasticsearch.cluster.node.DiscoveryNode;
import org.elasticsearch.common.settings.Setting;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.index.IndexSettingProvider;
import org.elasticsearch.plugins.Plugin;
import org.elasticsearch.xpack.core.XPackPlugin;

import java.util.Collection;
import java.util.List;

import static org.elasticsearch.xpack.logsdb.SyntheticSourceLicenseService.FALLBACK_SETTING;

public class LogsDBPlugin extends Plugin {

    private final Settings settings;
    private final SyntheticSourceLicenseService licenseService;

    public LogsDBPlugin(Settings settings) {
        this.settings = settings;
        this.licenseService = new SyntheticSourceLicenseService(settings);
    }

    @Override
    public Collection<?> createComponents(PluginServices services) {
        licenseService.setLicenseState(XPackPlugin.getSharedLicenseState());
        var clusterSettings = services.clusterService().getClusterSettings();
        clusterSettings.addSettingsUpdateConsumer(FALLBACK_SETTING, licenseService::setSyntheticSourceFallback);
        // Nothing to share here:
        return super.createComponents(services);
    }

    @Override
    public Collection<IndexSettingProvider> getAdditionalIndexSettingProviders(IndexSettingProvider.Parameters parameters) {
        if (DiscoveryNode.isStateless(settings)) {
            return List.of();
        }
        return List.of(new SyntheticSourceIndexSettingsProvider(licenseService));
    }

    @Override
    public List<Setting<?>> getSettings() {
        return List.of(FALLBACK_SETTING);
    }
}
