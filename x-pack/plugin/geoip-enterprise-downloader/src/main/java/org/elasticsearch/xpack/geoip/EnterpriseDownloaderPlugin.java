/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */
package org.elasticsearch.xpack.geoip;

import org.elasticsearch.client.internal.Client;
import org.elasticsearch.cluster.metadata.IndexNameExpressionResolver;
import org.elasticsearch.cluster.service.ClusterService;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.settings.SettingsModule;
import org.elasticsearch.license.XPackLicenseState;
import org.elasticsearch.persistent.PersistentTasksExecutor;
import org.elasticsearch.plugins.IngestPlugin;
import org.elasticsearch.plugins.PersistentTaskPlugin;
import org.elasticsearch.plugins.Plugin;
import org.elasticsearch.threadpool.ThreadPool;
import org.elasticsearch.xpack.core.XPackPlugin;

import java.util.Collection;
import java.util.List;

public class EnterpriseDownloaderPlugin extends Plugin implements IngestPlugin, PersistentTaskPlugin {

    private final Settings settings;
    private EnterpriseGeoIpDownloaderLicenseListener enterpriseGeoIpDownloaderTaskExecutor;

    public EnterpriseDownloaderPlugin(final Settings settings) {
        this.settings = settings;
    }

    protected XPackLicenseState getLicenseState() {
        return XPackPlugin.getSharedLicenseState();
    }

    @Override
    public Collection<?> createComponents(PluginServices services) {
        enterpriseGeoIpDownloaderTaskExecutor = new EnterpriseGeoIpDownloaderLicenseListener(
            services.client(),
            services.clusterService(),
            services.threadPool(),
            getLicenseState()
        );
        enterpriseGeoIpDownloaderTaskExecutor.init();
        return List.of(enterpriseGeoIpDownloaderTaskExecutor);
    }

    @Override
    public List<PersistentTasksExecutor<?>> getPersistentTasksExecutor(
        ClusterService clusterService,
        ThreadPool threadPool,
        Client client,
        SettingsModule settingsModule,
        IndexNameExpressionResolver expressionResolver
    ) {
        return List.of();
    }
}
