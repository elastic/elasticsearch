/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */
package org.elasticsearch.xpack.geoip;

import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.license.XPackLicenseState;
import org.elasticsearch.plugins.Plugin;
import org.elasticsearch.xpack.core.XPackPlugin;

import java.util.Collection;
import java.util.List;

/**
 * This plugin is used to start the enterprise geoip downloader task (See {@link org.elasticsearch.ingest.EnterpriseGeoIpTask}). That task
 * requires having a platinum license. But the geoip code is in a non-xpack module that doesn't know about licensing. This plugin has a
 * license listener that will start the task if the license is valid, and will stop the task if it becomes invalid. This lets us enforce
 * the license without having to either put license logic into a non-xpack module, or put a lot of shared geoip code (much of which does
 * not require a platinum license) into xpack.
 */
public class EnterpriseDownloaderPlugin extends Plugin {

    private final Settings settings;
    private EnterpriseGeoIpDownloaderLicenseListener enterpriseGeoIpDownloaderLicenseListener;

    public EnterpriseDownloaderPlugin(final Settings settings) {
        this.settings = settings;
    }

    protected XPackLicenseState getLicenseState() {
        return XPackPlugin.getSharedLicenseState();
    }

    @Override
    public Collection<?> createComponents(PluginServices services) {
        enterpriseGeoIpDownloaderLicenseListener = new EnterpriseGeoIpDownloaderLicenseListener(
            services.client(),
            services.clusterService(),
            services.threadPool(),
            getLicenseState()
        );
        enterpriseGeoIpDownloaderLicenseListener.init();
        return List.of(enterpriseGeoIpDownloaderLicenseListener);
    }
}
