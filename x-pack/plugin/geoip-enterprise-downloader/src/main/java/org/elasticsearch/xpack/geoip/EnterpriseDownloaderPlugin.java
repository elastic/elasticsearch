/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */
package org.elasticsearch.xpack.geoip;

import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.ingest.geoip.enterprise.EnterpriseGeoIpDownloaderTaskExecutor;
import org.elasticsearch.license.License;
import org.elasticsearch.license.LicensedFeature;
import org.elasticsearch.license.XPackLicenseState;
import org.elasticsearch.plugins.IngestPlugin;
import org.elasticsearch.plugins.Plugin;
import org.elasticsearch.xpack.core.XPackField;
import org.elasticsearch.xpack.core.XPackPlugin;

import java.util.Collection;
import java.util.List;
import java.util.function.Supplier;

public class EnterpriseDownloaderPlugin extends Plugin implements IngestPlugin {

    private final Settings settings;

    public EnterpriseDownloaderPlugin(final Settings settings) {
        this.settings = settings;
    }

    protected XPackLicenseState getLicenseState() {
        return XPackPlugin.getSharedLicenseState();
    }

    @Override
    public Collection<?> createComponents(PluginServices services) {

        Supplier<Boolean> licenseChecker = () -> LicensedFeature.momentary(
            null,
            XPackField.ENTERPRISE_GEOIP_DOWNLOADER,
            License.OperationMode.PLATINUM
        ).check(getLicenseState());
        EnterpriseGeoIpDownloaderTaskExecutor.setLicenseSupplier(licenseChecker);
        return List.of();
    }
}
