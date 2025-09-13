/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.logsdb;

import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.license.LicenseService;
import org.elasticsearch.license.XPackLicenseState;
import org.elasticsearch.xpack.core.LocalStateCompositeXPackPlugin;

import java.nio.file.Path;

public class LocalStateLogsdb extends LocalStateCompositeXPackPlugin {
    public LocalStateLogsdb(Settings settings, Path configPath) {
        super(settings, configPath);
    }

    @Override
    public void setLicenseService(LicenseService licenseService) {
        super.setLicenseService(licenseService);
        setSharedLicenseService(licenseService);
    }

    @Override
    public void setLicenseState(XPackLicenseState licenseState) {
        super.setLicenseState(licenseState);
        setSharedLicenseState(licenseState);
    }
}
