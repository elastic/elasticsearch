/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.logsdb;

import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.license.License;
import org.elasticsearch.license.LicenseService;
import org.elasticsearch.license.XPackLicenseState;
import org.elasticsearch.license.internal.XPackLicenseStatus;
import org.elasticsearch.xpack.core.LocalStateCompositeXPackPlugin;

import java.nio.file.Path;

import static org.elasticsearch.xpack.logsdb.SyntheticSourceLicenseServiceTests.createGoldOrPlatinumLicense;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

public class LocalStateLogsDPPlugin extends LocalStateCompositeXPackPlugin {

    public LocalStateLogsDPPlugin(Settings settings, Path configPath) throws Exception {
        super(settings, configPath);
        long startTime = SyntheticSourceLicenseService.DEFAULT_CUTOFF_DATE;
        var testLicence = createGoldOrPlatinumLicense(startTime);
        License.OperationMode operationMode;
        if (testLicence.type().equals("gold")) {
            operationMode = License.OperationMode.GOLD;
        } else if (testLicence.type().equals("platinum")) {
            operationMode = License.OperationMode.PLATINUM;
        } else {
            throw new IllegalStateException("Unsupported license type [" + testLicence.type() + "]");
        }

        var status = new XPackLicenseStatus(operationMode, true, null);
        var licenseState = new XPackLicenseState(() -> startTime, status);
        var licenseService = mock(LicenseService.class);
        when(licenseService.getLicense()).thenReturn(createGoldOrPlatinumLicense(startTime));
        var plugin = new LogsDBPlugin(settings) {

            @Override
            protected XPackLicenseState getLicenseState() {
                return licenseState;
            }

            @Override
            protected LicenseService getLicenseService() {
                return licenseService;
            }
        };
        plugins.add(plugin);
    }
}
