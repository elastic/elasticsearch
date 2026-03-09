/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.action;

import org.elasticsearch.license.License;
import org.elasticsearch.license.XPackLicenseState;
import org.elasticsearch.license.internal.XPackLicenseStatus;
import org.elasticsearch.xpack.esql.plugin.EsqlPlugin;

import static org.elasticsearch.test.ESTestCase.randomBoolean;
import static org.elasticsearch.test.ESTestCase.randomFrom;

/**
 * In tests, use an implementation of this class instead of the EsqlPlugin in order to test ES|QL features
 */
public abstract class EsqlPluginAbstract extends EsqlPlugin {
    private final XPackLicenseState licenseState;

    protected EsqlPluginAbstract(final boolean licenseShouldPass, final String expiryWarning) {
        final License.OperationMode operationMode;
        final boolean active;
        if (licenseShouldPass) {
            operationMode = randomFrom(License.OperationMode.ENTERPRISE, License.OperationMode.TRIAL);
            active = true;
        } else {
            if (randomBoolean()) {
                operationMode = randomFrom(
                    License.OperationMode.PLATINUM,
                    License.OperationMode.GOLD,
                    License.OperationMode.BASIC,
                    License.OperationMode.MISSING,
                    License.OperationMode.STANDARD
                );
                active = true;
            } else {
                operationMode = randomFrom(License.OperationMode.ENTERPRISE, License.OperationMode.TRIAL);
                active = false; // expired
            }
        }
        this.licenseState = new XPackLicenseState(
            () -> System.currentTimeMillis(),
            new XPackLicenseStatus(operationMode, active, expiryWarning)
        );
    }

    protected EsqlPluginAbstract(boolean licenseShouldPass) {
        this(licenseShouldPass, "Test license expired");
    }

    protected EsqlPluginAbstract(XPackLicenseState licenseState) {
        this.licenseState = licenseState;
    }

    protected XPackLicenseState getLicenseState() {
        return this.licenseState;
    }

    @Override
    public void loadExtensions(ExtensionLoader loader) {
        // nothing, else it would clash with super's SPI discoverer, which adds data source plugins
    }
}
