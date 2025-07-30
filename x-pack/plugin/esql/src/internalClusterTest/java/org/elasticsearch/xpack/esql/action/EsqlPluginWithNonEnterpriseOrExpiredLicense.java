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
 * In IT tests, use this instead of the EsqlPlugin in order to test ES|QL features
 * using either a:
 *  - an active (non-expired) basic, standard, missing, gold or platinum Elasticsearch license, OR
 *  - an expired enterprise or trial license
 */
public class EsqlPluginWithNonEnterpriseOrExpiredLicense extends EsqlPlugin {
    protected XPackLicenseState getLicenseState() {
        License.OperationMode operationMode;
        boolean active;
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
            active = false;  // expired
        }

        return new XPackLicenseState(
            () -> System.currentTimeMillis(),
            new XPackLicenseStatus(operationMode, active, "Test license expired")
        );
    }
}
