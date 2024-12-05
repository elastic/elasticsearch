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

/**
 * In IT tests, use this instead of the EsqlPlugin in order to use Esql features
 * that require a license.
 */
public class EsqlPluginWithTrialLicense extends EsqlPlugin {
    protected XPackLicenseState getLicenseState() {
        return new XPackLicenseState(
            () -> System.currentTimeMillis(),
            new XPackLicenseStatus(License.OperationMode.ENTERPRISE, true, "Test license expired")
        );
    }
}
