/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.action;

import org.elasticsearch.common.settings.Setting;
import org.elasticsearch.common.util.CollectionUtils;
import org.elasticsearch.license.License;
import org.elasticsearch.license.XPackLicenseState;
import org.elasticsearch.license.internal.XPackLicenseStatus;
import org.elasticsearch.xpack.core.XPackSettings;
import org.elasticsearch.xpack.esql.plugin.EsqlPlugin;

import java.util.List;

import static org.elasticsearch.test.ESTestCase.randomFrom;

/**
 * In IT tests, use this instead of the EsqlPlugin in order to use ES|QL features
 * that require an Enteprise (or Trial) license.
 */
public class EsqlPluginWithEnterpriseOrTrialLicense extends EsqlPlugin {
    protected XPackLicenseState getLicenseState() {
        License.OperationMode operationMode = randomFrom(License.OperationMode.ENTERPRISE, License.OperationMode.TRIAL);
        return new XPackLicenseState(() -> System.currentTimeMillis(), new XPackLicenseStatus(operationMode, true, "Test license expired"));
    }

    @Override
    public void loadExtensions(ExtensionLoader loader) {
        // nothing, else it would clash with super's SPI discoverer, which adds data source plugins
    }

    @Override
    public List<Setting<?>> getSettings() {
        // Internal-cluster tests do not load the security plugin, but xpack.security.enabled defaults
        // to true. Register the setting so those tests can disable it explicitly.
        return CollectionUtils.appendToCopy(super.getSettings(), XPackSettings.SECURITY_ENABLED);
    }
}
