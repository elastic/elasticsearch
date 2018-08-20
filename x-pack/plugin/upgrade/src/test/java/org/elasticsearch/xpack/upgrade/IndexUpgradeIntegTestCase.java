/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.xpack.upgrade;

import org.elasticsearch.analysis.common.CommonAnalysisPlugin;
import org.elasticsearch.common.unit.TimeValue;
import org.elasticsearch.index.reindex.ReindexPlugin;
import org.elasticsearch.license.AbstractLicensesIntegrationTestCase;
import org.elasticsearch.license.License;
import org.elasticsearch.license.TestUtils;
import org.elasticsearch.plugins.Plugin;
import org.elasticsearch.xpack.core.LocalStateCompositeXPackPlugin;
import org.elasticsearch.xpack.core.XPackClientPlugin;
import org.elasticsearch.xpack.core.monitoring.test.MockPainlessScriptEngine;

import java.util.Arrays;
import java.util.Collection;

public abstract class IndexUpgradeIntegTestCase extends AbstractLicensesIntegrationTestCase {
    @Override
    protected boolean ignoreExternalCluster() {
        return true;
    }

    @Override
    protected Collection<Class<? extends Plugin>> nodePlugins() {
        return Arrays.asList(LocalStateCompositeXPackPlugin.class, Upgrade.class, ReindexPlugin.class,
                             MockPainlessScriptEngine.TestPlugin.class, CommonAnalysisPlugin.class);
    }

    @Override
    protected Collection<Class<? extends Plugin>> transportClientPlugins() {
        return Arrays.asList(XPackClientPlugin.class, ReindexPlugin.class);
    }
    private static String randomValidLicenseType() {
        return randomFrom("trial", "platinum", "gold", "standard", "basic");
    }

    private static String randomInvalidLicenseType() {
        return "missing";
    }

    public void disableLicensing() throws Exception {
        updateLicensing(randomInvalidLicenseType());
    }

    public void enableLicensing() throws Exception {
        updateLicensing(randomValidLicenseType());
    }

    public void updateLicensing(String licenseType) throws Exception {
        wipeAllLicenses();
        if (licenseType.equals("missing")) {
            putLicenseTombstone();
        } else {
            License license = TestUtils.generateSignedLicense(licenseType, TimeValue.timeValueMinutes(1));
            putLicense(license);
        }
    }
}
