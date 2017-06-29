/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.xpack.upgrade;

import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.unit.TimeValue;
import org.elasticsearch.index.reindex.ReindexPlugin;
import org.elasticsearch.license.AbstractLicensesIntegrationTestCase;
import org.elasticsearch.license.License;
import org.elasticsearch.license.TestUtils;
import org.elasticsearch.plugins.Plugin;
import org.elasticsearch.xpack.XPackPlugin;
import org.elasticsearch.xpack.XPackSettings;
import org.elasticsearch.xpack.ml.MachineLearning;
import org.elasticsearch.xpack.monitoring.test.MockPainlessScriptEngine;

import java.util.Arrays;
import java.util.Collection;

public abstract class IndexUpgradeIntegTestCase extends AbstractLicensesIntegrationTestCase {
    @Override
    protected boolean ignoreExternalCluster() {
        return true;
    }

    @Override
    protected Settings nodeSettings(int nodeOrdinal) {
        Settings.Builder settings = Settings.builder().put(super.nodeSettings(nodeOrdinal));
        settings.put(MachineLearning.AUTODETECT_PROCESS.getKey(), false);
        settings.put(XPackSettings.MACHINE_LEARNING_ENABLED.getKey(), false);
        settings.put(XPackSettings.SECURITY_ENABLED.getKey(), false);
        settings.put(XPackSettings.WATCHER_ENABLED.getKey(), false);
        settings.put(XPackSettings.MONITORING_ENABLED.getKey(), false);
        settings.put(XPackSettings.GRAPH_ENABLED.getKey(), false);
        return settings.build();
    }

    @Override
    protected Settings transportClientSettings() {
        Settings.Builder settings = Settings.builder().put(super.transportClientSettings());
        settings.put(MachineLearning.AUTODETECT_PROCESS.getKey(), false);
        settings.put(XPackSettings.MACHINE_LEARNING_ENABLED.getKey(), false);
        settings.put(XPackSettings.SECURITY_ENABLED.getKey(), false);
        settings.put(XPackSettings.WATCHER_ENABLED.getKey(), false);
        settings.put(XPackSettings.MONITORING_ENABLED.getKey(), false);
        settings.put(XPackSettings.GRAPH_ENABLED.getKey(), false);
        return settings.build();
    }

    @Override
    protected Collection<Class<? extends Plugin>> nodePlugins() {
        return Arrays.asList(XPackPlugin.class, ReindexPlugin.class, MockPainlessScriptEngine.TestPlugin.class);
    }

    @Override
    protected Collection<Class<? extends Plugin>> transportClientPlugins() {
        return nodePlugins();
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
