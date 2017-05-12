/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.xpack.upgrade;

import org.elasticsearch.ElasticsearchSecurityException;
import org.elasticsearch.common.Strings;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.unit.TimeValue;
import org.elasticsearch.license.AbstractLicensesIntegrationTestCase;
import org.elasticsearch.license.License;
import org.elasticsearch.license.TestUtils;
import org.elasticsearch.plugins.Plugin;
import org.elasticsearch.xpack.XPackPlugin;
import org.elasticsearch.xpack.XPackSettings;
import org.elasticsearch.xpack.ml.MachineLearning;
import org.elasticsearch.xpack.upgrade.actions.IndexUpgradeInfoAction;
import org.elasticsearch.xpack.upgrade.actions.IndexUpgradeInfoAction.Response;
import org.junit.Before;

import java.util.Collection;
import java.util.Collections;

import static org.elasticsearch.test.hamcrest.ElasticsearchAssertions.assertAcked;
import static org.hamcrest.Matchers.containsString;
import static org.hamcrest.Matchers.empty;
import static org.hamcrest.core.IsEqual.equalTo;

public class IndexUpgradeIT extends AbstractLicensesIntegrationTestCase {

    @Before
    public void resetLicensing() throws Exception {
        enableLicensing();
    }

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
        return Collections.singleton(XPackPlugin.class);
    }

    @Override
    protected Collection<Class<? extends Plugin>> transportClientPlugins() {
        return nodePlugins();
    }


    public void testIndexUpgradeInfo() {
        assertAcked(client().admin().indices().prepareCreate("test").get());
        assertAcked(client().admin().indices().prepareCreate("kibana_test").get());
        ensureYellow("test", "kibana_test");
        Response response = client().prepareExecute(IndexUpgradeInfoAction.INSTANCE).setIndices("test", "kibana_test")
                .setExtraParams(Collections.singletonMap("kibana_indices", "kibana_test")).get();
        logger.info("Got response [{}]", Strings.toString(response));
        assertThat(response.getActions().size(), equalTo(1));
        assertThat(response.getActions().get("kibana_test"), equalTo(UpgradeActionRequired.UPGRADE));
        assertThat(Strings.toString(response), containsString("kibana_test"));
    }

    public void testIndexUpgradeInfoLicense() throws Exception {
        assertAcked(client().admin().indices().prepareCreate("test").get());
        ensureYellow("test");
        disableLicensing();
        ElasticsearchSecurityException e = expectThrows(ElasticsearchSecurityException.class,
                () -> client().prepareExecute(IndexUpgradeInfoAction.INSTANCE).setIndices("test").get());
        assertThat(e.getMessage(), equalTo("current license is non-compliant for [upgrade]"));
        enableLicensing();
        Response response = client().prepareExecute(IndexUpgradeInfoAction.INSTANCE).setIndices("test").get();
        assertThat(response.getActions().entrySet(), empty());
    }

    private static String randomValidLicenseType() {
        return randomFrom("platinum", "gold", "standard", "basic");
    }

    private static String randomInvalidLicenseType() {
        return randomFrom("missing", "trial");
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
