/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.license;

import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.license.internal.XPackLicenseStatus;
import org.elasticsearch.plugins.Plugin;
import org.elasticsearch.test.ESIntegTestCase;
import org.elasticsearch.xpack.core.LocalStateCompositeXPackPlugin;
import org.elasticsearch.xpack.diskbbq.DiskBBQPlugin;
import org.junit.Before;

import java.nio.file.Path;
import java.util.Arrays;
import java.util.Collection;

@ESIntegTestCase.ClusterScope(scope = ESIntegTestCase.Scope.TEST, numDataNodes = 1, numClientNodes = 0, supportsDedicatedMasters = false)
public class DiskBBQLicensingIT extends ESIntegTestCase {

    @Before
    public void resetLicensing() {
        enableLicensing();

        ensureStableCluster(1);
        ensureYellow();
    }

    @Override
    protected Settings nodeSettings(int nodeOrdinal, Settings otherSettings) {
        Settings.Builder settings = Settings.builder().put(super.nodeSettings(nodeOrdinal, otherSettings));
        settings.put(LicenseSettings.SELF_GENERATED_LICENSE_TYPE.getKey(), "trial");
        return settings.build();
    }

    public void testCreateDiskBBQIndexRestricted() {
        disableLicensing(randomInvalidLicenseType());
        Exception e = expectThrows(Exception.class, () -> client().admin().indices().prepareCreate("diskbbq-index").setMapping("""
              {
              "properties": {
                "vector": {
                  "type": "dense_vector",
                  "index_options": {
                    "type": "bbq_disk"
                  }
                }
              }
            }""").get());
        assertTrue(e.getMessage().contains("current license is non-compliant for [bbq_disk] usage"));
    }

    private static License.OperationMode randomInvalidLicenseType() {
        return randomFrom(
            License.OperationMode.GOLD,
            License.OperationMode.STANDARD,
            License.OperationMode.BASIC,
            License.OperationMode.PLATINUM
        );
    }

    public static void enableLicensing() {
        enableLicensing(randomValidLicenseType());
    }

    public static void disableLicensing() {
        disableLicensing(randomValidLicenseType());
    }

    public static void disableLicensing(License.OperationMode operationMode) {
        for (XPackLicenseState licenseState : internalCluster().getInstances(XPackLicenseState.class)) {
            licenseState.update(new XPackLicenseStatus(operationMode, false, null));
        }
    }

    private static License.OperationMode randomValidLicenseType() {
        return randomFrom(License.OperationMode.TRIAL, License.OperationMode.ENTERPRISE);
    }

    public static void enableLicensing(License.OperationMode operationMode) {
        for (XPackLicenseState licenseState : internalCluster().getInstances(XPackLicenseState.class)) {
            licenseState.update(new XPackLicenseStatus(operationMode, true, null));
        }
    }

    public static class LocalStateDiskBBQ extends LocalStateCompositeXPackPlugin {
        private final DiskBBQPlugin plugin;

        public LocalStateDiskBBQ(final Settings settings, final Path configPath) {
            super(settings, configPath);
            LocalStateDiskBBQ thisVar = this;
            plugin = new DiskBBQPlugin(settings) {
                @Override
                protected XPackLicenseState getLicenseState() {
                    return thisVar.getLicenseState();
                }
            };
            plugins.add(plugin);
        }
    }

    @Override
    protected Collection<Class<? extends Plugin>> nodePlugins() {
        return Arrays.asList(LocalStateDiskBBQ.class);
    }
}
