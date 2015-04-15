/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.watcher.test.rest;

import com.carrotsearch.randomizedtesting.annotations.Name;
import org.elasticsearch.common.settings.ImmutableSettings;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.license.plugin.core.LicenseExpiredException;
import org.elasticsearch.plugins.Plugin;
import org.elasticsearch.shield.ShieldPlugin;
import org.elasticsearch.test.rest.RestTestCandidate;
import org.elasticsearch.watcher.WatcherPlugin;
import org.elasticsearch.watcher.license.LicenseIntegrationTests;
import org.junit.Test;

import java.io.IOException;

import static org.hamcrest.Matchers.is;

/**
 */
public class WatcherDisabledLicenseRestTests extends WatcherRestTests {


    @Override
    protected Settings nodeSettings(int nodeOrdinal) {
        ImmutableSettings.Builder builder = ImmutableSettings.builder()
                .put(super.nodeSettings(nodeOrdinal))
                .put("scroll.size", randomIntBetween(1, 100))
                .put("plugin.types",
                        WatcherPlugin.class.getName() + "," +
                                (shieldEnabled ? ShieldPlugin.class.getName() + "," : "") +
                                licensePluginClass().getName())
                .put(ShieldSettings.settings(shieldEnabled));
        return builder.build();
    }

    protected Class<? extends Plugin> licensePluginClass() {
        return LicenseIntegrationTests.MockLicensePlugin.class;
    }


    public WatcherDisabledLicenseRestTests(@Name("yaml") RestTestCandidate testCandidate) {
        super(testCandidate);
    }

    @Test
    public void test() throws IOException {
        disableLicensing();
        try {
            super.test();
            fail();
        } catch(AssertionError ae) {
            assertThat(ae.getMessage().contains("401 Unauthorized"), is(true));
            assertThat(ae.getMessage().contains(LicenseExpiredException.class.getSimpleName()), is(true));
        }
    }

    public static void disableLicensing() {
        for (LicenseIntegrationTests.MockLicenseService service : internalCluster().getInstances(LicenseIntegrationTests.MockLicenseService.class)) {
            service.disable();
        }
    }
}
