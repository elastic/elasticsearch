/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.watcher.test.rest;

import com.carrotsearch.randomizedtesting.annotations.Name;
import org.elasticsearch.plugins.Plugin;
import org.elasticsearch.test.rest.RestTestCandidate;
import org.elasticsearch.watcher.license.LicenseIntegrationTests;
import org.elasticsearch.watcher.license.LicenseIntegrationTests.MockLicenseService;
import org.junit.Test;

import java.io.IOException;

import static org.hamcrest.Matchers.containsString;

/**
 */
public class WatcherDisabledLicenseRestTests extends WatcherRestTests {

    @Override
    protected Class<? extends Plugin> licensePluginClass() {
        return LicenseIntegrationTests.MockLicensePlugin.class;
    }

    public WatcherDisabledLicenseRestTests(@Name("yaml") RestTestCandidate testCandidate) {
        super(testCandidate);
    }

    @Test
    public void test() throws IOException {
        try {
            disableLicensing();
            super.test();
            fail();
        } catch(AssertionError e) {
            assertThat(e.getMessage(), containsString("license expired for feature [watcher]"));
        } finally {
            enableLicensing();
        }
    }

    public static void disableLicensing() {
        for (MockLicenseService service : internalCluster().getInstances(MockLicenseService.class)) {
            service.disable();
        }
    }

    public static void enableLicensing() {
        for (MockLicenseService service : internalCluster().getInstances(MockLicenseService.class)) {
            service.enable();
        }
    }

    @Override
    protected boolean enableShield() {
        return false;
    }
}
