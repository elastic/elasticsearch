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
        disableLicensing();
        try {
            super.test();
            fail();
        } catch(AssertionError ae) {
            if (ae.getMessage() == null || ae.getMessage().contains("not supported")){
                //This was a test testing the "hijacked" methods
                return;
            }
            assertThat(ae.getMessage(), containsString("license expired for feature [watcher]"));
        }
    }

    public static void disableLicensing() {
        for (MockLicenseService service : internalCluster().getInstances(MockLicenseService.class)) {
            service.disable();
        }
    }
}
