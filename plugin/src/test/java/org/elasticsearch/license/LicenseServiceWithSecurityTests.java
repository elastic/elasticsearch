/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.license;

import org.elasticsearch.analysis.common.CommonAnalysisPlugin;
import org.elasticsearch.common.unit.TimeValue;
import org.elasticsearch.plugins.Plugin;
import org.elasticsearch.test.SecurityIntegTestCase;
import org.elasticsearch.transport.Netty4Plugin;
import org.elasticsearch.xpack.XPackPlugin;

import java.util.Arrays;
import java.util.Collection;

import static org.hamcrest.CoreMatchers.equalTo;

/**
 * Basic integration test that checks if license can be upgraded to a production license if TLS is enabled and vice versa.
 */
public class LicenseServiceWithSecurityTests extends SecurityIntegTestCase {

    @Override
    protected Collection<Class<? extends Plugin>> nodePlugins() {
        return Arrays.asList(XPackPlugin.class, CommonAnalysisPlugin.class, Netty4Plugin.class);
    }

    @Override
    protected Collection<Class<? extends Plugin>> transportClientPlugins() {
        return nodePlugins();
    }

    public void testLicenseUpgradeFailsWithoutTLS() throws Exception {
        assumeFalse("transport ssl is enabled", isTransportSSLEnabled());
        LicensingClient licensingClient = new LicensingClient(client());
        License license = licensingClient.prepareGetLicense().get().license();
        License prodLicense = TestUtils.generateSignedLicense("platinum", TimeValue.timeValueHours(24));
        IllegalStateException ise = expectThrows(IllegalStateException.class, () -> licensingClient.preparePutLicense(prodLicense).get());
        assertEquals("Can not upgrade to a production license unless TLS is configured or security is disabled", ise.getMessage());
        assertThat(licensingClient.prepareGetLicense().get().license(), equalTo(license));
    }

    public void testLicenseUpgradeSucceedsWithTLS() throws Exception {
        assumeTrue("transport ssl is disabled", isTransportSSLEnabled());
        LicensingClient licensingClient = new LicensingClient(client());
        License prodLicense = TestUtils.generateSignedLicense("platinum", TimeValue.timeValueHours(24));
        PutLicenseResponse putLicenseResponse = licensingClient.preparePutLicense(prodLicense).get();
        assertEquals(putLicenseResponse.status(), LicensesStatus.VALID);
        assertThat(licensingClient.prepareGetLicense().get().license(), equalTo(prodLicense));
    }
}
