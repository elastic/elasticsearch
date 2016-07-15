/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.xpack.security;

import org.elasticsearch.common.network.NetworkModule;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.integration.LicensingTests;
import org.elasticsearch.test.SecurityIntegTestCase;
import org.elasticsearch.transport.Transport;
import org.elasticsearch.transport.TransportService;
import org.elasticsearch.xpack.XPackPlugin;
import org.elasticsearch.xpack.security.transport.SecurityServerTransportService;
import org.elasticsearch.xpack.security.transport.netty3.SecurityNetty3Transport;
import org.hamcrest.Matcher;
import org.junit.After;
import org.junit.BeforeClass;

import static org.hamcrest.Matchers.instanceOf;
import static org.hamcrest.Matchers.not;

public class SecurityPluginEnabledDisabledTests extends SecurityIntegTestCase {
    private static boolean enabled;

    @BeforeClass
    public static void init() {
        enabled = randomBoolean();
    }

    @After
    public void cleanup() throws Exception {
        // now that on a disabled license we block cluster health/stats and indices stats, we need
        // to make sure that after the tests (which disable the license for testing purposes) we
        // reenabled the license, so the internal cluster will be cleaned appropriately.
        logger.info("cleanup: enabling licensing...");
        LicensingTests.enableLicensing();
    }
    @Override
    protected Class<? extends XPackPlugin> xpackPluginClass() {
        return LicensingTests.InternalXPackPlugin.class;
    }

    @Override
    protected Settings nodeSettings(int nodeOrdinal) {
        logger.info("******* security is {}", enabled ? "enabled" : "disabled");
        return Settings.builder()
                .put(super.nodeSettings(nodeOrdinal))
                .put(XPackPlugin.featureEnabledSetting(Security.NAME), enabled)
                .put(NetworkModule.HTTP_ENABLED.getKey(), true)
                .build();
    }

    @Override
    protected Settings transportClientSettings() {
        return Settings.builder()
                .put(super.transportClientSettings())
                .put(XPackPlugin.featureEnabledSetting(Security.NAME), enabled)
                .build();
    }

    public void testTransportEnabledDisabled() throws Exception {
        for (TransportService service : internalCluster().getInstances(TransportService.class)) {
            Matcher<TransportService> matcher = instanceOf(SecurityServerTransportService.class);
            if (!enabled) {
                matcher = not(matcher);
            }
            assertThat(service, matcher);
        }
        for (Transport transport : internalCluster().getInstances(Transport.class)) {
            Matcher<Transport> matcher = instanceOf(SecurityNetty3Transport.class);
            if (!enabled) {
                matcher = not(matcher);
            }
            assertThat(transport, matcher);
        }
    }
}
