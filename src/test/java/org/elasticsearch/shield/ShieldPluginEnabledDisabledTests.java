/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.shield;

import org.elasticsearch.common.settings.ImmutableSettings;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.shield.transport.SecuredTransportService;
import org.elasticsearch.shield.transport.netty.NettySecuredTransport;
import org.elasticsearch.test.ElasticsearchIntegrationTest.ClusterScope;
import org.elasticsearch.test.ShieldIntegrationTest;
import org.elasticsearch.transport.Transport;
import org.elasticsearch.transport.TransportService;
import org.hamcrest.Matcher;
import org.junit.BeforeClass;
import org.junit.Test;

import static org.elasticsearch.test.ElasticsearchIntegrationTest.Scope.SUITE;
import static org.hamcrest.Matchers.instanceOf;
import static org.hamcrest.Matchers.not;

/**
 *
 */
@ClusterScope(scope = SUITE)
public class ShieldPluginEnabledDisabledTests extends ShieldIntegrationTest {

    private static boolean enabled;

    @BeforeClass
    public static void init() {
        enabled = randomBoolean();
    }

    @Override
    protected Settings nodeSettings(int nodeOrdinal) {
        logger.info("******* shield is " + (enabled ? "enabled" : "disabled"));
        return ImmutableSettings.settingsBuilder()
                .put(super.nodeSettings(nodeOrdinal))
                .put("shield.enabled", enabled)
                .build();
    }

    @Override
    protected Settings transportClientSettings() {
        return ImmutableSettings.settingsBuilder()
                .put(super.transportClientSettings())
                .put("shield.enabled", enabled)
                .build();
    }

    @Test @SuppressWarnings("unchecked")
    public void testEnabledDisabled() throws Exception {
        for (TransportService service : internalCluster().getInstances(TransportService.class)) {
            Matcher matcher = instanceOf(SecuredTransportService.class);
            if (!enabled) {
                matcher = not(matcher);
            }
            assertThat(service, matcher);
        }
        for (Transport transport : internalCluster().getInstances(Transport.class)) {
            Matcher matcher = instanceOf(NettySecuredTransport.class);
            if (!enabled) {
                matcher = not(matcher);
            }
            assertThat(transport, matcher);
        }
    }
}
