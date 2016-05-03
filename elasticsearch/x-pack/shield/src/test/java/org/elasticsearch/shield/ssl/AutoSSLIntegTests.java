/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.shield.ssl;

import org.elasticsearch.client.transport.NoNodeAvailableException;
import org.elasticsearch.client.transport.TransportClient;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.transport.TransportAddress;
import org.elasticsearch.shield.Security;
import org.elasticsearch.shield.transport.netty.ShieldNettyTransport;
import org.elasticsearch.test.ShieldIntegTestCase;
import org.elasticsearch.transport.Transport;
import org.elasticsearch.xpack.XPackPlugin;

import static org.elasticsearch.test.ShieldSettingsSource.DEFAULT_PASSWORD;
import static org.elasticsearch.test.ShieldSettingsSource.DEFAULT_USER_NAME;
import static org.hamcrest.Matchers.containsString;

public class AutoSSLIntegTests extends ShieldIntegTestCase {

    @Override
    public boolean sslTransportEnabled() {
        return true;
    }

    @Override
    public boolean autoSSLEnabled() {
        return true;
    }

    public void testTransportClient() {
        String clusterName = internalCluster().getClusterName();
        TransportAddress transportAddress = randomFrom(internalCluster().getInstance(Transport.class).boundAddress().boundAddresses());
        try (TransportClient transportClient = TransportClient.builder().addPlugin(XPackPlugin.class)
                .settings(Settings.builder()
                        .put("cluster.name", clusterName)
                        .put(Security.USER_SETTING.getKey(), DEFAULT_USER_NAME + ":" + DEFAULT_PASSWORD))
                .build()) {
            transportClient.addTransportAddress(transportAddress);
            assertGreenClusterState(transportClient);
        }

        // now try with SSL disabled and it should fail
        try (TransportClient transportClient = TransportClient.builder().addPlugin(XPackPlugin.class)
                .settings(Settings.builder()
                        .put("cluster.name", clusterName)
                        .put(ShieldNettyTransport.SSL_SETTING.getKey(), false)
                        .put(Security.USER_SETTING.getKey(), DEFAULT_USER_NAME + ":" + DEFAULT_PASSWORD))
                .build()) {
            transportClient.addTransportAddress(transportAddress);
            assertGreenClusterState(transportClient);
            fail("should not have been able to connect");
        } catch (NoNodeAvailableException e) {
            assertThat(e.getMessage(), containsString("None of the configured nodes are available"));
        }
    }
}
