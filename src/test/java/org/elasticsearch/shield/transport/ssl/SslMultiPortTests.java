/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.shield.transport.ssl;

import org.elasticsearch.client.transport.NoNodeAvailableException;
import org.elasticsearch.client.transport.TransportClient;
import org.elasticsearch.common.settings.ImmutableSettings;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.transport.InetSocketTransportAddress;
import org.elasticsearch.common.transport.TransportAddress;
import org.elasticsearch.plugins.PluginsService;
import org.elasticsearch.shield.test.ShieldIntegrationTest;
import org.elasticsearch.shield.transport.netty.NettySecuredTransport;
import org.elasticsearch.transport.Transport;
import org.elasticsearch.transport.TransportModule;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;

import java.io.File;

import static org.elasticsearch.common.settings.ImmutableSettings.settingsBuilder;
import static org.hamcrest.Matchers.is;

/**
 *
 */
public class SslMultiPortTests extends ShieldIntegrationTest {

    private ImmutableSettings.Builder builder;
    private static int randomClientPort;

    @BeforeClass
    public static void getRandomPort() {
        randomClientPort = randomIntBetween(49000, 65500); // ephemeral port
    }

    @Before
    public void setupBuilder() {
        builder = settingsBuilder()
                .put(TransportModule.TRANSPORT_TYPE_KEY, NettySecuredTransport.class.getName())
                .put("plugins." + PluginsService.LOAD_PLUGIN_FROM_CLASSPATH, false)
                .put("node.mode", "network")
                .put("cluster.name", internalCluster().getClusterName());

        setUser(builder);
    }

    @Override
    protected Settings nodeSettings(int nodeOrdinal) {
        String randomClientPortRange = randomClientPort + "-" + (randomClientPort+100);

        File store;
        try {
            store = new File(getClass().getResource("/org/elasticsearch/shield/transport/ssl/certs/simple/testnode-client-profile.jks").toURI());
            assertThat(store.exists(), is(true));
        } catch (Exception e) {
            throw new RuntimeException(e);
        }

        return settingsBuilder()
                .put(super.nodeSettings(nodeOrdinal))
                // settings for default key profile
                .put(getSSLSettingsForStore("/org/elasticsearch/shield/transport/ssl/certs/simple/testnode.jks", "testnode"))
                // client set up here
                .put("transport.profiles.client.port", randomClientPortRange)
                .put("transport.profiles.client.bind_host", "localhost") // make sure this is "localhost", no matter if ipv4 or ipv6, but be consistent
                .put("transport.profiles.client.shield.truststore.path", store.getAbsolutePath()) // settings for client truststore
                .put("transport.profiles.client.shield.truststore.password", "testnode-client-profile")
                .put("shield.audit.enabled", false )

                .build();
    }

    @Test
    public void  testThatStandardTransportClientCanConnectToDefaultProfile() throws Exception {
        builder.put(getSSLSettingsForStore("/org/elasticsearch/shield/transport/ssl/certs/simple/testclient.jks", "testclient"));
        try (TransportClient transportClient = new TransportClient(builder, false)) {
            TransportAddress transportAddress = internalCluster().getInstance(Transport.class).boundAddress().boundAddress();
            transportClient.addTransportAddress(transportAddress);
            assertGreenClusterState(transportClient);

        }
    }

    @Test(expected = NoNodeAvailableException.class)
    public void  testThatStandardTransportClientCannotConnectToClientProfile() throws Exception {
        builder.put(getSSLSettingsForStore("/org/elasticsearch/shield/transport/ssl/certs/simple/testclient.jks", "testclient"));
        try (TransportClient transportClient = new TransportClient(builder, false)) {
            transportClient.addTransportAddress(new InetSocketTransportAddress("localhost", randomClientPort));
            transportClient.admin().cluster().prepareHealth().get();
        }
    }

    @Test
    public void  testThatProfileTransportClientCanConnectToClientProfile() throws Exception {
        builder.put(getSSLSettingsForStore("/org/elasticsearch/shield/transport/ssl/certs/simple/testclient-client-profile.jks", "testclient-client-profile"));
        try (TransportClient transportClient = new TransportClient(builder, false)) {
            transportClient.addTransportAddress(new InetSocketTransportAddress("localhost", randomClientPort));
            assertGreenClusterState(transportClient);
        }
    }

    @Test(expected = NoNodeAvailableException.class)
    public void  testThatProfileTransportClientCannotConnectToDefaultProfile() throws Exception {
        builder.put(getSSLSettingsForStore("/org/elasticsearch/shield/transport/ssl/certs/simple/testclient-client-profile.jks", "testclient-client-profile"));
        try (TransportClient transportClient = new TransportClient(builder, false)) {
            TransportAddress transportAddress = internalCluster().getInstance(Transport.class).boundAddress().boundAddress();
            transportClient.addTransportAddress(transportAddress);
            transportClient.admin().cluster().prepareHealth().get();
        }
    }
}
