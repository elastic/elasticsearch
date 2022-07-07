/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */
package org.elasticsearch.xpack.security.qa;

import org.elasticsearch.ElasticsearchSecurityException;
import org.elasticsearch.action.admin.cluster.health.ClusterHealthResponse;
import org.elasticsearch.action.admin.cluster.node.info.NodeInfo;
import org.elasticsearch.action.admin.cluster.node.info.NodesInfoResponse;
import org.elasticsearch.client.transport.TransportClient;
import org.elasticsearch.common.network.NetworkModule;
import org.elasticsearch.common.settings.SecureString;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.transport.TransportAddress;
import org.elasticsearch.plugins.Plugin;
import org.elasticsearch.test.ESIntegTestCase;
import org.elasticsearch.transport.TransportInfo;
import org.elasticsearch.xpack.client.PreBuiltXPackTransportClient;
import org.elasticsearch.xpack.core.XPackClientPlugin;
import org.elasticsearch.xpack.core.security.SecurityField;

import java.util.Collection;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.TimeUnit;

import static org.elasticsearch.xpack.core.security.authc.support.UsernamePasswordToken.basicAuthHeaderValue;
import static org.hamcrest.Matchers.containsString;
import static org.hamcrest.Matchers.is;

/**
 * Integration tests that test a transport client with security being loaded that connect to an external cluster
 */
public class SecurityTransportClientIT extends ESIntegTestCase {
    static final String ADMIN_USER_PW = "test_user:x-pack-test-password";
    static final String TRANSPORT_USER_PW = "transport:x-pack-test-password";

    @Override
    protected Settings externalClusterClientSettings() {
        return Settings.builder()
            .put(SecurityField.USER_SETTING.getKey(), ADMIN_USER_PW)
            .put(NetworkModule.TRANSPORT_TYPE_KEY, "security4")
            .build();
    }

    @Override
    protected Collection<Class<? extends Plugin>> transportClientPlugins() {
        return Collections.singletonList(XPackClientPlugin.class);
    }

    public void testThatTransportClientWithoutAuthenticationDoesNotWork() throws Exception {
        try (TransportClient client = transportClient(Settings.EMPTY)) {
            boolean connected = waitUntil(() -> client.connectedNodes().size() > 0, 5L, TimeUnit.SECONDS);

            assertThat(connected, is(false));
        }
    }

    public void testThatTransportClientAuthenticationWithTransportClientRole() throws Exception {
        Settings settings = Settings.builder().put(SecurityField.USER_SETTING.getKey(), TRANSPORT_USER_PW).build();
        try (TransportClient client = transportClient(settings)) {
            assertBusy(() -> assertFalse(client.connectedNodes().isEmpty()), 5L, TimeUnit.SECONDS);

            // this checks that the transport client is really running in a limited state
            try {
                client.admin().cluster().prepareHealth().get();
                fail("the transport user should not be be able to get health!");
            } catch (ElasticsearchSecurityException e) {
                assertThat(e.toString(), containsString("unauthorized"));
            }
        }
    }

    public void testTransportClientWithAdminUser() throws Exception {
        final boolean useTransportUser = randomBoolean();
        Settings settings = Settings.builder()
            .put(SecurityField.USER_SETTING.getKey(), useTransportUser ? TRANSPORT_USER_PW : ADMIN_USER_PW)
            .build();
        try (TransportClient client = transportClient(settings)) {
            assertBusy(() -> assertFalse(client.connectedNodes().isEmpty()), 5L, TimeUnit.SECONDS);

            // this checks that the transport client is really running in a limited state
            ClusterHealthResponse response;
            if (useTransportUser) {
                response = client.filterWithHeader(
                    Collections.singletonMap(
                        "Authorization",
                        basicAuthHeaderValue("test_user", new SecureString("x-pack-test-password".toCharArray()))
                    )
                ).admin().cluster().prepareHealth().get();
            } else {
                response = client.admin().cluster().prepareHealth().get();
            }

            assertThat(response.isTimedOut(), is(false));
        }
    }

    TransportClient transportClient(Settings extraSettings) {
        NodesInfoResponse nodeInfos = client().admin().cluster().prepareNodesInfo().get();
        List<NodeInfo> nodes = nodeInfos.getNodes();
        assertTrue(nodes.isEmpty() == false);
        TransportAddress publishAddress = randomFrom(nodes).getInfo(TransportInfo.class).address().publishAddress();
        String clusterName = nodeInfos.getClusterName().value();

        Settings settings = Settings.builder().put(extraSettings).put("cluster.name", clusterName).build();

        TransportClient client = new PreBuiltXPackTransportClient(settings);
        client.addTransportAddress(publishAddress);
        return client;
    }
}
