/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.security.transport.netty4;

import org.elasticsearch.ElasticsearchSecurityException;
import org.elasticsearch.TransportVersion;
import org.elasticsearch.action.ActionListener;
import org.elasticsearch.action.support.PlainActionFuture;
import org.elasticsearch.cluster.ClusterName;
import org.elasticsearch.cluster.node.DiscoveryNodeRole;
import org.elasticsearch.cluster.node.VersionInformation;
import org.elasticsearch.common.io.stream.NamedWriteableRegistry;
import org.elasticsearch.common.network.NetworkService;
import org.elasticsearch.common.settings.MockSecureSettings;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.util.PageCacheRecycler;
import org.elasticsearch.core.IOUtils;
import org.elasticsearch.indices.breaker.NoneCircuitBreakerService;
import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.test.NodeRoles;
import org.elasticsearch.test.transport.MockTransportService;
import org.elasticsearch.threadpool.TestThreadPool;
import org.elasticsearch.threadpool.ThreadPool;
import org.elasticsearch.transport.NoSuchRemoteClusterException;
import org.elasticsearch.transport.ProxyConnectionStrategy;
import org.elasticsearch.transport.RemoteClusterPortSettings;
import org.elasticsearch.transport.RemoteClusterService;
import org.elasticsearch.transport.RemoteConnectionStrategy;
import org.elasticsearch.transport.TransportService;
import org.elasticsearch.transport.netty4.SharedGroupFactory;
import org.elasticsearch.xpack.core.XPackSettings;
import org.elasticsearch.xpack.core.ssl.SSLService;
import org.elasticsearch.xpack.security.authc.CrossClusterAccessAuthenticationService;
import org.junit.After;
import org.junit.Before;

import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.concurrent.TimeUnit;

import static org.elasticsearch.test.ActionListenerUtils.anyActionListener;
import static org.elasticsearch.test.NodeRoles.onlyRole;
import static org.hamcrest.Matchers.instanceOf;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.doAnswer;
import static org.mockito.Mockito.mock;

public class SecurityNetty4ServerTransportAuthenticationTests extends ESTestCase {

    private ThreadPool threadPool;
    private SecurityNetty4ServerTransport remoteSecurityNetty4ServerTransport;
    private MockTransportService remoteTransportService;
    private CrossClusterAccessAuthenticationService remoteCrossClusterAccessAuthenticationService;

    @Override
    @Before
    public void setUp() throws Exception {
        super.setUp();
        threadPool = new TestThreadPool(getClass().getName());
        Settings remoteSettings = Settings.builder()
            .put("node.name", getClass().getName())
            .put(ClusterName.CLUSTER_NAME_SETTING.getKey(), "remote_security_server_transport")
            .put(XPackSettings.TRANSPORT_SSL_ENABLED.getKey(), "false")
            .put(XPackSettings.REMOTE_CLUSTER_SERVER_SSL_ENABLED.getKey(), "false")
            .put(XPackSettings.REMOTE_CLUSTER_CLIENT_SSL_ENABLED.getKey(), "false")
            .put(RemoteClusterPortSettings.REMOTE_CLUSTER_SERVER_ENABLED.getKey(), "true")
            .build();
        remoteSettings = NodeRoles.nonRemoteClusterClientNode(remoteSettings);
        remoteCrossClusterAccessAuthenticationService = mock(CrossClusterAccessAuthenticationService.class);
        remoteSecurityNetty4ServerTransport = new SecurityNetty4ServerTransport(
            remoteSettings,
            TransportVersion.current(),
            threadPool,
            new NetworkService(List.of()),
            PageCacheRecycler.NON_RECYCLING_INSTANCE,
            new NamedWriteableRegistry(List.of()),
            new NoneCircuitBreakerService(),
            null,
            mock(SSLService.class),
            new SharedGroupFactory(remoteSettings),
            remoteCrossClusterAccessAuthenticationService
        );
        remoteTransportService = MockTransportService.createNewService(
            remoteSettings,
            remoteSecurityNetty4ServerTransport,
            VersionInformation.CURRENT,
            threadPool,
            null,
            Collections.emptySet(),
            TransportService.NOOP_TRANSPORT_INTERCEPTOR
        );
        remoteTransportService.start();
        remoteTransportService.acceptIncomingRequests();
    }

    @Override
    @After
    public void tearDown() throws Exception {
        super.tearDown();
        IOUtils.close(
            remoteTransportService,
            remoteSecurityNetty4ServerTransport,
            () -> ThreadPool.terminate(threadPool, 10, TimeUnit.SECONDS)
        );
    }

    @SuppressWarnings("unchecked")
    public void testConnectionClosesWhenAuthenticatorAlwaysFails() throws Exception {
        doAnswer(invocation -> {
            ((ActionListener<Void>) invocation.getArguments()[1]).onFailure(new ElasticsearchSecurityException("failed authn"));
            return null;
        }).when(remoteCrossClusterAccessAuthenticationService).tryAuthenticate(any(Map.class), anyActionListener());
        Settings localSettings = Settings.builder()
            .put(onlyRole(DiscoveryNodeRole.REMOTE_CLUSTER_CLIENT_ROLE))
            .put(
                RemoteConnectionStrategy.REMOTE_CONNECTION_MODE.getConcreteSettingForNamespace("remote_security_server_transport").getKey(),
                "proxy"
            )
            .put(
                ProxyConnectionStrategy.PROXY_ADDRESS.getConcreteSettingForNamespace("remote_security_server_transport").getKey(),
                remoteTransportService.boundRemoteAccessAddress().publishAddress().toString()
            )
            .put(
                ProxyConnectionStrategy.REMOTE_SOCKET_CONNECTIONS.getConcreteSettingForNamespace("remote_security_server_transport")
                    .getKey(),
                randomIntBetween(1, 3) // easier to debug with just 1 connection
            )
            .build();
        {
            final MockSecureSettings secureSettings = new MockSecureSettings();
            secureSettings.setString(
                RemoteClusterService.REMOTE_CLUSTER_CREDENTIALS.getConcreteSettingForNamespace("remote_security_server_transport").getKey(),
                randomAlphaOfLength(20)
            );
            localSettings = Settings.builder().put(localSettings).setSecureSettings(secureSettings).build();
        }
        try (
            MockTransportService localService = MockTransportService.createNewService(
                localSettings,
                VersionInformation.CURRENT,
                TransportVersion.current(),
                threadPool
            )
        ) {
            localService.start();
            RemoteClusterService remoteClusterService = localService.getRemoteClusterService();
            for (int i = 0; i < randomIntBetween(4, 16); i++) {
                PlainActionFuture<Void> closeFuture = PlainActionFuture.newFuture();
                remoteClusterService.maybeEnsureConnectedAndGetConnection(
                    "remote_security_server_transport",
                    true,
                    ActionListener.wrap(connection -> {
                        // {@code RemoteClusterService.REMOTE_CLUSTER_HANDSHAKE_ACTION_NAME} fails authn and the connection is closed,
                        // but it is usually closed AFTER the handshake response returned
                        logger.info("Connection will auto-close");
                        connection.addCloseListener(closeFuture);
                    }, e -> {
                        // {@code RemoteClusterService.REMOTE_CLUSTER_HANDSHAKE_ACTION_NAME} fails authn and the connection is closed
                        // before the handshake response returned
                        logger.info("A connection could not be established");
                        assertThat(e, instanceOf(NoSuchRemoteClusterException.class));
                        closeFuture.onResponse(null);
                    })
                );
                closeFuture.get(15L, TimeUnit.SECONDS);
            }
        }
    }

}
