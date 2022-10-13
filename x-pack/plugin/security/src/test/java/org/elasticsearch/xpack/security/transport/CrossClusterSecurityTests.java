/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */
package org.elasticsearch.xpack.security.transport;

import org.elasticsearch.action.support.DestructiveOperations;
import org.elasticsearch.cluster.ClusterState;
import org.elasticsearch.cluster.service.ClusterService;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.test.ClusterServiceUtils;
import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.threadpool.TestThreadPool;
import org.elasticsearch.threadpool.ThreadPool;
import org.elasticsearch.transport.TcpTransport;
import org.elasticsearch.xpack.core.security.SecurityContext;
import org.elasticsearch.xpack.core.ssl.SSLService;
import org.elasticsearch.xpack.security.authc.AuthenticationService;
import org.elasticsearch.xpack.security.authz.AuthorizationService;
import org.junit.After;

import static org.hamcrest.Matchers.is;
import static org.junit.Assume.assumeThat;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.spy;

public class CrossClusterSecurityTests extends ESTestCase {

    private ThreadPool threadPool;
    private ClusterService clusterService;

    @Override
    public void setUp() throws Exception {
        if (TcpTransport.isUntrustedRemoteClusterEnabled()) {
            super.setUp();
            threadPool = new TestThreadPool(getTestName());
        }

    }

    @After
    public void stopThreadPool() throws Exception {
        if (TcpTransport.isUntrustedRemoteClusterEnabled()) {
            clusterService.close();
            terminate(threadPool);
        }
    }

    public void testSendAsync() {
        assumeThat(TcpTransport.isUntrustedRemoteClusterEnabled(), is(true));
        final Settings settings = Settings.builder()
            .put("path.home", createTempDir())
            .put("cluster.remote.cluster1.authorization", "apikey1")
            .put("cluster.remote.cluster2.authorization", "apikey2")
            .build();

        clusterService = ClusterServiceUtils.createClusterService(threadPool);
        SecurityContext securityContext = spy(new SecurityContext(settings, threadPool.getThreadContext()));

        SecurityServerTransportInterceptor interceptor = new SecurityServerTransportInterceptor(
            settings,
            threadPool,
            mock(AuthenticationService.class),
            mock(AuthorizationService.class),
            mock(SSLService.class),
            securityContext,
            new DestructiveOperations(settings, clusterService.getClusterSettings()),
            new CrossClusterSecurity(settings, clusterService.getClusterSettings())
        );
        final ClusterState initialState = clusterService.state();
        ClusterServiceUtils.setState(clusterService, initialState); // force state update to trigger listener
    }
}
