/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */
package org.elasticsearch.xpack.security.transport;

import org.elasticsearch.action.support.DestructiveOperations;
import org.elasticsearch.cluster.ClusterName;
import org.elasticsearch.cluster.ClusterState;
import org.elasticsearch.cluster.metadata.Metadata;
import org.elasticsearch.cluster.node.DiscoveryNode;
import org.elasticsearch.cluster.node.DiscoveryNodes;
import org.elasticsearch.cluster.routing.RoutingTable;
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
        final String clusterNameA = "localCluster";
        final Settings initialSettings = Settings.builder()
            .put("cluster.remote.unchangedCluster.authorization", "apiKey1")
            .put("path.home", createTempDir())
            .build();

        clusterService = ClusterServiceUtils.createClusterService(threadPool);
        SecurityContext securityContext = spy(new SecurityContext(initialSettings, threadPool.getThreadContext()));

        SecurityServerTransportInterceptor interceptor = new SecurityServerTransportInterceptor(
            initialSettings,
            threadPool,
            mock(AuthenticationService.class),
            mock(AuthorizationService.class),
            mock(SSLService.class),
            securityContext,
            new DestructiveOperations(initialSettings, clusterService.getClusterSettings()),
            new CrossClusterSecurity(initialSettings, clusterService.getClusterSettings())
        );
        final DiscoveryNode masterNodeA = clusterService.state().nodes().getMasterNode();

        // Add changedCluster authorization setting
        final Settings newSettings1 = Settings.builder()
            .put(initialSettings)
            .put("cluster.remote.changedCluster.authorization", "apiKey2")
            .build();
        final ClusterState newClusterState1 = createClusterState(clusterNameA, masterNodeA, newSettings1);
        ClusterServiceUtils.setState(clusterService, newClusterState1);

        // Change changedCluster authorization setting
        final Settings newSettings2 = Settings.builder()
            .put(initialSettings)
            .put("cluster.remote.changedCluster.authorization", "apiKey3")
            .build();
        final ClusterState newClusterState2 = createClusterState(clusterNameA, masterNodeA, newSettings2);
        ClusterServiceUtils.setState(clusterService, newClusterState2);

        // Remove changedCluster authorization setting (revert to initialSettings)
        final ClusterState newClusterState3 = createClusterState(clusterNameA, masterNodeA, initialSettings);
        ClusterServiceUtils.setState(clusterService, newClusterState3);
    }

    private static ClusterState createClusterState(final String clusterName, final DiscoveryNode masterNode, final Settings newSettings) {
        final DiscoveryNodes.Builder discoBuilder = DiscoveryNodes.builder();
        discoBuilder.add(masterNode);
        discoBuilder.masterNodeId(masterNode.getId());

        final ClusterState.Builder state = ClusterState.builder(new ClusterName(clusterName));
        state.nodes(discoBuilder);
        state.metadata(Metadata.builder().persistentSettings(newSettings).generateClusterUuidIfNeeded());
        state.routingTable(RoutingTable.builder().build());
        return state.build();
    }
}
