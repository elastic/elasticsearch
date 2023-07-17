/*
 * ELASTICSEARCH CONFIDENTIAL
 * __________________
 *
 * Copyright Elasticsearch B.V. All rights reserved.
 *
 * NOTICE:  All information contained herein is, and remains
 * the property of Elasticsearch B.V. and its suppliers, if any.
 * The intellectual and technical concepts contained herein
 * are proprietary to Elasticsearch B.V. and its suppliers and
 * may be covered by U.S. and Foreign Patents, patents in
 * process, and are protected by trade secret or copyright
 * law.  Dissemination of this information or reproduction of
 * this material is strictly forbidden unless prior written
 * permission is obtained from Elasticsearch B.V.
 */

package co.elastic.elasticsearch.stateless.upgrade;

import org.elasticsearch.TransportVersion;
import org.elasticsearch.action.ActionListener;
import org.elasticsearch.action.ActionRequest;
import org.elasticsearch.action.ActionResponse;
import org.elasticsearch.action.ActionType;
import org.elasticsearch.cluster.ClusterChangedEvent;
import org.elasticsearch.cluster.ClusterName;
import org.elasticsearch.cluster.ClusterState;
import org.elasticsearch.cluster.node.DiscoveryNode;
import org.elasticsearch.cluster.node.DiscoveryNodes;
import org.elasticsearch.cluster.service.ClusterService;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.transport.TransportAddress;
import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.test.client.NoOpClient;
import org.junit.After;
import org.junit.Before;

import java.util.concurrent.atomic.AtomicBoolean;

import static org.mockito.ArgumentMatchers.same;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;

public class StatelessUpgraderTests extends ESTestCase {

    private final AtomicBoolean rerouteCalled = new AtomicBoolean(false);
    private ClusterService clusterService;
    private NoOpClient noOpClient;

    @Before
    public void setUp() throws Exception {
        super.setUp();
        clusterService = mock(ClusterService.class);

        noOpClient = new NoOpClient("stateless_upgrader_tests") {

            @Override
            protected <Request extends ActionRequest, Response extends ActionResponse> void doExecute(
                ActionType<Response> action,
                Request request,
                ActionListener<Response> listener
            ) {
                if ("cluster:admin/reroute".equals(action.name())) {
                    rerouteCalled.set(true);
                }
                super.doExecute(action, request, listener);
            }
        };
    }

    @After
    public void tearDown() throws Exception {
        super.tearDown();
        noOpClient.close();
    }

    public void testAttemptedRerouteIssuedOnMasterOnceClusterUpgraded() {
        StatelessUpgrader statelessUpgrader = new StatelessUpgrader(noOpClient, clusterService);

        DiscoveryNode localNode = DiscoveryNode.createLocal(
            Settings.EMPTY,
            new TransportAddress(TransportAddress.META_ADDRESS, 9201),
            "node1"
        );
        ClusterState noUpgradeNodes = ClusterState.builder(new ClusterName("cluster"))
            .nodes(
                DiscoveryNodes.builder()
                    .localNodeId(localNode.getId())
                    .masterNodeId(localNode.getId())
                    .add(localNode)
                    .add(DiscoveryNode.createLocal(Settings.EMPTY, new TransportAddress(TransportAddress.META_ADDRESS, 9202), "node2"))
            )
            .putTransportVersion("node1", TransportVersion.V_8_500_034)
            .putTransportVersion("node2", TransportVersion.V_8_500_034)
            .build();

        ClusterState oneUpgradedNode = ClusterState.builder(noUpgradeNodes)
            .putTransportVersion("node1", StatelessUpgrader.RETRY_ALLOCATION_VERSION)
            .build();

        statelessUpgrader.clusterChanged(new ClusterChangedEvent("test", oneUpgradedNode, noUpgradeNodes));

        assertFalse(rerouteCalled.get());

        ClusterState allUpgradedNodes = ClusterState.builder(oneUpgradedNode)
            .putTransportVersion("node2", StatelessUpgrader.RETRY_ALLOCATION_VERSION)
            .build();

        verify(clusterService, times(0)).removeListener(same(statelessUpgrader));

        statelessUpgrader.clusterChanged(new ClusterChangedEvent("test", allUpgradedNodes, oneUpgradedNode));

        assertTrue(rerouteCalled.get());

        verify(clusterService, times(1)).removeListener(same(statelessUpgrader));
    }

    public void testAttemptedRerouteIsNotIssuedOnNonMaster() {
        StatelessUpgrader statelessUpgrader = new StatelessUpgrader(noOpClient, clusterService);

        DiscoveryNode localNode = DiscoveryNode.createLocal(
            Settings.EMPTY,
            new TransportAddress(TransportAddress.META_ADDRESS, 9201),
            "node1"
        );
        DiscoveryNode masterNode = DiscoveryNode.createLocal(
            Settings.EMPTY,
            new TransportAddress(TransportAddress.META_ADDRESS, 9202),
            "node2"
        );
        ClusterState noUpgradeNodes = ClusterState.builder(new ClusterName("cluster"))
            .nodes(DiscoveryNodes.builder().localNodeId(localNode.getId()).masterNodeId(masterNode.getId()).add(localNode).add(masterNode))
            .putTransportVersion("node1", TransportVersion.V_8_500_034)
            .putTransportVersion("node2", TransportVersion.V_8_500_034)
            .build();

        ClusterState oneUpgradedNode = ClusterState.builder(noUpgradeNodes)
            .putTransportVersion("node1", StatelessUpgrader.RETRY_ALLOCATION_VERSION)
            .build();

        statelessUpgrader.clusterChanged(new ClusterChangedEvent("test", oneUpgradedNode, noUpgradeNodes));

        ClusterState allUpgradedNodes = ClusterState.builder(oneUpgradedNode)
            .putTransportVersion("node2", StatelessUpgrader.RETRY_ALLOCATION_VERSION)
            .build();

        verify(clusterService, times(0)).removeListener(same(statelessUpgrader));

        statelessUpgrader.clusterChanged(new ClusterChangedEvent("test", allUpgradedNodes, oneUpgradedNode));

        assertFalse(rerouteCalled.get());

        verify(clusterService, times(1)).removeListener(same(statelessUpgrader));
    }
}
