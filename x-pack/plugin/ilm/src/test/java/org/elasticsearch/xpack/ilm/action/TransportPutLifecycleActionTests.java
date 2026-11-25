/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.ilm.action;

import org.elasticsearch.TransportVersion;
import org.elasticsearch.action.support.ActionFilters;
import org.elasticsearch.action.support.master.AcknowledgedResponse;
import org.elasticsearch.cluster.ClusterName;
import org.elasticsearch.cluster.ClusterState;
import org.elasticsearch.cluster.metadata.ProjectId;
import org.elasticsearch.cluster.metadata.ProjectMetadata;
import org.elasticsearch.cluster.node.DiscoveryNodeUtils;
import org.elasticsearch.cluster.node.DiscoveryNodes;
import org.elasticsearch.cluster.service.ClusterService;
import org.elasticsearch.core.TimeValue;
import org.elasticsearch.license.XPackLicenseState;
import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.test.MockUtils;
import org.elasticsearch.test.TransportVersionUtils;
import org.elasticsearch.threadpool.ThreadPool;
import org.elasticsearch.transport.TransportService;
import org.elasticsearch.xcontent.NamedXContentRegistry;
import org.elasticsearch.xcontent.XContentParser;
import org.elasticsearch.xcontent.XContentParserConfiguration;
import org.elasticsearch.xcontent.XContentType;
import org.elasticsearch.xpack.core.ilm.LifecyclePolicy;
import org.elasticsearch.xpack.core.ilm.LifecyclePolicyMetadata;
import org.elasticsearch.xpack.core.ilm.Phase;
import org.elasticsearch.xpack.core.ilm.SearchableSnapshotAction;
import org.elasticsearch.xpack.core.ilm.action.PutLifecycleRequest;
import org.elasticsearch.xpack.ilm.LifecyclePolicyTestsUtils;
import org.elasticsearch.xpack.ilm.PutLifecycleMetadataService;

import java.util.Map;

import static org.hamcrest.Matchers.containsInAnyOrder;
import static org.hamcrest.Matchers.containsString;
import static org.hamcrest.Matchers.equalTo;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

public class TransportPutLifecycleActionTests extends ESTestCase {
    public void testIsNoop() {
        LifecyclePolicy policy1 = LifecyclePolicyTestsUtils.randomTimeseriesLifecyclePolicy("policy");
        LifecyclePolicy policy2 = randomValueOtherThan(policy1, () -> LifecyclePolicyTestsUtils.randomTimeseriesLifecyclePolicy("policy"));

        Map<String, String> headers1 = Map.of("foo", "bar");
        Map<String, String> headers2 = Map.of("foo", "eggplant");

        LifecyclePolicyMetadata existing = new LifecyclePolicyMetadata(policy1, headers1, randomNonNegativeLong(), randomNonNegativeLong());

        assertTrue(PutLifecycleMetadataService.isNoopUpdate(existing, policy1, headers1));
        assertFalse(PutLifecycleMetadataService.isNoopUpdate(existing, policy2, headers1));
        assertFalse(PutLifecycleMetadataService.isNoopUpdate(existing, policy1, headers2));
        assertFalse(PutLifecycleMetadataService.isNoopUpdate(null, policy1, headers1));
    }

    public void testReservedStateHandler() throws Exception {
        ThreadPool threadPool = mock(ThreadPool.class);
        TransportService transportService = MockUtils.setupTransportServiceWithThreadpoolExecutor(threadPool);
        ClusterService mockClusterService = mock(ClusterService.class);
        TransportPutLifecycleAction putAction = new TransportPutLifecycleAction(
            transportService,
            mockClusterService,
            threadPool,
            mock(ActionFilters.class),
            mock(PutLifecycleMetadataService.class)
        );
        assertEquals(ReservedLifecycleAction.NAME, putAction.reservedStateHandlerName().get());

        String json = """
            {
              "policy": {
                "phases": {
                  "warm": {
                    "min_age": "10s",
                    "actions": {
                    }
                  }
                }
              }
            }""";

        try (XContentParser parser = XContentType.JSON.xContent().createParser(XContentParserConfiguration.EMPTY, json)) {
            PutLifecycleRequest request = PutLifecycleRequest.parseRequest(new PutLifecycleRequest.Factory() {
                @Override
                public PutLifecycleRequest create(LifecyclePolicy lifecyclePolicy) {
                    return new PutLifecycleRequest(TEST_REQUEST_TIMEOUT, TEST_REQUEST_TIMEOUT, lifecyclePolicy);
                }

                @Override
                public String getPolicyName() {
                    return "my_timeseries_lifecycle2";
                }
            }, parser);

            assertThat(putAction.modifiedKeys(request), containsInAnyOrder("my_timeseries_lifecycle2"));
        }
    }

    public void testUnboundedTotalShardsPerNodeRejectedWhileOlderNodesArePresent() {
        TransportVersion olderVersion = TransportVersionUtils.randomVersionNotSupporting(
            SearchableSnapshotAction.UNBOUNDED_TOTAL_SHARDS_PER_NODE_TRANSPORT_VERSION
        );

        IllegalArgumentException e = expectThrows(
            IllegalArgumentException.class,
            () -> PutLifecycleMetadataService.validateUnboundedTotalShardsPerNodeSupported(policyWithTotalShardsPerNode(-1), olderVersion)
        );
        assertThat(
            e.getMessage(),
            equalTo(
                "[total_shards_per_node] of [-1] in the [searchable_snapshot] action of phase [cold] "
                    + "is not supported until all nodes in the cluster are upgraded"
            )
        );
    }

    public void testUnboundedTotalShardsPerNodeAcceptedOnceAllNodesSupportIt() {
        PutLifecycleMetadataService.validateUnboundedTotalShardsPerNodeSupported(
            policyWithTotalShardsPerNode(-1),
            TransportVersion.current()
        );
    }

    public void testBoundedTotalShardsPerNodeAcceptedWhileOlderNodesArePresent() {
        TransportVersion olderVersion = TransportVersionUtils.randomVersionNotSupporting(
            SearchableSnapshotAction.UNBOUNDED_TOTAL_SHARDS_PER_NODE_TRANSPORT_VERSION
        );
        Integer totalShardsPerNode = randomBoolean() ? null : randomIntBetween(1, 100);

        PutLifecycleMetadataService.validateUnboundedTotalShardsPerNodeSupported(
            policyWithTotalShardsPerNode(totalShardsPerNode),
            olderVersion
        );
    }

    /**
     * An older master does not run the check, so the coordinating node must reject the request before it forwards it.
     */
    public void testCoordinatingNodeRejectsUnboundedTotalShardsPerNodeWhileOlderNodesArePresent() {
        TransportVersion olderVersion = TransportVersionUtils.randomVersionNotSupporting(
            SearchableSnapshotAction.UNBOUNDED_TOTAL_SHARDS_PER_NODE_TRANSPORT_VERSION
        );
        ClusterState state = ClusterState.builder(ClusterName.DEFAULT)
            .nodes(DiscoveryNodes.builder().add(DiscoveryNodeUtils.create("old-node")).localNodeId("old-node"))
            .putCompatibilityVersions("old-node", olderVersion, Map.of())
            .build();
        ThreadPool threadPool = mock(ThreadPool.class);
        TransportService transportService = MockUtils.setupTransportServiceWithThreadpoolExecutor(threadPool);
        // A mock ClusterService is enough here: the request is rejected before anything other than the cluster state is used.
        ClusterService clusterService = mock(ClusterService.class);
        when(clusterService.state()).thenReturn(state);
        TransportPutLifecycleAction putAction = new TransportPutLifecycleAction(
            transportService,
            clusterService,
            threadPool,
            mock(ActionFilters.class),
            mock(PutLifecycleMetadataService.class)
        );
        PutLifecycleRequest request = new PutLifecycleRequest(TEST_REQUEST_TIMEOUT, TEST_REQUEST_TIMEOUT, policyWithTotalShardsPerNode(-1));

        IllegalArgumentException e = safeAwaitFailure(
            IllegalArgumentException.class,
            AcknowledgedResponse.class,
            listener -> putAction.doExecute(null, request, listener)
        );
        assertThat(e.getMessage(), containsString("is not supported until all nodes in the cluster are upgraded"));
    }

    /**
     * The master must also reject the policy, for example when it arrives through reserved cluster state.
     */
    public void testMasterRejectsUnboundedTotalShardsPerNodeWhileOlderNodesArePresent() {
        TransportVersion olderVersion = TransportVersionUtils.randomVersionNotSupporting(
            SearchableSnapshotAction.UNBOUNDED_TOTAL_SHARDS_PER_NODE_TRANSPORT_VERSION
        );
        ClusterState state = ClusterState.builder(ClusterName.DEFAULT)
            .nodes(DiscoveryNodes.builder().add(DiscoveryNodeUtils.create("old-node")).localNodeId("old-node"))
            .putCompatibilityVersions("old-node", olderVersion, Map.of())
            .putProjectMetadata(ProjectMetadata.builder(ProjectId.DEFAULT).build())
            .build();
        PutLifecycleRequest request = new PutLifecycleRequest(TEST_REQUEST_TIMEOUT, TEST_REQUEST_TIMEOUT, policyWithTotalShardsPerNode(-1));
        // The client is only used when an existing policy is updated, which this test does not do.
        PutLifecycleMetadataService.UpdateLifecyclePolicyTask task = new PutLifecycleMetadataService.UpdateLifecyclePolicyTask(
            ProjectId.DEFAULT,
            request,
            new XPackLicenseState(System::currentTimeMillis),
            NamedXContentRegistry.EMPTY,
            null
        );

        IllegalArgumentException e = expectThrows(IllegalArgumentException.class, () -> task.execute(state));
        assertThat(e.getMessage(), containsString("is not supported until all nodes in the cluster are upgraded"));
    }

    private static LifecyclePolicy policyWithTotalShardsPerNode(Integer totalShardsPerNode) {
        SearchableSnapshotAction action = new SearchableSnapshotAction("repo", randomBoolean(), totalShardsPerNode, null, null);
        Phase cold = new Phase("cold", TimeValue.ZERO, Map.of(SearchableSnapshotAction.NAME, action));
        return new LifecyclePolicy("policy", Map.of("cold", cold));
    }
}
