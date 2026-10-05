/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.ml.inference.assignment;

import org.elasticsearch.action.ActionListener;
import org.elasticsearch.cluster.ClusterName;
import org.elasticsearch.cluster.ClusterState;
import org.elasticsearch.cluster.metadata.Metadata;
import org.elasticsearch.cluster.node.DiscoveryNode;
import org.elasticsearch.cluster.node.DiscoveryNodeRole;
import org.elasticsearch.cluster.node.DiscoveryNodeUtils;
import org.elasticsearch.cluster.node.DiscoveryNodes;
import org.elasticsearch.common.settings.ClusterSettings;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.unit.ByteSizeValue;
import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.xpack.core.ml.MachineLearningField;
import org.elasticsearch.xpack.core.ml.action.StartTrainedModelDeploymentAction;
import org.elasticsearch.xpack.core.ml.autoscaling.MlAutoscalingStats;
import org.elasticsearch.xpack.core.ml.inference.assignment.AdaptiveAllocationsSettings;
import org.elasticsearch.xpack.core.ml.inference.assignment.AssignmentState;
import org.elasticsearch.xpack.core.ml.inference.assignment.Priority;
import org.elasticsearch.xpack.core.ml.inference.assignment.RoutingInfo;
import org.elasticsearch.xpack.core.ml.inference.assignment.RoutingState;
import org.elasticsearch.xpack.core.ml.inference.assignment.TrainedModelAssignment;
import org.elasticsearch.xpack.core.ml.inference.assignment.TrainedModelAssignmentMetadata;
import org.elasticsearch.xpack.ml.MachineLearning;
import org.elasticsearch.xpack.ml.autoscaling.MlAutoscalingResourceTracker;
import org.elasticsearch.xpack.ml.job.NodeLoad;
import org.elasticsearch.xpack.ml.job.NodeLoadDetector;
import org.elasticsearch.xpack.ml.process.MlMemoryTracker;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.atomic.AtomicReference;

import static org.mockito.Mockito.mock;

/**
 * End-to-end regression test for https://github.com/elastic/elasticsearch/issues/160923. It drives the real
 * {@link TrainedModelAssignmentRebalancer} (the placement planner) and {@link MlAutoscalingResourceTracker} (which tells
 * the external autoscaler how much hardware to provision) together, which is the seam where the two sides disagreed
 * after #156891 made the planner size placement from observed native memory (RSS) while the tracker and
 * {@code NodeLoadDetector} still used the a priori estimate.
 *
 * <p>This is a trimmed, asserting descendant of the diagnostic probe attached to #160923. It fixes an ELSER-shaped
 * deployment (32 requested allocations, 1 thread each) on 4 GiB / 2-processor ML nodes and checks two documented stall
 * points plus a control point. Before the fix the tracker reported {@code wantedExtraProcessors == 0} while allocations
 * were still unplaced (the deployment silently stalled); the invariant asserted here - "if allocations are unplaced the
 * tracker must request more hardware" - fails on the pre-fix code and holds afterwards.
 */
public class TrainedModelAssignmentMemoryAccountingConsistencyTests extends ESTestCase {

    private static final String MODEL = ".elser_model_2_linux-x86_64";
    private static final String DEPLOYMENT = "elser-deployment";
    private static final int REQUESTED_ALLOCATIONS = 32;
    private static final long MODEL_BYTES = ByteSizeValue.ofMb(274).getBytes();

    private DiscoveryNode mlNode(int i) {
        return DiscoveryNodeUtils.builder("ml-" + i)
            .name("ml-" + i)
            .roles(Set.of(DiscoveryNodeRole.ML_ROLE))
            .attributes(
                Map.of(
                    MachineLearning.MACHINE_MEMORY_NODE_ATTR,
                    "4294967296", // 4 GiB
                    MachineLearning.MAX_JVM_SIZE_NODE_ATTR,
                    "1717567488",
                    MachineLearning.ALLOCATED_PROCESSORS_NODE_ATTR,
                    "2"
                )
            )
            .build();
    }

    private record ScenarioResult(int placed, MlAutoscalingStats stats) {}

    /**
     * Rebalance the deployment onto {@code nodeCount} fresh nodes until placement is stable, then ask the autoscaling
     * resource tracker what extra hardware it wants.
     */
    private ScenarioResult runScenario(
        NodeLoadDetector detector,
        Settings settings,
        ClusterSettings clusterSettings,
        long observedPerAllocationBytes,
        int nodeCount
    ) {
        List<DiscoveryNode> nodes = new ArrayList<>();
        for (int i = 0; i < nodeCount; i++) {
            nodes.add(mlNode(i));
        }

        var taskParams = new StartTrainedModelDeploymentAction.TaskParams(
            MODEL,
            DEPLOYMENT,
            MODEL_BYTES,
            REQUESTED_ALLOCATIONS,
            1,
            1024,
            ByteSizeValue.ZERO,
            Priority.NORMAL,
            0,
            0
        );
        TrainedModelAssignmentMetadata current = TrainedModelAssignmentMetadata.Builder.empty()
            .addNewAssignment(
                DEPLOYMENT,
                TrainedModelAssignment.Builder.empty(taskParams, new AdaptiveAllocationsSettings(true, 0, REQUESTED_ALLOCATIONS))
                    .setObservedPerAllocationMemoryBytes(observedPerAllocationBytes)
                    .addRoutingEntry("ml-0", new RoutingInfo(1, 1, RoutingState.STARTED, ""))
                    .setAssignmentState(AssignmentState.STARTED)
            )
            .build();

        // Node loads are measured against an empty cluster state so each node is seen as fully free capacity; the
        // rebalancer then places the deployment against that capacity.
        ClusterState emptyState = ClusterState.builder(ClusterName.DEFAULT)
            .nodes(DiscoveryNodes.builder().add(nodes.get(0)).build())
            .metadata(Metadata.builder().build())
            .build();
        Map<DiscoveryNode, NodeLoad> loads = new HashMap<>();
        for (DiscoveryNode node : nodes) {
            loads.put(node, detector.detectNodeLoad(emptyState, node, 512, 30, true));
        }

        TrainedModelAssignmentMetadata result = current;
        for (int round = 0; round < 6; round++) {
            TrainedModelAssignmentMetadata next = new TrainedModelAssignmentRebalancer(
                result,
                loads,
                Map.of(List.of(), nodes),
                Optional.empty(),
                1,
                true
            ).rebalance().build();
            // Mark the resulting routing as started so the next round treats the previous placement as running.
            TrainedModelAssignment a = next.getDeploymentAssignment(DEPLOYMENT);
            TrainedModelAssignment.Builder b = TrainedModelAssignment.Builder.empty(a.getTaskParams(), a.getAdaptiveAllocationsSettings())
                .setObservedPerAllocationMemoryBytes(observedPerAllocationBytes)
                .setAssignmentState(AssignmentState.STARTED);
            for (var e : a.getNodeRoutingTable().entrySet()) {
                int t = e.getValue().getTargetAllocations();
                b.addRoutingEntry(e.getKey(), new RoutingInfo(t, t, RoutingState.STARTED, ""));
            }
            result = TrainedModelAssignmentMetadata.Builder.empty().addNewAssignment(DEPLOYMENT, b).build();
        }

        int placed = result.getDeploymentAssignment(DEPLOYMENT)
            .getNodeRoutingTable()
            .values()
            .stream()
            .mapToInt(RoutingInfo::getTargetAllocations)
            .sum();

        DiscoveryNodes.Builder dn = DiscoveryNodes.builder();
        nodes.forEach(dn::add);
        ClusterState state = ClusterState.builder(ClusterName.DEFAULT)
            .nodes(dn.build())
            .metadata(Metadata.builder().putCustom(TrainedModelAssignmentMetadata.NAME, result).build())
            .build();

        AtomicReference<MlAutoscalingStats> statsRef = new AtomicReference<>();
        MlAutoscalingResourceTracker.getMlAutoscalingStats(
            state,
            clusterSettings,
            mock(MlMemoryTracker.class),
            settings,
            ActionListener.wrap(statsRef::set, e -> fail(e))
        );
        return new ScenarioResult(placed, statsRef.get());
    }

    private void assertNoSilentStall(ScenarioResult r) {
        if (r.placed() < REQUESTED_ALLOCATIONS) {
            assertTrue(
                "allocations unplaced ("
                    + r.placed()
                    + "/"
                    + REQUESTED_ALLOCATIONS
                    + ") but tracker requested no extra hardware: "
                    + r.stats(),
                r.stats().wantedExtraProcessors() > 0 || r.stats().wantedExtraModelMemoryBytes() > 0
            );
        }
    }

    public void testMemoryBoundDeploymentNeverStallsSilently() {
        Settings settings = Settings.builder().put(MachineLearningField.USE_AUTO_MACHINE_MEMORY_PERCENT.getKey(), true).build();
        ClusterSettings clusterSettings = new ClusterSettings(settings, Set.of(MachineLearning.ALLOCATED_PROCESSORS_SCALE));
        NodeLoadDetector detector = new NodeLoadDetector(mock(MlMemoryTracker.class));

        // Window ~977-1114 MiB: after fix #3 this behaves as one allocation per node (a second needs ~2474 MiB but only
        // ~2258 MiB is available). Each node is then individually too full for another allocation, yet the cluster shows
        // ample aggregate free memory - the case per-node accounting (fix #2) must not mistake for spare capacity.
        ScenarioResult degenerateWindow = runScenario(detector, settings, clusterSettings, ByteSizeValue.ofMb(1100).getBytes(), 16);
        assertNoSilentStall(degenerateWindow);
        assertTrue("expected the deployment to still be below target at 16 nodes", degenerateWindow.placed() < REQUESTED_ALLOCATIONS);
        assertTrue(
            "memory-bound deployment should request extra processors, but got " + degenerateWindow.stats(),
            degenerateWindow.stats().wantedExtraProcessors() > 0
        );

        // Window ~1115-1984 MiB: one allocation per node. With n nodes the pre-fix tracker only raised extraProcessors
        // while missing (32 - n) exceeded free processors (n), so it went quiet at 16 nodes with 16/32 placed (fix #1/#2).
        ScenarioResult oneAllocationPerNode = runScenario(detector, settings, clusterSettings, ByteSizeValue.ofMb(1200).getBytes(), 16);
        assertNoSilentStall(oneAllocationPerNode);
        assertTrue("expected the deployment to still be below target at 16 nodes", oneAllocationPerNode.placed() < REQUESTED_ALLOCATIONS);
        assertTrue(
            "memory-bound deployment should request extra processors, but got " + oneAllocationPerNode.stats(),
            oneAllocationPerNode.stats().wantedExtraProcessors() > 0
        );
    }

    public void testWellProvisionedDeploymentPlacesFullyAndRequestsNothing() {
        Settings settings = Settings.builder().put(MachineLearningField.USE_AUTO_MACHINE_MEMORY_PERCENT.getKey(), true).build();
        ClusterSettings clusterSettings = new ClusterSettings(settings, Set.of(MachineLearning.ALLOCATED_PROCESSORS_SCALE));
        NodeLoadDetector detector = new NodeLoadDetector(mock(MlMemoryTracker.class));

        // Control point: at ~900 MiB per allocation two allocations fit per node, so 17 nodes host all 32 allocations and
        // the tracker should be satisfied. This guards against the memory cap over-requesting on a healthy deployment.
        ScenarioResult healthy = runScenario(detector, settings, clusterSettings, ByteSizeValue.ofMb(900).getBytes(), 17);
        assertEquals("expected all allocations placed on a well-provisioned tier", REQUESTED_ALLOCATIONS, healthy.placed());
        assertEquals("no extra processors expected once fully placed", 0, healthy.stats().wantedExtraProcessors());
        assertEquals("no extra model memory expected once fully placed", 0, healthy.stats().wantedExtraModelMemoryBytes());
    }
}
