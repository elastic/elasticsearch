/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.cluster.coordination;

import org.elasticsearch.cluster.node.DiscoveryNode;
import org.elasticsearch.cluster.node.DiscoveryNodeUtils;
import org.elasticsearch.cluster.node.DiscoveryNodes;
import org.elasticsearch.common.ReferenceDocs;
import org.elasticsearch.common.UUIDs;
import org.elasticsearch.test.ESTestCase;
import org.hamcrest.Matcher;

import java.util.concurrent.atomic.AtomicLong;

import static org.elasticsearch.cluster.coordination.Coordinator.Mode.CANDIDATE;
import static org.elasticsearch.cluster.coordination.Coordinator.Mode.LEADER;
import static org.hamcrest.Matchers.equalTo;

public class JoinReasonServiceTests extends ESTestCase {

    public void testJoinReasonService() {
        final AtomicLong currentTimeMillis = new AtomicLong(randomLong());
        final JoinReasonService joinReasonService = new JoinReasonService(currentTimeMillis::get);

        final DiscoveryNode master = randomDiscoveryNode();
        final DiscoveryNode discoveryNode = randomDiscoveryNode();

        final DiscoveryNodes withoutNode = DiscoveryNodes.builder()
            .add(master)
            .localNodeId(master.getId())
            .masterNodeId(master.getId())
            .build();

        final DiscoveryNodes withNode = DiscoveryNodes.builder(withoutNode).add(discoveryNode).build();

        assertThat(joinReasonService.getJoinReason(discoveryNode, CANDIDATE), matches("completing election"));
        assertThat(joinReasonService.getJoinReason(discoveryNode, LEADER), matches("joining"));

        joinReasonService.onClusterStateApplied(withoutNode);

        assertThat(joinReasonService.getJoinReason(discoveryNode, CANDIDATE), matches("completing election"));
        assertThat(joinReasonService.getJoinReason(discoveryNode, LEADER), matches("joining"));

        joinReasonService.onClusterStateApplied(withNode);

        assertThat(joinReasonService.getJoinReason(discoveryNode, CANDIDATE), matches("completing election"));
        assertThat(joinReasonService.getJoinReason(discoveryNode, LEADER), matches("rejoining"));

        joinReasonService.onClusterStateApplied(withoutNode);
        currentTimeMillis.addAndGet(1234L);

        assertThat(
            joinReasonService.getJoinReason(discoveryNode, LEADER),
            matchesNeedingGuidance("joining, removed [1.2s/1234ms] ago by [" + master.getName() + "]")
        );

        joinReasonService.onNodeRemoved(discoveryNode, "test removal");
        currentTimeMillis.addAndGet(4321L);

        assertThat(
            joinReasonService.getJoinReason(discoveryNode, LEADER),
            matchesNeedingGuidance("joining, removed [5.5s/5555ms] ago with reason [test removal]")
        );

        joinReasonService.onClusterStateApplied(withNode);
        joinReasonService.onClusterStateApplied(withoutNode);

        assertThat(
            joinReasonService.getJoinReason(discoveryNode, LEADER),
            matchesNeedingGuidance("joining, removed [0ms] ago by [" + master.getName() + "], [2] total removals")
        );

        joinReasonService.onNodeRemoved(discoveryNode, "second test removal");

        assertThat(
            joinReasonService.getJoinReason(discoveryNode, LEADER),
            matchesNeedingGuidance("joining, removed [0ms] ago with reason [second test removal], [2] total removals")
        );

        final DiscoveryNode rebootedNode = DiscoveryNodeUtils.builder(discoveryNode.getId())
            .name(discoveryNode.getName())
            .ephemeralId(UUIDs.randomBase64UUID(random()))
            .address(discoveryNode.getHostName(), discoveryNode.getHostAddress(), discoveryNode.getAddress())
            .attributes(discoveryNode.getAttributes())
            .roles(discoveryNode.getRoles())
            .version(discoveryNode.getVersionInformation())
            .build();

        assertThat(
            joinReasonService.getJoinReason(rebootedNode, LEADER),
            matches("joining after restart, removed [0ms] ago with reason [second test removal]")
        );

        final DiscoveryNodes withRebootedNode = DiscoveryNodes.builder(withoutNode).add(rebootedNode).build();
        joinReasonService.onClusterStateApplied(withRebootedNode);
        joinReasonService.onClusterStateApplied(withoutNode);
        joinReasonService.onNodeRemoved(rebootedNode, "third test removal");

        assertThat(
            joinReasonService.getJoinReason(rebootedNode, LEADER),
            matchesNeedingGuidance("joining, removed [0ms] ago with reason [third test removal]")
        );

        joinReasonService.onClusterStateApplied(withRebootedNode);
        joinReasonService.onClusterStateApplied(withoutNode);
        joinReasonService.onNodeRemoved(rebootedNode, "fourth test removal");

        assertThat(
            joinReasonService.getJoinReason(rebootedNode, LEADER),
            matchesNeedingGuidance("joining, removed [0ms] ago with reason [fourth test removal], [2] total removals")
        );

        joinReasonService.onClusterStateApplied(withRebootedNode);
        joinReasonService.onClusterStateApplied(withNode);
        joinReasonService.onClusterStateApplied(withoutNode);

        assertThat(
            joinReasonService.getJoinReason(discoveryNode, LEADER),
            matchesNeedingGuidance("joining, removed [0ms] ago by [" + master.getName() + "]")
        );

        assertThat(
            joinReasonService.getJoinReason(rebootedNode, LEADER),
            matches("joining after restart, removed [0ms] ago by [" + master.getName() + "]")
        );
    }

    public void testCleanup() {
        final AtomicLong currentTimeMillis = new AtomicLong(randomLong());
        final JoinReasonService joinReasonService = new JoinReasonService(currentTimeMillis::get);

        final DiscoveryNode masterNode = randomDiscoveryNode();
        final DiscoveryNode targetNode = randomDiscoveryNode();

        final DiscoveryNode[] discoveryNodes = new DiscoveryNode[between(2, 20)];
        for (int i = 0; i < discoveryNodes.length; i++) {
            discoveryNodes[i] = randomDiscoveryNode();
        }

        // we stop tracking the oldest absent node(s) when only 1/3 of the tracked nodes are present
        final int cleanupNodeCount = (discoveryNodes.length - 2) / 3;

        final DiscoveryNodes.Builder cleanupNodesBuilder = new DiscoveryNodes.Builder().add(masterNode)
            .localNodeId(masterNode.getId())
            .masterNodeId(masterNode.getId());
        for (int i = 0; i < cleanupNodeCount; i++) {
            cleanupNodesBuilder.add(discoveryNodes[i]);
        }
        final DiscoveryNodes cleanupNodes = cleanupNodesBuilder.build();
        final DiscoveryNodes almostCleanupNodes = DiscoveryNodes.builder(cleanupNodes).add(discoveryNodes[cleanupNodeCount]).build();

        final DiscoveryNodes.Builder allOtherNodesBuilder = DiscoveryNodes.builder(almostCleanupNodes);
        for (int i = cleanupNodeCount + 1; i < discoveryNodes.length; i++) {
            allOtherNodesBuilder.add(discoveryNodes[i]);
        }
        final DiscoveryNodes allOtherNodes = allOtherNodesBuilder.build();
        final DiscoveryNodes allNodes = DiscoveryNodes.builder(allOtherNodes).add(targetNode).build();

        // track all the nodes
        joinReasonService.onClusterStateApplied(allNodes);

        // remove the target node
        joinReasonService.onClusterStateApplied(allOtherNodes);
        joinReasonService.onNodeRemoved(targetNode, "test");

        // advance time so that the target node is the oldest
        currentTimeMillis.incrementAndGet();

        // remove almost enough other nodes and verify that we're still tracking the target node
        joinReasonService.onClusterStateApplied(almostCleanupNodes);
        assertThat(
            joinReasonService.getJoinReason(targetNode, LEADER),
            matchesNeedingGuidance("joining, removed [1ms] ago with reason [test]")
        );

        // remove one more node to trigger the cleanup and forget about the target node
        joinReasonService.onClusterStateApplied(cleanupNodes);
        assertThat(joinReasonService.getJoinReason(targetNode, LEADER), matches("joining"));
    }

    private DiscoveryNode randomDiscoveryNode() {
        return DiscoveryNodeUtils.builder(UUIDs.randomBase64UUID(random()))
            .name(randomAlphaOfLength(10))
            .ephemeralId(UUIDs.randomBase64UUID(random()))
            .build();
    }

    private static Matcher<JoinReason> matches(String message) {
        return equalTo(new JoinReason(message, null));
    }

    private static Matcher<JoinReason> matchesNeedingGuidance(String message) {
        return equalTo(new JoinReason(message, ReferenceDocs.UNSTABLE_CLUSTER_TROUBLESHOOTING));
    }
}
