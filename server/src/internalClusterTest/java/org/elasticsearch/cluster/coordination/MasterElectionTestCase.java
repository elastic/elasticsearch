/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.cluster.coordination;

import org.elasticsearch.action.ActionListener;
import org.elasticsearch.cluster.ClusterStateApplier;
import org.elasticsearch.cluster.node.DiscoveryNode;
import org.elasticsearch.cluster.service.ClusterService;
import org.elasticsearch.core.Releasable;
import org.elasticsearch.test.ClusterServiceUtils;
import org.elasticsearch.test.ESIntegTestCase;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.CyclicBarrier;

public abstract class MasterElectionTestCase extends ESIntegTestCase {
    /**
     * Block the cluster state applier on a node. Returns only when applier is blocked.
     *
     * @param nodeName The name of the node on which to block the applier
     * @param cleanupTasks The list of clean up tasks
     * @return A cyclic barrier which when awaited on will un-block the applier
     */
    protected static CyclicBarrier blockClusterStateApplier(String nodeName, ArrayList<Releasable> cleanupTasks) {
        final var stateApplierBarrier = new CyclicBarrier(2);
        internalCluster().getInstance(ClusterService.class, nodeName).getClusterApplierService().onNewClusterState("test", () -> {
            // Meet to signify application is blocked
            safeAwait(stateApplierBarrier);
            // Wait for the signal to unblock
            safeAwait(stateApplierBarrier);
            return null;
        }, ActionListener.noop());
        cleanupTasks.add(stateApplierBarrier::reset);

        // Wait until state application is blocked
        safeAwait(stateApplierBarrier);
        return stateApplierBarrier;
    }

    /**
     * Configure a latch that will be released when the existing master knows of the new master's election
     *
     * @param newMaster The name of the newMaster node
     * @param cleanupTasks The list of cleanup tasks
     * @return A latch that will be released when the old master acknowledges the new master's election
     */
    protected CountDownLatch configureElectionLatchForNewMaster(String newMaster, List<Releasable> cleanupTasks) {
        final String originalMasterName = internalCluster().getMasterName();
        logger.info("Original master was {}, new master will be {}", originalMasterName, newMaster);
        final var previousMasterKnowsNewMasterIsElectedLatch = new CountDownLatch(1);
        ClusterStateApplier newMasterMonitor = event -> {
            DiscoveryNode masterNode = event.state().nodes().getMasterNode();
            if (masterNode != null && masterNode.getName().equals(newMaster)) {
                previousMasterKnowsNewMasterIsElectedLatch.countDown();
            }
        };
        ClusterService originalMasterClusterService = internalCluster().getInstance(ClusterService.class, originalMasterName);
        originalMasterClusterService.addStateApplier(newMasterMonitor);
        cleanupTasks.add(() -> originalMasterClusterService.removeApplier(newMasterMonitor));
        return previousMasterKnowsNewMasterIsElectedLatch;
    }

    /**
     * Configure a latch that will be released when the existing master knows it has been re-elected
     *
     * @param masterNodeName The name of the current master node
     * @param originalTerm The term the current master node was elected
     * @param cleanupTasks The list of cleanup tasks
     * @return A latch that will be released when the master acknowledges it's re-election
     */
    protected CountDownLatch configureElectionLatchForReElectedMaster(String masterNodeName, long originalTerm, List<Releasable> cleanupTasks) {
        final var masterKnowsItIsReElectedLatch = new CountDownLatch(1);
        ClusterStateApplier newMasterMonitor = event -> {
            DiscoveryNode masterNode = event.state().nodes().getMasterNode();
            long currentTerm = event.state().coordinationMetadata().term();
            if (masterNode != null && masterNode.getName().equals(masterNodeName) && currentTerm > originalTerm) {
                masterKnowsItIsReElectedLatch.countDown();
            }
        };
        ClusterService masterClusterService = internalCluster().getInstance(ClusterService.class, masterNodeName);
        masterClusterService.addStateApplier(newMasterMonitor);
        cleanupTasks.add(() -> masterClusterService.removeApplier(newMasterMonitor));
        return masterKnowsItIsReElectedLatch;
    }

    /**
     * Add some master-only nodes and block until they've joined the cluster
     * <p>
     * Ensure that we've got 5 voting nodes in the cluster, this means even if the original
     * master accepts its own failed state update before standing down, we can still
     * establish a quorum without its (or our own) join.
     */
    protected static String ensureSufficientMasterEligibleNodes() {
        final var votingConfigSizeListener = ClusterServiceUtils.addTemporaryStateListener(
            cs -> 5 <= cs.coordinationMetadata().getLastCommittedConfiguration().getNodeIds().size()
        );

        try {
            final var newNodeNames = internalCluster().startMasterOnlyNodes(Math.max(1, 5 - internalCluster().numMasterNodes()));
            safeAwait(votingConfigSizeListener);
            return newNodeNames.get(0);
        } finally {
            votingConfigSizeListener.onResponse(null);
        }
    }
}
