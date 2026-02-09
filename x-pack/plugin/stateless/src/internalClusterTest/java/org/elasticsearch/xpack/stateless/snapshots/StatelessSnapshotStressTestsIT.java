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

package org.elasticsearch.xpack.stateless.snapshots;

import org.elasticsearch.cluster.coordination.stateless.StoreHeartbeatService;
import org.elasticsearch.cluster.node.DiscoveryNodes;
import org.elasticsearch.cluster.routing.allocation.decider.EnableAllocationDecider;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.snapshots.SnapshotStressTestsHelper;
import org.elasticsearch.xpack.stateless.AbstractStatelessPluginIntegTestCase;

import static org.elasticsearch.snapshots.SnapshotStressTestsHelper.nodeNames;

public class StatelessSnapshotStressTestsIT extends AbstractStatelessPluginIntegTestCase {

    @Override
    protected Settings.Builder nodeSettings() {
        return super.nodeSettings()
            // Rebalancing is causing some checks after restore to randomly fail
            // due to https://github.com/elastic/elasticsearch/issues/9421
            .put(EnableAllocationDecider.CLUSTER_ROUTING_REBALANCE_ENABLE_SETTING.getKey(), EnableAllocationDecider.Rebalance.NONE)
            // Speed up master failover
            .put(StoreHeartbeatService.HEARTBEAT_FREQUENCY.getKey(), "1s")
            // max 1 miss with 1s frequency may be unstable on slow CI machines, so keep it at 2
            .put(StoreHeartbeatService.MAX_MISSED_HEARTBEATS.getKey(), 2);
    }

    public void testRandomActivitiesStatelessSnapshotDisabled() throws InterruptedException {
        doTestRandomActivities(Settings.EMPTY);
    }

    public void testRandomActivitiesStatelessSnapshotReadFromObjectStore() throws InterruptedException {
        doTestRandomActivities(
            Settings.builder()
                .put(
                    StatelessSnapshotShardContextFactory.STATELESS_SNAPSHOT_ENABLED_SETTING.getKey(),
                    StatelessSnapshotShardContextFactory.StatelessSnapshotEnabledStatus.READ_FROM_OBJECT_STORE
                )
                .build()
        );
    }

    private void doTestRandomActivities(Settings extraSettings) throws InterruptedException {
        final int numIndexNodes = between(1, 3);
        logger.info("--> starting [{}] indexing nodes", numIndexNodes);
        for (int i = 0; i < numIndexNodes; i++) {
            startMasterAndIndexNode(extraSettings);
        }
        final int numSearchNodes = between(0, 3);
        logger.info("--> starting [{}] search nodes", numSearchNodes);
        if (numSearchNodes > 0) {
            startSearchNodes(numSearchNodes, extraSettings);
        }
        ensureStableCluster(numIndexNodes + numSearchNodes);

        final DiscoveryNodes discoveryNodes = clusterAdmin().prepareState(TEST_REQUEST_TIMEOUT)
            .clear()
            .setNodes(true)
            .get()
            .getState()
            .nodes();
        final var trackedCluster = new SnapshotStressTestsHelper.TrackedCluster(
            internalCluster(),
            nodeNames(discoveryNodes.getMasterNodes()),
            nodeNames(discoveryNodes.getDataNodes())
        ) {
            @Override
            protected int numberOfReplicasUpperBound() {
                return numSearchNodes;
            }
        };
        trackedCluster.run();
    }
}
