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

package co.elastic.elasticsearch.stateless.snapshots;

import co.elastic.elasticsearch.stateless.AbstractServerlessStatelessPluginIntegTestCase;

import org.elasticsearch.cluster.node.DiscoveryNodes;
import org.elasticsearch.cluster.routing.allocation.decider.EnableAllocationDecider;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.snapshots.SnapshotStressTestsHelper;

import static org.elasticsearch.snapshots.SnapshotStressTestsHelper.nodeNames;

public class StatelessSnapshotStressTestsIT extends AbstractServerlessStatelessPluginIntegTestCase {

    @Override
    protected Settings nodeSettings(int nodeOrdinal, Settings otherSettings) {
        return Settings.builder()
            .put(super.nodeSettings(nodeOrdinal, otherSettings))
            // Rebalancing is causing some checks after restore to randomly fail
            // due to https://github.com/elastic/elasticsearch/issues/9421
            .put(EnableAllocationDecider.CLUSTER_ROUTING_REBALANCE_ENABLE_SETTING.getKey(), EnableAllocationDecider.Rebalance.NONE)
            .build();
    }

    public void testRandomActivities() throws InterruptedException {
        final int numIndexNodes = between(1, 3);
        logger.info("--> starting [{}] indexing nodes", numIndexNodes);
        for (int i = 0; i < numIndexNodes; i++) {
            startMasterAndIndexNode();
        }
        final int numSearchNodes = between(0, 3);
        logger.info("--> starting [{}] search nodes", numSearchNodes);
        if (numSearchNodes > 0) {
            startSearchNodes(numSearchNodes);
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
