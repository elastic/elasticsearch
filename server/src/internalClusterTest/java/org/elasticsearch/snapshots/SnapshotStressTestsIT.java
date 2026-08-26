/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.snapshots;

import org.apache.lucene.tests.util.LuceneTestCase;
import org.elasticsearch.cluster.node.DiscoveryNodes;
import org.elasticsearch.snapshots.SnapshotStressTestsHelper.TrackedCluster;

import static org.elasticsearch.snapshots.SnapshotStressTestsHelper.nodeNames;

@LuceneTestCase.SuppressFileSystems(value = "HandleLimitFS") // we sometimes have >2048 open files
public class SnapshotStressTestsIT extends AbstractSnapshotIntegTestCase {

    public void testRandomActivities() throws InterruptedException {
        final DiscoveryNodes discoveryNodes = clusterAdmin().prepareState(TEST_REQUEST_TIMEOUT)
            .clear()
            .setNodes(true)
            .get()
            .getState()
            .nodes();
        new TrackedCluster(internalCluster(), nodeNames(discoveryNodes.getMasterNodes()), nodeNames(discoveryNodes.getDataNodes())).run();
        disableRepoConsistencyCheck("have not necessarily written to all repositories");
    }
}
