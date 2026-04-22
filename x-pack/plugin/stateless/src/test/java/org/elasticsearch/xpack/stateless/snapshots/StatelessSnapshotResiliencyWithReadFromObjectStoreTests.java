/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.stateless.snapshots;

import org.elasticsearch.cluster.node.DiscoveryNode;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.snapshots.SnapshotResiliencyTestHelper.TestClusterNodes.TransportInterceptorFactory;
import org.elasticsearch.xpack.stateless.snapshots.StatelessSnapshotSettings.StatelessSnapshotEnabledStatus;

public class StatelessSnapshotResiliencyWithReadFromObjectStoreTests extends StatelessSnapshotResiliencyTests {

    @Override
    protected void setupTestCluster(int masterNodes, int dataNodes, TransportInterceptorFactory transportInterceptorFactory) {
        testClusterNodes = new StatelessNodes(
            masterNodes,
            dataNodes,
            tempDir,
            deterministicTaskQueue,
            transportInterceptorFactory,
            this::assertCriticalWarnings
        ) {
            @Override
            protected Settings nodeSettings(DiscoveryNode node) {
                return Settings.builder()
                    .put(super.nodeSettings(node))
                    .put(
                        StatelessSnapshotSettings.STATELESS_SNAPSHOT_ENABLED_SETTING.getKey(),
                        StatelessSnapshotEnabledStatus.READ_FROM_OBJECT_STORE
                    )
                    .build();
            }
        };
        startCluster();
    }
}
