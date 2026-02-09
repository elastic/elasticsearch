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

import org.elasticsearch.cluster.node.DiscoveryNode;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.snapshots.SnapshotResiliencyTestHelper.TestClusterNodes.TransportInterceptorFactory;
import org.elasticsearch.xpack.stateless.snapshots.StatelessSnapshotShardContextFactory.StatelessSnapshotEnabledStatus;

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
                        StatelessSnapshotShardContextFactory.STATELESS_SNAPSHOT_ENABLED_SETTING.getKey(),
                        StatelessSnapshotEnabledStatus.READ_FROM_OBJECT_STORE
                    )
                    .build();
            }
        };
        startCluster();
    }
}
