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

package co.elastic.elasticsearch.stateless;

import co.elastic.elasticsearch.stateless.objectstore.ObjectStoreService;

import org.elasticsearch.TransportVersion;
import org.elasticsearch.cluster.coordination.stateless.StoreHeartbeatService;
import org.elasticsearch.cluster.service.ClusterService;
import org.elasticsearch.common.bytes.BytesArray;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.util.ByteUtils;
import org.elasticsearch.core.TimeValue;
import org.elasticsearch.test.InternalTestCluster;
import org.hamcrest.Matcher;

import java.io.InputStream;
import java.util.List;

import static org.elasticsearch.cluster.coordination.stateless.StoreHeartbeatService.HEARTBEAT_FREQUENCY;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.greaterThan;

public class StatelessPersistedStateIT extends AbstractStatelessIntegTestCase {

    static final Settings fastFullClusterRestartSettings = Settings.builder()
        // MAX_MISSED_HEARTBEATS x HEARTBEAT_FREQUENCY is how long it takes for the last master heartbeat to expire.
        // Speed up the time to master takeover/election after full cluster restart.
        .put(HEARTBEAT_FREQUENCY.getKey(), TimeValue.timeValueSeconds(1))
        .put(StoreHeartbeatService.MAX_MISSED_HEARTBEATS.getKey(), 2)
        .build();

    public void testNodeLeftIsWrittenInRootBlob() throws Exception {
        startMasterOnlyNode();
        String indexNode1 = startIndexNode();
        String indexNode2 = startIndexNode();

        ensureStableCluster(3);

        ObjectStoreService objectStoreService = getObjectStoreService(indexNode2);
        var blobContainerForTermLease = objectStoreService.getClusterStateBlobContainer();
        final long nodeLeftGenerationBeforeNodeLeft;
        try (InputStream inputStream = blobContainerForTermLease.readBlob(operationPurpose, "lease")) {
            BytesArray rootBlob = new BytesArray(inputStream.readAllBytes());
            StreamInput input = rootBlob.streamInput();
            input.readLong();
            nodeLeftGenerationBeforeNodeLeft = input.readLong();
        }

        internalCluster().stopNode(indexNode1);

        ensureStableCluster(2);

        try (InputStream inputStream = blobContainerForTermLease.readBlob(operationPurpose, "lease")) {
            BytesArray newRootBlob = new BytesArray(inputStream.readAllBytes());
            assertThat(newRootBlob.length(), equalTo(16));
            StreamInput input = newRootBlob.streamInput();
            input.readLong();
            assertEquals(nodeLeftGenerationBeforeNodeLeft + 1, input.readLong());
        }
    }

    public void testEncounter8ByteRootBlobUpgradesTo16ByteWithNodeLeft() throws Exception {
        startMasterOnlyNode();
        String indexNode1 = startIndexNode();

        ensureStableCluster(2);

        ObjectStoreService objectStoreService = getObjectStoreService(indexNode1);
        var blobContainerForTermLease = objectStoreService.getClusterStateBlobContainer();
        final long termBeforeRootDeleted;
        try (InputStream inputStream = blobContainerForTermLease.readBlob(operationPurpose, "lease")) {
            BytesArray rootBlob = new BytesArray(inputStream.readAllBytes());
            StreamInput input = rootBlob.streamInput();
            termBeforeRootDeleted = input.readLong();
            input.readLong();
        }

        byte[] bytes = new byte[Long.BYTES];
        ByteUtils.writeLongBE(termBeforeRootDeleted + 1, bytes, 0);
        blobContainerForTermLease.writeBlob(operationPurpose, "lease", new BytesArray(bytes), false);
        blobContainerForTermLease.deleteBlobsIgnoringIfNotExists(operationPurpose, List.of("heartbeat").iterator());

        // Add a node to force a cluster state update

        String indexNode2 = startIndexNode();
        ensureStableCluster(3);

        final Matcher<Long> nodeLeftGenerationMatcher;
        try (InputStream inputStream = blobContainerForTermLease.readBlob(operationPurpose, "lease")) {
            BytesArray rootBlob = new BytesArray(inputStream.readAllBytes());
            StreamInput input = rootBlob.streamInput();
            assertThat(input.readLong(), greaterThan(termBeforeRootDeleted + 1));
            if (rootBlob.length() == Long.BYTES) {
                // rarely no node-left event occurs during the master failover, in which case we just check the node-left gen exists
                nodeLeftGenerationMatcher = greaterThan(0L);
            } else {
                assertEquals(2 * Long.BYTES, rootBlob.length());
                nodeLeftGenerationMatcher = equalTo(input.readLong() + 1L);
            }
        }

        internalCluster().stopNode(indexNode2);
        ensureStableCluster(2);

        try (InputStream inputStream = blobContainerForTermLease.readBlob(operationPurpose, "lease")) {
            BytesArray rootBlob = new BytesArray(inputStream.readAllBytes());
            StreamInput input = rootBlob.streamInput();
            input.readLong();
            assertThat(input.readLong(), nodeLeftGenerationMatcher);
        }
    }

    private static void assertTransportVersionConsistency() {
        final var clusterService = internalCluster().getCurrentMasterNodeInstance(ClusterService.class);
        assertEquals(TransportVersion.current(), clusterService.state().getMinTransportVersion());
        assertEquals(clusterService.state().nodes().getNodes().keySet(), clusterService.state().compatibilityVersions().keySet());
    }

    public void testTransportVersions() throws Exception {
        final var node0 = startMasterOnlyNode(fastFullClusterRestartSettings);
        assertTransportVersionConsistency();

        internalCluster().fullRestart(new InternalTestCluster.RestartCallback() {
            @Override
            public Settings onNodeStopped(String nodeName) {
                return fastFullClusterRestartSettings;
            }
        });
        assertTransportVersionConsistency();

        final var node1 = startMasterOnlyNode(fastFullClusterRestartSettings);
        assertTransportVersionConsistency();

        internalCluster().stopNode(randomFrom(node0, node1));
        assertTransportVersionConsistency();
    }

    public void testNodeLeftGeneration() throws Exception {
        final var node0 = startMasterOnlyNode();
        final var node1 = startMasterOnlyNode();
        internalCluster().restartNode(node1);
        internalCluster().restartNode(node1);

        internalCluster().fullRestart(new InternalTestCluster.RestartCallback() {
            @Override
            public Settings onNodeStopped(String nodeName) {
                return fastFullClusterRestartSettings;
            }
        });

        internalCluster().restartNode(randomFrom(node0, node1));
    }
}
