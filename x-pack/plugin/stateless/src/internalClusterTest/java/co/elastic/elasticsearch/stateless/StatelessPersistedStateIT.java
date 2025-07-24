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
import org.elasticsearch.core.TimeValue;
import org.elasticsearch.test.InternalTestCluster;

import java.io.InputStream;

import static org.elasticsearch.cluster.coordination.stateless.StoreHeartbeatService.HEARTBEAT_FREQUENCY;
import static org.hamcrest.Matchers.equalTo;

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
            input.readInt();
            input.readLong();
            nodeLeftGenerationBeforeNodeLeft = input.readLong();
        }

        internalCluster().stopNode(indexNode1);

        ensureStableCluster(2);

        try (InputStream inputStream = blobContainerForTermLease.readBlob(operationPurpose, "lease")) {
            BytesArray newRootBlob = new BytesArray(inputStream.readAllBytes());
            assertThat(newRootBlob.length(), equalTo(28));
            StreamInput input = newRootBlob.streamInput();
            input.readInt();
            input.readLong();
            assertEquals(nodeLeftGenerationBeforeNodeLeft + 1, input.readLong());
        }
    }

    private void assertTransportVersionConsistency(String viaNode, String expectedMasterNodeName) {
        awaitMasterNode(viaNode, expectedMasterNodeName);
        final var clusterService = internalCluster().getCurrentMasterNodeInstance(ClusterService.class);
        assertEquals(TransportVersion.current(), clusterService.state().getMinTransportVersion());
        assertEquals(clusterService.state().nodes().getNodes().keySet(), clusterService.state().compatibilityVersions().keySet());
    }

    public void testTransportVersions() throws Exception {
        final var node0 = startMasterOnlyNode(fastFullClusterRestartSettings);
        assertTransportVersionConsistency(node0, node0);

        internalCluster().fullRestart(new InternalTestCluster.RestartCallback() {
            @Override
            public Settings onNodeStopped(String nodeName) {
                return fastFullClusterRestartSettings;
            }
        });
        assertTransportVersionConsistency(node0, node0);

        final var node1 = startMasterOnlyNode(fastFullClusterRestartSettings);
        awaitMasterNode(node1, node0);
        assertTransportVersionConsistency(node0, node0);

        final var nodeToStop = randomBoolean() ? node0 : node1;
        final var remainingNode = node0.equals(nodeToStop) ? node1 : node0;
        assertTrue("unable to stop node " + nodeToStop, internalCluster().stopNode(nodeToStop));
        assertTransportVersionConsistency(remainingNode, remainingNode);
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
