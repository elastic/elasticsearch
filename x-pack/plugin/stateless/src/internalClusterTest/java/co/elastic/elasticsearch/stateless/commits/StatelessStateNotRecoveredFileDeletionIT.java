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

package co.elastic.elasticsearch.stateless.commits;

import co.elastic.elasticsearch.stateless.AbstractStatelessIntegTestCase;
import co.elastic.elasticsearch.stateless.StatelessMockRepositoryPlugin;
import co.elastic.elasticsearch.stateless.StatelessMockRepositoryStrategy;
import co.elastic.elasticsearch.stateless.cluster.coordination.StatelessClusterConsistencyService;
import co.elastic.elasticsearch.stateless.objectstore.ObjectStoreService;

import org.elasticsearch.action.admin.cluster.health.ClusterHealthRequest;
import org.elasticsearch.action.admin.cluster.health.ClusterHealthResponse;
import org.elasticsearch.cluster.coordination.stateless.StoreHeartbeatService;
import org.elasticsearch.cluster.health.ClusterHealthStatus;
import org.elasticsearch.cluster.service.ClusterService;
import org.elasticsearch.common.Priority;
import org.elasticsearch.common.blobstore.OperationPurpose;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.util.CollectionUtils;
import org.elasticsearch.core.CheckedConsumer;
import org.elasticsearch.core.CheckedRunnable;
import org.elasticsearch.core.Releasable;
import org.elasticsearch.core.TimeValue;
import org.elasticsearch.gateway.GatewayService;
import org.elasticsearch.plugins.Plugin;
import org.elasticsearch.test.ESIntegTestCase;
import org.elasticsearch.test.InternalTestCluster;
import org.elasticsearch.test.LambdaMatchers;
import org.elasticsearch.test.transport.MockTransportService;

import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.util.Collection;
import java.util.concurrent.Semaphore;

import static org.hamcrest.Matchers.is;

@ESIntegTestCase.ClusterScope(scope = ESIntegTestCase.Scope.TEST, numDataNodes = 0, numClientNodes = 0, autoManageMasterNodes = false)
public class StatelessStateNotRecoveredFileDeletionIT extends AbstractStatelessIntegTestCase {

    @Override
    protected Collection<Class<? extends Plugin>> nodePlugins() {
        return CollectionUtils.appendToCopy(super.nodePlugins(), StatelessMockRepositoryPlugin.class);
    }

    @Override
    protected Settings.Builder nodeSettings() {
        return super.nodeSettings().put(ObjectStoreService.TYPE_SETTING.getKey(), ObjectStoreService.ObjectStoreType.MOCK)
            .put(StoreHeartbeatService.MAX_MISSED_HEARTBEATS.getKey(), 1)
            .put(StoreHeartbeatService.HEARTBEAT_FREQUENCY.getKey(), TimeValue.timeValueSeconds(1))
            // this allows us to control when state is recovered.
            .put(GatewayService.EXPECTED_DATA_NODES_SETTING.getKey(), 3)
            .put(StatelessClusterConsistencyService.DELAYED_CLUSTER_CONSISTENCY_INTERVAL_SETTING.getKey(), "100ms");

    }

    /**
     * Simplest test that utilize a single dedicated master node to provoke a situation where a node sees STATE_NOT_RECOVERED_BLOCK.
     * It utilizes that we have 3 expected data nodes,
     * thus when we full cluster restart (when restarting master) and we have less than 3 data nodes in the cluster, we get a controllable
     * period of having STATE_NOT_RECOVERED_BLOCK. It utilizes two search nodes to control this, those do not otherwise affect the test.
     * This test is still valid for production, since:
     * <ul>
     *     <li>The full cluster restart case can happen during a failover if the new master does not have the new state</li>
     *     <li>The period where we have STATE_NOTE_REOVERED_BLOCK is obviously small, but is present, since it takes another cs update to
     *     clear it</li>
     *     <li>In particular, the cluster state validation done would likely wake up on the STATE_NOT_RECOVERED_BLOCk cs update</li>
     * </ul>
     */
    public void testShardFilesNotDeletedOnStateNotRecoveredDedicatedMaster() throws Exception {
        internalCluster().setBootstrapMasterNodeIndex(0);
        String master = startMasterOnlyNode();
        String indexNodeA = startIndexNode();
        // need 3 data nodes for gateway to recover state.
        String search1 = startSearchNode();
        String search2 = startSearchNode();
        ensureStableCluster(4);
        createIndex("test", 1, 0);
        indexDocs("test", 10);
        flush("test");
        indexDocs("test", 10);
        flush("test");
        String indexNodeB = startIndexNode();
        ensureStableCluster(5);

        BlockUploads blockUploads = new BlockUploads();
        setNodeRepositoryStrategy(indexNodeA, blockUploads);
        // force merge 2 segments to 1 to generate 2 deletions.
        safeGet(client().admin().indices().prepareForceMerge("test").setFlush(false).setMaxNumSegments(1).execute());
        try (Releasable ignored = blockUploads.block()) {
            client().admin().indices().prepareFlush("test").execute();

            internalCluster().stopNode(search1);
            internalCluster().stopNode(search2);
            final MockTransportService indexNodeTransportService = MockTransportService.getInstance(indexNodeA);
            final MockTransportService masterTransportService = MockTransportService.getInstance(master);
            indexNodeTransportService.addUnresponsiveRule(masterTransportService);
            logger.info("--> restarting master 1st time");
            try {
                internalCluster().restartNode(master, new InternalTestCluster.RestartCallback() {
                    @Override
                    public boolean validateClusterForming() {
                        return false;
                    }
                });
                // master + node B.
                ensureStableCluster(2, master);
                logger.info("--> waiting for global block");
                assertThat(
                    internalCluster().getInstance(ClusterService.class, indexNodeB).state(),
                    LambdaMatchers.transformedMatch(
                        state -> state.blocks().hasGlobalBlock(GatewayService.STATE_NOT_RECOVERED_BLOCK),
                        is(true)
                    )
                );

                logger.info("--> starting 2nd/3rd data node to unblock state recovery");
                search1 = startSearchNode();
                search2 = startSearchNode();
                assertBusy(
                    () -> assertFalse(
                        internalCluster().getInstance(ClusterService.class, indexNodeB)
                            .state()
                            .blocks()
                            .hasGlobalBlock(GatewayService.STATE_NOT_RECOVERED_BLOCK)
                    )
                );

                // 4 nodes: 2 search + master + B. A is not in cluster, cannot join due to unresponsive rule.
                ensureGreenVia(master, 4, "test");

                logger.info("--> unblocking shard to wait for cluster state, must observe new term in lease");
                blockUploads.unblock();

                internalCluster().stopNode(search1);
                internalCluster().stopNode(search2);

                logger.info("--> restarting master 2nd time");
                internalCluster().restartNode(master, new InternalTestCluster.RestartCallback() {
                    @Override
                    public Settings onNodeStopped(String nodeName) throws Exception {
                        // ensure shard goes to node B
                        return Settings.builder()
                            .put(super.onNodeStopped(nodeName))
                            .put("cluster.routing.allocation.require.name", indexNodeB)
                            .build();
                    }

                    @Override
                    public boolean validateClusterForming() {
                        return false;
                    }
                });
            } finally {
                logger.info("--> clearing unresponsive rule, allow node A to join");
                indexNodeTransportService.clearAllRules();
            }
        }
        // master + A + B
        ensureStableCluster(3);
        // still only 2 data nodes
        assertTrue(
            internalCluster().getInstance(ClusterService.class, indexNodeA)
                .state()
                .blocks()
                .hasGlobalBlock(GatewayService.STATE_NOT_RECOVERED_BLOCK)
        );

        // not really needed
        safeSleep(10);

        // unblock gateway recovery
        startSearchNode();

        logger.info("--> restarting node B to verify file consistency");
        internalCluster().restartNode(indexNodeB, new InternalTestCluster.RestartCallback() {
            @Override
            public boolean validateClusterForming() {
                return false;
            }
        });
        ensureGreen("test");
    }

    public void testShardFilesNotDeletedOnStateNotRecoveredSharedMaster() {
        // todo: add this test.

    }

    private static ClusterHealthStatus ensureGreenVia(String via, int nodes, String... indices) {
        TimeValue timeout = TimeValue.timeValueSeconds(30);
        ClusterHealthRequest healthRequest = new ClusterHealthRequest(timeout, indices).timeout(timeout)
            .waitForStatus(ClusterHealthStatus.GREEN)
            .waitForEvents(Priority.LANGUID)
            .waitForNoRelocatingShards(true)
            .waitForNoInitializingShards(false)
            .waitForNodes(Integer.toString(nodes));

        ClusterHealthResponse clusterHealthResponse = internalCluster().client(via).admin().cluster().health(healthRequest).actionGet();

        assertFalse(clusterHealthResponse.isTimedOut());

        return clusterHealthResponse.getStatus();
    }

    private static class BlockUploads extends StatelessMockRepositoryStrategy {
        private Semaphore blocker = new Semaphore(Integer.MAX_VALUE);

        @Override
        public void blobContainerWriteBlobAtomic(
            CheckedRunnable<IOException> originalRunnable,
            OperationPurpose purpose,
            String blobName,
            InputStream inputStream,
            long blobSize,
            boolean failIfAlreadyExists
        ) throws IOException {
            safeAcquire(blocker);
            try {
                super.blobContainerWriteBlobAtomic(originalRunnable, purpose, blobName, inputStream, blobSize, failIfAlreadyExists);
            } finally {
                blocker.release();
            }
        }

        @Override
        public void blobContainerWriteMetadataBlob(
            CheckedRunnable<IOException> original,
            OperationPurpose purpose,
            String blobName,
            boolean failIfAlreadyExists,
            boolean atomic,
            CheckedConsumer<OutputStream, IOException> writer
        ) throws IOException {
            safeAcquire(blocker);
            try {
                super.blobContainerWriteMetadataBlob(original, purpose, blobName, failIfAlreadyExists, atomic, writer);
            } finally {
                blocker.release();
            }
        }

        public Releasable block() {
            safeAcquire(Integer.MAX_VALUE, blocker);
            return () -> {
                if (blocker.availablePermits() == 0) {
                    unblock();
                }
            };
        }

        public void unblock() {
            assert blocker.availablePermits() == 0;
            blocker.release(Integer.MAX_VALUE);
        }
    }
}
