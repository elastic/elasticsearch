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
import co.elastic.elasticsearch.stateless.action.NewCommitNotificationRequest;
import co.elastic.elasticsearch.stateless.action.TransportGetVirtualBatchedCompoundCommitChunkAction;
import co.elastic.elasticsearch.stateless.action.TransportNewCommitNotificationAction;
import co.elastic.elasticsearch.stateless.engine.SearchEngine;
import co.elastic.elasticsearch.stateless.objectstore.ObjectStoreService;

import org.elasticsearch.cluster.coordination.Coordinator;
import org.elasticsearch.cluster.service.ClusterService;
import org.elasticsearch.common.CheckedSupplier;
import org.elasticsearch.common.blobstore.OperationPurpose;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.unit.ByteSizeValue;
import org.elasticsearch.core.TimeValue;
import org.elasticsearch.plugins.Plugin;
import org.elasticsearch.test.ESIntegTestCase;
import org.elasticsearch.test.disruption.BlockClusterStateProcessing;
import org.elasticsearch.test.transport.MockTransportService;
import org.junit.Before;

import java.io.IOException;
import java.io.InputStream;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Locale;
import java.util.concurrent.CountDownLatch;

import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.greaterThan;
import static org.hamcrest.Matchers.hasItem;
import static org.hamcrest.Matchers.is;
import static org.hamcrest.Matchers.lessThan;
import static org.hamcrest.Matchers.not;

@ESIntegTestCase.ClusterScope(scope = ESIntegTestCase.Scope.TEST, numDataNodes = 0, numClientNodes = 0, autoManageMasterNodes = false)
public class VirtualBatchedCompoundCommitsDisruptionIT extends AbstractStatelessIntegTestCase {

    @Override
    protected boolean addMockFsRepository() {
        return false;
    }

    @Override
    protected Collection<Class<? extends Plugin>> nodePlugins() {
        var plugins = new ArrayList<>(super.nodePlugins());
        plugins.add(StatelessMockRepositoryPlugin.class);
        return plugins;
    }

    @Before
    public void startMaster() {
        internalCluster().setBootstrapMasterNodeIndex(1);
        startMasterOnlyNode(Settings.builder().put(Coordinator.PUBLISH_TIMEOUT_SETTING.getKey(), "1s").build());
    }

    @Override
    protected Settings.Builder nodeSettings() {
        return super.nodeSettings().put(StatelessCommitService.STATELESS_UPLOAD_MAX_SIZE.getKey(), ByteSizeValue.ofGb(1))
            .put(StatelessCommitService.STATELESS_UPLOAD_VBCC_MAX_AGE.getKey(), TimeValue.timeValueDays(1))
            .put(StatelessCommitService.STATELESS_UPLOAD_MONITOR_INTERVAL.getKey(), TimeValue.timeValueDays(1))
            .put(StatelessCommitService.STATELESS_UPLOAD_MAX_AMOUNT_COMMITS.getKey(), 1000)
            // tests in this suite check the number of commits, generations etc
            .put(disableIndexingDiskAndMemoryControllersNodeSettings())
            .put(ObjectStoreService.TYPE_SETTING.getKey(), ObjectStoreService.ObjectStoreType.MOCK);
    }

    /**
     * This test checks that a search shard processes new commit notifications immediately, without waiting to be updated to
     * the notification's cluster state version, as long as the node in notification exists in the current cluster state.
     **/
    public void testSearchShardNewCommitNotificationWhenNodeKnownAndWithDelayedClusterState() throws Exception {
        String indexNodeA = startIndexNode();
        String indexNodeB = startIndexNode();
        String indexNodeBId = getNodeId(indexNodeB);
        String searchNode = startSearchNode();
        ensureStableCluster(4);

        final String indexName = randomAlphaOfLength(10).toLowerCase(Locale.ROOT);
        createIndex(indexName, indexSettings(1, 1).put("index.routing.allocation.exclude._name", indexNodeB).build());
        ensureGreen(indexName);
        indexDocsAndRefresh(indexName, 10);

        long initialIndexGeneration = findIndexShard(indexName).commitStats().getGeneration();
        var searchClusterService = internalCluster().getInstance(ClusterService.class, searchNode);
        long searchClusterStateVersion = searchClusterService.state().version();

        logger.info("--> start disrupting search cluster");
        var searchDisruption = new BlockClusterStateProcessing(searchNode, random());
        internalCluster().setDisruptionScheme(searchDisruption);
        searchDisruption.startDisrupting();

        logger.info("--> relocating shard from {}", indexNodeA);
        indicesAdmin().prepareUpdateSettings(indexName)
            .setSettings(Settings.builder().put("index.routing.allocation.exclude._name", indexNodeA))
            .execute();
        awaitClusterState(
            indexNodeA,
            clusterState -> clusterState.routingTable().index(indexName).shard(0).primaryShard().currentNodeId().equals(indexNodeBId)
        );
        assertBusy(() -> assertThat(internalCluster().nodesInclude(indexName), not(hasItem(indexNodeA))));
        logger.info("--> relocated primary");

        var indexShard = findIndexShard(indexName);
        long indexGenerationAfterRelocation = indexShard.commitStats().getGeneration();

        // search node knows about indexNodeB, and the notification is for an uploaded BCC, so it processes it during relocation
        assertBusy(() -> {
            long searchGeneration = findSearchShard(indexName).commitStats().getGeneration();
            assertThat(searchGeneration, greaterThan(initialIndexGeneration));
            assertThat(searchGeneration, equalTo(indexGenerationAfterRelocation));
        });

        // produce a non-uploaded VBCC
        indexDocsAndRefresh(indexName, 10);
        var statelessCommitService = internalCluster().getInstance(StatelessCommitService.class, indexNodeB);
        assertNotNull(statelessCommitService.getCurrentVirtualBcc(indexShard.shardId()));
        logger.info("--> produced VBCC");
        long indexGenerationAfterVBCC = indexShard.commitStats().getGeneration();

        // search node knows about indexNodeB, so even if the notification is for a non-uploaded VBCC, it processes it
        assertBusy(() -> {
            long searchGeneration = findSearchShard(indexName).commitStats().getGeneration();
            assertThat(searchGeneration, greaterThan(indexGenerationAfterRelocation));
            assertThat(searchGeneration, equalTo(indexGenerationAfterVBCC));
        });

        long searchClusterStateVersionAfterRelocation = searchClusterService.state().version();
        assertEquals(searchClusterStateVersion, searchClusterStateVersionAfterRelocation);

        searchDisruption.stopDisrupting();
    }

    /**
     * This test checks that a search shard waits until the cluster state is updated if the node in a non-uploaded notification
     * doesn't exist in the current cluster state.
     **/
    public void testSearchShardWaitsForUnknownNodeBeforeProcessingNewCommitNotification() throws Exception {
        String indexNodeA = startIndexNode();
        String searchNode = startSearchNode();
        ensureStableCluster(3);

        final String indexName = randomAlphaOfLength(10).toLowerCase(Locale.ROOT);
        createIndex(indexName, indexSettings(1, 1).build());
        ensureGreen(indexName);
        indexDocsAndRefresh(indexName, 10);

        var searchEngine = asInstanceOf(SearchEngine.class, findSearchShard(indexName).getEngineOrNull());
        long initialIndexGeneration = findIndexShard(indexName).commitStats().getGeneration();
        var searchClusterService = internalCluster().getInstance(ClusterService.class, searchNode);
        long initialSearchClusterStateVersion = searchClusterService.state().version();

        logger.info("--> start disrupting the search node");
        var searchDisruption = new BlockClusterStateProcessing(searchNode, random());
        internalCluster().setDisruptionScheme(searchDisruption);
        searchDisruption.startDisrupting();

        String indexNodeB = startIndexNode();
        String indexNodeBId = getNodeId(indexNodeB);

        logger.info("--> relocating shard from {}", indexNodeA);
        indicesAdmin().prepareUpdateSettings(indexName)
            .setSettings(Settings.builder().put("index.routing.allocation.exclude._name", indexNodeA))
            .execute();

        // The search node can process the commit notification of the relocation since it is for an uploaded BCC
        assertBusy(() -> {
            long searchGeneration = findSearchShard(indexName).commitStats().getGeneration();
            assertThat(searchGeneration, greaterThan(initialIndexGeneration));
        });
        logger.info("--> search shard is on relocated generation");

        var indexShard = findIndexShard(indexName);
        long indexGenerationAfterRelocation = indexShard.commitStats().getGeneration();

        // Capture new commit notification for the upcoming non-uploaded VBCC
        CountDownLatch receivedNonUploadedCommitNotification = new CountDownLatch(1);
        MockTransportService.getInstance(searchNode)
            .addRequestHandlingBehavior(TransportNewCommitNotificationAction.NAME + "[u]", (handler, request, channel, task) -> {
                handler.messageReceived(request, channel, task);
                var notification = asInstanceOf(NewCommitNotificationRequest.class, request);
                if (notification.getNodeId().equals(indexNodeBId)
                    && notification.getGeneration() > indexGenerationAfterRelocation
                    && notification.isUploaded() == false
                    && receivedNonUploadedCommitNotification.getCount() > 0L) {
                    receivedNonUploadedCommitNotification.countDown();
                }
            });

        // produce a non-uploaded VBCC
        var statelessCommitService = internalCluster().getInstance(StatelessCommitService.class, indexNodeB);
        assertNull(statelessCommitService.getCurrentVirtualBcc(indexShard.shardId()));
        indexDocs(indexName, 10);
        logger.info("--> indexed docs");

        // refresh on a separate thread as it will not complete until the search node processes the commit notification
        Thread refreshThread = new Thread(() -> refresh(indexName));
        refreshThread.start();

        // wait until the VBCC is created
        assertBusy(() -> assertNotNull(statelessCommitService.getCurrentVirtualBcc(indexShard.shardId())));
        logger.info("--> created VBCC");

        long indexGenerationAfterVBCC = statelessCommitService.getCurrentVirtualBcc(indexShard.shardId()).getMaxGeneration();

        // The search node can't process the non-uploaded new commit notification, because it does not know the node
        safeAwait(receivedNonUploadedCommitNotification);
        assertThat(searchEngine.getLastCommittedSegmentInfos().getGeneration(), lessThan(indexGenerationAfterVBCC));

        assertThat(searchClusterService.state().version(), equalTo(initialSearchClusterStateVersion));
        searchDisruption.stopDisrupting();

        // After we unblocked the cluster state on the search node, it processes the commit notification
        assertBusy(() -> {
            assertThat(searchClusterService.state().version(), greaterThan(initialSearchClusterStateVersion));
            assertThat(searchEngine.getLastCommittedSegmentInfos().getGeneration(), equalTo(indexGenerationAfterVBCC));
        });
        refreshThread.join();
    }

    public void testSearchNodeDoesNotFallbackToBlobStoreOnIndexNodeCrash() throws Exception {
        var indexNode = startIndexNode();
        var searchNode = startSearchNode();

        var indexName = randomIdentifier();
        createIndex(indexName, 1, 1);
        ensureGreen(indexName);

        var indexShard = findIndexShard(indexName);
        var currentVBCCGen = indexShard.commitStats().getGeneration() + 1;

        var getVBCCChunkRequestBlocked = new CountDownLatch(1);
        var getVBCCSent = new CountDownLatch(1);
        MockTransportService.getInstance(searchNode).addSendBehavior((connection, requestId, action, request, options) -> {
            if (action.equals(TransportGetVirtualBatchedCompoundCommitChunkAction.NAME + "[p]")) {
                getVBCCSent.countDown();
                safeAwait(getVBCCChunkRequestBlocked);
            }
            connection.sendRequest(requestId, action, request, options);
        });

        setNodeRepositoryStrategy(searchNode, new StatelessMockRepositoryStrategy() {
            @Override
            public InputStream blobContainerReadBlob(
                CheckedSupplier<InputStream, IOException> originalSupplier,
                OperationPurpose purpose,
                String blobName,
                long position,
                long length
            ) throws IOException {
                // Ensure that the search node doesn't fall back to the blob store to read a non-uploaded BCC
                assertThat(blobName, is(not(equalTo(BatchedCompoundCommit.blobNameFromGeneration(currentVBCCGen)))));
                return super.blobContainerReadBlob(originalSupplier, purpose, blobName, position, length);
            }
        });

        indexDocs(indexName, 10);
        var refreshFuture = indicesAdmin().prepareRefresh(indexName).execute();

        safeAwait(getVBCCSent);
        internalCluster().stopNode(indexNode);

        getVBCCChunkRequestBlocked.countDown();
        var refreshResponse = refreshFuture.get();
        // Delete the index, otherwise the test cleanup process would try to flush the index and that would take 60s to timeout
        client().admin().indices().prepareDelete(indexName).get();
    }
}
