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

import co.elastic.elasticsearch.stateless.action.FetchShardCommitsInUseAction;
import co.elastic.elasticsearch.stateless.action.NewCommitNotificationRequest;
import co.elastic.elasticsearch.stateless.action.TransportFetchShardCommitsInUseAction;
import co.elastic.elasticsearch.stateless.action.TransportNewCommitNotificationAction;
import co.elastic.elasticsearch.stateless.cache.action.ClearBlobCacheNodesRequest;
import co.elastic.elasticsearch.stateless.cluster.coordination.StatelessClusterConsistencyService;
import co.elastic.elasticsearch.stateless.commits.StatelessCommitService;
import co.elastic.elasticsearch.stateless.commits.StatelessCommitServiceTestUtils;
import co.elastic.elasticsearch.stateless.engine.PrimaryTermAndGeneration;

import org.elasticsearch.action.admin.indices.forcemerge.ForceMergeRequest;
import org.elasticsearch.action.search.SearchScrollRequest;
import org.elasticsearch.action.support.SubscribableListener;
import org.elasticsearch.cluster.ClusterState;
import org.elasticsearch.cluster.service.ClusterService;
import org.elasticsearch.common.TriConsumer;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.unit.ByteSizeValue;
import org.elasticsearch.core.TimeValue;
import org.elasticsearch.index.query.QueryBuilders;
import org.elasticsearch.index.shard.ShardId;
import org.elasticsearch.test.junit.annotations.TestLogging;
import org.elasticsearch.test.transport.MockTransportService;
import org.hamcrest.Matchers;

import java.util.Set;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.atomic.AtomicInteger;

import static co.elastic.elasticsearch.stateless.Stateless.CLEAR_BLOB_CACHE_ACTION;
import static co.elastic.elasticsearch.stateless.commits.StatelessCommitService.SHARD_INACTIVITY_DURATION_TIME_SETTING;
import static co.elastic.elasticsearch.stateless.commits.StatelessCommitService.SHARD_INACTIVITY_MONITOR_INTERVAL_TIME_SETTING;
import static org.elasticsearch.test.hamcrest.ElasticsearchAssertions.assertHitCount;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.greaterThan;
import static org.hamcrest.Matchers.hasItem;
import static org.hamcrest.Matchers.hasItems;
import static org.hamcrest.Matchers.instanceOf;
import static org.hamcrest.Matchers.notNullValue;
import static org.hamcrest.core.IsNot.not;

public class StatelessCommitServiceIT extends AbstractStatelessIntegTestCase {

    /**
     * Moves all the index's shards away from searchNodeA and onto searchNodeB.
     */
    private void moveShardsFromNodeAToNodeB(String indexName, String searchNodeA, String searchNodeB) throws Exception {
        // Move shard away from searchNodeWithShard to the other node searchNodeWithoutShard.
        logger.info("--> moving shards in index [{}] away from node [{}]", indexName, searchNodeA);
        updateIndexSettings(Settings.builder().put("index.routing.allocation.exclude._name", searchNodeA), indexName);
        ensureGreen(indexName); // wait for any index shards to move.
        assertThat(internalCluster().nodesInclude(indexName), not(hasItem(searchNodeA)));
        assertThat(internalCluster().nodesInclude(indexName), hasItem(searchNodeB));
        logger.info("--> shards in index [{}] have been moved off of node [{}]", indexName, searchNodeA);
    }

    /**
     * Kick a node's StatelessClusterConsistencyService to validate membership and run pending tasks
     * Blob store deletes are one such task. Without this, they only execute on the service's timer (every 5 seconds by default).
     * @param indexNode node to kick
     */
    private void kickConsistencyService(String indexNode) {
        var sync = new SubscribableListener<Void>();
        internalCluster().getInstance(StatelessClusterConsistencyService.class, indexNode)
            .ensureClusterStateConsistentWithRootBlob(sync, TimeValue.MAX_VALUE);
        safeAwait(sync);
    }

    /**
     * Cycles the StatelessCommitService machinery to update search node tracking and ensure commits are uploaded to the blob store and any
     * unused VBCCs get deleted from the blob store. The VBCC vs commit distinction is important, since a commit cannot be deleted until all
     * of a VBCC's commits are unused. Assertions that the files are deleted must also be done in an assertBusy() loop, even after calling
     * this method, because a small amount of asynchronous behavior remains to complete blob store file deletions after the
     * StatelessClusterConsistencyService confirms the master state is unchanged.
     */
    private void flushAndUpdateCommitServiceTrackingAndBlobStoreFiles(String indexNode, String indexName) throws ExecutionException,
        InterruptedException {
        // Ensure all the latest data is committed.
        flush(indexName);

        // Force the StatelessCommitService to update search nodes tracking for each shard commit.
        StatelessCommitServiceTestUtils.updateCommitUseTrackingForInactiveShards(
            internalCluster().getInstance(StatelessCommitService.class, indexNode),
            () -> Long.MAX_VALUE
        );

        kickConsistencyService(indexNode);

        // Evict data from all node blob caches. This will force nodes to refresh their data caches from whatever remains on the remote blob
        // store.
        client().execute(CLEAR_BLOB_CACHE_ACTION, new ClearBlobCacheNodesRequest()).get();
    }

    /**
     * Tests that an index shard will retain commits in the blob store for active readers on search nodes that no longer own the search
     * shard. The index shard tracks commits in use by old search nodes that used to own a shard replica and still have active readers
     * depending on old shard commits, as well as search nodes that currently own a shard.
     * @param removeSearchShardFromSearchNodeA takes the indexName, searchNodeWithShard, searchNodeWithoutShard and moves the search shard
     *                                         off of searchNodeWithShard.
     */
    public void runTestIndexShardRetainsCommitsForOpenReadersAfterSearchShardIsRemoved(
        TriConsumer<String, String, String> removeSearchShardFromSearchNodeA
    ) throws Exception {
        final String indexNode = startMasterAndIndexNode(
            Settings.builder()
                // Put SHARD_INACTIVITY_MONITOR_INTERVAL_TIME_SETTING at a large value so the ShardInactivityMonitor will not run. This way
                // we can invoke the ShardInactivityMonitor logic deliberately, without it running on its own.
                .put(SHARD_INACTIVITY_MONITOR_INTERVAL_TIME_SETTING.getKey(), TimeValue.timeValueMinutes(30))
                .build()
        );
        final String searchNodeA = startSearchNode();
        final String searchNodeB = startSearchNode();

        final String indexName = randomIdentifier();
        createIndex(
            indexName,
            indexSettings(1, 1)
                // Start with the shard replica on searchNodeA.
                .put("index.routing.allocation.exclude._name", searchNodeB)
                .build()
        );
        ensureGreen(indexName);
        var indexNodeCommitService = internalCluster().getInstance(StatelessCommitService.class, indexNode);

        logger.info("--> Indexing documents and refreshing the index");

        // Set up some data to be read.
        final int numDocsToIndex = randomIntBetween(5, 100);
        indexDocsAndRefresh(indexName, numDocsToIndex);
        flushAndUpdateCommitServiceTrackingAndBlobStoreFiles(indexNode, indexName);

        final ShardId shardId = findSearchShard(indexName).shardId();
        final Set<PrimaryTermAndGeneration> commitsBeforeForceMerge;

        // Start a scroll to pin the reader state on the search node until the scroll is exhausted / released.
        final var scrollSearchResponse = client().prepareSearch(indexName)
            .setQuery(QueryBuilders.matchAllQuery())
            .setSize(1)
            .setScroll(TimeValue.timeValueMinutes(2))
            .get();
        try {
            assertThat(scrollSearchResponse.getScrollId(), Matchers.is(notNullValue()));
            assertHitCount(scrollSearchResponse, numDocsToIndex);
            assertThat(scrollSearchResponse.getHits().getHits().length, equalTo(1));

            // Save this information to check that the current commits of the indexing shard are later deleted, after a force merge, and
            // the search ends and finally releases any retained commit references.
            commitsBeforeForceMerge = listBlobsTermAndGenerations(shardId);
            logger.info("--> Blob store shard commit generations for search: " + commitsBeforeForceMerge);

            removeSearchShardFromSearchNodeA.apply(indexName, searchNodeA, searchNodeB);

            // Run some indexing and create a new commit. Then force merge down to a single segment (in another new commit). Newer commits
            // can reference information in older commit, rather than copying everything: force merge will ensure older commits are not
            // retained for this reason.
            indexDocsAndRefresh(indexName, numDocsToIndex);
            flushAndUpdateCommitServiceTrackingAndBlobStoreFiles(indexNode, indexName);

            logger.info("--> Blob store shard commit generations before force-merge: " + listBlobsTermAndGenerations(shardId));

            client().admin().indices().forceMerge(new ForceMergeRequest(indexName).maxNumSegments(1)).actionGet();
            flush(indexName);

            // The indexNode should retain at least one older commit than the force merge commit, even though searchNodeB is not using
            // older commits, because the old searchNodeA is still using an older commit. Cycle the tracking and ensure the old commit,
            // which the old search node searchNodeA is still using, is not removed.
            flushAndUpdateCommitServiceTrackingAndBlobStoreFiles(indexNode, indexName);
            Set<String> trackingSearchNodes = StatelessCommitServiceTestUtils.getAllSearchNodesRetainingCommitsForShard(
                indexNodeCommitService,
                shardId
            );
            assertTrue(
                "After shard movement and a commit, tracking search nodes: "
                    + trackingSearchNodes
                    + " should contain originally tracked search node:  "
                    + searchNodeA
                    + " (node ID "
                    + getNodeId(searchNodeA)
                    + ")",
                trackingSearchNodes.contains(getNodeId(searchNodeA))
            );

            final Set<PrimaryTermAndGeneration> commitsAfterForceMerge = listBlobsTermAndGenerations(shardId);
            var maxCommitAfterMerge = commitsAfterForceMerge.stream().max(PrimaryTermAndGeneration::compareTo).get();
            var maxCommitBeforeMerge = commitsBeforeForceMerge.stream().max(PrimaryTermAndGeneration::compareTo).get();
            logger.info("--> Blob store shard commit generations after force-merge: " + commitsAfterForceMerge);
            logger.info("--> Max commit gen before merge {}, max commit gen after merge {}", maxCommitBeforeMerge, maxCommitAfterMerge);
            assertThat(maxCommitAfterMerge, greaterThan(maxCommitBeforeMerge));

            // Try to fetch more data from the scroll using the old commit on the old search node (SearchNodeA). This should succeed because
            // the commit has not yet been deleted, since the index node tracks old search nodes until they release use of the old commits.
            client().searchScroll(new SearchScrollRequest(scrollSearchResponse.getScrollId())).get().decRef();
        } finally {
            // There's an implicit incRef in prepareSearch(), so call decRef() to release the response object back into the resource pool.
            scrollSearchResponse.decRef();
        }

        // Force the ShardInactivityMonitor to run on the indexing node, to fetch all the commits currently in use by search shards. Then we
        // can verify that the pre-merge commits are finally deleted once old searchNodeA reports that it no longer uses any commits.
        flushAndUpdateCommitServiceTrackingAndBlobStoreFiles(indexNode, indexName);

        Set<PrimaryTermAndGeneration> afterScrollReleaseCommitBlobs = listBlobsTermAndGenerations(shardId);
        logger.info("--> Blob store shard commit generations after search scroll release: " + afterScrollReleaseCommitBlobs);
        logger.info(
            "--> Index shard's tracked search nodes: {}",
            StatelessCommitServiceTestUtils.getAllSearchNodesRetainingCommitsForShard(indexNodeCommitService, shardId)
        );
        // There is still a little race with blob store file deletion after cycling the StatelessCommitService machinery above.
        assertBusy(() -> {
            kickConsistencyService(indexNode);

            Set<PrimaryTermAndGeneration> shardCommitBlobs = listBlobsTermAndGenerations(shardId);
            for (PrimaryTermAndGeneration generation : commitsBeforeForceMerge) {
                assertThat(shardCommitBlobs, not(hasItem(generation)));
            }
        });

    }

    /**
     * Runs {@link #runTestIndexShardRetainsCommitsForOpenReadersAfterSearchShardIsRemoved} with search shard removal done by moving the
     * search shard to another node, leveraging "index.routing.allocation.exclude._name".
     */
    @TestLogging(
        value = "co.elastic.elasticsearch.stateless.commits.StatelessCommitService:TRACE",
        reason = "This doesn't add a lot of logging and it's extremely useful for debugging"
    )
    public void testRetainCommitForReadersAfterShardMovedAway() throws Exception {
        runTestIndexShardRetainsCommitsForOpenReadersAfterSearchShardIsRemoved((indexName, searchNodeWithShard, searchNodeWithoutShard) -> {
            try {
                moveShardsFromNodeAToNodeB(indexName, searchNodeWithShard, searchNodeWithoutShard);
            } catch (Exception e) {
                assert false : "Encountered an exception: " + e;
            }
        });
    }

    /**
     * Runs {@link #runTestIndexShardRetainsCommitsForOpenReadersAfterSearchShardIsRemoved} with search shard removal done by updating the
     * index settings to 0 replicas.
     */
    @TestLogging(
        value = "co.elastic.elasticsearch.stateless.commits.StatelessCommitService:TRACE",
        reason = "This doesn't add a lot of logging and it's extremely useful for debugging"
    )
    public void testRetainCommitForReadersAfterShardReplicasSetToZero() throws Exception {
        runTestIndexShardRetainsCommitsForOpenReadersAfterSearchShardIsRemoved((indexName, searchNodeWithShard, searchNodeWithoutShard) -> {
            try {
                // Reduce the index's shard replica count to 0, eliminating the search shard that way.
                logger.info(
                    "--> reducing replicas of index [{}] to 0, eliminating the search shard on node [{}]",
                    indexName,
                    searchNodeWithShard
                );
                setReplicaCount(0, indexName);
                assertThat(internalCluster().nodesInclude(indexName), not(hasItems(searchNodeWithShard, searchNodeWithoutShard)));
                logger.info(
                    "--> shards in index [{}] have been removed from nodes [{}] and [{}]",
                    indexName,
                    searchNodeWithShard,
                    searchNodeWithoutShard
                );
            } catch (Exception e) {
                assert false : "Encountered an exception: " + e;
            }
        });
    }

    record MiniTestHarness(String indexNode, String searchNodeHoldingShard, String otherSearchNode, String indexName) {}

    /**
     * Sets up an index node and two search nodes. Populates some data, and then force merges down to a single commit so that the shard
     * predictably remains inactive: nothing further will happen without new writes. Then moves the search shard back and forth between the
     * two search nodes, ensuring that the index shard is tracking both nodes for the latest shard commit.
     *
     * @return A {@link MiniTestHarness} holding the node names and index name used for setup.
     */
    private MiniTestHarness setUpTwoSearchNodesAndDataWithInactiveShardTrackingBothSearchNodes() throws Exception {
        final String indexNode = startMasterAndIndexNode(
            Settings.builder()
                // Ensure that every commit is always immediately uploaded, not delayed for a batch. This ensures a predictable number of
                // NewCommitNotification actions are sent, so that they can be verified.
                .put(StatelessCommitService.STATELESS_UPLOAD_MAX_AMOUNT_COMMITS.getKey(), 1)
                .put(StatelessCommitService.STATELESS_UPLOAD_MAX_SIZE.getKey(), ByteSizeValue.ofGb(1))
                // Put SHARD_INACTIVITY_MONITOR_INTERVAL_TIME_SETTING at a large value so the ShardInactivityMonitor will not run, but set
                // SHARD_INACTIVITY_DURATION_TIME_SETTING to a small time window. This way we can invoke the ShardInactivityMonitor logic
                // deliberately, without it running on its own.
                .put(SHARD_INACTIVITY_MONITOR_INTERVAL_TIME_SETTING.getKey(), TimeValue.timeValueMinutes(30))
                .put(SHARD_INACTIVITY_DURATION_TIME_SETTING.getKey(), TimeValue.timeValueMillis(1))
                .build()
        );
        final String searchNodeHoldingShard = startSearchNode();
        final String otherSearchNode = startSearchNode();

        logger.info("--> Started an index node {} and search nodes {} and {}", indexNode, searchNodeHoldingShard, otherSearchNode);

        final String indexName = randomIdentifier();
        createIndex(
            indexName,
            indexSettings(1, 1)
                // Start with the search shard replica on searchNodeHoldingShard.
                .put("index.routing.allocation.exclude._name", otherSearchNode)
                .build()
        );
        ensureGreen(indexName);

        // Set up some data to be read.
        final int numDocsToIndex = randomIntBetween(5, 100);
        indexDocsAndRefresh(indexName, numDocsToIndex);

        logger.info("--> Created an index with search shards on node {} and indexed some data", otherSearchNode);

        // Force a merge and refresh so that it does not randomly occur later and invalidate the test results.
        client().admin().indices().forceMerge(new ForceMergeRequest(indexName).maxNumSegments(1)).actionGet();
        flush(indexName);

        logger.info("--> Force merged the shard to a single segment and refreshed. The shard will remain inactive now.");

        /**
         * Move the search shard back and forth between the search nodes, so that shard recovery adds tracking for both nodes to the latest
         * shard commit.
         */

        var nodeA = searchNodeHoldingShard;
        var nodeB = otherSearchNode;
        int numRounds = randomIntBetween(1, 5);
        for (int i = 1; i <= numRounds; ++i) {
            logger.info(
                "--> Starting round ({} of {}): moving shard(s) away from {} to {} and restarting node {}",
                i,
                numRounds,
                nodeA,
                nodeB,
                nodeA
            );

            moveShardsFromNodeAToNodeB(indexName, nodeA, nodeB);
            var temp = nodeA;
            nodeA = nodeB;
            nodeB = temp;

            // nodeA will hold the shard when this loop exits.
        }

        return new MiniTestHarness(indexNode, nodeA, nodeB, indexName);
    }

    /**
     * Tests that the index shard tracks all new search nodes that recover the shard for the latest shard commit, even when the index shard
     * is inactive / not doing any writes.
     *
     * The outline of the test is to
     * - Set up tracking for an old search node in the latest shard commit by moving a search shard back and forth between two search nodes.
     * - Index more data and provoke a new commit
     * - Verify that, for the new commit, a NewCommitNotificationAction request is sent by the index shard to the current search shard
     * owner, and a FetchCommitsInUseAction request is sent to the old search node
     */
    public void testSearchShardRecoveryAddsTrackingToLatestShardCommit() throws Exception {
        var miniTestHarness = setUpTwoSearchNodesAndDataWithInactiveShardTrackingBothSearchNodes();

        /**
         * Verify that a new commit to the index shard will send a NewCommitNotification action to the one search node and a Fetch action to
         * the other search node no longer holding the search shard.
         */

        // searchNodeHoldingShard should be the current owner of the search shard. The index node will look at the current routing table and
        // send a NewCommitNotification action to searchNodeHoldingShard. otherSearchNode should receive a FetchCommitsInUse action, since
        // it is no longer part of the current routing table for the shard, but should still be tracked / registered as using a shard
        // commit.

        AtomicInteger newCommitCounter = new AtomicInteger(0);
        AtomicInteger fetchCounter = new AtomicInteger(0);

        final var searchNodeHoldingShard = miniTestHarness.searchNodeHoldingShard;
        final var searchNodeThatPreviouslyHeldShard = miniTestHarness.otherSearchNode;

        logger.info("--> Registering network intercepts on the search node side to check transport action requests...");

        MockTransportService.getInstance(searchNodeHoldingShard)
            .addRequestHandlingBehavior(TransportNewCommitNotificationAction.NAME + "[u]", (handler, request, channel, task) -> {
                assertThat(request, instanceOf(NewCommitNotificationRequest.class));
                newCommitCounter.incrementAndGet();
                handler.messageReceived(request, channel, task);
            });

        MockTransportService.getInstance(searchNodeThatPreviouslyHeldShard)
            .addRequestHandlingBehavior(TransportNewCommitNotificationAction.NAME + "[u]", (handler, request, channel, task) -> {
                assertThat(request, instanceOf(NewCommitNotificationRequest.class));
                assert false
                    : searchNodeThatPreviouslyHeldShard
                        + " received a TransportNewCommitNotificationAction request that should not have been sent.";
            });

        MockTransportService.getInstance(searchNodeHoldingShard)
            .addRequestHandlingBehavior(TransportFetchShardCommitsInUseAction.NAME + "[n]", (handler, request, channel, task) -> {
                assertThat(request, instanceOf(FetchShardCommitsInUseAction.NodeRequest.class));
                assert false
                    : searchNodeHoldingShard + " received a TransportFetchShardCommitsInUseAction request that should not have been sent.";
            });

        MockTransportService.getInstance(searchNodeThatPreviouslyHeldShard)
            .addRequestHandlingBehavior(TransportFetchShardCommitsInUseAction.NAME + "[n]", (handler, request, channel, task) -> {
                assertThat(request, instanceOf(FetchShardCommitsInUseAction.NodeRequest.class));
                fetchCounter.incrementAndGet();
                handler.messageReceived(request, channel, task);
            });

        logger.info("--> Writing some new data to create a new commit and send out notifications");

        var numDocsToIndex = randomIntBetween(5, 100);
        indexDocsAndRefresh(miniTestHarness.indexName, numDocsToIndex);

        logger.info("--> Finished writing data, commit notifications should have been sent");

        // Verify NewCommitNotification actions were sent to the current search shard owner node, and a FetchCommitsInUse was sent to old
        // search node.
        assertBusy(() -> {
            // NewCommitNotif is sent twice for a new commit.
            assertThat("Expected new commit notification actions to be sent", newCommitCounter.get(), equalTo(2));
            assertThat("Expected a single fetch commits in use action to be sent", fetchCounter.get(), equalTo(1));
        });
    }

    /**
     * Tests that the index shard stops tracking an old search node (one that no longer owns the shard but might still be reading from it)
     * when the search node gets removed from the cluster.
     *
     * The outline of the test is to
     * - Set up tracking for an old search node in the latest shard commit by moving a search shard back and forth between two search nodes
     * - Reach into the StatelessCommitService to see what search nodes are tracked
     * - Restart the old search node, which removes it from the cluster and the StatelessCommitService should remove tracking for the node
     * - Verify that the StatelessCommitService has removed tracking for the removed search node
     */
    public void testSearchNodeRemovalRemovesTrackingInLatestShardCommit() throws Exception {
        final var miniTestHarness = setUpTwoSearchNodesAndDataWithInactiveShardTrackingBothSearchNodes();
        final var indexNodeCommitService = internalCluster().getInstance(StatelessCommitService.class, miniTestHarness.indexNode);
        final ShardId shardId = new ShardId(resolveIndex(miniTestHarness.indexName), 0);
        final ClusterState clusterState = internalCluster().getCurrentMasterNodeInstance(ClusterService.class).state();

        final Set<String> serviceTrackedSearchNodes = StatelessCommitServiceTestUtils.getAllSearchNodesRetainingCommitsForShard(
            indexNodeCommitService,
            shardId
        );
        final String nodeIdOfCurrentSearchNode = clusterState.nodes().resolveNode(miniTestHarness.searchNodeHoldingShard).getId();
        final String nodeIdOfOldSearchNode = clusterState.nodes().resolveNode(miniTestHarness.otherSearchNode).getId();

        logger.info(
            "--> The search node holding a copy is {} and the search node no longer holding a copy is {} ",
            nodeIdOfCurrentSearchNode,
            nodeIdOfOldSearchNode
        );

        assertThat(serviceTrackedSearchNodes, hasItems(nodeIdOfCurrentSearchNode, nodeIdOfOldSearchNode));

        logger.info(
            "--> Stopping node "
                + miniTestHarness.otherSearchNode
                + " that does not currently own the search shard. This should remove that node from the index shard tracking."
        );

        internalCluster().stopNode(miniTestHarness.otherSearchNode);

        logger.info(
            "--> Reaching into the StatelessCommitService to check whether the tracking for node {} (ID {}) has been removed",
            miniTestHarness.otherSearchNode,
            nodeIdOfOldSearchNode
        );

        assertBusy(
            () -> assertThat(
                StatelessCommitServiceTestUtils.getAllSearchNodesRetainingCommitsForShard(indexNodeCommitService, shardId),
                not(hasItem(nodeIdOfOldSearchNode))
            )
        );
    }
}
