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

package org.elasticsearch.xpack.stateless;

import org.elasticsearch.action.admin.indices.shrink.ResizeType;
import org.elasticsearch.action.support.replication.TransportReplicationAction;
import org.elasticsearch.cluster.metadata.IndexMetadata;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.core.TimeValue;
import org.elasticsearch.index.query.QueryBuilders;
import org.elasticsearch.index.shard.ShardId;
import org.elasticsearch.test.transport.MockTransportService;
import org.elasticsearch.xpack.stateless.cluster.coordination.StatelessClusterConsistencyService;
import org.elasticsearch.xpack.stateless.reshard.ReshardIndexRequest;
import org.elasticsearch.xpack.stateless.reshard.TransportReshardAction;
import org.elasticsearch.xpack.stateless.reshard.TransportReshardSplitAction;

import java.util.concurrent.CountDownLatch;

import static org.elasticsearch.action.admin.indices.ResizeIndexTestUtils.executeResize;
import static org.elasticsearch.test.hamcrest.ElasticsearchAssertions.assertAcked;
import static org.elasticsearch.test.hamcrest.ElasticsearchAssertions.assertHitCount;
import static org.elasticsearch.xpack.stateless.reshard.SplitSourceService.RESHARD_SPLIT_DELETE_UNOWNED_GRACE_PERIOD;
import static org.hamcrest.Matchers.containsString;
import static org.hamcrest.Matchers.greaterThan;
import static org.hamcrest.Matchers.hasSize;

public class StatelessResizeIT extends AbstractStatelessPluginIntegTestCase {

    @Override
    protected Settings.Builder nodeSettings() {
        return super.nodeSettings().put(StatelessClusterConsistencyService.DELAYED_CLUSTER_CONSISTENCY_INTERVAL_SETTING.getKey(), "100ms")
            // The following settings are needed for dynamic resharding
            .put(TransportReplicationAction.REPLICATION_RETRY_TIMEOUT.getKey(), "60s")
            .put(RESHARD_SPLIT_DELETE_UNOWNED_GRACE_PERIOD.getKey(), TimeValue.ZERO);
    }

    public void testShrinkIndex() throws Exception {
        startMasterOnlyNode();
        final int indexNodesNumber = randomIntBetween(1, 3);
        final var indexNodes = startIndexNodes(indexNodesNumber);
        final var searchNode = startSearchNode();
        int sourceShards = randomFrom(2, 4, 8);
        int targetShards = sourceShards / 2;
        String sourceIndex = "source";
        String targetIndex = "target";

        createIndex(sourceIndex, indexSettings(sourceShards, 1).build());
        ensureGreen(sourceIndex);

        int numDocs = randomIntBetween(0, 50);
        if (numDocs > 0) {
            indexDocsAndRefresh(sourceIndex, numDocs);
        }
        if (randomBoolean()) {
            flush(sourceIndex);
        }

        // Block writes and relocate all indexing shards to one indexing node
        final var indexNode = randomFrom(indexNodes);
        updateIndexSettings(
            Settings.builder()
                .put("index.routing.allocation.include._name", String.join(",", indexNode, searchNode))
                .put("index.blocks.write", true),
            sourceIndex
        );
        ensureGreen(sourceIndex);

        // Shrink
        assertAcked(
            executeResize(
                ResizeType.SHRINK,
                sourceIndex,
                targetIndex,
                Settings.builder()
                    .put(IndexMetadata.SETTING_NUMBER_OF_SHARDS, targetShards)
                    .put(IndexMetadata.SETTING_NUMBER_OF_REPLICAS, 1)
                    .putNull("index.blocks.write")
                    .putNull("index.routing.allocation.include._name")
            )
        );
        ensureGreen(targetIndex);

        checkIndicesAfterResize(sourceIndex, targetIndex, sourceShards, targetShards, numDocs);
    }

    public void testSplitIndex() throws Exception {
        startMasterOnlyNode();
        final int indexNodesNumber = randomIntBetween(1, 3);
        startIndexNodes(indexNodesNumber);
        startSearchNode();
        int sourceShards = randomFrom(1, 2, 4, 8);
        int targetShards = sourceShards * 2;
        String sourceIndex = "source";
        String targetIndex = "target";

        createIndex(sourceIndex, indexSettings(sourceShards, 1).build());
        ensureGreen(sourceIndex);

        int numDocs = randomIntBetween(0, 50);
        if (numDocs > 0) {
            indexDocsAndRefresh(sourceIndex, numDocs);
        }
        if (randomBoolean()) {
            flush(sourceIndex);
        }

        // Block writes
        updateIndexSettings(Settings.builder().put("index.blocks.write", true), sourceIndex);
        ensureGreen(sourceIndex);

        // Split
        assertAcked(
            executeResize(
                ResizeType.SPLIT,
                sourceIndex,
                targetIndex,
                Settings.builder()
                    .put(IndexMetadata.SETTING_NUMBER_OF_SHARDS, targetShards)
                    .put(IndexMetadata.SETTING_NUMBER_OF_REPLICAS, 1)
                    .putNull("index.blocks.write")
            )
        );
        ensureGreen(targetIndex);

        checkIndicesAfterResize(sourceIndex, targetIndex, sourceShards, targetShards, numDocs);
    }

    public void testCloneIndex() throws Exception {
        startMasterOnlyNode();
        final int indexNodesNumber = randomIntBetween(1, 3);
        startIndexNodes(indexNodesNumber);
        startSearchNode();
        int shards = randomFrom(2, 4, 8);
        String sourceIndex = "source";
        String targetIndex = "target";

        createIndex(sourceIndex, indexSettings(shards, 1).build());
        ensureGreen(sourceIndex);

        int numDocs = randomIntBetween(0, 50);
        if (numDocs > 0) {
            indexDocsAndRefresh(sourceIndex, numDocs);
        }
        if (randomBoolean()) {
            flush(sourceIndex);
        }

        // Block writes
        updateIndexSettings(Settings.builder().put("index.blocks.write", true), sourceIndex);
        ensureGreen(sourceIndex);

        // Clone
        assertAcked(
            executeResize(
                ResizeType.CLONE,
                sourceIndex,
                targetIndex,
                Settings.builder().put(IndexMetadata.SETTING_NUMBER_OF_REPLICAS, 1).putNull("index.blocks.write")
            )
        );
        ensureGreen(targetIndex);

        checkIndicesAfterResize(sourceIndex, targetIndex, shards, shards, numDocs);
    }

    public void testShrinkThenSplit() throws Exception {
        startMasterOnlyNode();
        final var indexNodes = startIndexNodes(randomIntBetween(1, 3));
        final var searchNode = startSearchNode();
        int sourceShards = randomFrom(4, 8);
        int shrunkShards = sourceShards / 2;
        int splitShards = sourceShards; // split back to the original count
        String sourceIndex = "source";
        String shrunkIndex = "shrunk";
        String splitIndex = "split_again";

        createIndex(sourceIndex, indexSettings(sourceShards, 1).build());
        ensureGreen(sourceIndex);

        int numDocs = randomIntBetween(1, 50);
        indexDocsAndRefresh(sourceIndex, numDocs);
        if (randomBoolean()) {
            flush(sourceIndex);
        }

        // Shrink: source -> shrunk
        final var indexNode = randomFrom(indexNodes);
        updateIndexSettings(
            Settings.builder()
                .put("index.routing.allocation.include._name", String.join(",", indexNode, searchNode))
                .put("index.blocks.write", true),
            sourceIndex
        );
        ensureGreen(sourceIndex);

        assertAcked(
            executeResize(
                ResizeType.SHRINK,
                sourceIndex,
                shrunkIndex,
                Settings.builder()
                    .put(IndexMetadata.SETTING_NUMBER_OF_SHARDS, shrunkShards)
                    .put(IndexMetadata.SETTING_NUMBER_OF_REPLICAS, 1)
                    .putNull("index.blocks.write")
                    .putNull("index.routing.allocation.include._name")
            )
        );
        ensureGreen(shrunkIndex);

        assertHitCount(prepareSearch(shrunkIndex).setSize(0).setQuery(QueryBuilders.matchAllQuery()), numDocs);
        for (int s = 0; s < shrunkShards; s++) {
            final ShardId shardId = findIndexShard(resolveIndex(shrunkIndex), s).shardId();
            assertThat(listBlobsTermAndGenerations(shardId), hasSize(greaterThan(0)));
        }

        // Split: shrunk -> split_again
        updateIndexSettings(Settings.builder().put("index.blocks.write", true), shrunkIndex);
        ensureGreen(shrunkIndex);

        assertAcked(
            executeResize(
                ResizeType.SPLIT,
                shrunkIndex,
                splitIndex,
                Settings.builder()
                    .put(IndexMetadata.SETTING_NUMBER_OF_SHARDS, splitShards)
                    .put(IndexMetadata.SETTING_NUMBER_OF_REPLICAS, 1)
                    .putNull("index.blocks.write")
            )
        );
        ensureGreen(splitIndex);

        checkIndicesAfterResize(sourceIndex, splitIndex, sourceShards, splitShards, numDocs);
    }

    public void testSplitFailsDuringResharding() throws Exception {
        String indexNode = startMasterAndIndexNode();
        startSearchNode();
        String sourceIndex = "source";
        int sourceShards = 2;

        createIndex(sourceIndex, indexSettings(sourceShards, 1).build());
        ensureGreen(sourceIndex);

        int numDocs = randomIntBetween(1, 20);
        indexDocsAndRefresh(sourceIndex, numDocs);
        if (randomBoolean()) {
            flush(sourceIndex);
        }

        // Set write block (required for resize).
        updateIndexSettings(Settings.builder().put("index.blocks.write", true), sourceIndex);

        // A hook for catching dynamic resharding in the middle of operations.
        final var splitHandoffInitiated = new CountDownLatch(1);
        final var splitHandoffToContinue = new CountDownLatch(1);
        MockTransportService.getInstance(indexNode)
            .addRequestHandlingBehavior(TransportReshardSplitAction.SPLIT_HANDOFF_ACTION_NAME, (handler, request, channel, task) -> {
                splitHandoffInitiated.countDown();
                safeAwait(splitHandoffToContinue);
                handler.messageReceived(request, channel, task);
            });

        // Start a reshard operation
        client(indexNode).execute(TransportReshardAction.TYPE, new ReshardIndexRequest(sourceIndex)).actionGet(SAFE_AWAIT_TIMEOUT);

        // Wait until the middle of the resharding operation.
        safeAwait(splitHandoffInitiated);

        // Attempt a split — should be rejected because resharding metadata is present
        IllegalStateException e = expectThrows(
            IllegalStateException.class,
            executeResize(
                ResizeType.SPLIT,
                sourceIndex,
                "target_split",
                Settings.builder()
                    .put(IndexMetadata.SETTING_NUMBER_OF_SHARDS, sourceShards * 2)
                    .put(IndexMetadata.SETTING_NUMBER_OF_REPLICAS, 0)
                    .putNull("index.blocks.write")
            )
        );
        assertThat(e.getMessage(), containsString("cannot be resized while a resharding operation is in progress"));

        // Continue resharding and wait for it to complete
        splitHandoffToContinue.countDown();
        awaitClusterState((state) -> state.projectState().metadata().index(sourceIndex).getReshardingMetadata() == null);
        assertThat(getNumShards(sourceIndex).numPrimaries, greaterThan(sourceShards));

        // Resharded index should still have only original docs
        ensureGreen(sourceIndex);
        assertHitCount(prepareSearch(sourceIndex).setSize(0).setQuery(QueryBuilders.matchAllQuery()), numDocs);
    }

    public void testSplitAfterResharding() throws Exception {
        String indexNode = startMasterAndIndexNode();
        startSearchNode();
        String sourceIndex = "source";
        int sourceShards = 2;

        createIndex(sourceIndex, indexSettings(sourceShards, 1).build());
        ensureGreen(sourceIndex);

        int numDocs = randomIntBetween(1, 20);
        indexDocsAndRefresh(sourceIndex, numDocs);
        if (randomBoolean()) {
            flush(sourceIndex);
        }

        client(indexNode).execute(TransportReshardAction.TYPE, new ReshardIndexRequest(sourceIndex)).actionGet(SAFE_AWAIT_TIMEOUT);

        // Wait for resharding to complete
        awaitClusterState((state) -> state.projectState().metadata().index(sourceIndex).getReshardingMetadata() == null);
        int reshardShards = getNumShards(sourceIndex).numPrimaries;
        assertThat(reshardShards, greaterThan(sourceShards));

        // Set write block (required for resize).
        updateIndexSettings(Settings.builder().put("index.blocks.write", true), sourceIndex);

        int targetShards = reshardShards * 2;
        String targetIndex = "target";

        // Split
        assertAcked(
            executeResize(
                ResizeType.SPLIT,
                sourceIndex,
                targetIndex,
                Settings.builder()
                    .put(IndexMetadata.SETTING_NUMBER_OF_SHARDS, targetShards)
                    .put(IndexMetadata.SETTING_NUMBER_OF_REPLICAS, 1)
                    .putNull("index.blocks.write")
            )
        );
        ensureGreen(targetIndex);

        checkIndicesAfterResize(sourceIndex, targetIndex, reshardShards, targetShards, numDocs);
    }

    private void checkIndicesAfterResize(String sourceIndex, String targetIndex, int sourceShards, int targetShards, int numDocs)
        throws Exception {
        // Verify all initial documents are present in the target index
        assertHitCount(prepareSearch(targetIndex).setSize(0).setQuery(QueryBuilders.matchAllQuery()), numDocs);

        // Write new documents to the target
        int numNewDocs = randomIntBetween(0, 50);
        if (numNewDocs > 0) {
            indexDocsAndRefresh(targetIndex, numNewDocs);
        }
        if (randomBoolean()) {
            flush(targetIndex);
        }

        // Verify all documents are present in the target index
        assertHitCount(prepareSearch(targetIndex).setSize(0).setQuery(QueryBuilders.matchAllQuery()), numDocs + numNewDocs);

        // Verify that each target shard has its own blob
        for (int s = 0; s < targetShards; s++) {
            final ShardId targetShardId = findIndexShard(resolveIndex(targetIndex), s).shardId();
            assertThat(listBlobsTermAndGenerations(targetShardId), hasSize(greaterThan(0)));
        }

        // Source should still have only original docs
        assertHitCount(prepareSearch(sourceIndex).setSize(0).setQuery(QueryBuilders.matchAllQuery()), numDocs);

        // Capture source shard IDs before deleting
        final var sourceShardIds = new ShardId[sourceShards];
        final var sourceIndexObj = resolveIndex(sourceIndex);
        for (int s = 0; s < sourceShards; s++) {
            sourceShardIds[s] = new ShardId(sourceIndexObj, s);
        }

        // Delete the source index
        assertAcked(indicesAdmin().prepareDelete(sourceIndex));

        // Source blobs should eventually be cleaned up
        assertBusy(() -> {
            for (ShardId shardId : sourceShardIds) {
                assertThat("expected blobs for " + shardId + " to be deleted", listBlobsTermAndGenerations(shardId), hasSize(0));
            }
        });

        // Target index is still intact and searchable
        ensureGreen(targetIndex);
        assertHitCount(prepareSearch(targetIndex).setSize(0).setQuery(QueryBuilders.matchAllQuery()), numDocs + numNewDocs);
    }

}
