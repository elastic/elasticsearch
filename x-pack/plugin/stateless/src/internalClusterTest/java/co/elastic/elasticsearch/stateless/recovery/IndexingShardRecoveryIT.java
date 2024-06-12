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

package co.elastic.elasticsearch.stateless.recovery;

import co.elastic.elasticsearch.stateless.AbstractStatelessIntegTestCase;
import co.elastic.elasticsearch.stateless.IndexingDiskController;
import co.elastic.elasticsearch.stateless.commits.BatchedCompoundCommit;
import co.elastic.elasticsearch.stateless.commits.StatelessCommitService;
import co.elastic.elasticsearch.stateless.commits.VirtualBatchedCompoundCommit;
import co.elastic.elasticsearch.stateless.engine.PrimaryTermAndGeneration;
import co.elastic.elasticsearch.stateless.objectstore.ObjectStoreService;

import org.elasticsearch.action.support.WriteRequest.RefreshPolicy;
import org.elasticsearch.common.TriConsumer;
import org.elasticsearch.common.blobstore.OperationPurpose;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.unit.ByteSizeValue;
import org.elasticsearch.common.util.CollectionUtils;
import org.elasticsearch.core.TimeValue;
import org.elasticsearch.index.IndexSettings;
import org.elasticsearch.index.MergePolicyConfig;
import org.elasticsearch.index.shard.ShardId;
import org.elasticsearch.plugins.Plugin;
import org.elasticsearch.test.InternalSettingsPlugin;
import org.elasticsearch.xpack.shutdown.ShutdownPlugin;

import java.util.Collection;
import java.util.List;
import java.util.function.BiConsumer;

import static co.elastic.elasticsearch.stateless.commits.StatelessCompoundCommit.blobNameFromGeneration;
import static org.elasticsearch.cluster.metadata.IndexMetadata.INDEX_ROUTING_EXCLUDE_GROUP_SETTING;
import static org.elasticsearch.cluster.metadata.IndexMetadata.SETTING_NUMBER_OF_SHARDS;
import static org.elasticsearch.test.hamcrest.ElasticsearchAssertions.assertAcked;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.hasItem;
import static org.hamcrest.Matchers.not;
import static org.hamcrest.Matchers.notNullValue;
import static org.hamcrest.Matchers.nullValue;

public class IndexingShardRecoveryIT extends AbstractStatelessIntegTestCase {

    @Override
    protected Collection<Class<? extends Plugin>> nodePlugins() {
        return CollectionUtils.concatLists(List.of(InternalSettingsPlugin.class, ShutdownPlugin.class), super.nodePlugins());
    }

    /**
     * Overrides default settings to prevent uncontrolled commit uploads
     */
    @Override
    protected Settings.Builder nodeSettings() {
        return super.nodeSettings().put(StatelessCommitService.STATELESS_UPLOAD_MAX_SIZE.getKey(), ByteSizeValue.ofGb(1))
            .put(StatelessCommitService.STATELESS_UPLOAD_VBCC_MAX_AGE.getKey(), TimeValue.timeValueDays(1))
            .put(StatelessCommitService.STATELESS_UPLOAD_MONITOR_INTERVAL.getKey(), TimeValue.timeValueDays(1))
            .put(IndexingDiskController.INDEXING_DISK_INTERVAL_TIME_SETTING.getKey(), TimeValue.MINUS_ONE);
    }

    /**
     * Overrides default settings to prevent uncontrolled flushes
     */
    private String createIndex(int shards, int replicas) {
        var indexName = randomIdentifier();
        createIndex(
            indexName,
            indexSettings(shards, replicas).put(IndexSettings.INDEX_TRANSLOG_FLUSH_THRESHOLD_SIZE_SETTING.getKey(), ByteSizeValue.ofGb(1L))
                .put(IndexSettings.INDEX_REFRESH_INTERVAL_SETTING.getKey(), TimeValue.MINUS_ONE)
                .put(MergePolicyConfig.INDEX_MERGE_ENABLED, false)
                .build()
        );
        return indexName;
    }

    public void testEmptyStoreRecovery() throws Exception {
        startMasterAndIndexNode();
        var indexName = createIndex(randomIntBetween(1, 3), 0);

        var initialTermAndGen = new PrimaryTermAndGeneration(1L, 3L);
        assertBusyCommitsMatchExpectedResults(indexName, expectedResults(initialTermAndGen, 0));

        var generated = generateCommits(indexName);
        assertDocsCount(indexName, generated.docs);

        var expected = expectedResults(initialTermAndGen, generated.commits);
        assertBusyCommitsMatchExpectedResults(indexName, expected);
    }

    public void testExistingStoreRecovery() throws Exception {
        startMasterOnlyNode();
        var indexNode = startIndexNode();
        var indexName = createIndex(randomIntBetween(1, 3), 0);

        var recoveredBcc = new PrimaryTermAndGeneration(1L, 3L);
        assertBusyCommitsMatchExpectedResults(indexName, expectedResults(recoveredBcc, 0));

        long totalDocs = 0L;
        assertDocsCount(indexName, totalDocs);

        int iters = randomIntBetween(1, 5);
        for (int i = 0; i < iters; i++) {

            long expectedGenerationAfterRecovery;
            if (randomBoolean()) {
                var generated = generateCommits(indexName);
                logger.info("--> iteration {}/{}: {} docs indexed in {} commits", i, iters, generated.docs, generated.commits);
                assertDocsCount(indexName, totalDocs + generated.docs);

                var expected = expectedResults(recoveredBcc, generated.commits);
                assertBusyCommitsMatchExpectedResults(indexName, expected);
                // we don't flush shards when the node closes, so non-uploaded commits in the virtual bcc will be lost and the shard will
                // recover from the latest one in the object store, will replay translog operations and then flush.
                expectedGenerationAfterRecovery = expected.lastUploadedCc.generation() + 1L;
                totalDocs += generated.docs;
            } else {
                expectedGenerationAfterRecovery = recoveredBcc.generation() + 1L;
            }

            if (randomBoolean()) {
                internalCluster().restartNode(indexNode);
            } else {
                internalCluster().stopNode(indexNode);
                indexNode = startIndexNode();
            }
            ensureGreen(indexName);

            // after recovery, term is incremented twice (+1 when it becomes unassigned and +1 when it is reassigned)
            var expected = new PrimaryTermAndGeneration(recoveredBcc.primaryTerm() + 2L, expectedGenerationAfterRecovery);
            assertBusyCommitsMatchExpectedResults(indexName, expectedResults(expected, 0));
            assertDocsCount(indexName, totalDocs);
            recoveredBcc = expected;
        }
    }

    public void testSnapshotRecovery() throws Exception {
        startMasterOnlyNode();
        startIndexNode();

        var indexName = createIndex(randomIntBetween(1, 3), 0);
        var lastUploaded = new PrimaryTermAndGeneration(1L, 3L);
        assertBusyCommitsMatchExpectedResults(indexName, expectedResults(lastUploaded, 0));

        int totalCommits = 0;
        long totalDocs = 0L;
        assertDocsCount(indexName, totalDocs);

        int iters = randomIntBetween(0, 5);
        for (int i = 0; i < iters; i++) {
            var generated = generateCommits(indexName);
            logger.info("--> iteration {}/{}: {} docs added in {} commits", i, iters, generated.docs, generated.commits);
            assertDocsCount(indexName, totalDocs + generated.docs);
            totalCommits += generated.commits;
            totalDocs += generated.docs;
        }

        var beforeSnapshot = expectedResults(lastUploaded, totalCommits);
        assertBusyCommitsMatchExpectedResults(indexName, beforeSnapshot);

        createRepository(logger, "snapshots", "fs");
        createSnapshot("snapshots", "snapshot", List.of(indexName), List.of());

        var afterSnapshot = beforeSnapshot;
        if (beforeSnapshot.lastVirtualBcc != null) {
            // shard snapshots trigger a flush before snapshotting, so any virtual BCC/CC has been flushed and becomes the latest uploaded
            afterSnapshot = new ExpectedCommits(beforeSnapshot.lastVirtualBcc, beforeSnapshot.lastVirtualCc, null, null);
        }
        assertBusyCommitsMatchExpectedResults(indexName, afterSnapshot);
        assertDocsCount(indexName, totalDocs);

        logger.info("--> deleting index {}", indexName);
        assertAcked(client().admin().indices().prepareDelete(indexName));

        logger.info("--> restoring snapshot of {}", indexName);
        var restore = clusterAdmin().prepareRestoreSnapshot("snapshots", "snapshot").setWaitForCompletion(true).get();
        assertThat(restore.getRestoreInfo().successfulShards(), equalTo(getNumShards(indexName).numPrimaries));
        assertThat(restore.getRestoreInfo().failedShards(), equalTo(0));
        ensureGreen(indexName);

        var afterRestore = new PrimaryTermAndGeneration(
            // term is incremented by 1
            afterSnapshot.lastUploadedBcc.primaryTerm() + 1L,
            // Lucene index is committed twice on snapshot recovery: one for bootstrapNewHistory and one for associateIndexWithNewTranslog
            afterSnapshot.lastUploadedCc.generation() + 2L
        );
        assertBusyCommitsMatchExpectedResults(indexName, expectedResults(afterRestore, 0));
        assertDocsCount(indexName, totalDocs);
    }

    public void testPeerRecovery() throws Exception {
        startMasterOnlyNode();
        var indexNode = startIndexNode();
        var indexName = createIndex(randomIntBetween(1, 3), 0);

        var currentGeneration = new PrimaryTermAndGeneration(1L, 3L);
        assertBusyCommitsMatchExpectedResults(indexName, expectedResults(currentGeneration, 0));

        long totalDocs = 0L;
        assertDocsCount(indexName, totalDocs);

        int iters = randomIntBetween(1, 5);
        for (int i = 0; i < iters; i++) {
            long expectedGenerationAfterRelocation;
            if (randomBoolean()) {
                var generated = generateCommits(indexName);
                logger.info("--> iteration {}/{}: {} docs indexed in {} commits", i, iters, generated.docs, generated.commits);
                assertDocsCount(indexName, totalDocs + generated.docs);

                var expected = expectedResults(currentGeneration, generated.commits);
                assertBusyCommitsMatchExpectedResults(indexName, expected);

                if (expected.lastVirtualBcc != null) {
                    // there is a flush before relocating the shard so last CC of VBCC is uploaded, in addition to
                    // generation increments due to a flush on the target shard after peer-recovery
                    expectedGenerationAfterRelocation = expected.lastVirtualCc.generation() + 1L;
                } else {
                    // generation increments due to a flush on the target shard after peer-recovery
                    expectedGenerationAfterRelocation = expected.lastUploadedCc.generation() + 1L;
                }
                totalDocs += generated.docs;
            } else {
                // generation increments due to a flush on the target shard after peer-recovery
                expectedGenerationAfterRelocation = currentGeneration.generation() + 1L;
            }

            var newIndexNode = startIndexNode();
            logger.info("--> iteration {}/{}: node {} started", i, iters, newIndexNode);

            var excludedNode = indexNode;
            updateIndexSettings(Settings.builder().put(INDEX_ROUTING_EXCLUDE_GROUP_SETTING.getKey() + "_name", excludedNode), indexName);
            assertBusy(() -> assertThat(internalCluster().nodesInclude(indexName), not(hasItem(excludedNode))));
            assertNodeHasNoCurrentRecoveries(newIndexNode);
            internalCluster().stopNode(excludedNode);
            indexNode = newIndexNode;
            ensureGreen(indexName);

            var expected = new PrimaryTermAndGeneration(currentGeneration.primaryTerm(), expectedGenerationAfterRelocation);
            assertBusyCommitsMatchExpectedResults(indexName, expectedResults(expected, 0));
            assertDocsCount(indexName, totalDocs);
            currentGeneration = expected;
        }
    }

    private static void assertBusyCommitsMatchExpectedResults(String indexName, ExpectedCommits expected) throws Exception {
        assertBusyCommits(indexName, (shardId, uploaded, virtual) -> {
            assertThat(uploaded, notNullValue());

            assertThat(uploaded.primaryTermAndGeneration(), equalTo(expected.lastUploadedBcc));
            assertThat(uploaded.last().primaryTermAndGeneration(), equalTo(expected.lastUploadedCc));
            assertBlobExists(shardId, expected.lastUploadedBcc);

            if (expected.lastVirtualBcc == null) {
                assertThat(virtual, nullValue());
            } else {
                assertThat(virtual, notNullValue());
                assertThat(virtual.primaryTermAndGeneration(), equalTo(expected.lastVirtualBcc));
                assertThat(virtual.getLastPendingCompoundCommit(), notNullValue());
                assertThat(virtual.lastCompoundCommit().primaryTermAndGeneration(), equalTo(expected.lastVirtualCc));
            }
        });
    }

    private static void assertBusyCommits(
        String indexName,
        TriConsumer<ShardId, BatchedCompoundCommit, VirtualBatchedCompoundCommit> consumer
    ) throws Exception {
        assertBusy(
            () -> forAllCommitServices(
                indexName,
                (shardId, commitService) -> consumer.apply(
                    shardId,
                    commitService.getLatestUploadedBcc(shardId),
                    commitService.getCurrentVirtualBcc(shardId)
                )
            )
        );
    }

    private static void forAllCommitServices(String indexName, BiConsumer<ShardId, StatelessCommitService> consumer) {
        var clusterState = clusterAdmin().prepareState().setMetadata(true).setNodes(true).setIndices(indexName).get().getState();
        assertThat("Index not found: " + indexName, clusterState.metadata().hasIndex(indexName), equalTo(true));

        var indexMetadata = clusterState.metadata().index(indexName);
        int shards = Integer.parseInt(indexMetadata.getSettings().get(SETTING_NUMBER_OF_SHARDS));
        for (int shard = 0; shard < shards; shard++) {
            var indexShard = findIndexShard(indexMetadata.getIndex(), shard);
            var routingEntry = indexShard.routingEntry();
            assertThat("Not active: " + routingEntry, indexShard.routingEntry().active(), equalTo(true));

            var nodeId = indexShard.routingEntry().currentNodeId();
            assertThat("Node does not exist: " + routingEntry, clusterState.nodes().nodeExists(nodeId), equalTo(true));

            var node = clusterState.getNodes().get(indexShard.routingEntry().currentNodeId());
            var commitService = internalCluster().getInstance(StatelessCommitService.class, node.getName());
            assertThat("Commit service does not exist: " + shard, commitService, notNullValue());
            assertThat(commitService.hasPendingBccUploads(indexShard.shardId()), equalTo(false));
            consumer.accept(indexShard.shardId(), commitService);
        }
    }

    private record DocsAndCommits(long docs, int commits) {}

    private DocsAndCommits generateCommits(String indexName) {
        int uploadMaxCommits = getUploadMaxCommits();
        int numCommits = randomIntBetween(0, Math.min(25, uploadMaxCommits * 3));

        long totalDocs = 0L;
        logger.info("--> generating {} commit(s) for index [{}] with {} upload max. commits", numCommits, indexName, uploadMaxCommits);
        for (int i = 0; i < numCommits; i++) {
            int numDocs = randomIntBetween(50, 100);
            indexDocs(indexName, numDocs, bulkRequest -> bulkRequest.setRefreshPolicy(RefreshPolicy.IMMEDIATE));
            totalDocs += numDocs;
        }
        return new DocsAndCommits(totalDocs, numCommits);
    }

    private record ExpectedCommits(
        PrimaryTermAndGeneration lastUploadedBcc,
        PrimaryTermAndGeneration lastUploadedCc,
        PrimaryTermAndGeneration lastVirtualBcc,
        PrimaryTermAndGeneration lastVirtualCc
    ) {}

    private ExpectedCommits expectedResults(PrimaryTermAndGeneration initial, int numCommits) {
        if (numCommits == 0) {
            return new ExpectedCommits(initial, initial, null, null);
        }
        int uploadMaxCommits = STATELESS_UPLOAD_DELAYED ? getUploadMaxCommits() : 1;
        int uploads = numCommits / uploadMaxCommits;
        if (uploads == 0) {
            return new ExpectedCommits(
                initial,
                initial,
                new PrimaryTermAndGeneration(initial.primaryTerm(), initial.generation() + 1L),
                new PrimaryTermAndGeneration(initial.primaryTerm(), initial.generation() + numCommits)
            );
        }
        int numUploaded = uploads * uploadMaxCommits;
        int numPending = numCommits % uploadMaxCommits;
        return new ExpectedCommits(
            // term/gen of latest uploaded BCC
            new PrimaryTermAndGeneration(initial.primaryTerm(), initial.generation() + (numUploaded - uploadMaxCommits + 1L)),
            // term/gen of last CC in latest uploaded BCC
            new PrimaryTermAndGeneration(initial.primaryTerm(), initial.generation() + numUploaded),
            // term/gen of virtual BCC
            numPending > 0 ? new PrimaryTermAndGeneration(initial.primaryTerm(), initial.generation() + numUploaded + 1L) : null,
            // term/gen of last CC in virtual BCC
            numPending > 0 ? new PrimaryTermAndGeneration(initial.primaryTerm(), initial.generation() + numUploaded + numPending) : null
        );
    }

    private static void assertBlobExists(ShardId shardId, PrimaryTermAndGeneration primaryTermAndGeneration) {
        try {
            var objectStoreService = internalCluster().getCurrentMasterNodeInstance(ObjectStoreService.class);
            var blobContainer = objectStoreService.getBlobContainer(shardId, primaryTermAndGeneration.primaryTerm());
            var blobName = blobNameFromGeneration(primaryTermAndGeneration.generation());
            assertTrue("Blob not found: " + blobContainer.path() + blobName, blobContainer.blobExists(OperationPurpose.INDICES, blobName));
        } catch (Exception e) {
            throw new AssertionError(e);
        }
    }

    private static void assertDocsCount(String indexName, long expectedDocsCount) {
        assertThat(
            client().admin().indices().prepareStats(indexName).setDocs(true).get().getTotal().getDocs().getCount(),
            equalTo(expectedDocsCount)
        );
    }
}
