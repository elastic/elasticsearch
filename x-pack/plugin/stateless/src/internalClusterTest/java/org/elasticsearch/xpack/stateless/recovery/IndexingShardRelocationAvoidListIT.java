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

package org.elasticsearch.xpack.stateless.recovery;

import org.elasticsearch.common.CheckedSupplier;
import org.elasticsearch.common.blobstore.BlobContainer;
import org.elasticsearch.common.blobstore.OperationPurpose;
import org.elasticsearch.common.blobstore.support.BlobMetadata;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.util.concurrent.ConcurrentCollections;
import org.elasticsearch.core.CheckedRunnable;
import org.elasticsearch.core.TimeValue;
import org.elasticsearch.index.IndexSettings;
import org.elasticsearch.plugins.Plugin;
import org.elasticsearch.test.transport.MockTransportService;
import org.elasticsearch.xpack.stateless.AbstractStatelessPluginIntegTestCase;
import org.elasticsearch.xpack.stateless.StatelessMockRepositoryPlugin;
import org.elasticsearch.xpack.stateless.StatelessMockRepositoryStrategy;
import org.elasticsearch.xpack.stateless.commits.BatchedCompoundCommit;
import org.elasticsearch.xpack.stateless.commits.StatelessCommitServiceTestUtils;
import org.elasticsearch.xpack.stateless.engine.IndexEngine;
import org.elasticsearch.xpack.stateless.engine.PrimaryTermAndGeneration;
import org.elasticsearch.xpack.stateless.objectstore.ObjectStoreService;
import org.junit.Assert;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.atomic.AtomicInteger;

import static org.elasticsearch.test.hamcrest.ElasticsearchAssertions.assertNoFailures;
import static org.elasticsearch.xpack.stateless.commits.StatelessCommitService.STATELESS_UPLOAD_MAX_AMOUNT_COMMITS;
import static org.elasticsearch.xpack.stateless.recovery.TransportStatelessPrimaryRelocationAction.PRIMARY_CONTEXT_HANDOFF_ACTION_NAME;
import static org.hamcrest.Matchers.empty;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.hasItem;
import static org.hamcrest.Matchers.instanceOf;
import static org.hamcrest.Matchers.not;

public class IndexingShardRelocationAvoidListIT extends AbstractStatelessPluginIntegTestCase {

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

    @Override
    protected Settings.Builder nodeSettings() {
        return super.nodeSettings()
            // To be able to install a custom repository strategy
            .put(ObjectStoreService.TYPE_SETTING.getKey(), ObjectStoreService.ObjectStoreType.MOCK);
    }

    public void testRelocatingIndexShardSkipsBlobsList() throws Exception {
        startMasterOnlyNode();

        final String indexNodeA = startIndexNode();

        final String indexName = randomIdentifier();
        createIndex(indexName, indexSettings(1, 0).build());
        ensureGreen(indexName);
        int numIndexRound = randomIntBetween(1, 10);
        for (int i = 0; i < numIndexRound; i++) {
            indexDocs(indexName, between(1, 100));
            refresh(indexName);
        }
        final String indexNodeB = startIndexNode();

        // Infrastructure to count blob accesses
        final var containerListCalls = new AtomicInteger(0);
        final var containerChildrenCalls = new AtomicInteger(0);
        setNodeRepositoryStrategy(indexNodeB, new StatelessMockRepositoryStrategy() {
            @Override
            public Map<String, BlobContainer> blobContainerChildren(
                CheckedSupplier<Map<String, BlobContainer>, IOException> originalSupplier,
                OperationPurpose purpose
            ) throws IOException {
                containerChildrenCalls.incrementAndGet();
                return super.blobContainerChildren(originalSupplier, purpose);
            }

            @Override
            public Map<String, BlobMetadata> blobContainerListBlobs(
                CheckedSupplier<Map<String, BlobMetadata>, IOException> originalSupplier,
                OperationPurpose purpose
            ) throws IOException {
                containerListCalls.incrementAndGet();
                return super.blobContainerListBlobs(originalSupplier, purpose);
            }
        });

        updateIndexSettings(Settings.builder().put("index.routing.allocation.exclude._name", indexNodeA), indexName);
        ensureGreen(indexName);
        // We expect to see 1 call to each from pre-warming. And no calls from preRecovery.
        assertEquals(1, containerChildrenCalls.get());
        assertEquals(1, containerListCalls.get());
    }

    public void testRelocatingIndexShardReceivesAllBlobs() throws Exception {
        var settings = Settings.builder().put(STATELESS_UPLOAD_MAX_AMOUNT_COMMITS.getKey(), 2).build();
        startMasterOnlyNode(settings);
        final String indexNodeA = startIndexNode(settings);

        final String indexName = randomIdentifier();
        createIndex(indexName, indexSettings(1, 0).put(IndexSettings.INDEX_REFRESH_INTERVAL_SETTING.getKey(), TimeValue.MINUS_ONE).build());
        ensureGreen(indexName);

        // Infrastructure to stop shard blobs deletions on node A
        final CountDownLatch blockDeleteLatch = new CountDownLatch(1);
        final CountDownLatch batchDeleteBlocked = new CountDownLatch(1);
        setNodeRepositoryStrategy(indexNodeA, new StatelessMockRepositoryStrategy() {
            @Override
            public void blobStoreDeleteBlobsIgnoringIfNotExists(
                CheckedRunnable<IOException> originalRunnable,
                OperationPurpose purpose,
                Iterator<String> blobNames
            ) throws IOException {
                // Only capture the BCC deletions, not the translog file deletions
                if (purpose == OperationPurpose.INDICES) {
                    batchDeleteBlocked.countDown();
                    safeAwait(blockDeleteLatch, TimeValue.timeValueSeconds(30));
                }
                originalRunnable.run();
            }
        });

        var shard = findIndexShard(resolveIndex(indexName), 0);
        assertNotNull(shard);
        var engine = shard.getEngineOrNull();
        assertThat(engine, instanceOf(IndexEngine.class));
        var statelessCommitService = ((IndexEngine) engine).getStatelessCommitService();
        assertNotNull(statelessCommitService);
        var commitCleanerA = StatelessCommitServiceTestUtils.getStatelessCommitCleaner(statelessCommitService);

        final Set<PrimaryTermAndGeneration> uploadedFiles = ConcurrentCollections.newConcurrentSet();
        statelessCommitService.addConsumerForNewUploadedBcc(
            shard.shardId(),
            info -> uploadedFiles.add(info.uploadedBcc().primaryTermAndGeneration())
        );
        var firstBcc = statelessCommitService.getLatestUploadedBcc(shard.shardId());
        assertNotNull(firstBcc);
        uploadedFiles.add(firstBcc.primaryTermAndGeneration());

        int numIndexRound = randomIntBetween(10, 20);
        for (int i = 0; i < numIndexRound; i++) {
            indexDocs(indexName, between(1, 100));
            flush(indexName);
            if (rarely()) {
                assertNoFailures(indicesAdmin().prepareForceMerge(indexName).setMaxNumSegments(1).get());
            }
        }
        // force merge to 1 segment to make previous BCCs eligible for deletion
        assertNoFailures(indicesAdmin().prepareForceMerge(indexName).setMaxNumSegments(1).get());

        // Speed up the file deletion consistency check (it's shared with the translog consistency checks)
        indexDocs(indexName, 1);
        ensureGreen(indexName);

        final String indexNodeB = startIndexNode(settings);
        Set<String> blobsDeletedOnTarget = ConcurrentCollections.newConcurrentSet();
        setNodeRepositoryStrategy(indexNodeB, new StatelessMockRepositoryStrategy() {
            @Override
            public void blobStoreDeleteBlobsIgnoringIfNotExists(
                CheckedRunnable<IOException> originalRunnable,
                OperationPurpose purpose,
                Iterator<String> blobNames
            ) throws IOException {
                // Allow actual deletion on node B
                if (purpose == OperationPurpose.INDICES) {
                    blobNames.forEachRemaining(blobsDeletedOnTarget::add);
                }
                originalRunnable.run();
            }
        });

        // Capture the blobs passed during relocation in the primary context handoff message.
        final Set<PrimaryTermAndGeneration> passedBlobFiles = ConcurrentCollections.newConcurrentSet();
        MockTransportService.getInstance(indexNodeB)
            .addRequestHandlingBehavior(PRIMARY_CONTEXT_HANDOFF_ACTION_NAME, (handler, request, channel, task) -> {
                var handoffRequest = (TransportStatelessPrimaryRelocationAction.PrimaryContextHandoffRequest) request;
                var latestBccBlob = handoffRequest.latestBccBlob();
                Assert.assertNotNull(latestBccBlob);
                passedBlobFiles.add(latestBccBlob.blobFile().termAndGeneration());
                for (var blob : handoffRequest.otherBlobFiles()) {
                    passedBlobFiles.add(blob.termAndGeneration());
                }
                handler.messageReceived(request, channel, task);
            });

        // Wait for at least one blob deletion to be requested on source node.
        safeAwait(batchDeleteBlocked);

        assertThat(commitCleanerA.getPendingDeletes(shard.shardId()), not(empty()));

        // Trigger relocation
        updateIndexSettings(Settings.builder().put("index.routing.allocation.exclude._name", indexNodeA), indexName);
        ensureGreen(indexName);
        assertBusy(() -> assertThat(internalCluster().nodesInclude(indexName), not(hasItem(indexNodeA))));
        // Assert we passed all the uploaded blobs during relocation since we blocked their deletion on source.
        assertThat(passedBlobFiles, equalTo(uploadedFiles));

        // Extra activity to trigger deletions on target node after relocation.
        indexDocs(indexName, 10);
        assertNoFailures(indicesAdmin().prepareForceMerge(indexName).setMaxNumSegments(1).get());
        indexDocs(indexName, 1);

        // Non-empty as we blocked the deletions. So we still have some pending deletions.
        assertThat(commitCleanerA.getPendingDeletes(shard.shardId()), not(empty()));
        // Also unblock deletions on source node
        blockDeleteLatch.countDown();

        // Assert that we actually delete all the blobs uploaded from source after the force merge on target.
        assertBusy(() -> { assertThat(blobsDeletedOnTarget.size(), equalTo(uploadedFiles.size() + 1)); });
        // check that exact names of uploaded blobs were deleted
        List<String> uploadedFileNames = uploadedFiles.stream()
            .map(termAndGen -> BatchedCompoundCommit.blobNameFromGeneration(termAndGen.generation()))
            .toList();
        assertTrue(uploadedFileNames.stream().allMatch(u -> blobsDeletedOnTarget.stream().anyMatch(d -> d.endsWith(u))));

        // Assert we clear the tracking of pending deletes on source node after we close the shard
        // and the previously pending deletes are processed.
        assertBusy(() -> assertThat(commitCleanerA.getPendingDeletes(shard.shardId()), empty()));

        // Relocate back to node A
        updateIndexSettings(Settings.builder().put("index.routing.allocation.exclude._name", indexNodeB), indexName);
        ensureGreen(indexName);
    }
}
