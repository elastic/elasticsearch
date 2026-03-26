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

import org.elasticsearch.action.ActionListener;
import org.elasticsearch.action.support.ChannelActionListener;
import org.elasticsearch.client.internal.Client;
import org.elasticsearch.cluster.service.ClusterService;
import org.elasticsearch.common.CheckedSupplier;
import org.elasticsearch.common.blobstore.BlobContainer;
import org.elasticsearch.common.blobstore.OperationPurpose;
import org.elasticsearch.common.blobstore.support.BlobMetadata;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.util.concurrent.ConcurrentCollections;
import org.elasticsearch.core.CheckedRunnable;
import org.elasticsearch.core.TimeValue;
import org.elasticsearch.index.IndexSettings;
import org.elasticsearch.index.shard.IndexShardNotRecoveringException;
import org.elasticsearch.index.shard.ShardId;
import org.elasticsearch.indices.IndicesService;
import org.elasticsearch.node.PluginComponentBinding;
import org.elasticsearch.plugins.Plugin;
import org.elasticsearch.telemetry.TelemetryProvider;
import org.elasticsearch.test.transport.MockTransportService;
import org.elasticsearch.transport.TestTransportChannel;
import org.elasticsearch.xpack.stateless.AbstractStatelessPluginIntegTestCase;
import org.elasticsearch.xpack.stateless.StatelessMockRepositoryPlugin;
import org.elasticsearch.xpack.stateless.StatelessMockRepositoryStrategy;
import org.elasticsearch.xpack.stateless.TestUtils;
import org.elasticsearch.xpack.stateless.cache.SharedBlobCacheWarmingService;
import org.elasticsearch.xpack.stateless.cache.StatelessSharedBlobCacheService;
import org.elasticsearch.xpack.stateless.commits.BatchedCompoundCommit;
import org.elasticsearch.xpack.stateless.commits.BlobFile;
import org.elasticsearch.xpack.stateless.commits.HollowShardsService;
import org.elasticsearch.xpack.stateless.commits.StatelessCommitCleaner;
import org.elasticsearch.xpack.stateless.commits.StatelessCommitService;
import org.elasticsearch.xpack.stateless.commits.StatelessCommitServiceTestUtils;
import org.elasticsearch.xpack.stateless.engine.IndexEngine;
import org.elasticsearch.xpack.stateless.engine.PrimaryTermAndGeneration;
import org.elasticsearch.xpack.stateless.lucene.BlobStoreCacheDirectory;
import org.elasticsearch.xpack.stateless.objectstore.ObjectStoreService;
import org.junit.Assert;

import java.io.IOException;
import java.io.InputStream;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;

import static org.elasticsearch.test.hamcrest.ElasticsearchAssertions.assertNoFailures;
import static org.elasticsearch.xpack.stateless.commits.HollowShardsService.STATELESS_HOLLOW_INDEX_SHARDS_ENABLED;
import static org.elasticsearch.xpack.stateless.commits.StatelessCommitService.STATELESS_UPLOAD_MAX_AMOUNT_COMMITS;
import static org.elasticsearch.xpack.stateless.lucene.BlobStoreCacheDirectoryTestUtils.getCacheService;
import static org.elasticsearch.xpack.stateless.recovery.TransportStatelessPrimaryRelocationAction.PREWARM_RELOCATION_ACTION_NAME;
import static org.elasticsearch.xpack.stateless.recovery.TransportStatelessPrimaryRelocationAction.PRIMARY_CONTEXT_HANDOFF_ACTION_NAME;
import static org.hamcrest.Matchers.empty;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.greaterThan;
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
        plugins.remove(TestUtils.StatelessPluginWithTrialLicense.class);
        plugins.add(AvoidListTestStatelessPlugin.class);
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
        // We expect to see no calls to list blobs or children.
        assertEquals(0, containerChildrenCalls.get());
        assertEquals(0, containerListCalls.get());
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

    public void testRelocatingIndexShardReceivesDeletedBlobForPrewarming() throws Exception {
        // Disable hollow shards as we don't prewarm cache during relocation for to-be-hollowed shards
        var settings = Settings.builder()
            .put(STATELESS_UPLOAD_MAX_AMOUNT_COMMITS.getKey(), 2)
            .put(STATELESS_HOLLOW_INDEX_SHARDS_ENABLED.getKey(), false)
            .build();

        startMasterOnlyNode(settings);
        final String indexNodeA = startIndexNode(settings);

        Set<String> blobsDeleted = ConcurrentCollections.newConcurrentSet();
        setNodeRepositoryStrategy(indexNodeA, new StatelessMockRepositoryStrategy() {
            @Override
            public void blobStoreDeleteBlobsIgnoringIfNotExists(
                CheckedRunnable<IOException> originalRunnable,
                OperationPurpose purpose,
                Iterator<String> blobNames
            ) throws IOException {
                blobNames.forEachRemaining(blobsDeleted::add);
                originalRunnable.run();
            }
        });

        final String indexName = randomIdentifier();
        createIndex(indexName, indexSettings(1, 0).put(IndexSettings.INDEX_REFRESH_INTERVAL_SETTING.getKey(), TimeValue.MINUS_ONE).build());
        ensureGreen(indexName);
        var shard = findIndexShard(resolveIndex(indexName), 0);
        assertNotNull(shard);

        final String indexNodeB = startIndexNode(settings);

        // Pause pre-warming on target until we are done with relocation
        CountDownLatch prewarmBlockLatch = new CountDownLatch(1);
        // Capture the blob passed during pre-warming in relocation
        AtomicReference<TransportStatelessPrimaryRelocationAction.BlobFileWithLength> prewarmBccBlobRef = new AtomicReference<>();
        CountDownLatch prewarmMessageFailedLatch = new CountDownLatch(1);
        MockTransportService.getInstance(indexNodeB)
            .addRequestHandlingBehavior(PREWARM_RELOCATION_ACTION_NAME, (handler, request, channel, task) -> {
                var handoffRequest = (TransportStatelessPrimaryRelocationAction.PrewarmRelocationRequest) request;
                var latestBccBlob = handoffRequest.latestBccBlob();
                Assert.assertNotNull(latestBccBlob);
                prewarmBccBlobRef.set(latestBccBlob);

                safeAwait(prewarmBlockLatch);
                // We expect the pre-warming to fail since we will delete the blob before unblocking
                handler.messageReceived(request, new TestTransportChannel(new ChannelActionListener<>(channel).delegateResponse((l, e) -> {
                    assertThat(e, instanceOf(IndexShardNotRecoveringException.class));
                    prewarmMessageFailedLatch.countDown();
                    l.onFailure(e);
                })), task);
            });
        // Capture the blob store operations on target
        final var bccAccesses = new AtomicInteger(0);
        setNodeRepositoryStrategy(indexNodeB, new StatelessMockRepositoryStrategy() {
            @Override
            public void blobStoreDeleteBlobsIgnoringIfNotExists(
                CheckedRunnable<IOException> originalRunnable,
                OperationPurpose purpose,
                Iterator<String> blobNames
            ) throws IOException {
                blobNames.forEachRemaining(blobsDeleted::add);
                originalRunnable.run();
            }

            @Override
            public InputStream blobContainerReadBlob(
                CheckedSupplier<InputStream, IOException> originalSupplier,
                OperationPurpose purpose,
                String blobName,
                long position,
                long length
            ) throws IOException {
                if (purpose == OperationPurpose.INDICES) {
                    bccAccesses.incrementAndGet();
                }
                return super.blobContainerReadBlob(originalSupplier, purpose, blobName, position, length);
            }
        });

        // Index some docs
        indexDocs(indexName, between(1, 100));
        ensureGreen(indexName);
        // Trigger relocation
        updateIndexSettings(Settings.builder().put("index.routing.allocation.exclude._name", indexNodeA), indexName);
        ensureGreen(indexName);
        assertBusy(() -> assertThat(internalCluster().nodesInclude(indexName), not(hasItem(indexNodeA))));

        // Extra activity to trigger deletions on target node after relocation.
        indexDocs(indexName, 10);
        assertNoFailures(indicesAdmin().prepareForceMerge(indexName).setMaxNumSegments(1).get());
        indexDocs(indexName, 1);
        // Confirm that the blob passed in pre-warming message was deleted from the blob store
        assertBusy(() -> {
            var prewarmBccBlob = prewarmBccBlobRef.get();
            assertThat(prewarmBccBlob, not(equalTo(null)));
            var bccBlobName = BatchedCompoundCommit.blobNameFromGeneration(prewarmBccBlob.blobFile().generation());
            assertTrue(blobsDeleted.stream().anyMatch(d -> d.endsWith(bccBlobName)));
        });

        assertThat(bccAccesses.get(), greaterThan(0));
        bccAccesses.set(0);

        // Evict the cache on target to force evict the deleted blob
        final var indexNodeBCacheService = getCacheService(
            BlobStoreCacheDirectory.unwrapDirectory(findIndexShard(indexName).store().directory())
        );
        indexNodeBCacheService.forceEvict((key) -> true);

        // Unblock pre-warming message that now includes a path to the deleted blob
        prewarmBlockLatch.countDown();
        safeAwait(prewarmMessageFailedLatch);
        assertEquals(0, bccAccesses.get());

        ensureGreen(indexName);
    }

    public void testRelocatingShardExcludesNonUploadedBlobsFromHandoff() throws Exception {
        var settings = Settings.builder()
            .put(STATELESS_UPLOAD_MAX_AMOUNT_COMMITS.getKey(), 1)
            .put(disableIndexingDiskAndMemoryControllersNodeSettings())
            // Disable hollow shards to let force merge to proceed
            .put(HollowShardsService.STATELESS_HOLLOW_INDEX_SHARDS_ENABLED.getKey(), false)
            .build();
        startMasterOnlyNode(settings);
        final String indexNodeA = startIndexNode(settings);

        final String indexName = randomIdentifier();
        createIndex(indexName, indexSettings(1, 0).put(IndexSettings.INDEX_REFRESH_INTERVAL_SETTING.getKey(), TimeValue.MINUS_ONE).build());
        ensureGreen(indexName);

        for (int i = 0; i < 5; i++) {
            indexDocs(indexName, between(100, 300));
            flush(indexName);
        }

        final var sourceShard = findIndexShard(resolveIndex(indexName), 0);
        assertNotNull(sourceShard);
        assertThat(sourceShard.getEngineOrNull(), instanceOf(IndexEngine.class));
        final var sourceCommitService = ((IndexEngine) sourceShard.getEngineOrNull()).getStatelessCommitService();

        final String indexNodeB = startIndexNode(settings);

        // Capture the blob generations passed in the handoff message on the target node
        final Set<Long> handoffBlobGenerations = ConcurrentCollections.newConcurrentSet();
        MockTransportService.getInstance(indexNodeB)
            .addRequestHandlingBehavior(PRIMARY_CONTEXT_HANDOFF_ACTION_NAME, (handler, request, channel, task) -> {
                var handoffRequest = (TransportStatelessPrimaryRelocationAction.PrimaryContextHandoffRequest) request;
                var latestBccBlob = handoffRequest.latestBccBlob();
                if (latestBccBlob != null) {
                    handoffBlobGenerations.add(latestBccBlob.blobFile().termAndGeneration().generation());
                }
                for (var blob : handoffRequest.otherBlobFiles()) {
                    handoffBlobGenerations.add(blob.termAndGeneration().generation());
                }
                handler.messageReceived(request, channel, task);
            });

        // Set up latches on the source node's plugin to pause inside getTrackedUploadedBlobFilesUpTo.
        // At that point markRelocating() has already set state=RELOCATING and maxGenerationToUpload,
        // so any new commit from a force merge will create a phantom entry.
        final var plugin = findPlugin(indexNodeA, AvoidListTestStatelessPlugin.class);
        final CountDownLatch enteredLatch = new CountDownLatch(1);
        final CountDownLatch blockerLatch = new CountDownLatch(1);
        plugin.getTrackedBlobFilesEnteredLatch.set(enteredLatch);
        plugin.getTrackedBlobFilesBlockerLatch.set(blockerLatch);

        // Trigger relocation
        updateIndexSettings(Settings.builder().put("index.routing.allocation.exclude._name", indexNodeA), indexName);

        safeAwait(enteredLatch, TimeValue.timeValueSeconds(30));

        final long preMergeMaxFlushGen = sourceCommitService.getMaxGenerationToUploadForFlush(sourceShard.shardId());

        // Fire force merge asynchronously. The merge itself plus onCommitCreation (which adds the
        // phantom BlobReference to primaryTermAndGenToBlobReference) will complete, but the
        // subsequent flush's waitForCommitDurability will block forever because pauseUpload prevents
        // the upload of the new generation. We don't wait for the client response.
        client(indexNodeA).admin().indices().prepareForceMerge(indexName).setMaxNumSegments(1).execute(ActionListener.noop());

        // Poll until the force merge's flush has advanced maxGenerationToUploadForFlush.
        // getMaxGenerationToUploadForFlush advances after onCommitCreation (which adds the phantom
        // entry to primaryTermAndGenToBlobReference) but before waitForCommitDurability (which blocks
        // because pauseUpload prevents the upload of generations > maxGenerationToUpload).
        assertBusy(() -> {
            assertThat(sourceCommitService.getMaxGenerationToUploadForFlush(sourceShard.shardId()), greaterThan(preMergeMaxFlushGen));
        });

        final long phantomGeneration = sourceCommitService.getMaxGenerationToUploadForFlush(sourceShard.shardId());

        blockerLatch.countDown();

        ensureGreen(indexName);

        plugin.getTrackedBlobFilesEnteredLatch.set(null);
        plugin.getTrackedBlobFilesBlockerLatch.set(null);

        assertThat(
            "Handoff blob list should not contain the non-uploaded phantom generation",
            handoffBlobGenerations,
            not(hasItem(phantomGeneration))
        );

        indexDocs(indexName, between(1, 50));
        flush(indexName);
        assertNoFailures(indicesAdmin().prepareForceMerge(indexName).setMaxNumSegments(1).get());
        indexDocs(indexName, 1);
        ensureGreen(indexName);
    }

    public static class AvoidListTestStatelessPlugin extends TestUtils.StatelessPluginWithTrialLicense {

        public final AtomicReference<CountDownLatch> getTrackedBlobFilesEnteredLatch = new AtomicReference<>();
        public final AtomicReference<CountDownLatch> getTrackedBlobFilesBlockerLatch = new AtomicReference<>();

        public AvoidListTestStatelessPlugin(Settings settings) {
            super(settings);
        }

        @Override
        public Collection<Object> createComponents(PluginServices services) {
            final Collection<Object> components = super.createComponents(services);
            components.add(
                new PluginComponentBinding<>(
                    StatelessCommitService.class,
                    components.stream().filter(c -> c instanceof StatelessCommitService).findFirst().orElseThrow()
                )
            );
            return components;
        }

        @Override
        protected StatelessCommitService createStatelessCommitService(
            Settings settings,
            ObjectStoreService objectStoreService,
            ClusterService clusterService,
            IndicesService indicesService,
            Client client,
            StatelessCommitCleaner commitCleaner,
            StatelessSharedBlobCacheService cacheService,
            SharedBlobCacheWarmingService cacheWarmingService,
            TelemetryProvider telemetryProvider
        ) {
            return new StatelessCommitService(
                settings,
                objectStoreService,
                clusterService,
                indicesService,
                client,
                commitCleaner,
                cacheService,
                cacheWarmingService,
                telemetryProvider
            ) {
                @Override
                public Set<BlobFile> getTrackedUploadedBlobFilesUpTo(ShardId shardId, long maxUploadedGeneration) {
                    CountDownLatch entered = getTrackedBlobFilesEnteredLatch.get();
                    CountDownLatch blocker = getTrackedBlobFilesBlockerLatch.get();
                    if (entered != null && blocker != null) {
                        entered.countDown();
                        safeAwait(blocker);
                    }
                    return super.getTrackedUploadedBlobFilesUpTo(shardId, maxUploadedGeneration);
                }
            };
        }
    }
}
