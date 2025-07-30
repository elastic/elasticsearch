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

import co.elastic.elasticsearch.stateless.cluster.coordination.StatelessClusterConsistencyService;
import co.elastic.elasticsearch.stateless.commits.BatchedCompoundCommit;
import co.elastic.elasticsearch.stateless.commits.HollowShardsService;
import co.elastic.elasticsearch.stateless.commits.StatelessCommitService;
import co.elastic.elasticsearch.stateless.engine.HollowIndexEngine;
import co.elastic.elasticsearch.stateless.engine.IndexEngine;
import co.elastic.elasticsearch.stateless.engine.IndexEngineTestUtils;
import co.elastic.elasticsearch.stateless.lucene.IndexDirectory;
import co.elastic.elasticsearch.stateless.objectstore.ObjectStoreService;

import org.apache.lucene.index.LeafReaderContext;
import org.apache.lucene.search.Collector;
import org.apache.lucene.search.LeafCollector;
import org.apache.lucene.search.MatchAllDocsQuery;
import org.apache.lucene.search.Scorable;
import org.apache.lucene.search.ScoreMode;
import org.elasticsearch.ElasticsearchException;
import org.elasticsearch.action.delete.DeleteRequest;
import org.elasticsearch.action.index.IndexRequest;
import org.elasticsearch.action.support.WriteRequest;
import org.elasticsearch.blobcache.shared.SharedBlobCacheService;
import org.elasticsearch.common.blobstore.OperationPurpose;
import org.elasticsearch.common.io.Streams;
import org.elasticsearch.common.lucene.store.InputStreamIndexInput;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.unit.ByteSizeValue;
import org.elasticsearch.core.CheckedRunnable;
import org.elasticsearch.core.IOUtils;
import org.elasticsearch.core.TimeValue;
import org.elasticsearch.index.IndexSettings;
import org.elasticsearch.index.engine.Engine;
import org.elasticsearch.index.mapper.SourceFieldMapper;
import org.elasticsearch.index.shard.IndexShard;
import org.elasticsearch.indices.IndicesRequestCache;
import org.elasticsearch.plugins.Plugin;
import org.elasticsearch.test.transport.MockTransportService;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Iterator;
import java.util.List;
import java.util.Set;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.stream.IntStream;

import static co.elastic.elasticsearch.stateless.commits.HollowShardsService.SETTING_HOLLOW_INGESTION_TTL;
import static co.elastic.elasticsearch.stateless.commits.HollowShardsService.STATELESS_HOLLOW_INDEX_SHARDS_ENABLED;
import static co.elastic.elasticsearch.stateless.recovery.TransportStatelessPrimaryRelocationAction.PRIMARY_CONTEXT_HANDOFF_ACTION_NAME;
import static org.elasticsearch.cluster.routing.allocation.decider.MaxRetryAllocationDecider.SETTING_ALLOCATION_MAX_RETRY;
import static org.elasticsearch.test.hamcrest.ElasticsearchAssertions.assertNoFailures;
import static org.hamcrest.CoreMatchers.equalTo;
import static org.hamcrest.CoreMatchers.hasItem;
import static org.hamcrest.CoreMatchers.sameInstance;
import static org.hamcrest.Matchers.not;

public class StatelessFileDeletionHollowIT extends AbstractStatelessIntegTestCase {

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
        return super.nodeSettings().put(STATELESS_HOLLOW_INDEX_SHARDS_ENABLED.getKey(), true)
            // To ensure tests are not reading file from cache
            .put(SharedBlobCacheService.SHARED_CACHE_SIZE_SETTING.getKey(), ByteSizeValue.ZERO)
            .put(StatelessCommitService.STATELESS_UPLOAD_MAX_SIZE.getKey(), ByteSizeValue.ofGb(1L))
            .put(StatelessCommitService.STATELESS_UPLOAD_VBCC_MAX_AGE.getKey(), TimeValue.timeValueDays(1L))
            // To speed up blobs deletion in the object store
            .put(StatelessClusterConsistencyService.DELAYED_CLUSTER_CONSISTENCY_INTERVAL_SETTING.getKey(), TimeValue.timeValueMillis(100L))
            // To avoid unwanted flushes that produce commits
            .put(disableIndexingDiskAndMemoryControllersNodeSettings())
            // To ensure searches are not served from request cache
            .put(IndicesRequestCache.INDICES_CACHE_QUERY_SIZE.getKey(), ByteSizeValue.ZERO)
            // To be able to install a custom repository strategy
            .put(ObjectStoreService.TYPE_SETTING.getKey(), ObjectStoreService.ObjectStoreType.MOCK);
    }

    @Override
    public int getUploadMaxCommits() {
        // To have 1 BCC per refresh/flush, it's easier to reason about for this test suite
        return 1;
    }

    public void testSnapshotCommitAcquiredOnIndexShardRetainedAfterHollow() throws Exception {
        startMasterOnlyNode();

        final var indexNodeSettings = Settings.builder().put(SETTING_HOLLOW_INGESTION_TTL.getKey(), TimeValue.timeValueMillis(1)).build();
        final var indexNodeA = startIndexNode(indexNodeSettings);
        final var hollowShardsServiceA = internalCluster().getInstance(HollowShardsService.class, indexNodeA);

        final var indexName = randomIdentifier();
        createIndex(
            indexName,
            indexSettings(1, 0).put(SETTING_ALLOCATION_MAX_RETRY.getKey(), 1)
                .put(IndexSettings.INDEX_REFRESH_INTERVAL_SETTING.getKey(), TimeValue.MINUS_ONE)
                .build()
        );

        final var indexShard = findIndexShard(indexName);
        final long primaryTerm = indexShard.getOperationPrimaryTerm();

        // segments_3 (generation: 3):
        // segments_3
        logLastCommittedSegmentInfos(indexShard);

        // Create first commit with segment _0
        indexDocsAndRefresh(indexName, IntStream.range(0, 10));

        // segments_4 (generation: 4):
        // _0.cfe
        // _0.cfs
        // _0.si
        // segments_4
        logLastCommittedSegmentInfos(indexShard);

        // create second commit with segment _0 and _1
        indexDocsAndRefresh(indexName, IntStream.range(10, 20));

        // segments_5 (generation: 5):
        // _0.cfe
        // _0.cfs
        // _0.si
        // _1.cfe
        // _1.cfs
        // _1.si
        // segments_5
        logLastCommittedSegmentInfos(indexShard);

        // Acquire commit for snapshot
        var snapshot = indexShard.acquireIndexCommitForSnapshot();
        assertThat(snapshot.getIndexCommit().getGeneration(), equalTo(5L));
        assertThat(snapshot.getIndexCommit().getSegmentCount(), equalTo(2));

        // Force-merge to 1 segment
        forceMerge(true);

        // segments_6 (generation: 6):
        // _2.cfe
        // _2.cfs
        // _2.si
        // segments_6
        logLastCommittedSegmentInfos(indexShard);

        // Start hollowing shard
        final var indexNodeB = startIndexNode(indexNodeSettings);
        assertBusy(() -> assertThat(hollowShardsServiceA.isHollowableIndexShard(indexShard), equalTo(true)));

        var isAllowedToDeleteSnapshotBackingBCCs = installSnapshotBackingBCCDeletionWatcher(indexNodeA, snapshot);
        isAllowedToDeleteSnapshotBackingBCCs.set(false);

        var indexEngine = asInstanceOf(IndexEngine.class, indexShard.getEngineOrNull());

        final var handOffStarted = new CountDownLatch(1);
        final var failHandOff = new CountDownLatch(1);
        MockTransportService.getInstance(indexNodeB)
            .addRequestHandlingBehavior(PRIMARY_CONTEXT_HANDOFF_ACTION_NAME, (handler, request, channel, task) -> {
                handOffStarted.countDown();
                logger.debug("--> primary context handoff request received, now waiting for signal to send failure");
                safeAwait(failHandOff);
                channel.sendResponse(new ElasticsearchException("Test fails primary context handoff on purpose"));
            });

        // Attempt to relocate index as hollow from node A to node B
        updateIndexSettings(Settings.builder().put("index.routing.allocation.exclude._name", indexNodeA), indexName);
        safeAwait(handOffStarted);

        // Wait for index engine to be closed due to hollowing/reset
        IndexEngineTestUtils.awaitClose(indexEngine);

        // Shard is hollow now
        var hollowEngine = asInstanceOf(HollowIndexEngine.class, indexShard.getEngineOrNull());

        // Test that snapshot files can still be fully read after the engine is hollow
        ensureSnapshotCanReadFiles(snapshot);

        // Fail relocation
        failHandOff.countDown();
        ensureGreen(indexName);

        // Trigger force-merge to unhollow shard
        var forceMergeFuture = client().admin().indices().prepareForceMerge(indexName).execute();

        // Wait for hollow index engine to be closed
        IndexEngineTestUtils.awaitClose(hollowEngine);
        safeGet(forceMergeFuture);

        // Shard is unhollow now
        var unhollowIndexEngine = asInstanceOf(IndexEngine.class, indexShard.getEngineOrNull());

        // segments_8 (generation: 8):
        // _2.cfe
        // _2.cfs
        // _2.si
        // segments_8
        logLastCommittedSegmentInfos(indexShard);

        // Ensure that the source index shard didn't fail during the relocation
        final var unhollowShard = findIndexShard(indexName);
        assertThat(unhollowShard, sameInstance(indexShard));
        assertThat(unhollowShard.getOperationPrimaryTerm(), equalTo(primaryTerm));

        // The commit acquired on the IndexEngine for snapshotting has been retained during hollowing
        var blobName = BatchedCompoundCommit.blobNameFromGeneration(snapshot.getIndexCommit().getGeneration());
        var blobs = getObjectStoreService(indexNodeA).getProjectBlobContainer(unhollowShard.shardId(), primaryTerm)
            .listBlobs(OperationPurpose.INDICES)
            .keySet();
        assertThat("BCC for generation 5 exists in object store", blobs.contains(blobName), equalTo(true));

        // Test that snapshot files can still be fully read after the engine is hollow
        ensureSnapshotCanReadFiles(snapshot);

        isAllowedToDeleteSnapshotBackingBCCs.set(true);
        // Release snapshot commit
        IOUtils.close(snapshot);

        // Create a new commit to revisit the index deletion policy and deletes the fully released commit
        flushAndRefresh(indexName);

        // Wait for BCC to be deleted from the object store
        assertBusy(() -> {
            var remainingBlobs = getObjectStoreService(indexNodeA).getProjectBlobContainer(unhollowShard.shardId(), primaryTerm)
                .listBlobs(OperationPurpose.INDICES)
                .keySet();
            assertThat("BCC for generation 5 is deleted from object store", remainingBlobs.contains(blobName), equalTo(false));
        });
    }

    public void testSnapshotCommitAcquiredOnHollowShardRetainedAfterUnhollow() throws Exception {
        startMasterOnlyNode();

        final var indexNodeSettings = Settings.builder().put(SETTING_HOLLOW_INGESTION_TTL.getKey(), TimeValue.timeValueMillis(1)).build();
        final var indexNodeA = startIndexNode(indexNodeSettings);
        final var hollowShardsServiceA = internalCluster().getInstance(HollowShardsService.class, indexNodeA);

        final var indexName = randomIdentifier();
        createIndex(indexName, indexSettings(1, 0).put(IndexSettings.INDEX_REFRESH_INTERVAL_SETTING.getKey(), TimeValue.MINUS_ONE).build());

        final var indexShard = findIndexShard(indexName);
        final long primaryTerm = indexShard.getOperationPrimaryTerm();

        // segments_3 (generation: 3):
        // segments_3
        logLastCommittedSegmentInfos(indexShard);

        // Create first commit with segment _0
        indexDocsAndRefresh(indexName, IntStream.range(0, 10));

        // segments_4 (generation: 4):
        // _0.cfe
        // _0.cfs
        // _0.si
        // segments_4
        logLastCommittedSegmentInfos(indexShard);

        // create second commit with segment _0 and _1
        indexDocsAndRefresh(indexName, IntStream.range(10, 20));

        // segments_5 (generation: 5):
        // _0.cfe
        // _0.cfs
        // _0.si
        // _1.cfe
        // _1.cfs
        // _1.si
        // segments_5
        logLastCommittedSegmentInfos(indexShard);

        // Start hollowing shard
        final var indexNodeB = startIndexNode(indexNodeSettings);
        assertBusy(() -> assertThat(hollowShardsServiceA.isHollowableIndexShard(indexShard), equalTo(true)));

        var indexEngine = asInstanceOf(IndexEngine.class, indexShard.getEngineOrNull());

        // Relocate index as hollow from node A to node B
        updateIndexSettings(Settings.builder().put("index.routing.allocation.exclude._name", indexNodeA), indexName);
        ensureGreen(indexName);

        // Wait for index engine to be closed due to hollowing/reset
        IndexEngineTestUtils.awaitClose(indexEngine);

        // Shard is hollow now
        final var hollowShard = findIndexShard(indexName);
        var hollowEngine = asInstanceOf(HollowIndexEngine.class, hollowShard.getEngineOrNull());

        // segments_6 (generation: 6):
        // _0.cfe
        // _0.cfs
        // _0.si
        // _1.cfe
        // _1.cfs
        // _1.si
        // segments_6
        logLastCommittedSegmentInfos(hollowShard);

        // Acquire commit for snapshot on hollow shard
        var snapshot = hollowShard.acquireIndexCommitForSnapshot();
        assertThat(snapshot.getIndexCommit().getGeneration(), equalTo(6L));
        assertThat(snapshot.getIndexCommit().getSegmentCount(), equalTo(2));

        var isAllowedToDeleteSnapshotBackingBCCs = installSnapshotBackingBCCDeletionWatcher(indexNodeA, snapshot);
        isAllowedToDeleteSnapshotBackingBCCs.set(false);

        // Test that snapshot files can be fully read on the hollow engine
        ensureSnapshotCanReadFiles(snapshot);

        // Trigger force-merge to unhollow shard and merge away the two segments
        var forceMergeFuture = client().admin().indices().prepareForceMerge(indexName).setMaxNumSegments(1).execute();

        // Wait for hollow index engine to be closed
        IndexEngineTestUtils.awaitClose(hollowEngine);
        safeGet(forceMergeFuture);

        // Shard is unhollow now
        var unhollowIndexEngine = asInstanceOf(IndexEngine.class, hollowShard.getEngineOrNull());

        // segments_8 (generation: 8):
        // _2.cfe
        // _2.cfs
        // _2.si
        // segments_8
        logLastCommittedSegmentInfos(hollowShard);

        // Delete all docs, then flush and force-merge to get rid of segments retained for snapshot
        deleteDocs(indexName, IntStream.range(0, 20), WriteRequest.RefreshPolicy.NONE);
        flushAndRefresh(indexName);
        forceMerge(true);

        // segments_a (generation: 10):
        // _4.cfe
        // _4.cfs
        // _4.si
        // segments_a
        logLastCommittedSegmentInfos(hollowShard);

        final var blobName = BatchedCompoundCommit.blobNameFromGeneration(snapshot.getIndexCommit().getGeneration());

        // The commit acquired on the HollowIndexEngine for snapshotting has been retained after unhollowing
        {
            var blobs = getObjectStoreService(indexNodeA).getProjectBlobContainer(hollowShard.shardId(), primaryTerm)
                .listBlobs(OperationPurpose.INDICES)
                .keySet();
            assertThat("BCC for generation 6 exists in object store", blobs.contains(blobName), equalTo(true));
        }

        // Test that snapshot files can still be fully read after the engine is unhollow
        ensureSnapshotCanReadFiles(snapshot);

        isAllowedToDeleteSnapshotBackingBCCs.set(true);
        // Release snapshot commit
        IOUtils.close(snapshot);

        // Even if the commit is fully released, it is not deleted from the object store because HollowIndexEngine does not delete commits
        // from disk (no IndexWriter). We need the IndexEngine to revisit the index deletion policy.
        {
            var blobs = getObjectStoreService(indexNodeA).getProjectBlobContainer(hollowShard.shardId(), primaryTerm)
                .listBlobs(OperationPurpose.INDICES)
                .keySet();
            assertThat("BCC for generation 6 exists in object store", blobs.contains(blobName), equalTo(true));
        }

        // Create a new commit to revisit the index deletion policy and deletes the fully released commit
        flushAndRefresh(indexName);

        // Wait for BCC to be deleted from the object store
        assertBusy(() -> {
            var remainingBlobs = getObjectStoreService(indexNodeA).getProjectBlobContainer(hollowShard.shardId(), primaryTerm)
                .listBlobs(OperationPurpose.INDICES)
                .keySet();
            assertThat("BCC for generation 6 is deleted from object store", remainingBlobs.contains(blobName), equalTo(false));
        });
    }

    public void testSearcherAcquiredOnIndexShardRetainedAfterHollow() throws Exception {
        startMasterOnlyNode();

        final var indexNodeSettings = Settings.builder().put(SETTING_HOLLOW_INGESTION_TTL.getKey(), TimeValue.timeValueMillis(1)).build();
        final var indexNodeA = startIndexNode(indexNodeSettings);
        final var hollowShardsServiceA = internalCluster().getInstance(HollowShardsService.class, indexNodeA);

        final var indexName = randomIdentifier();
        createIndex(
            indexName,
            indexSettings(1, 0).put(SETTING_ALLOCATION_MAX_RETRY.getKey(), 1)
                .put(IndexSettings.INDEX_REFRESH_INTERVAL_SETTING.getKey(), TimeValue.MINUS_ONE)
                .put(IndicesRequestCache.INDEX_CACHE_REQUEST_ENABLED_SETTING.getKey(), false)
                .build()
        );

        final var indexShard = findIndexShard(indexName);
        final long primaryTerm = indexShard.getOperationPrimaryTerm();

        // segments_3 (generation: 3):
        // segments_3
        logLastCommittedSegmentInfos(indexShard);

        // Create first commit with segment _0
        indexDocsAndRefresh(indexName, IntStream.range(0, 10));

        // segments_4 (generation: 4):
        // _0.cfe
        // _0.cfs
        // _0.si
        // segments_4
        logLastCommittedSegmentInfos(indexShard);

        // create second commit with segment _0 and _1
        indexDocsAndRefresh(indexName, IntStream.range(10, 20));

        // segments_5 (generation: 5):
        // _0.cfe
        // _0.cfs
        // _0.si
        // _1.cfe
        // _1.cfs
        // _1.si
        // segments_5
        logLastCommittedSegmentInfos(indexShard);

        // Acquire commit for searcher
        var searcher = indexShard.acquireSearcher("searcher");
        assertThat(searcher.getDirectoryReader().getIndexCommit().getGeneration(), equalTo(5L));
        assertThat(searcher.getDirectoryReader().getIndexCommit().getSegmentCount(), equalTo(2));

        // Force-merge to 1 segment, it executes a flush and a refresh too
        forceMerge(true);

        // segments_6 (generation: 6):
        // _2.cfe
        // _2.cfs
        // _2.si
        // segments_6
        logLastCommittedSegmentInfos(indexShard);

        // Start hollowing shard
        final var indexNodeB = startIndexNode(indexNodeSettings);
        assertBusy(() -> assertThat(hollowShardsServiceA.isHollowableIndexShard(indexShard), equalTo(true)));

        var indexEngine = asInstanceOf(IndexEngine.class, indexShard.getEngineOrNull());

        final var handOffStarted = new CountDownLatch(1);
        final var failHandOff = new CountDownLatch(1);
        MockTransportService.getInstance(indexNodeB)
            .addRequestHandlingBehavior(PRIMARY_CONTEXT_HANDOFF_ACTION_NAME, (handler, request, channel, task) -> {
                handOffStarted.countDown();
                logger.debug("--> primary context handoff request received, now waiting for signal to send failure");
                safeAwait(failHandOff);
                channel.sendResponse(new ElasticsearchException("Test fails primary context handoff on purpose"));
            });

        // Attempt to relocate index as hollow from node A to node B
        updateIndexSettings(Settings.builder().put("index.routing.allocation.exclude._name", indexNodeA), indexName);
        safeAwait(handOffStarted);

        // Wait for index engine to be closed due to hollowing/reset
        IndexEngineTestUtils.awaitClose(indexEngine);

        // Shard is hollow now
        var hollowEngine = asInstanceOf(HollowIndexEngine.class, indexShard.getEngineOrNull());

        // Test that searcher is working and can execute searches
        var top = searcher.search(new MatchAllDocsQuery(), 100);
        assertThat(top.totalHits.value(), equalTo(20L));

        // Test that searcher can access .fdt file during searches
        ensureSearcherCanAccessStoredFieldFile(searcher);

        // Segment files are still in the directory after hollowing
        var indexDirectory = IndexDirectory.unwrapDirectory(indexShard.store().directory());
        assertThat(Set.of(indexDirectory.listAll()), hasItem("_0.si"));
        assertThat(Set.of(indexDirectory.listAll()), hasItem("_1.si"));

        // Fail relocation
        failHandOff.countDown();
        ensureGreen(indexName);

        // Trigger force-merge to unhollow shard, executes a flush and a refresh on the new index engine
        var forceMergeFuture = client().admin().indices().prepareForceMerge(indexName).execute();

        // Wait for hollow index engine to be closed
        IndexEngineTestUtils.awaitClose(hollowEngine);
        safeGet(forceMergeFuture);

        // Shard is unhollow now
        var unhollowIndexEngine = asInstanceOf(IndexEngine.class, indexShard.getEngineOrNull());

        // segments_8 (generation: 8):
        // _2.cfe
        // _2.cfs
        // _2.si
        // segments_8
        logLastCommittedSegmentInfos(indexShard);

        // Ensure that the source index shard didn't fail during the relocation
        final var unhollowShard = findIndexShard(indexName);
        assertThat(unhollowShard, sameInstance(indexShard));
        assertThat(unhollowShard.getOperationPrimaryTerm(), equalTo(primaryTerm));

        // The searcher acquired on the IndexEngine uses segment files that have been merged away during the first force-merge. Lucene
        // uses the IndexFileDeleter to count the references on segment files to delete them when they are not needed anymore, and we
        // track the local reader to ensure the BCC is not deleted from the object store. At the time the IndexWriter was closed during
        // hollowing, the files remain in the directory (because reader can live after the IndexWriter is closed) and the local reader
        // of the BlobReference is still considered as opened in StatelessCommitService/ShardCommitState.
        //
        // But when the new IndexEngine is created at unhollowing time, it has no open readers. The first refresh calls
        // StatelessCommitService.ShardCommitState.onLocalReaderClosed, which will call closedLocalReaders() on any BlobReference that
        // is not a remaining reference anymore, closing the BlobReference retained for the searcher and causing its deletion from the
        // object store.
        //
        // The state of the IndexDirectory / IndexBlobStoreCacheDirectory is not fully clear as the BCC upload updates the metadata
        // while the prewarming (reading commits in BCC using cache) also updates the IndexBlobStoreCacheDirectory metadata. Also,
        // metadata are updated on upload so if there are not more uploads some metadata remains even if the file were deleted.

        var blobName = BatchedCompoundCommit.blobNameFromGeneration(searcher.getDirectoryReader().getIndexCommit().getGeneration());
        var blobs = getObjectStoreService(indexNodeA).getProjectBlobContainer(unhollowShard.shardId(), primaryTerm)
            .listBlobs(OperationPurpose.INDICES)
            .keySet();
        assertThat("BCC for generation 5 is still in the object store", blobs.contains(blobName), equalTo(true));

        assertThat(Set.of(indexDirectory.listAll()), not(hasItem("_0.si")));
        assertThat(Set.of(indexDirectory.listAll()), not(hasItem("_1.si")));

        assertThat(Set.of(indexDirectory.getBlobStoreCacheDirectory().listAll()), hasItem("_0.si"));
        assertThat(Set.of(indexDirectory.getBlobStoreCacheDirectory().listAll()), hasItem("_1.si"));

        ensureSearcherCanAccessStoredFieldFile(searcher);

        IOUtils.close(searcher);

        assertBusy(() -> {
            var blobsAfterRelease = getObjectStoreService(indexNodeA).getProjectBlobContainer(unhollowShard.shardId(), primaryTerm)
                .listBlobs(OperationPurpose.INDICES)
                .keySet();
            assertThat("BCC for generation 5 is deleted from object store", blobsAfterRelease.contains(blobName), equalTo(false));
        });

        // In order to clear the IndexDirectory references to the deleted blobs, we force a flush that would upload a new BCC
        // and clear the deleted files from the IndexDirectory
        indicesAdmin().prepareFlush(indexName).setForce(true).get();

        assertThat(Set.of(indexDirectory.getBlobStoreCacheDirectory().listAll()), not(hasItem("_0.si")));
        assertThat(Set.of(indexDirectory.getBlobStoreCacheDirectory().listAll()), not(hasItem("_1.si")));
    }

    private void logLastCommittedSegmentInfos(IndexShard shard) throws IOException {
        var segmentInfos = shard.withEngine(Engine::getLastCommittedSegmentInfos);
        if (logger.isDebugEnabled()) {
            var details = new StringBuilder("--> ").append(segmentInfos.getSegmentsFileName());
            details.append(" (generation: ").append(segmentInfos.getGeneration()).append("): \n");

            var files = new ArrayList<>(segmentInfos.files(true));
            files.sort(String::compareTo);
            files.forEach(f -> details.append("\t").append(f).append('\n'));
            logger.debug(details);
        }
    }

    private static void indexDocsAndRefresh(String indexName, IntStream docsIdsStream) {
        indexDocs(indexName, docsIdsStream, WriteRequest.RefreshPolicy.IMMEDIATE);
    }

    private static void indexDocs(String indexName, IntStream docsIdsStream, WriteRequest.RefreshPolicy refreshPolicy) {
        var bulkRequest = client().prepareBulk();
        docsIdsStream.forEach(n -> {
            var indexRequest = new IndexRequest(indexName).id(String.valueOf(n));
            // doc source must be large enough to not fit into a single index input buffer,
            // so that the test will try to access the file on disk using the Directory
            indexRequest.source("text", randomAlphaOfLength(256), "value", n);
            bulkRequest.add(indexRequest);
        });
        bulkRequest.setRefreshPolicy(refreshPolicy);
        assertNoFailures(bulkRequest.get());
    }

    private static void deleteDocs(String indexName, IntStream docsIdsStream, WriteRequest.RefreshPolicy refreshPolicy) {
        var bulkRequest = client().prepareBulk();
        docsIdsStream.forEach(n -> bulkRequest.add(new DeleteRequest(indexName).id(String.valueOf(n))));
        bulkRequest.setRefreshPolicy(refreshPolicy);
        assertNoFailures(bulkRequest.get());
    }

    private static void ensureSearcherCanAccessStoredFieldFile(Engine.Searcher searcher) throws IOException {
        searcher.search(new MatchAllDocsQuery(), new Collector() {
            @Override
            public ScoreMode scoreMode() {
                return ScoreMode.COMPLETE_NO_SCORES;
            }

            @Override
            public LeafCollector getLeafCollector(LeafReaderContext context) throws IOException {
                var storeFields = context.reader().storedFields();
                return new LeafCollector() {
                    @Override
                    public void setScorer(Scorable scorer) {}

                    @Override
                    public void collect(int doc) throws IOException {
                        // Read the .fdt file by loading the source
                        storeFields.document(doc).getBinaryValue(SourceFieldMapper.NAME);
                    }
                };
            }
        });
    }

    private static void ensureSnapshotCanReadFiles(Engine.IndexCommitRef snapshot) throws IOException {
        var directory = snapshot.getIndexCommit().getDirectory();
        for (var file : snapshot.getIndexCommit().getFileNames()) {
            try (var input = directory.openChecksumInput(file)) {
                Streams.readFully(new InputStreamIndexInput(input, directory.fileLength(file)));
            }
        }
    }

    private AtomicBoolean installSnapshotBackingBCCDeletionWatcher(String indexNodeA, Engine.IndexCommitRef snapshot) {
        var isAllowedToDeleteSnapshotBackingBCCs = new AtomicBoolean(false);
        setNodeRepositoryStrategy(indexNodeA, new StatelessMockRepositoryStrategy() {
            @Override
            public void blobStoreDeleteBlobsIgnoringIfNotExists(
                CheckedRunnable<IOException> originalRunnable,
                OperationPurpose purpose,
                Iterator<String> blobNames
            ) throws IOException {
                List<String> blobsToDelete = new ArrayList<>();
                blobNames.forEachRemaining(blobsToDelete::add);
                var deletingSnapshotBackingBCC = blobsToDelete.stream()
                    .anyMatch(
                        name -> name.contains(BatchedCompoundCommit.blobNameFromGeneration(snapshot.getIndexCommit().getGeneration()))
                    );
                if (deletingSnapshotBackingBCC && isAllowedToDeleteSnapshotBackingBCCs.get() == false) {
                    throw new AssertionError("Blob deletion should not be triggered " + blobsToDelete);
                }
                super.blobStoreDeleteBlobsIgnoringIfNotExists(originalRunnable, purpose, blobNames);
            }
        });
        return isAllowedToDeleteSnapshotBackingBCCs;
    }
}
