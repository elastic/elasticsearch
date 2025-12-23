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

import co.elastic.elasticsearch.stateless.cache.action.ClearBlobCacheNodesRequest;
import co.elastic.elasticsearch.stateless.commits.StatelessCommitService;
import co.elastic.elasticsearch.stateless.objectstore.ObjectStoreService;

import org.apache.lucene.index.FilterMergePolicy;
import org.apache.lucene.index.MergePolicy;
import org.apache.lucene.index.MergeTrigger;
import org.apache.lucene.index.SegmentInfos;
import org.elasticsearch.action.bulk.BulkRequest;
import org.elasticsearch.action.index.IndexRequest;
import org.elasticsearch.action.update.UpdateRequest;
import org.elasticsearch.common.CheckedSupplier;
import org.elasticsearch.common.UUIDs;
import org.elasticsearch.common.blobstore.OperationPurpose;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.unit.ByteSizeValue;
import org.elasticsearch.core.CheckedRunnable;
import org.elasticsearch.index.engine.EngineConfig;
import org.elasticsearch.index.shard.ShardId;
import org.elasticsearch.plugins.Plugin;
import org.elasticsearch.threadpool.ThreadPool;
import org.elasticsearch.xpack.stateless.StatelessPlugin;

import java.io.IOException;
import java.io.InputStream;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Map;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicReference;

import static co.elastic.elasticsearch.stateless.ServerlessStatelessPlugin.CLEAR_BLOB_CACHE_ACTION;
import static co.elastic.elasticsearch.stateless.StatelessMergeIT.blockMergePool;
import static co.elastic.elasticsearch.stateless.commits.StatelessCommitService.STATELESS_UPLOAD_MAX_AMOUNT_COMMITS;
import static co.elastic.elasticsearch.stateless.commits.StatelessCommitService.STATELESS_UPLOAD_MAX_SIZE;
import static org.elasticsearch.blobcache.shared.SharedBlobCacheService.SHARED_CACHE_REGION_SIZE_SETTING;
import static org.elasticsearch.blobcache.shared.SharedBlobCacheService.SHARED_CACHE_SIZE_SETTING;
import static org.elasticsearch.index.engine.ThreadPoolMergeScheduler.USE_THREAD_POOL_MERGE_SCHEDULER_SETTING;
import static org.elasticsearch.test.hamcrest.ElasticsearchAssertions.assertNoFailures;

public class StatelessMergePreWarmingIT extends AbstractServerlessStatelessPluginIntegTestCase {

    @Override
    protected boolean addMockFsRepository() {
        return false;
    }

    @Override
    protected Collection<Class<? extends Plugin>> nodePlugins() {
        var plugins = new ArrayList<>(super.nodePlugins());
        plugins.remove(ServerlessStatelessPlugin.class);
        plugins.add(DeterministicMergesServerlessStatelessPluginTestPlugin.class);
        plugins.add(StatelessMockRepositoryPlugin.class);
        return plugins;
    }

    public static class DeterministicMergesServerlessStatelessPluginTestPlugin extends ServerlessStatelessPlugin {
        private static AtomicReference<MergeFinder> mergeFinderRef = new AtomicReference<>();

        public DeterministicMergesServerlessStatelessPluginTestPlugin(Settings settings) {
            super(settings);
        }

        @Override
        protected MergePolicy getMergePolicy(EngineConfig engineConfig) {
            return new FilterMergePolicy(super.getMergePolicy(engineConfig)) {
                @Override
                public MergeSpecification findMerges(MergeTrigger mergeTrigger, SegmentInfos segmentInfos, MergeContext mergeContext)
                    throws IOException {
                    var findMergeFn = mergeFinderRef.get();
                    if (findMergeFn == null) {
                        return super.findMerges(mergeTrigger, segmentInfos, mergeContext);
                    }
                    return findMergeFn.findMerges(segmentInfos, mergeContext);
                }
            };
        }

        static void setTestMergeFinder(MergeFinder mergeFinder) {
            mergeFinderRef.set(mergeFinder);
        }

        interface MergeFinder {
            MergePolicy.MergeSpecification findMerges(SegmentInfos segmentInfos, MergePolicy.MergeContext mergeContext);
        }
    }

    @Override
    protected Settings.Builder nodeSettings() {
        return super.nodeSettings().put(ObjectStoreService.TYPE_SETTING.getKey(), ObjectStoreService.ObjectStoreType.MOCK)
            .put(USE_THREAD_POOL_MERGE_SCHEDULER_SETTING.getKey(), true)
            .put(disableIndexingDiskAndMemoryControllersNodeSettings())
            .put(SHARED_CACHE_SIZE_SETTING.getKey(), ByteSizeValue.ofMb(1))
            .put(SHARED_CACHE_REGION_SIZE_SETTING.getKey(), ByteSizeValue.ofKb(100));
    }

    public void testMergePreWarmingFailureDoesNotFailTheEngine() throws Exception {
        var indexNode = startMasterAndIndexNode(
            Settings.builder()
                .put(STATELESS_UPLOAD_MAX_AMOUNT_COMMITS.getKey(), 1000)
                .put(STATELESS_UPLOAD_MAX_SIZE.getKey(), ByteSizeValue.ofGb(1))
                .put(ObjectStoreService.TYPE_SETTING.getKey(), ObjectStoreService.ObjectStoreType.MOCK)
                .build()
        );
        String indexName = randomIdentifier();
        createIndex(indexName, 1, 0);
        ensureGreen(indexName);
        var shardId = new ShardId(resolveIndex(indexName), 0);

        var segmentCountMergedTogether = 2;
        var mergeScheduled = new AtomicBoolean(false);
        DeterministicMergesServerlessStatelessPluginTestPlugin.setTestMergeFinder((segmentInfos, mergeContext) -> {
            var mergeCandidates = segmentInfos.asList()
                .stream()
                .filter(segmentCommitInfo -> mergeContext.getMergingSegments().contains(segmentCommitInfo) == false)
                .toList();

            if (mergeCandidates.size() < segmentCountMergedTogether || mergeScheduled.get()) {
                return null;
            }
            mergeScheduled.set(true);
            var mergeSpec = new MergePolicy.MergeSpecification();
            mergeSpec.add(new MergePolicy.OneMerge(mergeCandidates.stream().limit(segmentCountMergedTogether).toList()));
            return mergeSpec;
        });

        var unblockMergePoolLatch = new CountDownLatch(1);
        var threadPool = internalCluster().getInstance(ThreadPool.class, indexNode);
        // By blocking the merge thread pool we ensure that the merge will be executed once the BCC is getting uploaded
        // and the blob locations point to the yet to be uploaded blob
        blockMergePool(threadPool, unblockMergePoolLatch);

        // Create a document that we will update later on and provoke a read failure
        var bulkRequest = new BulkRequest();
        var id = UUIDs.randomBase64UUID();
        bulkRequest.add(new IndexRequest(indexName).id(id).source(Map.of("data", randomAlphanumericOfLength(50))));
        client().bulk(bulkRequest).get();

        // The merge policy will merge the first two segments together
        for (int i = 0; i < segmentCountMergedTogether; i++) {
            indexDocs(indexName, 100);
            refresh(indexName);
        }

        var commitService = internalCluster().getInstance(StatelessCommitService.class);
        var currentVirtualBcc = commitService.getCurrentVirtualBcc(shardId);
        var bccToUpload = currentVirtualBcc.getBlobName();

        var blockUploadLatch = new CountDownLatch(1);
        var uploadInProgressLatch = new CountDownLatch(1);
        setNodeRepositoryStrategy(indexNode, new StatelessMockRepositoryStrategy() {
            @Override
            public void blobContainerWriteBlobAtomic(
                CheckedRunnable<IOException> originalRunnable,
                OperationPurpose purpose,
                String blobName,
                InputStream inputStream,
                long blobSize,
                boolean failIfAlreadyExists
            ) throws IOException {
                if (blobName.equals(bccToUpload)) {
                    uploadInProgressLatch.countDown();
                    safeAwait(blockUploadLatch);
                }
                super.blobContainerWriteBlobAtomic(originalRunnable, purpose, blobName, inputStream, blobSize, failIfAlreadyExists);
            }

            @Override
            public InputStream blobContainerReadBlob(
                CheckedSupplier<InputStream, IOException> originalSupplier,
                OperationPurpose purpose,
                String blobName,
                long position,
                long length
            ) throws IOException {
                assert (blobName.equals(bccToUpload)
                    && Thread.currentThread().getName().contains(StatelessPlugin.PREWARM_THREAD_POOL)) == false
                    : "Unexpected read from the pre-warmed thread";
                return super.blobContainerReadBlob(originalSupplier, purpose, blobName, position, length);
            }
        });

        var flushFuture = indicesAdmin().prepareFlush(indexName).execute();

        // Wait until the upload reached the blob store and clear the cache since the upload does pre-warm the cache before being scheduled
        safeAwait(uploadInProgressLatch);
        client().execute(CLEAR_BLOB_CACHE_ACTION, new ClearBlobCacheNodesRequest()).get();

        // This prevents the shard to be allocated if the shard fails due to a bug in the tested code
        client().admin()
            .indices()
            .prepareUpdateSettings(indexName)
            .setSettings(Settings.builder().put("index.routing.allocation.exclude._name", indexNode));

        // Let the merge proceed and wait until the merge pre-warming kicks in
        unblockMergePoolLatch.countDown();

        // Let the upload continue and wait until it finishes
        blockUploadLatch.countDown();
        flushFuture.actionGet();

        // Send an update request that would require reading from the cache
        var updateBulkRequest = new BulkRequest();
        updateBulkRequest.add(new UpdateRequest(indexName, id).fetchSource(true).doc(Map.of("data2", randomAlphanumericOfLength(50))));
        var updateFuture = client().bulk(updateBulkRequest);

        // Wait a bit until all the reads are waiting for the same cache region initiated by the merge pre-warming
        // maybe there's a better way to achieve this?
        safeSleep(1_000);

        assertNoFailures(updateFuture.get());
        ensureGreen(indexName);
    }

}
