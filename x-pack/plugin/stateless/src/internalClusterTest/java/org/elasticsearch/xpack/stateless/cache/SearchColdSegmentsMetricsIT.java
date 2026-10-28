/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.stateless.cache;

import org.apache.lucene.index.DirectoryReader;
import org.apache.lucene.index.IndexCommit;
import org.apache.lucene.index.IndexFileNames;
import org.apache.lucene.index.IndexWriter;
import org.apache.lucene.index.SegmentCommitInfo;
import org.elasticsearch.action.ActionFuture;
import org.elasticsearch.action.support.broadcast.BroadcastResponse;
import org.elasticsearch.cluster.metadata.IndexMetadata;
import org.elasticsearch.common.blobstore.BlobContainer;
import org.elasticsearch.common.blobstore.OperationPurpose;
import org.elasticsearch.common.lucene.Lucene;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.unit.ByteSizeValue;
import org.elasticsearch.common.util.concurrent.ConcurrentCollections;
import org.elasticsearch.core.CheckedRunnable;
import org.elasticsearch.core.TimeValue;
import org.elasticsearch.index.IndexSettings;
import org.elasticsearch.index.MergePolicyConfig;
import org.elasticsearch.index.shard.ShardId;
import org.elasticsearch.index.store.LuceneFilesExtensions;
import org.elasticsearch.plugins.Plugin;
import org.elasticsearch.telemetry.Measurement;
import org.elasticsearch.telemetry.TestTelemetryPlugin;
import org.elasticsearch.xpack.stateless.AbstractStatelessPluginIntegTestCase;
import org.elasticsearch.xpack.stateless.StatelessMockRepositoryPlugin;
import org.elasticsearch.xpack.stateless.StatelessMockRepositoryStrategy;
import org.elasticsearch.xpack.stateless.commits.BatchedCompoundCommit;
import org.elasticsearch.xpack.stateless.commits.StatelessCommitService;
import org.elasticsearch.xpack.stateless.commits.StatelessCompoundCommit;
import org.elasticsearch.xpack.stateless.commits.VirtualBatchedCompoundCommit;
import org.elasticsearch.xpack.stateless.engine.PrimaryTermAndGeneration;
import org.elasticsearch.xpack.stateless.objectstore.ObjectStoreService;

import java.io.IOException;
import java.io.InputStream;
import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.Objects;
import java.util.Set;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.Executor;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.stream.Collectors;

import static org.elasticsearch.test.hamcrest.ElasticsearchAssertions.assertNoFailures;
import static org.hamcrest.Matchers.empty;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.greaterThan;
import static org.hamcrest.Matchers.greaterThanOrEqualTo;
import static org.hamcrest.Matchers.hasKey;
import static org.hamcrest.Matchers.hasSize;
import static org.hamcrest.Matchers.not;
import static org.hamcrest.Matchers.notNullValue;
import static org.hamcrest.Matchers.nullValue;

public class SearchColdSegmentsMetricsIT extends AbstractStatelessPluginIntegTestCase {

    @Override
    protected boolean addMockFsRepository() {
        return false;
    }

    @Override
    protected Collection<Class<? extends Plugin>> nodePlugins() {
        var plugins = new ArrayList<>(super.nodePlugins());
        plugins.add(StatelessMockRepositoryPlugin.class);
        plugins.add(TestTelemetryPlugin.class);
        return plugins;
    }

    @Override
    protected Settings.Builder nodeSettings() {
        return super.nodeSettings().put(disableIndexingDiskAndMemoryControllersNodeSettings())
            // Opt-in feature: the indexing shard only attaches merge-produced segment sizes to notifications when enabled.
            .put(StatelessCommitService.STATELESS_COMMIT_NOTIFICATION_INCLUDE_MERGE_PRODUCED_SEGMENTS.getKey(), true)
            .put(SearchCommitPrefetcher.PREFETCH_NON_UPLOADED_COMMITS_SETTING.getKey(), true)
            .put(StatelessOnlinePrewarmingService.STATELESS_ONLINE_PREWARMING_ENABLED.getKey(), false)
            // Only explicit flushes (and the merge) trigger a BCC upload.
            .put(StatelessCommitService.STATELESS_UPLOAD_MAX_AMOUNT_COMMITS.getKey(), 100)
            .put(StatelessCommitService.STATELESS_UPLOAD_MAX_SIZE.getKey(), ByteSizeValue.ofGb(1))
            .put(StatelessCommitService.STATELESS_UPLOAD_VBCC_MAX_AGE.getKey(), TimeValue.timeValueHours(12))
            .put(ObjectStoreService.TYPE_SETTING.getKey(), ObjectStoreService.ObjectStoreType.MOCK);
    }

    public void testColdSegmentsRecordedDuringMergeBccUpload() throws Exception {
        var uploadBlocker = new BlockBccUploadStrategy();
        var indexNode = startMasterAndIndexNode(Settings.EMPTY, uploadBlocker);
        var searchNode = startSearchNode();

        var indexName = randomIdentifier();
        // No replica initially: the search shard is added only once all data is uploaded, so it recovers cleanly and the only cold segments
        // it later sees come from the merge commit we stall. Disabling automatic merges guarantees that merge is the only one.
        createIndex(
            indexName,
            indexSettings(1, 0).put(IndexSettings.INDEX_REFRESH_INTERVAL_SETTING.getKey(), -1)
                .put(MergePolicyConfig.INDEX_MERGE_POLICY_SEGMENTS_PER_TIER_SETTING.getKey(), 1000.0)
                .put(MergePolicyConfig.INDEX_MERGE_POLICY_MAX_MERGE_AT_ONCE_SETTING.getKey(), 1000)
                .build()
        );
        ensureGreen(indexName);

        var shardId = new ShardId(resolveIndex(indexName), 0);
        var commitService = internalCluster().getInstance(StatelessCommitService.class, indexNode);

        // Create a few BCCs with docs; each flush uploads its BCC. The random extra flush just varies how much the merge rewrites.
        var numberOfCommits = randomIntBetween(3, 5);
        for (int i = 0; i < numberOfCommits; i++) {
            indexDocs(indexName, randomIntBetween(200, 400));
            flush(indexName);
        }
        if (randomBoolean()) {
            indexDocs(indexName, randomIntBetween(50, 200));
            flush(indexName);
        }

        // Wait until everything is uploaded, then add the search shard so it recovers from a fully uploaded state (no cold segments yet).
        assertBusy(() -> assertThat(commitService.getCurrentVirtualBcc(shardId), nullValue()));
        updateIndexSettings(Settings.builder().put(IndexMetadata.SETTING_NUMBER_OF_REPLICAS, 1), indexName);
        ensureGreen(indexName);

        var searchTelemetry = getTelemetryPlugin(searchNode);
        searchTelemetry.resetMeter();

        // Stall every BCC upload from now on: the merge BCC we are about to create will be served cold to the search shard.
        uploadBlocker.blocking.set(true);

        // Randomly break the search-idle barrier so the recorded "reason" dimension varies: a recent search means the commit is prefetched
        // (foreground/background), otherwise the shard stays idle ("idling").
        if (randomBoolean()) {
            assertNoFailures(prepareSearch(indexName));
        }

        // Trigger the merge: its segments are notified to the search shard while their BCC upload is still stalled, i.e. read cold.
        ActionFuture<BroadcastResponse> forceMergeFuture = indicesAdmin().prepareForceMerge(indexName)
            .setMaxNumSegments(1)
            .setFlush(true)
            .execute();

        try {
            // Wait until the merge BCC upload has started and is stalled.
            assertTrue(uploadBlocker.uploadReached.await(30, TimeUnit.SECONDS));

            // Retrieve the stalled VBCC and compute from it the exact sizes the search shard should record. With automatic merges disabled
            // and all prior data uploaded, the force merge produces exactly one non-uploaded commit.
            assertThat(uploadBlocker.blockedBlobs, not(empty()));
            var indexShard = findIndexShard(indexName);
            long primaryTerm = indexShard.getOperationPrimaryTerm();

            var stalledCommits = uploadBlocker.blockedBlobs.stream()
                .map(blobName -> new PrimaryTermAndGeneration(primaryTerm, BatchedCompoundCommit.parseGenerationFromBlobName(blobName)))
                .map(termAndGen -> commitService.getVirtualBatchedCompoundCommit(shardId, termAndGen))
                .peek(vbcc -> assertThat(vbcc, notNullValue()))
                .flatMap(vbcc -> vbcc.getPendingCompoundCommits().stream())
                .map(VirtualBatchedCompoundCommit.PendingCompoundCommit::getStatelessCompoundCommit)
                .toList();
            assertThat("exactly one cold (merge) commit is expected", stalledCommits, hasSize(1));

            // Total cold data = sum of the stalled commit's internal files (header/other content excluded); merge part = sizes of its
            // merge-produced segments only.
            long expectedTotalBytes = stalledCommits.stream().mapToLong(StatelessCompoundCommit::getInternalFilesSizeInBytes).sum();

            Set<String> newSegmentNames = stalledCommits.stream()
                .flatMap(commit -> commit.getInternalFiles().stream())
                .filter(file -> Objects.equals(LuceneFilesExtensions.fromFile(file), LuceneFilesExtensions.SI))
                .map(IndexFileNames::stripExtension)
                .collect(Collectors.toUnmodifiableSet());

            // Lucene diagnostics tell which new segments a merge produced (IndexWriter.SOURCE == SOURCE_MERGE), deduplicated by name.
            var mergeSegmentSizes = new HashMap<String, Long>();
            var store = indexShard.store();
            store.incRef();
            try {
                for (IndexCommit commit : DirectoryReader.listCommits(store.directory())) {
                    for (SegmentCommitInfo segmentCommitInfo : Lucene.readSegmentInfos(commit)) {
                        if (newSegmentNames.contains(segmentCommitInfo.info.name)
                            && IndexWriter.SOURCE_MERGE.equals(segmentCommitInfo.info.getDiagnostics().get(IndexWriter.SOURCE))) {
                            mergeSegmentSizes.put(segmentCommitInfo.info.name, segmentCommitInfo.sizeInBytes());
                        }
                    }
                }
            } finally {
                store.decRef();
            }
            long expectedMergeBytes = mergeSegmentSizes.values().stream().mapToLong(Long::longValue).sum();
            assertThat("the merge should have produced at least one segment", expectedMergeBytes, greaterThan(0L));
            assertThat(expectedTotalBytes, greaterThanOrEqualTo(expectedMergeBytes));

            // Wait until the search shard refreshes and records the cold segments, and check the values before re-allowing uploads.
            assertBusy(() -> {
                var mergeMeasurements = searchTelemetry.getLongHistogramMeasurement(
                    SearchColdSegmentsMetrics.COLD_SEGMENTS_MERGE_SIZE_HISTOGRAM_METRIC
                );
                var totalMeasurements = searchTelemetry.getLongHistogramMeasurement(
                    SearchColdSegmentsMetrics.COLD_SEGMENTS_TOTAL_SIZE_HISTOGRAM_METRIC
                );
                assertThat(mergeMeasurements, hasSize(1));
                assertThat(totalMeasurements, hasSize(1));
                assertThat(mergeMeasurements.stream().mapToLong(Measurement::getLong).sum(), equalTo(expectedMergeBytes));
                assertThat(totalMeasurements.stream().mapToLong(Measurement::getLong).sum(), equalTo(expectedTotalBytes));
                // Each sample carries the prefetch outcome as a dimension.
                for (var measurement : mergeMeasurements) {
                    assertThat(measurement.attributes(), hasKey(SearchColdSegmentsMetrics.PREFETCHING_ATTRIBUTE));
                }
            });
        } finally {
            // Re-allow the stalled merge BCC upload to complete.
            uploadBlocker.blocking.set(false);
            uploadBlocker.release.countDown();
            forceMergeFuture.actionGet();
        }
    }

    /**
     * Object store strategy that stalls the upload of Batched Compound Commit blobs once {@link #blocking} is set, until
     * {@link #release} is counted down. It records the stalled blob names so the test can retrieve the matching VBCC and
     * assert an upload was actually in flight.
     */
    private static final class BlockBccUploadStrategy extends StatelessMockRepositoryStrategy {
        private final AtomicBoolean blocking = new AtomicBoolean(false);
        private final CountDownLatch release = new CountDownLatch(1);
        private final CountDownLatch uploadReached = new CountDownLatch(1);
        private final Set<String> blockedBlobs = ConcurrentCollections.newConcurrentSet();

        private void maybeBlock(String blobName) {
            if (blocking.get() && BatchedCompoundCommit.startsWithBlobPrefix(blobName)) {
                blockedBlobs.add(blobName);
                uploadReached.countDown();
                try {
                    if (release.await(60, TimeUnit.SECONDS) == false) {
                        throw new AssertionError("timed out waiting to release the stalled BCC upload for " + blobName);
                    }
                } catch (InterruptedException e) {
                    Thread.currentThread().interrupt();
                    throw new AssertionError(e);
                }
            }
        }

        @Override
        public void blobContainerWriteBlobAtomic(
            CheckedRunnable<IOException> originalRunnable,
            OperationPurpose purpose,
            String blobName,
            InputStream inputStream,
            long blobSize,
            boolean failIfAlreadyExists
        ) throws IOException {
            maybeBlock(blobName);
            originalRunnable.run();
        }

        @Override
        public void blobContainerWriteBlobAtomic(
            CheckedRunnable<IOException> originalRunnable,
            OperationPurpose purpose,
            String blobName,
            long blobSize,
            BlobContainer.BlobMultiPartInputStreamProvider provider,
            boolean failIfAlreadyExists,
            Executor executor
        ) throws IOException {
            maybeBlock(blobName);
            originalRunnable.run();
        }
    }
}
