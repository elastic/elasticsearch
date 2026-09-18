/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.stateless.commits;

import org.elasticsearch.ElasticsearchTimeoutException;
import org.elasticsearch.action.bulk.BulkResponse;
import org.elasticsearch.action.support.PlainActionFuture;
import org.elasticsearch.common.blobstore.OperationPurpose;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.unit.ByteSizeValue;
import org.elasticsearch.core.CheckedRunnable;
import org.elasticsearch.core.TimeValue;
import org.elasticsearch.indices.IndexingMemoryController;
import org.elasticsearch.plugins.Plugin;
import org.elasticsearch.plugins.PluginsService;
import org.elasticsearch.telemetry.TestTelemetryPlugin;
import org.elasticsearch.threadpool.ThreadPool;
import org.elasticsearch.xpack.stateless.AbstractStatelessPluginIntegTestCase;
import org.elasticsearch.xpack.stateless.StatelessMockRepositoryPlugin;
import org.elasticsearch.xpack.stateless.StatelessMockRepositoryStrategy;
import org.elasticsearch.xpack.stateless.objectstore.ObjectStoreService;

import java.io.IOException;
import java.io.InputStream;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Map;
import java.util.concurrent.CountDownLatch;

import static org.elasticsearch.xpack.stateless.commits.UploadQueueControllerService.STATELESS_UPLOAD_QUEUE_CONTROLLER_INDEX_THROTTLE_REMOVAL_THRESHOLD;
import static org.elasticsearch.xpack.stateless.commits.UploadQueueControllerService.STATELESS_UPLOAD_QUEUE_CONTROLLER_INDEX_THROTTLE_THRESHOLD;

public class UploadQueueControllerServiceIT extends AbstractStatelessPluginIntegTestCase {
    public void testQueueControllerAppliesIndexThrottling() throws Exception {
        final var indexNode = startMasterAndIndexNode(
            uploadThrottlingSettings()
                // Block indexing completely on throttle to observe it reliably.
                .put(IndexingMemoryController.PAUSE_INDEXING_ON_THROTTLE.getKey(), true)
                .build()
        );

        final String indexName = randomIndexName();
        createIndex(indexName, 1, 0);
        ensureGreen(indexName);

        // Block uploads to create a backlog.
        var uploadStarted = new CountDownLatch(1);
        var blockUploadLatch = new CountDownLatch(1);
        blockCommitUploads(indexNode, uploadStarted, blockUploadLatch);

        indexDocs(indexName, 1000);
        refresh(indexName);
        safeAwait(uploadStarted);

        // Wait longer than the 1 ms activation threshold before polling.
        var threadPool = internalCluster().getInstance(ThreadPool.class, indexNode);
        var currentTime = threadPool.relativeTimeInMillis();

        assertBusy(() -> assertTrue(threadPool.relativeTimeInMillis() - currentTime > 1));

        var uploadQueueControllerService = internalCluster().getInstance(UploadQueueControllerService.class, indexNode);
        uploadQueueControllerService.runNow();

        // Indexing should be completely blocked now.
        var bulkFuture = client().prepareBulk()
            .add(client().prepareIndex(indexName).setSource(Map.of("field", randomAlphanumericOfLength(10))))
            .execute();
        assertThrows(ElasticsearchTimeoutException.class, () -> bulkFuture.actionGet(TimeValue.timeValueMillis(500)));

        // Drain the backlog.
        blockUploadLatch.countDown();
        flush(indexName);

        assertFalse(bulkFuture.isDone());

        // And previously blocked bulk now can proceed.
        uploadQueueControllerService.runNow();

        var response = safeGet(bulkFuture);
        assertFalse(response.hasFailures());
    }

    /** Checks that a poll between thresholds cannot strand a real shard's indexing throttle. */
    public void testQueueControllerRemovesIndexThrottlingAfterAgeBetweenThresholds() throws Exception {
        var indexNode = startMasterAndIndexNode(
            uploadThrottlingSettings().put(
                STATELESS_UPLOAD_QUEUE_CONTROLLER_INDEX_THROTTLE_REMOVAL_THRESHOLD.getKey(),
                TimeValue.timeValueMillis(1)
            )
                // Make a stranded throttle observable as a blocked bulk request.
                .put(IndexingMemoryController.PAUSE_INDEXING_ON_THROTTLE.getKey(), true)
                .build()
        );
        var indexName = randomIndexName();
        createIndex(indexName, 1, 0);
        ensureGreen(indexName);
        var shard = findIndexShard(indexName);
        var controller = internalCluster().getInstance(UploadQueueControllerService.class, indexNode);

        // Block uploads to create a backlog.
        var uploadStarted = new CountDownLatch(1);
        var blockUploadLatch = new CountDownLatch(1);
        blockCommitUploads(indexNode, uploadStarted, blockUploadLatch);

        var bulkFuture = new PlainActionFuture<BulkResponse>();
        boolean bulkSubmitted = false;
        try {
            indexDocs(indexName, 1);
            refresh(indexName);
            safeAwait(uploadStarted);
            assertBusy(() -> {
                controller.runNow();
                assertTrue(shard.indexingStats().getTotal().isThrottled());
            });
            client().prepareBulk().add(client().prepareIndex(indexName).setSource(Map.of("field", "value"))).execute(bulkFuture);
            bulkSubmitted = true;
            assertThrows(ElasticsearchTimeoutException.class, () -> bulkFuture.actionGet(TimeValue.timeValueMillis(500)));

            // Raise the throttle threshold so the pending upload's age falls between the two thresholds. Should stay throttled
            updateClusterSettings(
                Settings.builder().put(STATELESS_UPLOAD_QUEUE_CONTROLLER_INDEX_THROTTLE_THRESHOLD.getKey(), TimeValue.timeValueHours(1))
            );
            controller.runNow();
            assertTrue(shard.indexingStats().getTotal().isThrottled());
            assertFalse(bulkFuture.isDone());

            // Once the queue drains, the controller must remove its throttle and unblock the bulk.
            blockUploadLatch.countDown();
            flush(indexName);
            assertFalse(bulkFuture.isDone());
            controller.runNow();
            assertFalse("Throttle must be removed once uploads drain", shard.indexingStats().getTotal().isThrottled());
            assertFalse(safeGet(bulkFuture).hasFailures());
        } finally {
            // Release blocked work even if an assertion fails.
            blockUploadLatch.countDown();
            if (shard.indexingStats().getTotal().isThrottled()) {
                shard.deactivateThrottling();
            }
            updateClusterSettings(Settings.builder().putNull(STATELESS_UPLOAD_QUEUE_CONTROLLER_INDEX_THROTTLE_THRESHOLD.getKey()));
            if (bulkSubmitted) {
                safeGet(bulkFuture);
            }
        }
    }

    public void testQueueControllerEmitsIndexingThrottlingMetrics() throws Exception {
        final var indexNode = startMasterAndIndexNode(uploadThrottlingSettings().build());

        final String indexName = randomIndexName();
        createIndex(indexName, 1, 0);
        ensureGreen(indexName);

        // Block uploads to create a backlog.
        var uploadStarted = new CountDownLatch(1);
        var blockUploadLatch = new CountDownLatch(1);
        blockCommitUploads(indexNode, uploadStarted, blockUploadLatch);

        indexDocs(indexName, 1000);
        refresh(indexName);
        safeAwait(uploadStarted);

        // Wait longer than the 1 ms activation threshold before polling.
        var threadPool = internalCluster().getInstance(ThreadPool.class, indexNode);
        var currentTime = threadPool.relativeTimeInMillis();

        assertBusy(() -> assertTrue(threadPool.relativeTimeInMillis() - currentTime > 1));

        var uploadQueueControllerService = internalCluster().getInstance(UploadQueueControllerService.class, indexNode);
        uploadQueueControllerService.runNow();

        blockUploadLatch.countDown();

        var metricsPlugin = internalCluster().getInstance(PluginsService.class, indexNode)
            .filterPlugins(TestTelemetryPlugin.class)
            .findFirst()
            .orElseThrow();

        // See UploadQueueControllerService.MonitoringThrottler.
        var activateThrottleCounterMeasurements = metricsPlugin.getLongCounterMeasurement(
            "es.stateless.upload_queue.indexing_throttling.activated.total"
        );
        assertEquals(1, activateThrottleCounterMeasurements.size());
        assertEquals(1, activateThrottleCounterMeasurements.get(0).getLong());

        // See UploadQueueControllerService.ThrottleCalculator.
        var ageOfTheOldestCommit = metricsPlugin.getLongHistogramMeasurement("es.stateless.upload_queue.oldest_commit_age.histogram");
        assertEquals(1, ageOfTheOldestCommit.size());
        // It's in seconds so in this test it's always 0.
        assertEquals(0, ageOfTheOldestCommit.get(0).getLong());

        // Wait for all pending commits to finish uploading.
        flush(indexName);

        // Now that there is no queue throttling will be removed (since we set cooldown period to 0);
        uploadQueueControllerService.runNow();

        var deactivateThrottleCounterMeasurements = metricsPlugin.getLongCounterMeasurement(
            "es.stateless.upload_queue.indexing_throttling.deactivated.total"
        );
        assertEquals(1, deactivateThrottleCounterMeasurements.size());
        assertEquals(1, deactivateThrottleCounterMeasurements.get(0).getLong());
    }

    private static Settings.Builder uploadThrottlingSettings() {
        return Settings.builder()
            // We run it on demand.
            .put(UploadQueueControllerService.STATELESS_UPLOAD_QUEUE_CONTROLLER_ENABLED.getKey(), false)
            // Enable throttling
            .put(UploadQueueControllerService.STATELESS_UPLOAD_QUEUE_CONTROLLER_INDEXING_THROTTLING_ENABLED.getKey(), true)
            // Always throttle.
            .put(STATELESS_UPLOAD_QUEUE_CONTROLLER_INDEX_THROTTLE_THRESHOLD.getKey(), TimeValue.timeValueMillis(1))
            .put(UploadQueueControllerService.STATELESS_UPLOAD_QUEUE_CONTROLLER_INDEX_THROTTLE_COOLDOWN.getKey(), TimeValue.ZERO)
            .put(StatelessCommitService.STATELESS_UPLOAD_MAX_SIZE.getKey(), ByteSizeValue.ofBytes(1))
            // Disable caching of time values to make sure we make progress every time UploadQueueControllerService#runNow() is called.
            .put(ThreadPool.ESTIMATED_TIME_INTERVAL_SETTING.getKey(), TimeValue.ZERO);
    }

    private void blockCommitUploads(String indexNode, CountDownLatch uploadStarted, CountDownLatch releaseUpload) {
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
                uploadStarted.countDown();
                safeAwait(releaseUpload);
                super.blobContainerWriteBlobAtomic(originalRunnable, purpose, blobName, inputStream, blobSize, failIfAlreadyExists);
            }
        });
    }

    @Override
    protected Collection<Class<? extends Plugin>> nodePlugins() {
        var plugins = new ArrayList<>(super.nodePlugins());
        plugins.add(TestTelemetryPlugin.class);
        plugins.add(StatelessMockRepositoryPlugin.class);
        return plugins;
    }

    @Override
    protected Settings.Builder nodeSettings() {
        return super.nodeSettings().put(ObjectStoreService.TYPE_SETTING.getKey(), ObjectStoreService.ObjectStoreType.MOCK)
            .put(disableIndexingDiskAndMemoryControllersNodeSettings());
    }

    @Override
    protected boolean addMockFsRepository() {
        return false;
    }
}
