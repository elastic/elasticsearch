/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.stateless.commits;

import org.elasticsearch.ElasticsearchTimeoutException;
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

public class UploadQueueControllerServiceIT extends AbstractStatelessPluginIntegTestCase {
    public void testQueueControllerAppliesIndexThrottling() throws Exception {
        final var indexNode = startMasterAndIndexNode(
            Settings.builder()
                // We run it on demand.
                .put(UploadQueueControllerService.STATELESS_UPLOAD_QUEUE_CONTROLLER_ENABLED.getKey(), false)
                // Enable throttling
                .put(UploadQueueControllerService.STATELESS_UPLOAD_QUEUE_CONTROLLER_INDEXING_THROTTLING_ENABLED.getKey(), true)
                // Always throttle.
                .put(UploadQueueControllerService.STATELESS_UPLOAD_QUEUE_CONTROLLER_INDEX_THROTTLE_THRESHOLD.getKey(), TimeValue.ZERO)
                .put(UploadQueueControllerService.STATELESS_UPLOAD_QUEUE_CONTROLLER_INDEX_THROTTLE_COOLDOWN.getKey(), TimeValue.ZERO)
                .put(StatelessCommitService.STATELESS_UPLOAD_MAX_SIZE.getKey(), ByteSizeValue.ofBytes(1))
                // Force the throughput to be very low to get artificially large queue and observe throttling.
                .put(StatelessCommitService.STATELESS_UPLOAD_AVERAGE_THROUGHPUT_INITIAL_VALUE.getKey(), ByteSizeValue.ofBytes(1))
                // Disable caching of time values to make sure we make progress every time UploadQueueControllerService#runNow() is called.
                .put(ThreadPool.ESTIMATED_TIME_INTERVAL_SETTING.getKey(), TimeValue.ZERO)
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
                safeAwait(blockUploadLatch);
                super.blobContainerWriteBlobAtomic(originalRunnable, purpose, blobName, inputStream, blobSize, failIfAlreadyExists);
            }
        });

        indexDocs(indexName, 1000);
        refresh(indexName);
        safeAwait(uploadStarted);

        // Now we need to build sufficient backlog.
        var statelessCommitService = internalCluster().getInstance(StatelessCommitService.class, indexNode);
        while (statelessCommitService.getShardCommitStats().iterator().next().pendingUploadBytes() < ByteSizeValue.ofMb(1).getBytes()) {
            indexDocs(indexName, 1000);
            refresh(indexName);
        }

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

    public void testQueueControllerEmitsIndexingThrottlingMetrics() throws Exception {
        final var indexNode = startMasterAndIndexNode(
            Settings.builder()
                // We run it on demand.
                .put(UploadQueueControllerService.STATELESS_UPLOAD_QUEUE_CONTROLLER_ENABLED.getKey(), false)
                // Always throttle.
                .put(UploadQueueControllerService.STATELESS_UPLOAD_QUEUE_CONTROLLER_INDEX_THROTTLE_THRESHOLD.getKey(), TimeValue.ZERO)
                .put(UploadQueueControllerService.STATELESS_UPLOAD_QUEUE_CONTROLLER_INDEX_THROTTLE_COOLDOWN.getKey(), TimeValue.ZERO)
                .put(StatelessCommitService.STATELESS_UPLOAD_MAX_SIZE.getKey(), ByteSizeValue.ofBytes(1))
                // Force the throughput to be very low to get artificially large queue and observe throttling.
                .put(StatelessCommitService.STATELESS_UPLOAD_AVERAGE_THROUGHPUT_INITIAL_VALUE.getKey(), ByteSizeValue.ofBytes(1))
                // Disable caching of time values to make sure we make progress every time UploadQueueControllerService#runNow() is called.
                .put(ThreadPool.ESTIMATED_TIME_INTERVAL_SETTING.getKey(), TimeValue.ZERO)
                .build()
        );

        final String indexName = randomIndexName();
        createIndex(indexName, 1, 0);
        ensureGreen(indexName);

        // Block uploads to create a backlog.
        var uploadStarted = new CountDownLatch(1);
        var blockUploadLatch = new CountDownLatch(1);
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
                safeAwait(blockUploadLatch);
                super.blobContainerWriteBlobAtomic(originalRunnable, purpose, blobName, inputStream, blobSize, failIfAlreadyExists);
            }
        });

        indexDocs(indexName, 1000);
        refresh(indexName);
        safeAwait(uploadStarted);

        // Now we need to build sufficient backlog.
        var statelessCommitService = internalCluster().getInstance(StatelessCommitService.class, indexNode);
        while (statelessCommitService.getShardCommitStats().iterator().next().pendingUploadBytes() < ByteSizeValue.ofMb(1).getBytes()) {
            indexDocs(indexName, 1000);
            refresh(indexName);
        }

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
