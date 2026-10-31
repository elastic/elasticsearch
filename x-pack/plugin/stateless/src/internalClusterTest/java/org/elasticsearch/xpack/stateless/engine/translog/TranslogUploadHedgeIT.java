/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.stateless.engine.translog;

import org.elasticsearch.action.ActionListener;
import org.elasticsearch.cluster.project.ProjectResolver;
import org.elasticsearch.cluster.service.ClusterService;
import org.elasticsearch.common.bytes.BytesReference;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.core.TimeValue;
import org.elasticsearch.index.IndexSettings;
import org.elasticsearch.plugins.Plugin;
import org.elasticsearch.repositories.RepositoriesService;
import org.elasticsearch.telemetry.TestTelemetryPlugin;
import org.elasticsearch.telemetry.metric.MeterRegistry;
import org.elasticsearch.threadpool.ThreadPool;
import org.elasticsearch.xpack.stateless.AbstractStatelessPluginIntegTestCase;
import org.elasticsearch.xpack.stateless.TestUtils;
import org.elasticsearch.xpack.stateless.objectstore.ObjectStoreService;
import org.junit.Before;

import java.util.ArrayList;
import java.util.Collection;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.BooleanSupplier;
import java.util.function.IntConsumer;

import static org.elasticsearch.xpack.stateless.engine.translog.TranslogReplicator.TRANSLOG_UPLOAD_HEDGES_TOTAL;
import static org.elasticsearch.xpack.stateless.engine.translog.TranslogReplicator.TRANSLOG_UPLOAD_HEDGES_WON_TOTAL;
import static org.hamcrest.Matchers.empty;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.lessThanOrEqualTo;

public class TranslogUploadHedgeIT extends AbstractStatelessPluginIntegTestCase {
    private static final AtomicInteger uploadCalls = new AtomicInteger();
    private static volatile IntConsumer afterOnBeforeUpload;

    @Before
    public void resetUploadHooks() {
        uploadCalls.set(0);

        // call before `onBeforeUpload`: for `call == 1` this corresponds to the normal upload (not the hedge) because the first
        // `uploadTranslogFile` is the normal upload that gets `call == 1` and schedules the hedge upload
        afterOnBeforeUpload = call -> {};
    }

    public static class TestStatelessPlugin extends TestUtils.StatelessPluginWithTrialLicense {
        public TestStatelessPlugin(Settings settings) {
            super(settings);
        }

        @Override
        protected ObjectStoreService createObjectStoreService(
            Settings settings,
            RepositoriesService repositoriesService,
            ThreadPool threadPool,
            ClusterService clusterService,
            ProjectResolver projectResolver,
            MeterRegistry meterRegistry
        ) {
            return new ObjectStoreService(settings, repositoriesService, threadPool, clusterService, projectResolver, meterRegistry) {
                @Override
                public void uploadTranslogFile(
                    String fileName,
                    BytesReference reference,
                    BooleanSupplier isAlreadyUploaded,
                    Runnable onBeforeUpload,
                    ActionListener<Void> listener
                ) {
                    final int call = uploadCalls.incrementAndGet();
                    super.uploadTranslogFile(fileName, reference, isAlreadyUploaded, () -> {
                        onBeforeUpload.run();
                        afterOnBeforeUpload.accept(call);
                    }, listener);
                }
            };
        }
    }

    @Override
    protected Collection<Class<? extends Plugin>> nodePlugins() {
        final var plugins = new ArrayList<>(super.nodePlugins());
        plugins.remove(TestUtils.StatelessPluginWithTrialLicense.class);
        plugins.add(TestStatelessPlugin.class);
        plugins.add(TestTelemetryPlugin.class);
        return plugins;
    }

    public void testNormalAndHedgeUploadsWithRandomDelays() throws Exception {
        startMasterOnlyNode();

        // set `hedgeDelay` to 0, so the normal and hedge uploads race with each other,
        // because so a hedge upload starts when a normal uploads starts
        final var indexNode = startIndexNode(hedgeNodeSettings(TimeValue.ZERO));
        ensureStableCluster(2);
        final var indexName = createIndexWithNoAutomaticFlushes();

        // random delay for both normal and hedge uploads
        afterOnBeforeUpload = call -> safeSleep(random().nextLong(0, 100));

        // each bulk is acked only when its translog upload completed, whichever upload wins
        for (int i = 0; i < randomIntBetween(1, 20); i++) {
            assertFalse(indexDocs(indexName, randomIntBetween(1, 20)).hasFailures());
        }

        assertThat(
            counter(TRANSLOG_UPLOAD_HEDGES_WON_TOTAL, indexNode),
            lessThanOrEqualTo(counter(TRANSLOG_UPLOAD_HEDGES_TOTAL, indexNode))
        );

        flush(indexName);
        assertBusy(() -> assertThat(getTranslogReplicator(indexNode).getActiveTranslogFiles(), empty()));
    }

    public void testHedgeWinsWhenNormalUploadIsHeld() throws Exception {
        startMasterOnlyNode();
        final var indexNode = startIndexNode(hedgeNodeSettings(TimeValue.ZERO));
        ensureStableCluster(2);
        final var indexName = createIndexWithNoAutomaticFlushes();

        // hold the normal upload (call 1) so that the hedge upload wins
        final var releaseNormalUpload = new CountDownLatch(1);
        uploadCalls.set(0);
        afterOnBeforeUpload = call -> {
            if (call == 1) {
                safeAwait(releaseNormalUpload);
            }
        };

        assertFalse(indexDocs(indexName, randomIntBetween(1, 20)).hasFailures());

        // since the normal upload is not yet released, the hedge wins
        assertThat(counter(TRANSLOG_UPLOAD_HEDGES_TOTAL, indexNode), equalTo(1L));
        assertThat(counter(TRANSLOG_UPLOAD_HEDGES_WON_TOTAL, indexNode), equalTo(1L));
        releaseNormalUpload.countDown();

        flush(indexName);
        assertBusy(() -> assertThat(getTranslogReplicator(indexNode).getActiveTranslogFiles(), empty()));
    }

    public void testNormalUploadWinsWhenHedgeIsHeld() throws Exception {
        startMasterOnlyNode();
        final var indexNode = startIndexNode(hedgeNodeSettings(TimeValue.ZERO));
        ensureStableCluster(2);
        final var indexName = createIndexWithNoAutomaticFlushes();

        final var hedgeStarted = new CountDownLatch(1);
        final var releaseHedgeUpload = new CountDownLatch(1);
        uploadCalls.set(0);
        afterOnBeforeUpload = call -> {
            if (call == 1) {
                // normal upload waits for the hedge to start (so that we count the hedge)
                safeAwait(hedgeStarted);
            } else if (call == 2) {
                // hold the hedge upload (call 2) until
                hedgeStarted.countDown();
                safeAwait(releaseHedgeUpload);
            }
        };

        assertFalse(indexDocs(indexName, randomIntBetween(1, 20)).hasFailures());

        // since the hedge upload is not yet released, the normal upload wins
        assertThat(counter(TRANSLOG_UPLOAD_HEDGES_TOTAL, indexNode), equalTo(1L));
        assertThat(counter(TRANSLOG_UPLOAD_HEDGES_WON_TOTAL, indexNode), equalTo(0L));
        releaseHedgeUpload.countDown();

        flush(indexName);
        assertBusy(() -> assertThat(getTranslogReplicator(indexNode).getActiveTranslogFiles(), empty()));
    }

    public void testNoHedgeWhenDisabled() {
        startMasterOnlyNode();
        final var indexNode = startIndexNode(hedgeNodeSettings(TimeValue.MINUS_ONE));
        ensureStableCluster(2);
        final var indexName = createIndexWithNoAutomaticFlushes();

        assertFalse(indexDocs(indexName, randomIntBetween(1, 20)).hasFailures());

        // no hedges took place since hedge uploads are disabled
        assertThat(counter(TRANSLOG_UPLOAD_HEDGES_TOTAL, indexNode), equalTo(0L));
        assertThat(counter(TRANSLOG_UPLOAD_HEDGES_WON_TOTAL, indexNode), equalTo(0L));
    }

    private static long counter(String name, String node) {
        return getTotalLongCounterValue(name, getTelemetryPlugin(node));
    }

    private static Settings hedgeNodeSettings(TimeValue hedgeDelay) {
        return Settings.builder()
            .put(disableIndexingDiskAndMemoryControllersNodeSettings())
            .put(TranslogReplicator.UPLOAD_HEDGE_DELAY_SETTING.getKey(), hedgeDelay)
            .build();
    }

    // create an index that keeps translogs until flushed
    private String createIndexWithNoAutomaticFlushes() {
        final var indexName = randomIndexName();
        createIndex(
            indexName,
            indexSettings(1, 0)
                // no refresh and no automatic flush: the ack-ed documents must only exist in the translog
                .put(IndexSettings.INDEX_REFRESH_INTERVAL_SETTING.getKey(), TimeValue.MINUS_ONE)
                .put(IndexSettings.INDEX_TRANSLOG_FLUSH_THRESHOLD_SIZE_SETTING.getKey(), "1gb")
                .build()
        );

        ensureGreen(indexName);
        return indexName;
    }
}
