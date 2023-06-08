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

package co.elastic.elasticsearch.stateless.engine;

import co.elastic.elasticsearch.stateless.Stateless;

import org.elasticsearch.action.ActionListener;
import org.elasticsearch.action.support.PlainActionFuture;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.core.TimeValue;
import org.elasticsearch.index.IndexSettings;
import org.elasticsearch.index.engine.EngineConfig;
import org.elasticsearch.index.translog.Translog;
import org.mockito.Mockito;
import org.mockito.stubbing.Answer;

import java.io.IOException;
import java.util.Collections;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.function.LongSupplier;

import static org.mockito.Mockito.any;
import static org.mockito.Mockito.doAnswer;
import static org.mockito.Mockito.eq;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

public class IndexEngineTests extends AbstractEngineTestCase {

    public void testPeriodicallyFlushesRegardlessOfIndexing() throws Exception {
        var settings = Settings.builder().put(IndexSettings.INDEX_REFRESH_INTERVAL_SETTING.getKey(), TimeValue.timeValueMillis(10)).build();
        try (var engine = Mockito.spy(newIndexEngine(copy(indexConfig(), settings, System::nanoTime)));) {
            int numberOfFlushes = randomIntBetween(1, 10);
            CountDownLatch latch = new CountDownLatch(numberOfFlushes);
            Mockito.doAnswer(invocation -> {
                latch.countDown();
                return invocation.callRealMethod();
            }).when(engine).performScheduledFlush();
            engine.onSettingsChanged(); // Refresh the reference on the scheduledFlush method

            assertTrue(latch.await(10, TimeUnit.SECONDS));
        }
    }

    public void testAdjustsPeriodicFlushingIntervalInCaseOfManualFlushes() throws Exception {
        long flushInterval = TimeUnit.MILLISECONDS.toNanos(10);
        var settings = Settings.builder()
            .put(IndexSettings.INDEX_REFRESH_INTERVAL_SETTING.getKey(), TimeValue.timeValueNanos(flushInterval))
            .build();
        try (var engine = Mockito.spy(newIndexEngine(copy(indexConfig(), settings, System::nanoTime)));) {
            int numberOfFlushes = randomIntBetween(5, 10);
            CountDownLatch latch = new CountDownLatch(numberOfFlushes);
            Mockito.doAnswer(invocation -> {
                // Keep flushing at flushInterval, don't flush if a manual flush happened recently
                assertTrue(System.nanoTime() - engine.getLastFlushNanos() >= flushInterval);
                latch.countDown();
                return invocation.callRealMethod();
            }).when(engine).performScheduledFlush();
            engine.onSettingsChanged(); // Refresh the reference on the scheduledFlush method

            AtomicBoolean running = new AtomicBoolean(true);
            Thread manualFlushThread = new Thread(() -> {
                while (running.get()) {
                    if (randomBoolean()) {
                        engine.flush();
                    }
                    try {
                        Thread.sleep(randomIntBetween(10, 50));
                    } catch (InterruptedException e) {
                        Thread.currentThread().interrupt();
                        break;
                    }
                }
            });
            manualFlushThread.start();

            assertTrue(latch.await(10, TimeUnit.SECONDS));
            running.set(false);
            manualFlushThread.join();
        }
    }

    public void testPeriodicFlushGracefullyHandlesException() throws Exception {
        var settings = Settings.builder().put(IndexSettings.INDEX_REFRESH_INTERVAL_SETTING.getKey(), TimeValue.timeValueMillis(10)).build();
        try (var engine = Mockito.spy(newIndexEngine(copy(indexConfig(), settings, System::nanoTime)));) {
            int numberOfFlushes = 2;
            CountDownLatch latch = new CountDownLatch(numberOfFlushes);
            Mockito.doAnswer(invocation -> {
                latch.countDown();
                if (latch.getCount() > 0) {
                    throw new IOException("Flush Exception");
                }
                return invocation.callRealMethod();
            }).when(engine).performScheduledFlush();
            engine.onSettingsChanged(); // Refresh the reference on the scheduledFlush method

            assertTrue(latch.await(10, TimeUnit.SECONDS));
        }
    }

    public void testAsyncEnsureSync() throws Exception {
        TranslogReplicator replicator = mock(TranslogReplicator.class);
        try (var engine = newIndexEngine(indexConfig(), replicator)) {
            Translog.Location location = new Translog.Location(0, 0, 0);
            PlainActionFuture<Void> future = PlainActionFuture.newFuture();
            doAnswer((Answer<Void>) invocation -> {
                future.onResponse(null);
                return null;
            }).when(replicator).sync(eq(engine.config().getShardId()), eq(location), any());
            engine.asyncEnsureTranslogSynced(location, e -> {
                if (e == null) {
                    future.onResponse(null);
                } else {
                    future.onFailure(e);
                }
            });
            verify(replicator).sync(eq(engine.config().getShardId()), eq(location), any());
            future.actionGet();
        }
    }

    public void testSync() throws Exception {
        TranslogReplicator replicator = mock(TranslogReplicator.class);
        try (var engine = newIndexEngine(indexConfig(), replicator)) {
            doAnswer((Answer<Void>) invocation -> {
                ActionListener<Void> listener = invocation.getArgument(1);
                listener.onResponse(null);
                return null;
            }).when(replicator).syncAll(eq(engine.config().getShardId()), any());
            engine.syncTranslog();
            verify(replicator).syncAll(eq(engine.config().getShardId()), any());
        }
    }

    public void testSyncIsNeededIfTranslogReplicatorHasUnsyncedData() throws Exception {
        TranslogReplicator replicator = mock(TranslogReplicator.class);
        try (var engine = newIndexEngine(indexConfig(), replicator)) {
            assertFalse(engine.isTranslogSyncNeeded());
            when(replicator.isSyncNeeded(engine.config().getShardId())).thenReturn(true);
            assertTrue(engine.isTranslogSyncNeeded());
        }
    }

    public void testRefreshNeededBasedOnFastRefresh() throws Exception {
        Settings nodeSettings = Settings.builder().put(Stateless.STATELESS_ENABLED.getKey(), true).build();
        try (var engine = newIndexEngine(indexConfig(Settings.EMPTY, nodeSettings))) {
            engine.index(randomDoc(String.valueOf(0)));
            // Refresh to warm-up engine
            engine.maybeRefresh("test");
            assertFalse(engine.refreshNeeded());
            engine.index(randomDoc(String.valueOf(1)));
            assertFalse(engine.refreshNeeded());
        }

        try (
            var engine = newIndexEngine(
                indexConfig(
                    Settings.builder()
                        .put(IndexSettings.INDEX_FAST_REFRESH_SETTING.getKey(), true)
                        .put(IndexSettings.INDEX_REFRESH_INTERVAL_SETTING.getKey(), TimeValue.timeValueSeconds(60))
                        .build(),
                    nodeSettings
                )
            )
        ) {
            engine.index(randomDoc(String.valueOf(0)));
            // Refresh to warm-up engine
            engine.maybeRefresh("test");
            assertFalse(engine.refreshNeeded());
            engine.index(randomDoc(String.valueOf(1)));
            assertTrue(engine.refreshNeeded());
        }
    }

    private EngineConfig copy(EngineConfig config, Settings additionalIndexSettings, LongSupplier relativeTimeInNanosSupplier) {
        return new EngineConfig(
            config.getShardId(),
            config.getThreadPool(),
            new IndexSettings(
                config.getIndexSettings().getIndexMetadata(),
                Settings.builder().put(config.getIndexSettings().getNodeSettings()).put(additionalIndexSettings).build()
            ),
            config.getWarmer(),
            config.getStore(),
            config.getMergePolicy(),
            config.getAnalyzer(),
            config.getSimilarity(),
            config.getCodecService(),
            config.getEventListener(),
            config.getQueryCache(),
            config.getQueryCachingPolicy(),
            config.getTranslogConfig(),
            config.getFlushMergesAfter(),
            config.getExternalRefreshListener(),
            Collections.emptyList(),
            config.getIndexSort(),
            config.getCircuitBreakerService(),
            config.getGlobalCheckpointSupplier(),
            config.retentionLeasesSupplier(),
            config.getPrimaryTermSupplier(),
            config.getSnapshotCommitSupplier(),
            config.getLeafSorter(),
            relativeTimeInNanosSupplier,
            config.getIndexCommitListener(),
            config.isPromotableToPrimary()
        );
    }
}
