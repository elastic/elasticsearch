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

import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.core.TimeValue;
import org.elasticsearch.index.IndexSettings;
import org.elasticsearch.index.engine.EngineConfig;
import org.mockito.Mockito;

import java.io.IOException;
import java.util.Collections;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.function.LongSupplier;

public class IndexEngineTests extends AbstractEngineTestCase {

    public void testPeriodicallyFlushesRegardlessOfIndexing() throws Exception {
        var settings = Settings.builder().put(IndexEngine.INDEX_FLUSH_INTERVAL_SETTING.getKey(), TimeValue.timeValueMillis(10)).build();
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
            .put(IndexEngine.INDEX_FLUSH_INTERVAL_SETTING.getKey(), TimeValue.timeValueNanos(flushInterval))
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

            Thread manualFlushThread = new Thread(() -> {
                while (Thread.currentThread().isInterrupted() == false) {
                    if (randomBoolean()) {
                        engine.flush(false, false);
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
            manualFlushThread.interrupt();
            manualFlushThread.join();
        }
    }

    public void testPeriodicFlushGracefullyHandlesException() throws Exception {
        var settings = Settings.builder().put(IndexEngine.INDEX_FLUSH_INTERVAL_SETTING.getKey(), TimeValue.timeValueMillis(10)).build();
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
