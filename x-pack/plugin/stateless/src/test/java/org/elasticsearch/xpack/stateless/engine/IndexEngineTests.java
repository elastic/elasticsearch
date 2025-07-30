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
import co.elastic.elasticsearch.stateless.cache.SharedBlobCacheWarmingService;
import co.elastic.elasticsearch.stateless.commits.HollowShardsService;
import co.elastic.elasticsearch.stateless.commits.ShardLocalCommitsTracker;
import co.elastic.elasticsearch.stateless.commits.ShardLocalReadersTracker;
import co.elastic.elasticsearch.stateless.commits.StatelessCommitService;
import co.elastic.elasticsearch.stateless.engine.translog.TranslogRecoveryMetrics;
import co.elastic.elasticsearch.stateless.engine.translog.TranslogReplicator;
import co.elastic.elasticsearch.stateless.objectstore.ObjectStoreService;

import org.apache.lucene.document.KnnFloatVectorField;
import org.apache.lucene.index.NoMergePolicy;
import org.apache.lucene.index.SegmentInfos;
import org.elasticsearch.action.ActionListener;
import org.elasticsearch.action.support.PlainActionFuture;
import org.elasticsearch.common.CheckedBiFunction;
import org.elasticsearch.common.UUIDs;
import org.elasticsearch.common.lucene.uid.Versions;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.core.TimeValue;
import org.elasticsearch.index.IndexSettings;
import org.elasticsearch.index.VersionType;
import org.elasticsearch.index.engine.Engine;
import org.elasticsearch.index.engine.EngineConfig;
import org.elasticsearch.index.engine.EngineException;
import org.elasticsearch.index.engine.MergeMemoryEstimator;
import org.elasticsearch.index.engine.MergeMetrics;
import org.elasticsearch.index.mapper.MapperService;
import org.elasticsearch.index.merge.OnGoingMerge;
import org.elasticsearch.index.translog.Translog;
import org.elasticsearch.plugins.internal.DocumentParsingProvider;
import org.elasticsearch.plugins.internal.DocumentSizeAccumulator;
import org.elasticsearch.plugins.internal.DocumentSizeReporter;
import org.elasticsearch.telemetry.TelemetryProvider;
import org.junit.After;
import org.mockito.Mockito;
import org.mockito.stubbing.Answer;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.NavigableMap;
import java.util.Set;
import java.util.TreeMap;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.BiConsumer;
import java.util.stream.LongStream;

import static co.elastic.elasticsearch.stateless.commits.StatelessCompoundCommit.HOLLOW_TRANSLOG_RECOVERY_START_FILE;
import static org.elasticsearch.index.engine.Engine.Operation.Origin.PRIMARY;
import static org.elasticsearch.index.engine.EngineTestCase.newUid;
import static org.elasticsearch.index.seqno.SequenceNumbers.UNASSIGNED_SEQ_NO;
import static org.elasticsearch.search.vectors.KnnSearchBuilderTests.randomVector;
import static org.elasticsearch.test.ActionListenerUtils.anyActionListener;
import static org.hamcrest.Matchers.empty;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.greaterThan;
import static org.hamcrest.Matchers.lessThanOrEqualTo;
import static org.mockito.ArgumentMatchers.anyLong;
import static org.mockito.Mockito.any;
import static org.mockito.Mockito.doAnswer;
import static org.mockito.Mockito.eq;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

public class IndexEngineTests extends AbstractEngineTestCase {

    @After
    public void assertWarningHeaders() {
        assertWarnings(
            "[indices.merge.scheduler.use_thread_pool] setting was deprecated in Elasticsearch and will be removed in a future release. "
                + "See the breaking changes documentation for the next major version."
        );
    }

    public void testAsyncEnsureSync() throws Exception {
        TranslogReplicator replicator = mock(TranslogReplicator.class);
        try (var engine = newIndexEngine(indexConfig(), replicator)) {
            Translog.Location location = new Translog.Location(0, 0, 0);
            PlainActionFuture<Void> future = new PlainActionFuture<>();
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

    public void testRefreshNeededBasedOnSearcherAndCommits() throws Exception {
        Settings nodeSettings = Settings.builder().put(Stateless.STATELESS_ENABLED.getKey(), true).build();
        try (
            var engine = newIndexEngine(
                indexConfig(
                    Settings.builder()
                        .put(IndexSettings.INDEX_FAST_REFRESH_SETTING.getKey(), randomBoolean())
                        .put(IndexSettings.INDEX_REFRESH_INTERVAL_SETTING.getKey(), TimeValue.timeValueSeconds(60))
                        .build(),
                    nodeSettings,
                    () -> 1L,
                    NoMergePolicy.INSTANCE
                )
            )
        ) {
            engine.index(randomDoc(String.valueOf(0)));
            // Refresh to warm-up engine
            engine.refresh("test");
            // Need refresh because only maybeRefresh / externalRefreshes commit
            assertTrue(engine.refreshNeeded());
            engine.index(randomDoc(String.valueOf(1)));
            // Still need refresh
            assertTrue(engine.refreshNeeded());
            if (randomBoolean()) {
                PlainActionFuture<Engine.RefreshResult> future = new PlainActionFuture<>();
                engine.externalRefresh("test", future);
                future.actionGet();
            } else {
                PlainActionFuture<Engine.RefreshResult> future = new PlainActionFuture<>();
                engine.maybeRefresh("test", future);
                future.actionGet();
            }
            assertFalse(engine.refreshNeeded());
        }
    }

    public void testRefreshesDoesNotWaitForUploadWithStatelessUploadDelayed() throws IOException {
        Settings nodeSettings = Settings.builder().put(Stateless.STATELESS_ENABLED.getKey(), true).build();

        try (
            var engine = newIndexEngine(
                indexConfig(
                    Settings.builder().put(IndexSettings.INDEX_REFRESH_INTERVAL_SETTING.getKey(), TimeValue.MINUS_ONE).build(),
                    nodeSettings,
                    () -> 1L,
                    NoMergePolicy.INSTANCE
                )
            )
        ) {
            doTestRefreshesWaitForUploadBehaviours(engine);
        }
    }

    private void doTestRefreshesWaitForUploadBehaviours(IndexEngine engine) throws IOException {
        var statelessCommitService = engine.getStatelessCommitService();

        // External refresh
        engine.index(randomDoc(String.valueOf(0)));
        assertTrue(engine.refreshNeeded());
        var future = new PlainActionFuture<Engine.RefreshResult>();
        engine.externalRefresh("test", future);
        Engine.RefreshResult refreshResult = future.actionGet();
        assertFalse(engine.refreshNeeded());
        verify(statelessCommitService, never()).addListenerForUploadedGeneration(any(), anyLong(), anyActionListener());

        // Scheduled refresh
        engine.index(randomDoc(String.valueOf(1)));
        assertTrue(engine.refreshNeeded());
        future = new PlainActionFuture<>();
        engine.maybeRefresh("test", future);
        refreshResult = future.actionGet();
        assertFalse(engine.refreshNeeded());
        verify(statelessCommitService, never()).addListenerForUploadedGeneration(any(), anyLong(), anyActionListener());

        // Internal refresh RTG
        engine.index(randomDoc(String.valueOf(2)));
        assertTrue(engine.refreshNeeded());
        refreshResult = engine.refreshInternalSearcher(randomFrom("realtime_get", "unsafe_version_map"), true);
        verify(statelessCommitService, never()).addListenerForUploadedGeneration(any(), anyLong(), anyActionListener());
    }

    public void testFlushesWaitForUpload() throws IOException {
        Settings nodeSettings = Settings.builder().put(Stateless.STATELESS_ENABLED.getKey(), true).build();

        try (
            var engine = newIndexEngine(
                indexConfig(
                    Settings.builder().put(IndexSettings.INDEX_REFRESH_INTERVAL_SETTING.getKey(), TimeValue.MINUS_ONE).build(),
                    nodeSettings,
                    () -> 1L,
                    NoMergePolicy.INSTANCE
                )
            )
        ) {
            final var statelessCommitService = engine.getStatelessCommitService();
            engine.index(randomDoc(String.valueOf(0)));
            final PlainActionFuture<Engine.FlushResult> future = new PlainActionFuture<>();
            final boolean force = randomBoolean();
            engine.flush(force, force || randomBoolean(), future);
            final Engine.FlushResult flushResult = future.actionGet();
            verify(statelessCommitService, times(1)).addListenerForUploadedGeneration(
                any(),
                eq(flushResult.generation()),
                anyActionListener()
            );
        }
    }

    public void testStartingTranslogFileIsPlaceInUserData() throws Exception {
        Settings nodeSettings = Settings.builder().put(Stateless.STATELESS_ENABLED.getKey(), true).build();

        TranslogReplicator replicator = mock(TranslogReplicator.class);
        long maxUploadedFile = randomLongBetween(10, 20);
        when(replicator.getMaxUploadedFile()).thenReturn(maxUploadedFile, maxUploadedFile + 1);

        try (
            var engine = newIndexEngine(
                indexConfig(
                    Settings.builder().put(IndexSettings.INDEX_REFRESH_INTERVAL_SETTING.getKey(), TimeValue.timeValueSeconds(60)).build(),
                    nodeSettings
                ),
                replicator
            )
        ) {
            engine.index(randomDoc(String.valueOf(0)));
            // Refresh to warm-up engine
            engine.refresh("test");
            engine.flush();
            try (Engine.IndexCommitRef indexCommitRef = engine.acquireLastIndexCommit(false)) {
                assertEquals(
                    maxUploadedFile + 1,
                    Long.parseLong(indexCommitRef.getIndexCommit().getUserData().get(IndexEngine.TRANSLOG_RECOVERY_START_FILE))
                );
                assertFalse(indexCommitRef.getIndexCommit().getUserData().containsKey(IndexEngine.TRANSLOG_RELEASE_END_FILE));
            }
        }
    }

    public void testStartingAndEndTranslogFilesForHollowCommitInUserData() throws Exception {
        Settings nodeSettings = Settings.builder().put(Stateless.STATELESS_ENABLED.getKey(), true).build();

        TranslogReplicator replicator = mock(TranslogReplicator.class);
        long maxUploadedFile = randomLongBetween(10, 20);
        when(replicator.getMaxUploadedFile()).thenReturn(maxUploadedFile, maxUploadedFile + 1);

        try (
            var engine = newIndexEngine(
                indexConfig(
                    Settings.builder().put(IndexSettings.INDEX_REFRESH_INTERVAL_SETTING.getKey(), TimeValue.timeValueSeconds(60)).build(),
                    nodeSettings
                ),
                replicator
            )
        ) {
            engine.index(randomDoc(String.valueOf(0)));
            engine.flushHollow(ActionListener.noop());
            try (Engine.IndexCommitRef indexCommitRef = engine.acquireLastIndexCommit(false)) {
                assertEquals(
                    HOLLOW_TRANSLOG_RECOVERY_START_FILE,
                    Long.parseLong(indexCommitRef.getIndexCommit().getUserData().get(IndexEngine.TRANSLOG_RECOVERY_START_FILE))
                );
                assertEquals(
                    maxUploadedFile + 1,
                    Long.parseLong(indexCommitRef.getIndexCommit().getUserData().get(IndexEngine.TRANSLOG_RELEASE_END_FILE))
                );
            }
        }
    }

    /**
     * Test that the engine calls the closed reader notification for every reader that it no longer uses.
     */
    public void testClosedReaders() throws IOException {
        TranslogReplicator translogReplicator = mock(TranslogReplicator.class);
        StatelessCommitService commitService = mockCommitService(Settings.EMPTY);
        Set<PrimaryTermAndGeneration> openReaderGenerations = new HashSet<>();
        Set<Long> closedReaderGenerations = new HashSet<>();
        when(commitService.getCommitBCCResolverForShard(any())).thenReturn(commitGeneration -> {
            var commitPrimaryTermAndGeneration = new PrimaryTermAndGeneration(1, commitGeneration);
            openReaderGenerations.add(commitPrimaryTermAndGeneration);
            return Set.of(commitPrimaryTermAndGeneration);
        });
        when(commitService.getShardLocalCommitsTracker(any())).thenReturn(
            new ShardLocalCommitsTracker(new ShardLocalReadersTracker((bccHoldingClosedCommit, openBCCs) -> {
                for (PrimaryTermAndGeneration primaryTermAndGeneration : openReaderGenerations) {
                    if (openBCCs.contains(primaryTermAndGeneration) == false) {
                        closedReaderGenerations.add(primaryTermAndGeneration.generation());
                    }
                }
                openReaderGenerations.removeIf(gen -> openBCCs.contains(gen) == false);
            }), null)
        );
        Settings nodeSettings = Settings.builder().put(Stateless.STATELESS_ENABLED.getKey(), true).build();
        try (
            var engine = newIndexEngine(
                indexConfig(
                    Settings.builder()
                        .put(IndexSettings.INDEX_FAST_REFRESH_SETTING.getKey(), true)
                        .put(IndexSettings.INDEX_REFRESH_INTERVAL_SETTING.getKey(), TimeValue.timeValueSeconds(60))
                        .build(),
                    nodeSettings,
                    () -> 1L,
                    NoMergePolicy.INSTANCE
                ),
                translogReplicator,
                mock(ObjectStoreService.class),
                commitService,
                mock(HollowShardsService.class)
            )
        ) {
            final var initialGen = engine.getCurrentGeneration();
            engine.index(randomDoc(String.valueOf(0)));
            if (randomBoolean()) {
                engine.refresh("test");
            }
            engine.flush(true, true);
            assertThat(closedReaderGenerations, empty());
            // external reader manager refresh.
            engine.refresh("test");
            Set<Long> expectedClosedReaderGenerations = new HashSet<>();
            expectedClosedReaderGenerations.add(initialGen);
            assertThat(closedReaderGenerations, equalTo(expectedClosedReaderGenerations));

            final var gen = engine.getCurrentGeneration();
            int commits = between(1, 10);
            NavigableMap<Long, Engine.Searcher> searchers = new TreeMap<>();
            for (int i = 0; i < commits; ++i) {
                if (randomBoolean()) {
                    searchers.put(i + gen, engine.acquireSearcher("test"));
                }
                engine.index(randomDoc(String.valueOf(i + 1)));
                engine.flush(true, true);
                engine.refresh("test");
            }

            // closedReaderGenerations must contain [initialGen .. gen + commits) - {open searchers}
            LongStream.range(initialGen, gen + commits)
                .filter(generation -> searchers.containsKey(generation) == false)
                .forEach(expectedClosedReaderGenerations::add);
            assertThat(closedReaderGenerations, equalTo(expectedClosedReaderGenerations));

            while (searchers.isEmpty() == false) {
                long generation = randomFrom(searchers.keySet());
                Engine.Searcher searcher = searchers.remove(generation);
                searcher.close();
                expectedClosedReaderGenerations.add(generation);
                assertThat(closedReaderGenerations, equalTo(expectedClosedReaderGenerations));
            }
        }
    }

    public void testDocSizeIsReportedUponSuccessfulIndexAndAlwaysWhenParsingFinished() throws IOException {
        TranslogReplicator mockTranslogReplicator = mock(TranslogReplicator.class);
        StatelessCommitService mockCommitService = mockCommitService(Settings.EMPTY);
        DocumentParsingProvider documentParsingProvider = mock(DocumentParsingProvider.class);
        when(documentParsingProvider.createDocumentSizeAccumulator()).thenReturn(DocumentSizeAccumulator.EMPTY_INSTANCE);
        MapperService mapperService = mock(MapperService.class);
        EngineConfig indexConfig = indexConfig(mapperService);
        DocumentSizeReporter documentSizeReporter = mock(DocumentSizeReporter.class);
        when(
            documentParsingProvider.newDocumentSizeReporter(
                eq(indexConfig.getShardId().getIndex()),
                eq(mapperService),
                any(DocumentSizeAccumulator.class)
            )
        ).thenReturn(documentSizeReporter);
        try (
            var engine = newIndexEngine(
                indexConfig,
                mockTranslogReplicator,
                mock(ObjectStoreService.class),
                mockCommitService,
                mock(HollowShardsService.class),
                mock(SharedBlobCacheWarmingService.class),
                documentParsingProvider,
                new IndexEngine.EngineMetrics(TranslogRecoveryMetrics.NOOP, MergeMetrics.NOOP, HollowShardsMetrics.NOOP)
            )
        ) {
            Engine.Index index = randomDoc("id");

            Engine.IndexResult result = engine.index(index);
            assertThat(result.getResultType(), equalTo(Engine.Result.Type.SUCCESS));
            verify(documentSizeReporter).onParsingCompleted(eq(index.parsedDoc()));
            verify(documentSizeReporter).onIndexingCompleted(eq(index.parsedDoc()));

            Engine.Index newIndex = randomDoc("id");
            var failIndex = versionConflictingIndexOperation(newIndex);
            result = engine.index(failIndex);

            assertThat(result.getResultType(), equalTo(Engine.Result.Type.FAILURE));
            verify(documentSizeReporter).onParsingCompleted(eq(index.parsedDoc())); // we report parsing before indexing
            verify(documentSizeReporter, times(0)).onIndexingCompleted(eq(newIndex.parsedDoc()));
        }
    }

    public void testCommitUserDataIsEnrichedByAccumulatorData() throws IOException {
        TranslogReplicator mockTranslogReplicator = mock(TranslogReplicator.class);
        when(mockTranslogReplicator.getMaxUploadedFile()).thenReturn(456L);

        StatelessCommitService mockCommitService = mockCommitService(Settings.EMPTY);
        DocumentParsingProvider documentParsingProvider = mock(DocumentParsingProvider.class);
        DocumentSizeAccumulator documentSizeAccumulator = mock(DocumentSizeAccumulator.class);
        when(documentSizeAccumulator.getAsCommitUserData(any(SegmentInfos.class))).thenReturn(Map.of("accumulator_field", "123"));
        when(documentParsingProvider.createDocumentSizeAccumulator()).thenReturn(documentSizeAccumulator);

        MapperService mapperService = Mockito.mock(MapperService.class);
        EngineConfig indexConfig = indexConfig(mapperService);
        DocumentSizeReporter documentSizeReporter = mock(DocumentSizeReporter.class);
        when(
            documentParsingProvider.newDocumentSizeReporter(
                eq(indexConfig.getShardId().getIndex()),
                eq(mapperService),
                eq(documentSizeAccumulator)
            )
        ).thenReturn(documentSizeReporter);

        try (
            var engine = newIndexEngine(
                indexConfig,
                mockTranslogReplicator,
                mock(ObjectStoreService.class),
                mockCommitService,
                mock(HollowShardsService.class),
                mock(SharedBlobCacheWarmingService.class),
                documentParsingProvider,
                new IndexEngine.EngineMetrics(TranslogRecoveryMetrics.NOOP, MergeMetrics.NOOP, HollowShardsMetrics.NOOP)
            )
        ) {
            engine.index(randomDoc(String.valueOf(0)));
            engine.flush();

            try (Engine.IndexCommitRef indexCommitRef = engine.acquireLastIndexCommit(false)) {
                assertEquals(123L, Long.parseLong(indexCommitRef.getIndexCommit().getUserData().get("accumulator_field")));
                assertEquals(
                    457L,
                    Long.parseLong(indexCommitRef.getIndexCommit().getUserData().get(IndexEngine.TRANSLOG_RECOVERY_START_FILE))
                );
            }
        }
    }

    public void testRefreshCanReleaseFlushListener() throws IOException {
        try (
            var engine = newIndexEngine(
                indexConfig(
                    Settings.builder().put(IndexSettings.INDEX_REFRESH_INTERVAL_SETTING.getKey(), TimeValue.MINUS_ONE).build(),
                    Settings.builder().put(Stateless.STATELESS_ENABLED.getKey(), true).build(),
                    () -> 1L,
                    NoMergePolicy.INSTANCE
                )
            )
        ) {
            final var statelessCommitService = engine.getStatelessCommitService();
            engine.index(randomDoc(String.valueOf(0)));
            final Translog.Location location = engine.getTranslogLastWriteLocation();
            // Refresh for a new commit generation. It is not uploaded.
            var future = new PlainActionFuture<Engine.RefreshResult>();
            engine.externalRefresh("test", future);
            final long generation = safeGet(future).generation();

            // Add a flush listener for the flushed translog location. It should be completed without
            // the need to register and wait for the commit generation to be uploaded.
            final PlainActionFuture<Long> flushListener = new PlainActionFuture<>();
            engine.addFlushListener(location, flushListener);
            // No commit should be uploaded and no listener should be registered for waiting any uploads
            verify(statelessCommitService, never()).ensureMaxGenerationToUploadForFlush(any(), anyLong());
            verify(statelessCommitService, never()).addListenerForUploadedGeneration(any(), anyLong(), anyActionListener());
            assertThat(safeGet(flushListener), equalTo(generation));
        }
    }

    private static Engine.Index versionConflictingIndexOperation(Engine.Index indexOp) throws IOException {
        return new Engine.Index(
            newUid(indexOp.parsedDoc()),
            indexOp.parsedDoc(),
            UNASSIGNED_SEQ_NO,
            1,
            Versions.MATCH_DELETED,
            VersionType.INTERNAL,
            PRIMARY,
            0,
            -1,
            false,
            UNASSIGNED_SEQ_NO,
            0
        );
    }

    public void testHollowEngineUserData() throws Exception {
        try (var engine = newIndexEngine(indexConfig())) {
            engine.index(randomDoc(String.valueOf(0)));
            assertFalse(engine.isLastCommitHollow());
            final PlainActionFuture<Engine.FlushResult> future = new PlainActionFuture<>();
            engine.flushHollow(future);
            future.actionGet();
            assertTrue(engine.isLastCommitHollow());
        }
    }

    public void testHollowEngineCannotIngest() throws Exception {
        try (var engine = newIndexEngine(indexConfig())) {
            var indexedDoc = randomDoc(String.valueOf(0));
            engine.index(indexedDoc);
            final PlainActionFuture<Engine.FlushResult> future = new PlainActionFuture<>();
            engine.flushHollow(future);
            future.actionGet();
            final var maxSeqNo = engine.getMaxSeqNo();

            expectThrows(IllegalStateException.class, () -> {
                if (randomBoolean()) {
                    engine.index(randomDoc(String.valueOf(1)));
                } else {
                    engine.delete(new Engine.Delete(indexedDoc.id(), indexedDoc.uid(), 1L));
                }
            });
            assertThat(engine.getMaxSeqNo(), equalTo(maxSeqNo));
        }
    }

    public void testHollowEngineWithConcurrentNonHollowFlush() throws Exception {
        // We issue a first regular flush and we make it stop at the time it tries to get the commit user data.
        // We then issue the hollow flush. We ensure that the regular flush's commit user data is NOT hollow, since it may not have
        // flushed all operations. Only the hollow flush should be marked as hollow.
        final AtomicInteger flushesInitiated = new AtomicInteger(0);
        final var firstFlushGettingUserDataLatch = new CountDownLatch(1);
        final var firstFlushProceedToGetUserDataLatch = new CountDownLatch(1);
        final var hollowFlushProceedToGetUserDataLatch = new CountDownLatch(1);

        final var indexConfig = indexConfig();
        final var commitService = mockCommitService(indexConfig.getIndexSettings().getNodeSettings());
        final var objectStoreService = mock(ObjectStoreService.class);
        try (
            var engine = new IndexEngine(
                indexConfig,
                mock(TranslogReplicator.class),
                objectStoreService::getTranslogBlobContainer,
                mockCommitService(indexConfig.getIndexSettings().getNodeSettings()),
                mock(HollowShardsService.class),
                mock(SharedBlobCacheWarmingService.class),
                RefreshThrottler.Noop::new,
                commitService.getCommitBCCResolverForShard(indexConfig.getShardId()),
                DocumentParsingProvider.EMPTY_INSTANCE,
                new IndexEngine.EngineMetrics(TranslogRecoveryMetrics.NOOP, MergeMetrics.NOOP, HollowShardsMetrics.NOOP),
                commitService.getShardLocalCommitsTracker(indexConfig.getShardId()).shardLocalReadersTracker()
            ) {
                @Override
                public void close() throws IOException {
                    try {
                        super.close();
                    } finally {
                        indexConfig.getStore().decRef();
                    }
                }

                @Override
                protected void flushHoldingLock(boolean force, boolean waitIfOngoing, ActionListener<FlushResult> listener)
                    throws EngineException {
                    flushesInitiated.incrementAndGet();
                    super.flushHoldingLock(force, waitIfOngoing, listener);
                }

                @Override
                protected Map<String, String> getCommitExtraUserData(final long localCheckpoint) {
                    firstFlushGettingUserDataLatch.countDown();
                    int flushNumber = flushesInitiated.get();
                    logger.debug("flush {} trying to get user data", flushNumber);
                    if (flushesInitiated.get() > 1) {
                        safeAwait(hollowFlushProceedToGetUserDataLatch);
                    } else {
                        safeAwait(firstFlushProceedToGetUserDataLatch);
                    }
                    logger.debug("flush {} proceeding to get user data", flushNumber);
                    return super.getCommitExtraUserData(localCheckpoint);
                }
            }
        ) {
            engine.skipTranslogRecovery();
            engine.index(randomDoc(String.valueOf(0)));
            assertFalse(engine.isLastCommitHollow());

            final PlainActionFuture<Engine.FlushResult> firstFlushFuture = new PlainActionFuture<>();
            Thread firstFlushThread = new Thread(() -> { engine.flush(true, true, firstFlushFuture); });
            firstFlushThread.start();
            safeAwait(firstFlushGettingUserDataLatch);

            // This operation is not included in the first flush
            engine.index(randomDoc(String.valueOf(1)));
            assertFalse(engine.isLastCommitHollow());

            final PlainActionFuture<Engine.FlushResult> hollowFlushFuture = new PlainActionFuture<>();
            Thread hollowFlushThread = new Thread(() -> { engine.flushHollow(hollowFlushFuture); });
            hollowFlushThread.start();
            assertBusy(() -> assertThat(flushesInitiated.get(), equalTo(2))); // ensures that the hollow max seqno has been marked

            firstFlushProceedToGetUserDataLatch.countDown();
            firstFlushFuture.actionGet();
            firstFlushThread.join();

            // The first flush should not be marked as hollow, as it did not include doc with ID 1.
            assertFalse(engine.isLastCommitHollow());

            hollowFlushProceedToGetUserDataLatch.countDown();
            hollowFlushFuture.actionGet();
            hollowFlushThread.join();

            // The hollow flush should be marked as hollow, as it includes all operations.
            assertTrue(engine.isLastCommitHollow());
        }
    }

    public void testMergeMemoryEstimationForVectors() throws Exception {
        // Generates docs with vector values
        CheckedBiFunction<Integer, Integer, Engine.Index, IOException> docGenerator = (id, numDimensions) -> randomDoc(
            String.valueOf(id),
            (builder, doc) -> {
                float[] vectorValue = randomVector(numDimensions);

                builder.startObject().field("vector", vectorValue).endObject();
                doc.add(new KnnFloatVectorField("vector", vectorValue));

                int additionalVectors = randomIntBetween(0, 2);
                for (int i = 0; i < additionalVectors; i++) {
                    builder.startObject().field("vector" + i, vectorValue).endObject();
                    doc.add(new KnnFloatVectorField("vector" + i, vectorValue));
                }
            }
        );

        // Checks that the estimated memory size is always greater than 0
        BiConsumer<OnGoingMerge, Long> estimationAssertion = (merge, size) -> {
            assertThat(size, greaterThan(0L));
            int estimatedDocs = merge.getMergedSegments().stream().map(s -> s.info.maxDoc() - s.getDelCount()).reduce(0, Integer::sum);
            // Even if more than one field vector is there, we only estimate the memory for each field
            assertThat(
                "Estimated docs=" + estimatedDocs + ", size=" + size,
                size,
                lessThanOrEqualTo(estimatedDocs * MergeMemoryEstimator.HNSW_PER_DOC_ESTIMATION)
            );
        };

        testMergeMemoryEstimation(estimationAssertion, docGenerator);
    }

    public void testMergeMemoryEstimationForNonVectors() throws Exception {
        // Generates docs without vector values
        CheckedBiFunction<Integer, Integer, Engine.Index, IOException> docGenerator = (id, numDimensions) -> randomDoc(String.valueOf(id));
        // Checks that the estimated memory size is always 0
        BiConsumer<OnGoingMerge, Long> estimationAssertion = (merge, size) -> assertEquals(Long.valueOf(0), size);

        testMergeMemoryEstimation(estimationAssertion, docGenerator);
    }

    private void testMergeMemoryEstimation(
        BiConsumer<OnGoingMerge, Long> estimationAssertion,
        CheckedBiFunction<Integer, Integer, Engine.Index, IOException> docGenerator
    ) throws Exception {
        AtomicInteger ongoingMerges = new AtomicInteger();
        List<Long> ongoingMemoryEstimations = Collections.synchronizedList(new ArrayList<>());
        List<AssertionError> assertionErrors = Collections.synchronizedList(new ArrayList<>());

        MergeMetrics mergeMetrics = new MergeMetrics(TelemetryProvider.NOOP.getMeterRegistry()) {
            @Override
            public void incrementQueuedMergeBytes(OnGoingMerge currentMerge, long estimatedMemorySize) {
                ongoingMerges.getAndAdd(1);
                ongoingMemoryEstimations.add(estimatedMemorySize);
                try {
                    estimationAssertion.accept(currentMerge, estimatedMemorySize);
                } catch (AssertionError e) {
                    // Don't fail here, or merges won't be able to abort
                    assertionErrors.add(e);
                }
            }

            @Override
            public void moveQueuedMergeBytesToRunning(OnGoingMerge currentMerge, long estimatedMemorySize) {
                ongoingMerges.getAndAdd(-1);
                try {
                    assertTrue(ongoingMemoryEstimations.remove(estimatedMemorySize));
                } catch (AssertionError e) {
                    // Don't fail here, or merges won't be able to abort
                    assertionErrors.add(e);
                }
            }
        };

        EngineConfig indexConfig = indexConfig();
        try (
            IndexEngine engine = newIndexEngine(
                indexConfig,
                mock(TranslogReplicator.class),
                mock(ObjectStoreService.class),
                mockCommitService(indexConfig.getIndexSettings().getNodeSettings()),
                mock(HollowShardsService.class),
                mock(SharedBlobCacheWarmingService.class),
                DocumentParsingProvider.EMPTY_INSTANCE,
                new IndexEngine.EngineMetrics(TranslogRecoveryMetrics.NOOP, mergeMetrics, HollowShardsMetrics.NOOP)
            )
        ) {
            int numDocs = randomIntBetween(20, 100);
            int numDimensions = randomIntBetween(10, 1024);
            for (int i = 0; i < numDocs; i++) {
                Engine.Index docIndex = docGenerator.apply(i, numDimensions);
                engine.index(docIndex);

                // Flush 25% of the time
                if (randomFloat() <= 0.25) {
                    engine.flush();
                }
            }
            engine.flush();
            engine.forceMerge(true, 1, false, UUIDs.randomBase64UUID());

            assertBusy(() -> {
                if (assertionErrors.isEmpty() == false) {
                    throw new AssertionError("Merge memory estimation assertion errors: " + assertionErrors.getFirst().getMessage());
                }
                assertEquals(0, ongoingMerges.get());
                assertEquals(0, ongoingMemoryEstimations.size());
            });
        }
    }
}
