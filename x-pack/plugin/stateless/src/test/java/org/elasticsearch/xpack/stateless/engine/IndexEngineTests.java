/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.stateless.engine;

import org.apache.lucene.document.KnnFloatVectorField;
import org.apache.lucene.index.NoMergePolicy;
import org.apache.lucene.index.SegmentInfos;
import org.apache.lucene.util.BytesRef;
import org.elasticsearch.action.ActionListener;
import org.elasticsearch.action.index.IndexRequest;
import org.elasticsearch.action.support.PlainActionFuture;
import org.elasticsearch.cluster.ClusterState;
import org.elasticsearch.cluster.metadata.IndexMetadata;
import org.elasticsearch.common.CheckedBiFunction;
import org.elasticsearch.common.UUIDs;
import org.elasticsearch.common.blobstore.BlobContainer;
import org.elasticsearch.common.blobstore.BlobPath;
import org.elasticsearch.common.blobstore.OperationPurpose;
import org.elasticsearch.common.blobstore.fs.FsBlobContainer;
import org.elasticsearch.common.blobstore.fs.FsBlobStore;
import org.elasticsearch.common.bytes.BytesReference;
import org.elasticsearch.common.component.Lifecycle;
import org.elasticsearch.common.lucene.uid.Versions;
import org.elasticsearch.common.settings.ClusterSettings;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.util.set.Sets;
import org.elasticsearch.core.TimeValue;
import org.elasticsearch.escf.EscfBatch;
import org.elasticsearch.escf.EscfEncoder;
import org.elasticsearch.index.IndexSettings;
import org.elasticsearch.index.VersionType;
import org.elasticsearch.index.engine.Engine;
import org.elasticsearch.index.engine.EngineBatch;
import org.elasticsearch.index.engine.EngineConfig;
import org.elasticsearch.index.engine.EngineException;
import org.elasticsearch.index.engine.EngineTestCase;
import org.elasticsearch.index.engine.IndexOperationBatch;
import org.elasticsearch.index.engine.MergeMemoryEstimator;
import org.elasticsearch.index.engine.MergeMetrics;
import org.elasticsearch.index.engine.ThreadPoolMergeScheduler;
import org.elasticsearch.index.mapper.MapperService;
import org.elasticsearch.index.mapper.MappingLookup;
import org.elasticsearch.index.mapper.SourceFieldMapper;
import org.elasticsearch.index.mapper.Uid;
import org.elasticsearch.index.merge.OnGoingMerge;
import org.elasticsearch.index.shard.DocsStats;
import org.elasticsearch.index.shard.ShardFieldStats;
import org.elasticsearch.index.shard.ShardId;
import org.elasticsearch.index.shard.ShardSplittingQuery;
import org.elasticsearch.index.translog.Translog;
import org.elasticsearch.index.translog.TranslogConfig;
import org.elasticsearch.plugins.internal.DocumentParsingProvider;
import org.elasticsearch.plugins.internal.DocumentSizeAccumulator;
import org.elasticsearch.plugins.internal.DocumentSizeReporter;
import org.elasticsearch.plugins.internal.XContentMeteringParserDecorator;
import org.elasticsearch.sourcebatch.MappedColumns;
import org.elasticsearch.sourcebatch.SourceBatch;
import org.elasticsearch.telemetry.TelemetryProvider;
import org.elasticsearch.threadpool.ThreadPool;
import org.elasticsearch.xcontent.XContentParser;
import org.elasticsearch.xcontent.XContentParserConfiguration;
import org.elasticsearch.xcontent.XContentType;
import org.elasticsearch.xpack.stateless.StatelessPlugin;
import org.elasticsearch.xpack.stateless.cache.SharedBlobCacheWarmingService;
import org.elasticsearch.xpack.stateless.cluster.coordination.StatelessClusterConsistencyService;
import org.elasticsearch.xpack.stateless.commits.HollowShardsService;
import org.elasticsearch.xpack.stateless.commits.ShardLocalCommitsTracker;
import org.elasticsearch.xpack.stateless.commits.ShardLocalReadersTracker;
import org.elasticsearch.xpack.stateless.commits.StatelessCommitService;
import org.elasticsearch.xpack.stateless.commits.StatelessCompoundCommit;
import org.elasticsearch.xpack.stateless.engine.translog.TranslogRecoveryMetrics;
import org.elasticsearch.xpack.stateless.engine.translog.TranslogReplicator;
import org.elasticsearch.xpack.stateless.engine.translog.TranslogReplicatorReader;
import org.elasticsearch.xpack.stateless.objectstore.ObjectStoreService;
import org.elasticsearch.xpack.stateless.reshard.ReshardIndexService;
import org.junit.After;
import org.mockito.Mockito;
import org.mockito.stubbing.Answer;

import java.io.IOException;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
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

import static org.elasticsearch.index.engine.Engine.Operation.Origin.PRIMARY;
import static org.elasticsearch.index.engine.EngineTestCase.newUid;
import static org.elasticsearch.index.seqno.SequenceNumbers.UNASSIGNED_SEQ_NO;
import static org.elasticsearch.search.vectors.KnnSearchBuilderTests.randomVector;
import static org.elasticsearch.test.ActionListenerUtils.anyActionListener;
import static org.elasticsearch.xpack.stateless.commits.StatelessCompoundCommit.HOLLOW_TRANSLOG_RECOVERY_START_FILE;
import static org.hamcrest.Matchers.empty;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.greaterThan;
import static org.hamcrest.Matchers.lessThanOrEqualTo;
import static org.hamcrest.Matchers.not;
import static org.mockito.ArgumentMatchers.anyLong;
import static org.mockito.Mockito.any;
import static org.mockito.Mockito.argThat;
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
        Settings nodeSettings = Settings.builder().put(StatelessPlugin.STATELESS_ENABLED.getKey(), true).build();
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
        Settings nodeSettings = Settings.builder().put(StatelessPlugin.STATELESS_ENABLED.getKey(), true).build();

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
        Settings nodeSettings = Settings.builder().put(StatelessPlugin.STATELESS_ENABLED.getKey(), true).build();

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
        Settings nodeSettings = Settings.builder().put(StatelessPlugin.STATELESS_ENABLED.getKey(), true).build();

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
                    Long.parseLong(indexCommitRef.getIndexCommit().getUserData().get(StatelessCompoundCommit.TRANSLOG_RECOVERY_START_FILE))
                );
                assertFalse(indexCommitRef.getIndexCommit().getUserData().containsKey(IndexEngine.TRANSLOG_RELEASE_END_FILE));
            }
        }
    }

    public void testStartingAndEndTranslogFilesForHollowCommitInUserData() throws Exception {
        Settings nodeSettings = Settings.builder().put(StatelessPlugin.STATELESS_ENABLED.getKey(), true).build();

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
                    Long.parseLong(indexCommitRef.getIndexCommit().getUserData().get(StatelessCompoundCommit.TRANSLOG_RECOVERY_START_FILE))
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
        Settings nodeSettings = Settings.builder().put(StatelessPlugin.STATELESS_ENABLED.getKey(), true).build();
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
        when(mapperService.mappingLookup()).thenReturn(MappingLookup.EMPTY);
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
            verify(documentSizeReporter).onIndexingCompleted(eq(index.parsedDoc()), eq(PRIMARY));

            Engine.Index newIndex = randomDoc("id");
            var failIndex = versionConflictingIndexOperation(newIndex);
            result = engine.index(failIndex);

            assertThat(result.getResultType(), equalTo(Engine.Result.Type.FAILURE));
            verify(documentSizeReporter).onParsingCompleted(eq(index.parsedDoc())); // we report parsing before indexing
            verify(documentSizeReporter, times(0)).onIndexingCompleted(eq(newIndex.parsedDoc()), any(Engine.Operation.Origin.class));
        }
    }

    public void testDocSizeOfReplayedOperationIsReportedWithItsOrigin() throws IOException {
        TranslogReplicator mockTranslogReplicator = mock(TranslogReplicator.class);
        StatelessCommitService mockCommitService = mockCommitService(Settings.EMPTY);
        DocumentParsingProvider documentParsingProvider = mock(DocumentParsingProvider.class);
        when(documentParsingProvider.createDocumentSizeAccumulator()).thenReturn(DocumentSizeAccumulator.EMPTY_INSTANCE);
        MapperService mapperService = mock(MapperService.class);
        when(mapperService.mappingLookup()).thenReturn(MappingLookup.EMPTY);
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
            Engine.Index source = randomDoc("id");
            // an operation that is re-applied from the translog, it has an assigned seqNo and no version type
            Engine.Index replayed = new Engine.Index(
                newUid(source.parsedDoc()),
                source.parsedDoc(),
                0,
                1,
                1,
                null,
                Engine.Operation.Origin.LOCAL_TRANSLOG_RECOVERY,
                0,
                -1,
                false,
                UNASSIGNED_SEQ_NO,
                0
            );

            Engine.IndexResult result = engine.index(replayed);
            assertThat(result.getResultType(), equalTo(Engine.Result.Type.SUCCESS));
            // the document is reported to allow recording its size, the origin allows reporters to not report it as new ingest
            verify(documentSizeReporter).onParsingCompleted(eq(replayed.parsedDoc()));
            verify(documentSizeReporter).onIndexingCompleted(eq(replayed.parsedDoc()), eq(Engine.Operation.Origin.LOCAL_TRANSLOG_RECOVERY));
        }
    }

    public void testMeteringParserDecoratorIsProvidedByDocumentParsingProvider() throws IOException {
        DocumentParsingProvider documentParsingProvider = mock(DocumentParsingProvider.class);
        when(documentParsingProvider.createDocumentSizeAccumulator()).thenReturn(DocumentSizeAccumulator.EMPTY_INSTANCE);
        XContentMeteringParserDecorator meteringDecorator = mock(XContentMeteringParserDecorator.class);
        when(documentParsingProvider.newMeteringParserDecorator()).thenReturn(meteringDecorator);
        try (
            var engine = newIndexEngine(
                indexConfig(),
                mock(TranslogReplicator.class),
                mock(ObjectStoreService.class),
                mockCommitService(Settings.EMPTY),
                mock(HollowShardsService.class),
                mock(SharedBlobCacheWarmingService.class),
                documentParsingProvider,
                new IndexEngine.EngineMetrics(TranslogRecoveryMetrics.NOOP, MergeMetrics.NOOP, HollowShardsMetrics.NOOP)
            )
        ) {
            assertSame(meteringDecorator, engine.newMeteringParserDecorator());
        }
    }

    public void testDocSizeIsReportedUponBatchIndex() throws IOException {
        TranslogReplicator mockTranslogReplicator = mock(TranslogReplicator.class);
        StatelessCommitService mockCommitService = mockCommitService(Settings.EMPTY);
        DocumentParsingProvider documentParsingProvider = mock(DocumentParsingProvider.class);
        when(documentParsingProvider.createDocumentSizeAccumulator()).thenReturn(DocumentSizeAccumulator.EMPTY_INSTANCE);
        MapperService mapperService = mock(MapperService.class);
        when(mapperService.mappingLookup()).thenReturn(MappingLookup.EMPTY);
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
            // Success case: all docs in the batch succeed. The columnar batch path reports metering for the
            // materialized per-op views (see IndexOperationBatch#materializeIndexOps), not the original
            // Engine.Index#parsedDoc() instances, so we match the reported document by id rather than by
            // object identity.
            // TODO: The materialized ParsedDocument carries only id/routing, not the ingested source, so
            // document-size metering for batch-indexed docs is not yet accurate. Revisit once the columnar
            // path threads real per-document sizes through to the reporter.
            List<Engine.Index> ops = List.of(randomDoc("id1"), randomDoc("id2"), randomDoc("id3"));
            List<Engine.IndexResult> results = engine.indexBatch(engineBatch(ops, encodeAsEscfBatch(ops)));
            for (int i = 0; i < results.size(); i++) {
                final String id = ops.get(i).id();
                assertThat(results.get(i).getResultType(), equalTo(Engine.Result.Type.SUCCESS));
                verify(documentSizeReporter).onParsingCompleted(argThat(doc -> doc.id().equals(id)));
                verify(documentSizeReporter).onIndexingCompleted(argThat(doc -> doc.id().equals(id)), eq(PRIMARY));
            }

            // Failure case: a version-conflicting op is parsed but never gets onIndexingCompleted. It reuses
            // id "id1", so onParsingCompleted has now fired twice for that id (success batch + this batch)
            // while onIndexingCompleted stays at the single success-batch call.
            Engine.Index conflictingOp = versionConflictingIndexOperation(randomDoc("id1"));
            List<Engine.Index> failOps = List.of(conflictingOp);
            List<Engine.IndexResult> failResults = engine.indexBatch(engineBatch(failOps, encodeAsEscfBatch(failOps)));
            assertThat(failResults.get(0).getResultType(), equalTo(Engine.Result.Type.FAILURE));
            verify(documentSizeReporter, times(2)).onParsingCompleted(argThat(doc -> doc.id().equals("id1")));
            verify(documentSizeReporter, times(1)).onIndexingCompleted(argThat(doc -> doc.id().equals("id1")), eq(PRIMARY));
        }
    }

    public void testHollowEngineCannotIngestBatch() throws Exception {
        try (var engine = newIndexEngine(indexConfig())) {
            var indexedDoc = randomDoc(String.valueOf(0));
            engine.index(indexedDoc);
            final PlainActionFuture<Engine.FlushResult> future = new PlainActionFuture<>();
            engine.flushHollow(future);
            future.actionGet();
            final var maxSeqNo = engine.getMaxSeqNo();

            List<Engine.Index> batchOps = List.of(randomDoc(String.valueOf(1)));
            expectThrows(IllegalStateException.class, () -> engine.indexBatch(engineBatch(batchOps, encodeAsEscfBatch(batchOps))));
            assertThat(engine.getMaxSeqNo(), equalTo(maxSeqNo));
        }
    }

    public void testReplicaBatchWithSeqNoGapIsReplicatedAsContiguousRecords() throws Exception {
        // Only promotable shards receive replica batches and stateless search shards are not promotable, so this engine never
        // indexes one today. The translog replicator still expands every record it is handed into the record's full seqNo range,
        // so if a replica batch ever reached it, the engine must have cut the batch at every gap first. This is that path.
        final EngineConfig baseConfig = indexConfig();
        final ShardId shardId = baseConfig.getShardId();
        final Path translogStorePath = createTempDir();
        final BlobContainer translogBlobContainer = new FsBlobContainer(
            new FsBlobStore(randomIntBetween(1, 8) * 1024, translogStorePath, false),
            BlobPath.EMPTY,
            translogStorePath
        );
        final TranslogReplicator translogReplicator = new TranslogReplicator(
            baseConfig.getThreadPool(),
            Settings.builder()
                .put(TranslogReplicator.FLUSH_RETRY_INITIAL_DELAY_SETTING.getKey(), TimeValue.timeValueMillis(10))
                .put(TranslogReplicator.FLUSH_INTERVAL_SETTING.getKey(), TimeValue.timeValueMillis(50))
                .build(),
            translogObjectStoreService(translogBlobContainer),
            immediateConsistencyService(),
            false,
            ignored -> 1L
        );
        translogReplicator.start();
        // as in StatelessPlugin#getEngineFactory: the local translog hands every record it writes to the replicator
        final TranslogConfig baseTranslogConfig = baseConfig.getTranslogConfig();
        final EngineConfig config = EngineConfig.builder(baseConfig)
            .translogConfig(
                new TranslogConfig(
                    shardId,
                    baseTranslogConfig.getTranslogPath(),
                    baseTranslogConfig.getIndexSettings(),
                    baseTranslogConfig.getBigArrays(),
                    baseTranslogConfig.getBufferSize(),
                    baseTranslogConfig.getDiskIoBufferPool(),
                    translogReplicator.listenerFor(shardId),
                    false
                )
            )
            .build();
        final List<Long> persistedSeqNos = Collections.synchronizedList(new ArrayList<>());
        try (var engine = newIndexEngine(config, translogReplicator)) {
            // as in StatelessIndexNodeLifecycleListener#afterIndexShardCreated: an upload marks its seqNos persisted in the engine
            translogReplicator.register(shardId, 1L, seqNos -> {
                for (int i = seqNos.offset; i < seqNos.offset + seqNos.length; i++) {
                    persistedSeqNos.add(seqNos.longs[i]);
                }
                engine.objectStorePersistedSeqNoConsumer().accept(seqNos);
            });

            // the primary split this batch at a contended id, and another bulk reserved 3 and 4 in between
            final List<Engine.Index> ops = new ArrayList<>();
            for (long seqNo : new long[] { 0, 1, 2, 5, 6 }) {
                ops.add(replicaDoc("d" + seqNo, seqNo));
            }
            final List<Engine.IndexResult> results = engine.indexBatch(engineBatch(ops, encodeAsEscfBatch(ops)));
            for (Engine.IndexResult result : results) {
                assertThat(result.getResultType(), equalTo(Engine.Result.Type.SUCCESS));
            }
            // the engine ended a sub-batch at the gap, so the replicator was handed two records: [0, 1, 2] and [5, 6]
            final Translog.Location firstRecord = results.get(0).getTranslogLocation();
            assertThat(results.get(1).getTranslogLocation(), equalTo(firstRecord));
            assertThat(results.get(2).getTranslogLocation(), equalTo(firstRecord));
            assertThat(results.get(3).getTranslogLocation(), not(equalTo(firstRecord)));
            assertThat(results.get(4).getTranslogLocation(), equalTo(results.get(3).getTranslogLocation()));

            engine.syncTranslog();
            // the node buffer expanded each record into exactly its own rows: 3 and 4 were neither counted nor reported persisted
            assertThat(persistedSeqNos.stream().sorted().toList(), equalTo(List.of(0L, 1L, 2L, 5L, 6L)));
            assertThat(engine.getPersistedLocalCheckpoint(), equalTo(2L));
            assertReplicatedTranslog(translogBlobContainer, shardId, Map.of(0L, "d0", 1L, "d1", 2L, "d2", 5L, "d5", 6L, "d6"));

            // the ops that fill the gap belong to the other bulk and arrive through its own replication request
            assertThat(engine.index(replicaDoc("d3", 3)).getResultType(), equalTo(Engine.Result.Type.SUCCESS));
            assertThat(engine.index(replicaDoc("d4", 4)).getResultType(), equalTo(Engine.Result.Type.SUCCESS));
            engine.syncTranslog();
            assertThat(persistedSeqNos.stream().sorted().toList(), equalTo(List.of(0L, 1L, 2L, 3L, 4L, 5L, 6L)));
            assertThat(engine.getPersistedLocalCheckpoint(), equalTo(6L));
            assertReplicatedTranslog(
                translogBlobContainer,
                shardId,
                Map.of(0L, "d0", 1L, "d1", 2L, "d2", 3L, "d3", 4L, "d4", 5L, "d5", 6L, "d6")
            );
        }
    }

    /** A replica's copy of a document the primary indexed at the given seqNo, in term 1 at version 1. */
    private static Engine.Index replicaDoc(String id, long seqNo) throws IOException {
        final Engine.Index doc = randomDoc(id);
        return new Engine.Index(
            doc.uid(),
            doc.parsedDoc(),
            seqNo,
            1L,
            1L,
            null,
            Engine.Operation.Origin.REPLICA,
            doc.startTime(),
            IndexRequest.UNSET_AUTO_GENERATED_TIMESTAMP,
            false,
            UNASSIGNED_SEQ_NO,
            0
        );
    }

    /** Reads the shard's translog back from the object store the way recovery does and checks it holds exactly these operations. */
    private static void assertReplicatedTranslog(BlobContainer translogBlobContainer, ShardId shardId, Map<Long, String> expectedIdsBySeqNo)
        throws IOException {
        final Map<Long, String> replicated = new HashMap<>();
        try (TranslogReplicatorReader reader = new TranslogReplicatorReader(translogBlobContainer, shardId)) {
            // the count comes from the node buffer's per-record accounting; a densified gap would surface here as extra operations
            assertThat(reader.totalOperations(), equalTo(expectedIdsBySeqNo.size()));
            Translog.Operation op;
            while ((op = reader.next()) != null) {
                final String id = Uid.decodeId(((Translog.Index) op).uid());
                assertNull("seqNo [" + op.seqNo() + "] was replicated twice", replicated.put(op.seqNo(), id));
            }
        }
        assertThat(replicated, equalTo(expectedIdsBySeqNo));
    }

    /**
     * The replicator needs the object store for two things only: uploading compound translogs and handing out the container they
     * are read back from. The real {@link ObjectStoreService} needs a repository and a cluster, so those two calls are stubbed on top
     * of a real {@link FsBlobContainer}, which keeps the read-back on the real {@link TranslogReplicatorReader}.
     */
    private static ObjectStoreService translogObjectStoreService(BlobContainer translogBlobContainer) {
        final ObjectStoreService objectStoreService = mock(ObjectStoreService.class);
        when(objectStoreService.lifecycleState()).thenReturn(Lifecycle.State.STARTED);
        when(objectStoreService.getTranslogBlobContainer()).thenReturn(translogBlobContainer);
        doAnswer(invocation -> {
            final String fileName = invocation.getArgument(0);
            final BytesReference bytes = invocation.getArgument(1);
            translogBlobContainer.writeBlob(OperationPurpose.TRANSLOG, fileName, bytes, true);
            invocation.<ActionListener<Void>>getArgument(2).onResponse(null);
            return null;
        }).when(objectStoreService).uploadTranslogFile(any(), any(), any());
        return objectStoreService;
    }

    /** Before an upload the replicator checks the cluster state against the root blob, which needs a master; this answers at once. */
    private static StatelessClusterConsistencyService immediateConsistencyService() {
        final StatelessClusterConsistencyService consistencyService = mock(StatelessClusterConsistencyService.class);
        doAnswer(invocation -> {
            invocation.<ActionListener<Void>>getArgument(0).onResponse(null);
            return null;
        }).when(consistencyService).ensureClusterStateConsistentWithRootBlob(any(), any());
        doAnswer(invocation -> {
            invocation.<ActionListener<Void>>getArgument(0).onResponse(null);
            return null;
        }).when(consistencyService).delayedEnsureClusterStateConsistentWithRootBlob(any());
        when(consistencyService.state()).thenReturn(ClusterState.EMPTY_STATE);
        return consistencyService;
    }

    /**
     * Builds an {@link EngineBatch} for the given operations and source batch, assembling a minimal
     * {@link MappedColumns} (a {@code _source} column plus the seqNo/primaryTerm/version byte arrays
     * shared by reference with the {@link IndexOperationBatch}).
     *
     * <p>Note: this does not run the metadata-mapper columnar pipeline (this test's {@link MapperService}
     * is a mock with {@link MappingLookup#EMPTY}); the id/seqNo/version metadata is carried by the
     * {@link IndexOperationBatch} itself, so a minimal {@code _source} column is enough for the engine to
     * index the batch.
     */
    private static EngineBatch engineBatch(List<Engine.Index> operations, SourceBatch batch) {
        final IndexOperationBatch indexBatch = EngineTestCase.fromIndexOps(operations, batch);
        final BytesRef[] sources = new BytesRef[operations.size()];
        for (int i = 0; i < operations.size(); i++) {
            sources[i] = operations.get(i).source().originalBytes().toBytesRef();
        }
        final MappedColumns columns = new MappedColumns(
            0,
            operations.size(),
            indexBatch.seqNoBytes(),
            indexBatch.primaryTermBytes(),
            indexBatch.versionBytes(),
            List.of(MappedColumns.binaryColumn(sources, SourceFieldMapper.NAME, SourceFieldMapper.Defaults.FIELD_TYPE))
        );
        return new EngineBatch(indexBatch, columns);
    }

    /**
     * Encodes a list of index operations as an {@link EscfBatch}. All operations must share the same
     * {@link XContentType}.
     */
    private static SourceBatch encodeAsEscfBatch(List<Engine.Index> operations) throws IOException {
        List<BytesReference> sources = new ArrayList<>(operations.size());
        XContentType xContentType = operations.get(0).parsedDoc().getXContentType();
        for (Engine.Index op : operations) {
            sources.add(op.source().originalBytes());
        }
        return EscfEncoder.encode(sources, xContentType);
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
        when(mapperService.mappingLookup()).thenReturn(MappingLookup.EMPTY);
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
                    Long.parseLong(indexCommitRef.getIndexCommit().getUserData().get(StatelessCompoundCommit.TRANSLOG_RECOVERY_START_FILE))
                );
            }
        }
    }

    public void testRefreshCanReleaseFlushListener() throws IOException {
        try (
            var engine = newIndexEngine(
                indexConfig(
                    Settings.builder().put(IndexSettings.INDEX_REFRESH_INTERVAL_SETTING.getKey(), TimeValue.MINUS_ONE).build(),
                    Settings.builder().put(StatelessPlugin.STATELESS_ENABLED.getKey(), true).build(),
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

    /**
     * Verifies that {@code StatelessThreadPoolMergeScheduler}'s throttle callbacks are correctly wired to the engine's
     * throttle state, that {@code getMaxMergeCount()} returns the expected threshold for a given factor, and that the
     * factor is read dynamically so live cluster-settings updates are reflected without restarting the engine.
     */
    public void testStatelessMergeThrottleConfiguration() throws IOException {
        int factor = randomIntBetween(1, 5);
        Settings nodeSettings = Settings.builder()
            .put(ThreadPoolMergeScheduler.USE_THREAD_POOL_MERGE_SCHEDULER_SETTING.getKey(), true)
            .put(IndexEngine.MERGE_BACKLOG_THROTTLE_FACTOR.getKey(), factor)
            .build();
        // Build ClusterSettings explicitly so we can push a dynamic update after engine construction.
        ClusterSettings clusterSettings = new ClusterSettings(
            nodeSettings,
            Sets.addToCopy(
                ClusterSettings.BUILT_IN_CLUSTER_SETTINGS,
                IndexEngine.MERGE_FORCE_REFRESH_SIZE,
                IndexEngine.MERGE_BACKLOG_THROTTLE_FACTOR
            )
        );
        IndexEngineDynamicSettings dynamicSettings = new IndexEngineDynamicSettings(clusterSettings);
        try (var engine = newIndexEngine(indexConfig(Settings.EMPTY, nodeSettings), dynamicSettings)) {
            var scheduler = (IndexEngine.StatelessThreadPoolMergeScheduler) engine.getMergeScheduler();

            // threshold = factor × allocatedProcessors (= thread pool merge max)
            int maxConcurrentMerges = engine.config().getThreadPool().info(ThreadPool.Names.MERGE).getMax();
            assertThat(scheduler.getMaxMergeCount(), equalTo(factor * maxConcurrentMerges));

            // callbacks wire through to the engine's throttle state
            assertFalse(engine.isThrottled());
            scheduler.enableIndexingThrottling(0, 1, factor * maxConcurrentMerges);
            assertTrue(engine.isThrottled());
            scheduler.disableIndexingThrottling(0, 0, factor * maxConcurrentMerges);
            assertFalse(engine.isThrottled());

            // dynamic update: changing the factor is reflected immediately in getMaxMergeCount()
            int newFactor = factor + randomIntBetween(1, 5);
            clusterSettings.applySettings(Settings.builder().put(IndexEngine.MERGE_BACKLOG_THROTTLE_FACTOR.getKey(), newFactor).build());
            assertThat(scheduler.getMaxMergeCount(), equalTo(newFactor * maxConcurrentMerges));
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

    /**
     * Hollow commit user data is snapshotted from the internal searcher. Documents buffered in the
     * {@code IndexWriter} since the last refresh must still be included in both doc stats and shard field stats.
     */
    public void testHollowCommitStatsIncludeUnrefreshedDocuments() throws Exception {
        try (var engine = newIndexEngine(indexConfig(Settings.EMPTY, Settings.EMPTY, () -> 1L, NoMergePolicy.INSTANCE))) {
            engine.index(randomDoc("0"));
            engine.refresh("test");
            assertThat(engine.docStats().getCount(), equalTo(1L));
            final ShardFieldStats staleShardFieldStats = engine.shardFieldStats();

            engine.index(randomDoc("1"));
            // The new document is buffered in the IndexWriter and not yet visible to the internal searcher.
            assertThat(engine.docStats().getCount(), equalTo(1L));
            assertThat(engine.shardFieldStats(), equalTo(staleShardFieldStats));

            final PlainActionFuture<Engine.FlushResult> future = new PlainActionFuture<>();
            engine.flushHollow(future);
            future.actionGet();

            try (Engine.IndexCommitRef commitRef = engine.acquireLastIndexCommit(false)) {
                final Map<String, String> userData = commitRef.getIndexCommit().getUserData();
                try (
                    XContentParser parser = XContentType.JSON.xContent()
                        .createParser(XContentParserConfiguration.EMPTY, userData.get(IndexEngine.DOC_STATS))
                ) {
                    assertThat(DocsStats.PARSER.parse(parser, null).getCount(), equalTo(2L));
                }
                try (
                    XContentParser parser = XContentType.JSON.xContent()
                        .createParser(XContentParserConfiguration.EMPTY, userData.get(IndexEngine.SHARD_FIELD_STATS))
                ) {
                    final ShardFieldStats committedShardFieldStats = ShardFieldStats.PARSER.parse(parser, null);
                    assertThat(committedShardFieldStats.numSegments(), greaterThan(staleShardFieldStats.numSegments()));
                    assertThat(committedShardFieldStats, equalTo(engine.shardFieldStats()));
                }
            }
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
                new RefreshManagerService.Noop(),
                mock(ReshardIndexService.class),
                commitService.getCommitBCCResolverForShard(indexConfig.getShardId()),
                DocumentParsingProvider.EMPTY_INSTANCE,
                new IndexEngine.EngineMetrics(TranslogRecoveryMetrics.NOOP, MergeMetrics.NOOP, HollowShardsMetrics.NOOP),
                newIndexEngineDynamicSettings(indexConfig.getIndexSettings().getNodeSettings()),
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
                protected void flushHoldingLock(boolean force, boolean waitIfOngoing, FlushResultListener listener) throws EngineException {
                    flushesInitiated.incrementAndGet();
                    super.flushHoldingLock(force, waitIfOngoing, listener);
                }

                @Override
                protected Map<String, String> getCommitExtraUserData(final long localCheckpoint) {
                    int flushNumber = flushesInitiated.get();
                    firstFlushGettingUserDataLatch.countDown();
                    logger.debug("flush {} trying to get user data", flushNumber);
                    if (flushNumber > 1) {
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

    public void testDeleteUnownedDocumentsBumpsForceMergeUUID() throws Exception {
        Settings settings = Settings.builder()
            .put(IndexMetadata.SETTING_NUMBER_OF_SHARDS, 2)
            .put(IndexMetadata.SETTING_NUMBER_OF_REPLICAS, 0)
            .build();
        try (var engine = newIndexEngine(indexConfig(settings))) {
            engine.index(randomDoc("doc-1"));
            engine.flush(true, true);

            assertNull(engine.getForceMergeUUID());
            assertNull(engine.getLastCommittedSegmentInfos().getUserData().get(Engine.FORCE_MERGE_UUID_KEY));

            IndexMetadata metadata = engine.config().getIndexSettings().getIndexMetadata();
            engine.deleteUnownedDocuments(new ShardSplittingQuery(metadata, 0, false));
            engine.flush(true, true);

            String firstUuid = engine.getForceMergeUUID();
            assertNotNull(firstUuid);
            assertThat(engine.getLastCommittedSegmentInfos().getUserData().get(Engine.FORCE_MERGE_UUID_KEY), equalTo(firstUuid));

            engine.deleteUnownedDocuments(new ShardSplittingQuery(metadata, 0, false));
            engine.flush(true, true);

            String secondUuid = engine.getForceMergeUUID();
            assertNotNull(secondUuid);
            assertThat(secondUuid, not(equalTo(firstUuid)));
            assertThat(engine.getLastCommittedSegmentInfos().getUserData().get(Engine.FORCE_MERGE_UUID_KEY), equalTo(secondUuid));
        }
    }
}
