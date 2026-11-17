/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.stateless.engine;

import org.apache.lucene.index.NoMergePolicy;
import org.apache.lucene.index.SegmentInfos;
import org.apache.lucene.store.Directory;
import org.apache.lucene.tests.util.LuceneTestCase;
import org.elasticsearch.action.ActionListener;
import org.elasticsearch.common.blobstore.BlobContainer;
import org.elasticsearch.common.lucene.Lucene;
import org.elasticsearch.core.IOUtils;
import org.elasticsearch.index.engine.CommitStats;
import org.elasticsearch.index.engine.EngineConfig;
import org.elasticsearch.index.engine.EngineTestCase;
import org.elasticsearch.index.engine.MergeMetrics;
import org.elasticsearch.index.engine.SegmentsStats;
import org.elasticsearch.index.mapper.SourceToParse;
import org.elasticsearch.index.seqno.RetentionLeases;
import org.elasticsearch.index.seqno.SeqNoStats;
import org.elasticsearch.index.seqno.SequenceNumbers;
import org.elasticsearch.index.shard.DocsStats;
import org.elasticsearch.index.shard.ShardFieldStats;
import org.elasticsearch.index.store.Store;
import org.elasticsearch.index.translog.Translog;
import org.elasticsearch.indices.breaker.NoneCircuitBreakerService;
import org.elasticsearch.plugins.internal.DocumentParsingProvider;
import org.elasticsearch.xcontent.XContentType;
import org.elasticsearch.xcontent.json.JsonXContent;
import org.elasticsearch.xpack.stateless.cache.SharedBlobCacheWarmingService;
import org.elasticsearch.xpack.stateless.commits.HollowIndexEngineDeletionPolicy;
import org.elasticsearch.xpack.stateless.commits.HollowShardsService;
import org.elasticsearch.xpack.stateless.commits.ShardLocalCommitsRefs;
import org.elasticsearch.xpack.stateless.commits.ShardLocalReadersTracker;
import org.elasticsearch.xpack.stateless.commits.StatelessCommitService;
import org.elasticsearch.xpack.stateless.engine.translog.TranslogRecoveryMetrics;
import org.elasticsearch.xpack.stateless.engine.translog.TranslogReplicator;
import org.elasticsearch.xpack.stateless.reshard.ReshardIndexService;

import java.util.Map;
import java.util.Set;
import java.util.concurrent.atomic.AtomicLong;
import java.util.function.LongSupplier;

import static org.elasticsearch.common.bytes.BytesReference.bytes;
import static org.elasticsearch.index.codec.vectors.VectorTestUtils.randomFloatVector;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.greaterThan;
import static org.mockito.Mockito.mock;

public class HollowIndexEngineTests extends EngineTestCase {

    private StatelessCommitService statelessCommitService = mock(StatelessCommitService.class);
    private HollowShardsService hollowShardsService = mock(HollowShardsService.class);

    private HollowIndexEngine newHollowIndexEngine(EngineConfig config, SegmentInfos segmentInfos) {
        return new HollowIndexEngine(
            config,
            statelessCommitService,
            hollowShardsService,
            HollowCommitMetadata.of(segmentInfos),
            segmentInfos
        );
    }

    private EngineConfig hollowEngineConfig(Store store, LongSupplier globalCheckpointSupplier) {
        return config(
            defaultSettings,
            store,
            createTempDir(),
            NoMergePolicy.INSTANCE,
            null,
            null,
            null,
            globalCheckpointSupplier,
            globalCheckpointSupplier == null ? null : () -> RetentionLeases.EMPTY,
            new NoneCircuitBreakerService(),
            null,
            policy -> new HollowIndexEngineDeletionPolicy(new ShardLocalCommitsRefs())
        );
    }

    private void createEmptyStore(Store store, EngineConfig engineConfig) throws Exception {
        final Directory directory = store.directory();
        if (Lucene.indexExists(directory) == false) {
            store.createEmpty();
            final String translogUuid = Translog.createEmptyTranslog(
                engineConfig.getTranslogConfig().getTranslogPath(),
                SequenceNumbers.NO_OPS_PERFORMED,
                shardId,
                primaryTerm.get()
            );
            store.associateIndexWithNewTranslog(translogUuid);
        }
    }

    private IndexEngine newIndexEngine(EngineConfig engineConfig) throws Exception {
        return new IndexEngine(
            engineConfig,
            mock(TranslogReplicator.class),
            (s) -> mock(BlobContainer.class),
            statelessCommitService,
            hollowShardsService,
            mock(SharedBlobCacheWarmingService.class),
            new RefreshManagerService.Noop(),
            mock(ReshardIndexService.class),
            (g) -> Set.of(),
            DocumentParsingProvider.EMPTY_INSTANCE,
            new IndexEngine.EngineMetrics(TranslogRecoveryMetrics.NOOP, MergeMetrics.NOOP, HollowShardsMetrics.NOOP),
            AbstractEngineTestCase.newIndexEngineDynamicSettings(engineConfig.getIndexSettings().getNodeSettings()),
            (si) -> true,
            mock(ShardLocalReadersTracker.class)
        );
    }

    public void testReadOnly() throws Exception {
        IOUtils.close(engine, store);
        try (Store store = createStore()) {
            var globalCheckpoint = new AtomicLong(SequenceNumbers.NO_OPS_PERFORMED);
            var engineConfig = config(defaultSettings, store, createTempDir(), NoMergePolicy.INSTANCE, globalCheckpoint::get);
            int numDocs = randomIntBetween(8, 32);

            createEmptyStore(store, engineConfig);

            try (IndexEngine engine = newIndexEngine(engineConfig)) {
                for (int i = 0; i < numDocs; i++) {
                    engine.index(indexForDoc(createParsedDoc(String.valueOf(i), null)));
                }
                waitForOpsToComplete(engine, numDocs - 1);
                engine.refresh("test");
                engine.flushHollow(ActionListener.noop());
            }

            var hollowConfig = hollowEngineConfig(store, globalCheckpoint::get);
            var segmentInfos = hollowConfig.getStore().readLastCommittedSegmentsInfo();
            try (var hollowIndexEngine = newHollowIndexEngine(hollowConfig, segmentInfos)) {
                var exception = LuceneTestCase.TEST_ASSERTS_ENABLED ? AssertionError.class : UnsupportedOperationException.class;
                expectThrows(exception, () -> hollowIndexEngine.index(null));
                expectThrows(exception, () -> hollowIndexEngine.delete(null));
                expectThrows(exception, () -> hollowIndexEngine.noOp(null));
            }
        }
    }

    public void testOpenFromCommitMetadataLoadsSegmentInfosLazily() throws Exception {
        IOUtils.close(engine, store);
        try (Store store = createStore()) {
            var globalCheckpoint = new AtomicLong(SequenceNumbers.NO_OPS_PERFORMED);
            var engineConfig = config(defaultSettings, store, createTempDir(), NoMergePolicy.INSTANCE, globalCheckpoint::get);
            int numDocs = randomIntBetween(8, 32);

            createEmptyStore(store, engineConfig);

            try (IndexEngine engine = newIndexEngine(engineConfig)) {
                for (int i = 0; i < numDocs; i++) {
                    engine.index(indexForDoc(createParsedDoc(String.valueOf(i), null)));
                    if (rarely()) {
                        engine.refresh("test");
                    }
                }
                waitForOpsToComplete(engine, numDocs - 1);
                engine.refresh("test");
                engine.flushHollow(ActionListener.noop());
            }

            var hollowConfig = hollowEngineConfig(store, globalCheckpoint::get);
            var segmentInfos = hollowConfig.getStore().readLastCommittedSegmentsInfo();
            var commit = HollowCommitMetadata.of(segmentInfos);
            final DocsStats expectedDocsStats;
            final SeqNoStats expectedSeqNoStats;
            final String expectedHistoryUUID;
            try (var eagerEngine = newHollowIndexEngine(hollowConfig, segmentInfos)) {
                expectedDocsStats = eagerEngine.docStats();
                expectedSeqNoStats = eagerEngine.getSeqNoStats(globalCheckpoint.get());
                expectedHistoryUUID = eagerEngine.getHistoryUUID();
            }
            hollowConfig = hollowEngineConfig(store, globalCheckpoint::get);
            try (var hollowIndexEngine = new HollowIndexEngine(hollowConfig, statelessCommitService, hollowShardsService, commit, null)) {
                assertNull(hollowIndexEngine.getLastCommittedSegmentInfosIfLoaded());
                assertEquals(new CommitStats(segmentInfos), hollowIndexEngine.commitStats());
                assertEquals(expectedDocsStats, hollowIndexEngine.docStats());
                assertEquals(expectedSeqNoStats, hollowIndexEngine.getSeqNoStats(globalCheckpoint.get()));
                assertEquals(expectedHistoryUUID, hollowIndexEngine.getHistoryUUID());
                try (var commitRef = hollowIndexEngine.acquireLastIndexCommit(false)) {
                    var expected = Lucene.getIndexCommit(segmentInfos, store.directory());
                    var actual = commitRef.getIndexCommit();
                    assertEquals(expected.getGeneration(), actual.getGeneration());
                    assertEquals(expected.getSegmentsFileName(), actual.getSegmentsFileName());
                    assertEquals(Set.copyOf(expected.getFileNames()), Set.copyOf(actual.getFileNames()));
                    assertEquals(expected.getSegmentCount(), actual.getSegmentCount());
                    assertEquals(expected.getUserData(), actual.getUserData());
                }
                assertNull("nothing above needs the segment infos", hollowIndexEngine.getLastCommittedSegmentInfosIfLoaded());

                // the lazily loaded segment infos are checked against the commit metadata the engine was opened with
                var loaded = hollowIndexEngine.getLastCommittedSegmentInfos();
                assertEquals(commit, HollowCommitMetadata.of(loaded));
                assertSame(loaded, hollowIndexEngine.getLastCommittedSegmentInfosIfLoaded());
                assertSame(loaded, hollowIndexEngine.getLastCommittedSegmentInfos());
            }
        }
    }

    public void testShardFieldStats() throws Exception {
        IOUtils.close(engine, store);
        try (Store store = createStore()) {
            var globalCheckpoint = new AtomicLong(SequenceNumbers.NO_OPS_PERFORMED);
            var engineConfig = config(defaultSettings, store, createTempDir(), NoMergePolicy.INSTANCE, globalCheckpoint::get);
            int numDocs = randomIntBetween(8, 32);

            createEmptyStore(store, engineConfig);

            ShardFieldStats expectedShardFieldStats;
            try (IndexEngine engine = newIndexEngine(engineConfig)) {
                for (int i = 0; i < numDocs; i++) {
                    engine.index(indexForDoc(createParsedDoc(String.valueOf(i), null)));
                }
                waitForOpsToComplete(engine, numDocs - 1);
                engine.refresh("test");
                expectedShardFieldStats = engine.shardFieldStats();
                assertEquals(expectedShardFieldStats.numSegments(), engine.segments().size());
                engine.flushHollow(ActionListener.noop());
            }

            var hollowConfig = hollowEngineConfig(store, globalCheckpoint::get);
            var segmentInfos = hollowConfig.getStore().readLastCommittedSegmentsInfo();
            try (var hollowIndexEngine = newHollowIndexEngine(hollowConfig, segmentInfos)) {
                assertEquals(expectedShardFieldStats, hollowIndexEngine.shardFieldStats());
            }
        }
    }

    public void testDocStats() throws Exception {
        IOUtils.close(engine, store);
        try (Store store = createStore()) {
            var globalCheckpoint = new AtomicLong(SequenceNumbers.NO_OPS_PERFORMED);
            var engineConfig = config(defaultSettings, store, createTempDir(), NoMergePolicy.INSTANCE, globalCheckpoint::get);
            int numDocs = randomIntBetween(8, 32);

            createEmptyStore(store, engineConfig);

            DocsStats expectedDocStats;
            try (IndexEngine engine = newIndexEngine(engineConfig)) {
                for (int i = 0; i < numDocs; i++) {
                    engine.index(indexForDoc(createParsedDoc(String.valueOf(i), null)));
                }
                waitForOpsToComplete(engine, numDocs - 1);
                engine.refresh("test");
                expectedDocStats = engine.docStats();
                assertEquals(expectedDocStats.getCount(), numDocs);
                engine.flushHollow(ActionListener.noop());
            }

            var hollowConfig = hollowEngineConfig(store, globalCheckpoint::get);
            var segmentInfos = hollowConfig.getStore().readLastCommittedSegmentsInfo();
            try (var hollowIndexEngine = newHollowIndexEngine(hollowConfig, segmentInfos)) {
                assertEquals(expectedDocStats, hollowIndexEngine.docStats());
            }
        }
    }

    public void testSegmentStats() throws Exception {
        IOUtils.close(engine, store);
        try (Store store = createStore()) {
            var globalCheckpoint = new AtomicLong(SequenceNumbers.NO_OPS_PERFORMED);
            var engineConfig = config(defaultSettings, store, createTempDir(), NoMergePolicy.INSTANCE, globalCheckpoint::get);
            int numDocs = randomIntBetween(8, 32);

            createEmptyStore(store, engineConfig);

            try (IndexEngine engine = newIndexEngine(engineConfig)) {
                for (int i = 0; i < numDocs; i++) {
                    engine.index(indexForDoc(createParsedDoc(String.valueOf(i), null)));
                }
                waitForOpsToComplete(engine, numDocs - 1);
                engine.refresh("test");
                final var segmentsStats = engine.segmentsStats(true, true);
                assertEquals(segmentsStats.getCount(), engine.segments().size());
                engine.flushHollow(ActionListener.noop());
            }

            var hollowConfig = hollowEngineConfig(store, globalCheckpoint::get);
            var segmentInfos = hollowConfig.getStore().readLastCommittedSegmentsInfo();
            try (var hollowIndexEngine = newHollowIndexEngine(hollowConfig, segmentInfos)) {
                var segmentsStats = hollowIndexEngine.segmentsStats(true, true);
                assertThat(segmentsStats, equalTo(new SegmentsStats())); // should be empty for hollow shards
            }
        }
    }

    @Override
    protected String defaultMapping() {
        return """
            {
              "properties": {
                "dv": {
                  "type": "dense_vector",
                  "dims": 3,
                  "similarity": "cosine"
                },
                "sv": {
                  "type": "sparse_vector"
                }
              }
            }
            """;
    }

    public void testVectorStats() throws Exception {
        IOUtils.close(engine, store);
        try (Store store = createStore()) {
            var globalCheckpoint = new AtomicLong(SequenceNumbers.NO_OPS_PERFORMED);
            var engineConfig = config(defaultSettings, store, createTempDir(), NoMergePolicy.INSTANCE, globalCheckpoint::get);
            var documentMapper = engineConfig.getMapperService().documentMapper();
            var mappingLookup = engineConfig.getMapperService().mappingLookup();
            int numDocs = randomIntBetween(8, 32);

            createEmptyStore(store, engineConfig);

            try (IndexEngine engine = newIndexEngine(engineConfig)) {
                for (int i = 0; i < numDocs; i++) {
                    var denseVectorSource = JsonXContent.contentBuilder().startObject().array("dv", randomFloatVector(3)).endObject();
                    var denseVectorDoc = documentMapper.parse(new SourceToParse("dv_" + i, bytes(denseVectorSource), XContentType.JSON));
                    engine.index(indexForDoc(denseVectorDoc));

                    var sparseVectorSource = JsonXContent.contentBuilder()
                        .startObject()
                        .field("sv")
                        .value(Map.of("a", randomPositiveFloat(), "b", randomPositiveFloat()))
                        .endObject();
                    var sparseVectorDoc = documentMapper.parse(new SourceToParse("sv_" + i, bytes(sparseVectorSource), XContentType.JSON));
                    engine.index(indexForDoc(sparseVectorDoc));
                }

                waitForOpsToComplete(engine, numDocs - 1);
                engine.refresh("test");
                assertThat(engine.denseVectorStats(mappingLookup).getValueCount(), greaterThan(0L));
                assertThat(engine.sparseVectorStats(mappingLookup).getValueCount(), greaterThan(0L));
                engine.flushHollow(ActionListener.noop());
            }

            var hollowConfig = hollowEngineConfig(store, globalCheckpoint::get);
            var segmentInfos = hollowConfig.getStore().readLastCommittedSegmentsInfo();
            try (var e = newHollowIndexEngine(hollowConfig, segmentInfos)) {
                assertThat(e.denseVectorStats(mappingLookup).getValueCount(), equalTo(0L)); // should be empty for hollow shards
                assertThat(e.sparseVectorStats(mappingLookup).getValueCount(), equalTo(0L)); // should be empty for hollow shards
            }
        }
    }

    private static float randomPositiveFloat() {
        return randomFloatBetween(0, 1, true);
    }
}
