/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.indiceswriteloadtracker;

import org.apache.lucene.analysis.Analyzer;
import org.apache.lucene.analysis.TokenFilter;
import org.apache.lucene.analysis.Tokenizer;
import org.apache.lucene.analysis.standard.StandardTokenizer;
import org.apache.lucene.store.Directory;
import org.apache.lucene.store.FilterDirectory;
import org.apache.lucene.store.IOContext;
import org.apache.lucene.store.IndexOutput;
import org.elasticsearch.Version;
import org.elasticsearch.action.ActionListener;
import org.elasticsearch.action.admin.indices.forcemerge.ForceMergeRequest;
import org.elasticsearch.action.bulk.BulkItemRequest;
import org.elasticsearch.action.bulk.BulkItemResponse;
import org.elasticsearch.action.bulk.BulkShardRequest;
import org.elasticsearch.action.bulk.BulkShardResponse;
import org.elasticsearch.action.bulk.TransportShardBulkAction;
import org.elasticsearch.action.delete.DeleteRequest;
import org.elasticsearch.action.index.IndexRequest;
import org.elasticsearch.action.support.PlainActionFuture;
import org.elasticsearch.action.support.WriteRequest;
import org.elasticsearch.action.support.replication.TransportReplicationAction;
import org.elasticsearch.action.update.UpdateHelper;
import org.elasticsearch.cluster.ClusterName;
import org.elasticsearch.cluster.ClusterState;
import org.elasticsearch.cluster.metadata.DataStream;
import org.elasticsearch.cluster.metadata.IndexMetadata;
import org.elasticsearch.cluster.metadata.Metadata;
import org.elasticsearch.cluster.routing.RecoverySource;
import org.elasticsearch.cluster.routing.ShardRouting;
import org.elasticsearch.cluster.routing.ShardRoutingState;
import org.elasticsearch.cluster.routing.TestShardRouting;
import org.elasticsearch.cluster.service.ClusterService;
import org.elasticsearch.common.UUIDs;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.core.IOUtils;
import org.elasticsearch.core.TimeValue;
import org.elasticsearch.env.NodeEnvironment;
import org.elasticsearch.index.Index;
import org.elasticsearch.index.IndexMode;
import org.elasticsearch.index.IndexModule;
import org.elasticsearch.index.codec.CodecService;
import org.elasticsearch.index.engine.EngineConfig;
import org.elasticsearch.index.engine.EngineFactory;
import org.elasticsearch.index.engine.InternalEngine;
import org.elasticsearch.index.seqno.RetentionLeaseSyncer;
import org.elasticsearch.index.shard.IndexShard;
import org.elasticsearch.index.shard.IndexShardTestCase;
import org.elasticsearch.index.shard.ShardId;
import org.elasticsearch.index.shard.ShardPath;
import org.elasticsearch.index.store.Store;
import org.elasticsearch.index.translog.Translog;
import org.elasticsearch.test.DummyShardLock;
import org.elasticsearch.threadpool.ThreadPool;
import org.elasticsearch.xcontent.XContentType;

import java.io.IOException;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.Function;
import java.util.function.LongSupplier;
import java.util.stream.Stream;

import static org.hamcrest.Matchers.closeTo;
import static org.hamcrest.Matchers.empty;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.greaterThan;
import static org.hamcrest.Matchers.hasSize;
import static org.hamcrest.Matchers.is;
import static org.hamcrest.Matchers.lessThanOrEqualTo;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

public class IndicesWriteLoadStatsCollectorTests extends IndexShardTestCase {
    public void testRegularIndicesLoadIsNotTracked() throws Exception {
        final var samplingFrequency = TimeValue.timeValueSeconds(1);
        try (var shardRef = createRegularIndexShard()) {
            final var shard = shardRef.shard();

            final var indicesWriteLoadStatsCollector = new IndicesWriteLoadStatsCollector(
                mock(ClusterService.class),
                fakeClock(samplingFrequency)
            ) {
                @Override
                String getParentDataStreamName(String indexName) {
                    assert false : "unexpected call";
                    return "";
                }
            };
            indicesWriteLoadStatsCollector.afterIndexShardStarted(shard);

            for (int i = 0; i < 10; i++) {
                indexDocs(shard, randomIntBetween(1, 10));
                indicesWriteLoadStatsCollector.collectWriteLoadStats();
                assertThat(shard.getTotalIndexingTimeInNanos(), is(greaterThan(0L)));
            }
            final var shardLoadHistogramSnapshots = indicesWriteLoadStatsCollector.getWriteLoadHistogramSnapshotsAndReset();
            assertThat(shardLoadHistogramSnapshots, is(empty()));
        }
    }

    public void testFsyncIsTracked() throws Exception {
        final var samplingFrequency = TimeValue.timeValueSeconds(1);
        try (var shardRef = createDataStreamShard()) {
            final var shard = shardRef.shard();

            final var clusterService = mock(ClusterService.class);
            final var clusterState = clusterStateWithShardDataStream(shard);
            when(clusterService.state()).thenReturn(clusterState);
            final var indicesWriteLoadStatsCollector = new IndicesWriteLoadStatsCollector(clusterService, fakeClock(samplingFrequency));
            indicesWriteLoadStatsCollector.afterIndexShardStarted(shard);

            int numberOfProcessors = randomIntBetween(2, 4);
            final int maxCPUsUsed = randomIntBetween(1, numberOfProcessors);
            for (int i = 0; i < 60; i++) {
                if (randomBoolean()) {
                    // Simulate a few bulk operations running concurrently taking maxCPUsUsed
                    for (int j = 0; j < maxCPUsUsed; j++) {
                        int numDocs = randomIntBetween(1, 10);
                        indexDocs(shard, numDocs);
                    }
                } else {
                    // Otherwise, simulate a few quick indexing ops
                    indexDocs(shard, randomIntBetween(1, 20));
                }

                indicesWriteLoadStatsCollector.collectWriteLoadStats();
            }

            {
                final var shardLoadHistogramSnapshots = indicesWriteLoadStatsCollector.getWriteLoadHistogramSnapshotsAndReset();

                assertThat(shardLoadHistogramSnapshots, hasSize(1));
                final var shardWriteLoadHistogramSnapshot = shardLoadHistogramSnapshots.get(0);
                final var indexingLoadHistogramSnapshot = shardWriteLoadHistogramSnapshot.indexLoadHistogramSnapshot();
                assertThat(indexingLoadHistogramSnapshot.max(), is(closeTo(maxCPUsUsed, 0.5)));
                assertThat(indexingLoadHistogramSnapshot.p90(), is(lessThanOrEqualTo(indexingLoadHistogramSnapshot.max())));
                assertThat(indexingLoadHistogramSnapshot.p50(), is(lessThanOrEqualTo(indexingLoadHistogramSnapshot.p90())));
            }

            {
                // We didn't have any readings after the previous reset
                final var shardLoadHistogramSnapshots = indicesWriteLoadStatsCollector.getWriteLoadHistogramSnapshotsAndReset();

                assertThat(shardLoadHistogramSnapshots, hasSize(1));
                final var shardWriteLoadHistogramSnapshot = shardLoadHistogramSnapshots.get(0);
                final var indexingLoadHistogramSnapshot = shardWriteLoadHistogramSnapshot.indexLoadHistogramSnapshot();
                assertThat(indexingLoadHistogramSnapshot.max(), is(equalTo(0.0)));
                assertThat(indexingLoadHistogramSnapshot.p90(), is(equalTo(0.0)));
                assertThat(indexingLoadHistogramSnapshot.p50(), is(equalTo(0.0)));
            }
        }
    }

    public void testIndexingLoadTracking() throws Exception {
        final var samplingFrequency = TimeValue.timeValueSeconds(1);
        try (var shardRef = createDataStreamShard()) {
            final var shard = shardRef.shard();

            final var clusterService = mock(ClusterService.class);
            final var clusterState = clusterStateWithShardDataStream(shard);
            when(clusterService.state()).thenReturn(clusterState);
            final var indicesWriteLoadStatsCollector = new IndicesWriteLoadStatsCollector(clusterService, fakeClock(samplingFrequency));
            indicesWriteLoadStatsCollector.afterIndexShardStarted(shard);

            long previousTotalIndexingTimeNanos = 0;
            int numberOfProcessors = randomIntBetween(2, 4);
            final int maxCPUsUsed = randomIntBetween(1, numberOfProcessors);
            for (int i = 0; i < 2; i++) {
                indexDocs(shard, maxCPUsUsed);
                indicesWriteLoadStatsCollector.collectWriteLoadStats();
                final long totalIndexingTimeInNanos = shard.getTotalIndexingTimeInNanos();
                assertThat(totalIndexingTimeInNanos, is(greaterThan(previousTotalIndexingTimeNanos)));
                previousTotalIndexingTimeNanos = totalIndexingTimeInNanos;
            }
            // for (int i = 0; i < 60; i++) {
            // if (randomBoolean()) {
            // fakeClock.setTickValue(samplingFrequency);
            // // Simulate a few bulk operations running concurrently taking maxCPUsUsed
            // for (int j = 0; j < maxCPUsUsed; j++) {
            // int numDocs = randomIntBetween(1, 10);
            // indexDocs(shard, numDocs);
            // }
            // } else {
            // // Otherwise, simulate a few quick indexing ops
            // fakeClock.setTickValue(TimeValue.timeValueMillis(randomIntBetween(1, 20)));
            // indexDocs(shard, randomIntBetween(1, 20));
            // }
            //
            // indicesWriteLoadStatsCollector.collectWriteLoadStats();
            // }

            {
                final var shardLoadHistogramSnapshots = indicesWriteLoadStatsCollector.getWriteLoadHistogramSnapshotsAndReset();

                assertThat(shardLoadHistogramSnapshots, hasSize(1));
                final var shardWriteLoadHistogramSnapshot = shardLoadHistogramSnapshots.get(0);
                final var indexingLoadHistogramSnapshot = shardWriteLoadHistogramSnapshot.indexLoadHistogramSnapshot();
                assertThat(indexingLoadHistogramSnapshot.max(), is(closeTo(maxCPUsUsed, 0.5)));
                assertThat(indexingLoadHistogramSnapshot.p90(), is(lessThanOrEqualTo(indexingLoadHistogramSnapshot.max())));
                assertThat(indexingLoadHistogramSnapshot.p50(), is(lessThanOrEqualTo(indexingLoadHistogramSnapshot.p90())));
            }

            {
                // We didn't have any readings after the previous reset
                final var shardLoadHistogramSnapshots = indicesWriteLoadStatsCollector.getWriteLoadHistogramSnapshotsAndReset();

                assertThat(shardLoadHistogramSnapshots, hasSize(1));
                final var shardWriteLoadHistogramSnapshot = shardLoadHistogramSnapshots.get(0);
                final var indexingLoadHistogramSnapshot = shardWriteLoadHistogramSnapshot.indexLoadHistogramSnapshot();
                assertThat(indexingLoadHistogramSnapshot.max(), is(equalTo(0.0)));
                assertThat(indexingLoadHistogramSnapshot.p90(), is(equalTo(0.0)));
                assertThat(indexingLoadHistogramSnapshot.p50(), is(equalTo(0.0)));
            }

            shard.refresh("test");

            indexDocs(shard, 1);

            shard.forceMerge(new ForceMergeRequest().maxNumSegments(1));
        }
    }

    public void testRefreshLoadTracking() throws Exception {
        final var samplingFrequency = TimeValue.timeValueSeconds(1);
        try (var shardRef = createDataStreamShard()) {
            final var shard = shardRef.shard();

            final var clusterService = mock(ClusterService.class);
            final var clusterState = clusterStateWithShardDataStream(shard);
            when(clusterService.state()).thenReturn(clusterState);
            final var indicesWriteLoadStatsCollector = new IndicesWriteLoadStatsCollector(clusterService, fakeClock(samplingFrequency));
            indicesWriteLoadStatsCollector.afterIndexShardStarted(shard);

            indexDocs(shard, 1);
            indicesWriteLoadStatsCollector.collectWriteLoadStats();

            // .put(IndexingMemoryController.INDEX_BUFFER_SIZE_SETTING.getKey(), "1kb")

            SlowDirectory.slow.set(true);
            shard.refresh("test");

            indicesWriteLoadStatsCollector.collectWriteLoadStats();

            final var shardLoadHistograms = indicesWriteLoadStatsCollector.getWriteLoadHistogramSnapshotsAndReset();

            assertThat(shardLoadHistograms, hasSize(1));
            final var shardWriteLoadDistribution = shardLoadHistograms.get(0);

            // assertThat(shardWriteLoadDistribution.indexLoadHistogramSnapshot().max(), is(greaterThan(0.0)));

            assertThat(shardWriteLoadDistribution.refreshLoadHistogramSnapshot().max(), is(closeTo(2.0, 0.5)));
            assertThat(
                shardWriteLoadDistribution.refreshLoadHistogramSnapshot().p90(),
                is(lessThanOrEqualTo(shardWriteLoadDistribution.refreshLoadHistogramSnapshot().max()))
            );
            assertThat(
                shardWriteLoadDistribution.refreshLoadHistogramSnapshot().p50(),
                is(lessThanOrEqualTo(shardWriteLoadDistribution.refreshLoadHistogramSnapshot().p90()))
            );
        }
    }

    public void testMergeLoadTracking() throws Exception {
        // fail("configure merges to wait until force merge");
        final var samplingFrequency = TimeValue.timeValueSeconds(1);

        try (var shardRef = createDataStreamShard(SlowDirectory::new)) {
            final var shard = shardRef.shard();

            final var clusterState = clusterStateWithShardDataStream(shard);
            final var clusterService = mock(ClusterService.class);
            when(clusterService.state()).thenReturn(clusterState);
            final var indicesWriteLoadStatsCollector = new IndicesWriteLoadStatsCollector(clusterService, fakeClock(samplingFrequency));
            indicesWriteLoadStatsCollector.afterIndexShardStarted(shard);

            for (int i = 0; i < 2; i++) {
                indexDocs(shard, randomIntBetween(1, 20), false);
                shard.refresh("test");

                indicesWriteLoadStatsCollector.collectWriteLoadStats();
            }
            assertThat(shard.getTotalMergeTimeInNanos(), is(equalTo(0L)));

            SlowDirectory.slow.set(true);

            shard.forceMerge(new ForceMergeRequest().maxNumSegments(1));

            indicesWriteLoadStatsCollector.collectWriteLoadStats();

            // Now force-merge every second and every force-merge should take 1 second

            // assertThat(shard.getTotalMergeTimeInMillis(), is(equalTo(TimeValue.timeValueSeconds(numTicks).millis())));

            final var shardLoadHistograms = indicesWriteLoadStatsCollector.getWriteLoadHistogramSnapshotsAndReset();

            assertThat(shardLoadHistograms, hasSize(1));
            final var shardWriteLoadDistribution = shardLoadHistograms.get(0);

            assertThat(shardWriteLoadDistribution.mergeLoadHistogramSnapshot().max(), is(closeTo(2.0, 0.1)));
        }
    }

    public void testDeleteTimeIsTracked() throws Exception {
        final var samplingFrequency = TimeValue.timeValueSeconds(1);
        try (var shardRef = createDataStreamShard()) {
            final var shard = shardRef.shard();

            final var clusterState = clusterStateWithShardDataStream(shard);
            final var clusterService = mock(ClusterService.class);
            when(clusterService.state()).thenReturn(clusterState);
            final var indicesWriteLoadStatsCollector = new IndicesWriteLoadStatsCollector(clusterService, fakeClock(samplingFrequency));
            indicesWriteLoadStatsCollector.afterIndexShardStarted(shard);

            final var docIds = indexDocs(shard, 500, false);

            indicesWriteLoadStatsCollector.collectWriteLoadStats();

            indicesWriteLoadStatsCollector.getWriteLoadHistogramSnapshotsAndReset();

            deleteDocs(shard, docIds);

            indicesWriteLoadStatsCollector.collectWriteLoadStats();

            final var shardLoadHistograms = indicesWriteLoadStatsCollector.getWriteLoadHistogramSnapshotsAndReset();

            assertThat(shardLoadHistograms, hasSize(1));
            final var shardWriteLoadDistribution = shardLoadHistograms.get(0);

            double max = shardWriteLoadDistribution.indexLoadHistogramSnapshot().max();
            assertThat(max, is(greaterThan(0.0)));
        }
    }

    public void testShardLoadDistributionInfoIsClearedAfterDeletion() throws Exception {
        final var samplingFrequency = TimeValue.timeValueSeconds(1);
        try (var shardRef = createDataStreamShard()) {
            final var shard = shardRef.shard();

            final var clusterState = clusterStateWithShardDataStream(shard);
            final var clusterService = mock(ClusterService.class);
            when(clusterService.state()).thenReturn(clusterState);
            final var indicesWriteLoadStatsCollector = new IndicesWriteLoadStatsCollector(clusterService, fakeClock(samplingFrequency));
            indicesWriteLoadStatsCollector.afterIndexShardStarted(shard);

            for (int i = 0; i < 60; i++) {
                indexDocs(shard, randomIntBetween(1, 20));

                indicesWriteLoadStatsCollector.collectWriteLoadStats();
            }

            final var shardLoadHistograms = indicesWriteLoadStatsCollector.getWriteLoadHistogramSnapshotsAndReset();

            assertThat(shardLoadHistograms, hasSize(1));
            final var shardWriteLoadDistribution = shardLoadHistograms.get(0);
            assertThat(shardWriteLoadDistribution.shardId(), is(equalTo(shard.shardId())));

            indicesWriteLoadStatsCollector.afterIndexShardClosed(shard.shardId(), shard, shard.indexSettings().getSettings());

            assertThat(indicesWriteLoadStatsCollector.getWriteLoadHistogramSnapshotsAndReset(), is(empty()));
        }
    }

    public void testRolledOverDataStreamIndicesAreRemovedAfterCollection() throws Exception {
        final var samplingFrequency = TimeValue.timeValueSeconds(1);

        try (var firstDataStreamShardRef = createDataStreamShard(); var rolledOverDataStreamShardRef = createDataStreamShard()) {

            final var firstDataStreamShard = firstDataStreamShardRef.shard();
            final var firstDataStreamIndex = firstDataStreamShard.shardId().getIndex();

            final var rolledOverShard = rolledOverDataStreamShardRef.shard();
            final var rolledOverIndex = rolledOverShard.shardId().getIndex();

            final AtomicReference<List<IndexShard>> shardsProvider = new AtomicReference<>(List.of(firstDataStreamShard));
            final var clusterService = mock(ClusterService.class);
            final var clusterState = ClusterState.builder(new ClusterName("cluster"))
                .metadata(
                    Metadata.builder()
                        .put(getDataStream(firstDataStreamIndex))
                        .put(
                            IndexMetadata.builder(firstDataStreamIndex.getName())
                                .settings(settings(Version.CURRENT))
                                .numberOfShards(1)
                                .numberOfReplicas(0)
                                .build(),
                            false
                        )
                        .build()
                )
                .build();
            when(clusterService.state()).thenReturn(clusterState);

            final var indicesWriteLoadStatsCollector = new IndicesWriteLoadStatsCollector(clusterService, fakeClock(samplingFrequency));
            indicesWriteLoadStatsCollector.afterIndexShardStarted(firstDataStreamShard);

            for (int i = 0; i < 60; i++) {
                indexDocs(firstDataStreamShard, randomIntBetween(1, 20));

                indicesWriteLoadStatsCollector.collectWriteLoadStats();
            }

            final var shardLoadHistogramSnapshots = indicesWriteLoadStatsCollector.getWriteLoadHistogramSnapshotsAndReset();

            assertThat(shardLoadHistogramSnapshots, hasSize(1));
            final var shardWriteLoadHistogramSnapshot = shardLoadHistogramSnapshots.get(0);
            assertThat(shardWriteLoadHistogramSnapshot.shardId(), is(equalTo(firstDataStreamShard.shardId())));

            // take some new samples before rolling over
            for (int i = 0; i < 10; i++) {
                indexDocs(firstDataStreamShard, randomIntBetween(1, 20));

                indicesWriteLoadStatsCollector.collectWriteLoadStats();
            }

            final var clusterStateWithRolledOverDataStream = ClusterState.builder(clusterState)
                .metadata(
                    Metadata.builder(clusterState.metadata())
                        .put(getDataStream(firstDataStreamIndex, rolledOverIndex))
                        .put(
                            IndexMetadata.builder(rolledOverIndex.getName())
                                .settings(settings(Version.CURRENT))
                                .numberOfShards(1)
                                .numberOfReplicas(0)
                                .build(),
                            false
                        )
                        .build()
                )
                .build();
            when(clusterService.state()).thenReturn(clusterStateWithRolledOverDataStream);
            indicesWriteLoadStatsCollector.afterIndexShardStarted(rolledOverShard);

            for (int i = 0; i < 10; i++) {
                indexDocs(rolledOverShard, randomIntBetween(1, 20));

                indicesWriteLoadStatsCollector.collectWriteLoadStats();
            }

            final var writeLoadHistogramAfterRollOver = indicesWriteLoadStatsCollector.getWriteLoadHistogramSnapshotsAndReset();
            assertThat(writeLoadHistogramAfterRollOver, hasSize(2));
            assertThat(
                writeLoadHistogramAfterRollOver.stream().anyMatch(s -> s.shardId().getIndex().equals(firstDataStreamIndex)),
                is(true)
            );
            assertThat(writeLoadHistogramAfterRollOver.stream().anyMatch(s -> s.shardId().getIndex().equals(rolledOverIndex)), is(true));

            final var writeLoadAfterRolloverCleanup = indicesWriteLoadStatsCollector.getWriteLoadHistogramSnapshotsAndReset();
            assertThat(writeLoadAfterRolloverCleanup, hasSize(1));
            final var shardWriteLoadHistogramAfterRollover = writeLoadAfterRolloverCleanup.get(0);
            assertThat(shardWriteLoadHistogramAfterRollover.shardId(), is(equalTo(rolledOverShard.shardId())));
        }
    }

    private ClusterState clusterStateWithShardDataStream(IndexShard shard) {
        final var index = shard.shardId().getIndex();
        return ClusterState.builder(new ClusterName("cluster"))
            .metadata(
                Metadata.builder()
                    .put(getDataStream(index))
                    .put(
                        IndexMetadata.builder(index.getName())
                            .settings(settings(Version.CURRENT))
                            .numberOfShards(1)
                            .numberOfReplicas(0)
                            .build(),
                        false
                    )
                    .build()
            )
            .build();
    }

    private DataStream getDataStream(Index... indices) {
        return new DataStream(
            "datastream",
            Arrays.stream(indices).toList(),
            1,
            Collections.emptyMap(),
            false,
            false,
            false,
            false,
            IndexMode.STANDARD
        );
    }

    record ShardRef(IndexShard shard) implements AutoCloseable {
        @Override
        public void close() throws IOException {
            IOUtils.close(shard.store(), () -> shard.close("test", false));
        }
    }

    private ShardRef createDataStreamShard() throws Exception {
        return createShard(true, new SleepingAnalyzer(), () -> {}, d -> d);
    }

    private ShardRef createDataStreamShard(Function<Directory, Directory> directoryWrapper) throws Exception {
        return createShard(true, null, () -> {}, directoryWrapper);
    }

    private ShardRef createRegularIndexShard() throws Exception {
        return createShard(false, new SleepingAnalyzer(), () -> {}, d -> d);
    }

    private ShardRef createShard(
        boolean createAsDataStream,
        Analyzer analyzer,
        Runnable runBeforeTranslogFsync,
        Function<Directory, Directory> directoryWrapper
    ) throws Exception {
        final var shardId = new ShardId(new Index(randomAlphaOfLength(10), "_na_"), 0);
        final RecoverySource recoverySource = RecoverySource.EmptyStoreRecoverySource.INSTANCE;
        final ShardRouting shardRouting = TestShardRouting.newShardRouting(
            shardId,
            randomAlphaOfLength(10),
            true,
            ShardRoutingState.INITIALIZING,
            recoverySource
        );
        final var indexMetadata = IndexMetadata.builder(shardRouting.getIndexName())
            .settings(
                Settings.builder()
                    .put(IndexMetadata.SETTING_VERSION_CREATED, Version.CURRENT)
                    .put(IndexMetadata.SETTING_NUMBER_OF_REPLICAS, 0)
                    .put(IndexMetadata.SETTING_NUMBER_OF_SHARDS, 1)
                    .build()
            )
            .primaryTerm(0, primaryTerm);

        if (createAsDataStream) {
            indexMetadata.putMapping("""
                {
                    "_doc": {
                        "properties": {
                            "@timestamp": {
                                "type": "date"
                            },
                           "test": {
                                "type": "text"
                            }
                        },
                        "_data_stream_timestamp": {
                            "enabled": true
                        }
                    }
                }
                """);
        } else {
            indexMetadata.putMapping("""
                {
                    "_doc": {
                        "properties": {
                            "@timestamp": {
                                "type": "date"
                            },
                            "test": {
                                "type": "text"
                            }
                        }
                    }
                }
                """);
        }
        final EngineFactory engineFactory = config -> {
            final EngineConfig engineConfigWithCustomAnalyzer = new EngineConfig(
                config.getShardId(),
                config.getThreadPool(),
                config.getIndexSettings(),
                config.getWarmer(),
                config.getStore(),
                config.getMergePolicy(),
                analyzer == null ? config.getAnalyzer() : analyzer,
                config.getSimilarity(),
                new CodecService(null),
                config.getEventListener(),
                config.getQueryCache(),
                config.getQueryCachingPolicy(),
                config.getTranslogConfig(),
                config.getFlushMergesAfter(),
                config.getExternalRefreshListener(),
                config.getInternalRefreshListener(),
                config.getIndexSort(),
                config.getCircuitBreakerService(),
                config.getGlobalCheckpointSupplier(),
                config.retentionLeasesSupplier(),
                config.getPrimaryTermSupplier(),
                IndexModule.DEFAULT_SNAPSHOT_COMMIT_SUPPLIER,
                config.getLeafSorter(),
                config.getWriteLoadTracker()
            );
            return new InternalEngine(engineConfigWithCustomAnalyzer) {
                @Override
                public boolean ensureTranslogSynced(Stream<Translog.Location> locations) throws IOException {
                    runBeforeTranslogFsync.run();
                    return super.ensureTranslogSynced(locations);
                }
            };
        };
        final NodeEnvironment.DataPath dataPath = new NodeEnvironment.DataPath(createTempDir());
        final ShardPath shardPath = new ShardPath(false, dataPath.resolve(shardId), dataPath.resolve(shardId), shardId);
        final var shard = newShard(
            shardRouting,
            shardPath,
            indexMetadata.build(),
            (indexSettings) -> new Store(
                shardId,
                indexSettings,
                directoryWrapper.apply(newFSDirectory(shardPath.resolveIndex())),
                new DummyShardLock(shardId)
            ),
            null,
            engineFactory,
            () -> {},
            RetentionLeaseSyncer.EMPTY,
            EMPTY_EVENT_LISTENER
        );
        recoverShardFromStore(shard);
        assertThat(shard.isDataStreamIndex(), is(createAsDataStream));
        return new ShardRef(shard);
    }

    static class SlowDirectory extends FilterDirectory {
        SlowDirectory(Directory in) {
            super(in);
        }

        static final AtomicBoolean slow = new AtomicBoolean();

        @Override
        public IndexOutput createOutput(String name, IOContext context) throws IOException {
            if (slow.compareAndSet(true, false)) {
                try {
                    Thread.sleep(2000);
                } catch (InterruptedException e) {
                    throw new RuntimeException(e);
                }
            }
            return super.createOutput(name, context);
        }
    }

    static class SleepingAnalyzer extends Analyzer {
        @Override
        protected TokenStreamComponents createComponents(String fieldName) {
            Tokenizer tokenizer = new StandardTokenizer();
            TokenFilter filter = new TokenFilter(tokenizer) {
                @Override
                public boolean incrementToken() throws IOException {
                    boolean hasMoreTokens = input.incrementToken();

                    if (hasMoreTokens) {
                        try {
                            Thread.sleep(1000);
                        } catch (InterruptedException e) {
                            Thread.currentThread().interrupt();
                            throw new AssertionError(e);
                        }
                    }

                    return hasMoreTokens;
                }
            };
            return new TokenStreamComponents(tokenizer, filter);
        }
    }

    private List<String> indexDocs(IndexShard shard, int numDocs) throws Exception {
        return indexDocs(shard, numDocs, true);
    }

    private List<String> indexDocs(IndexShard shard, int numDocs, boolean slowIndexing) throws Exception {
        BulkItemRequest[] bulkItemRequests = new BulkItemRequest[numDocs];
        for (int i = 0; i < numDocs; i++) {
            String source = slowIndexing ? "{\"@timestamp\": \"2020-12-12\", \"test\": \"test\"}" : "{\"@timestamp\": \"2020-12-12\"}";
            bulkItemRequests[i] = new BulkItemRequest(
                i,
                new IndexRequest(shard.shardId().getIndexName()).source(source, XContentType.JSON).id(UUIDs.randomBase64UUID())
            );
        }

        final var bulkShardRequest = new BulkShardRequest(shard.shardId(), WriteRequest.RefreshPolicy.NONE, bulkItemRequests);
        return executeBulkShardRequest(shard, bulkShardRequest);
    }

    private void deleteDocs(IndexShard shard, List<String> docIds) throws Exception {
        BulkItemRequest[] bulkItemRequests = new BulkItemRequest[docIds.size()];
        for (int i = 0; i < docIds.size(); i++) {
            final var docId = docIds.get(i);
            bulkItemRequests[i] = new BulkItemRequest(i, new DeleteRequest(shard.shardId().getIndexName()).id(docId));
        }
        final var bulkShardRequest = new BulkShardRequest(shard.shardId(), WriteRequest.RefreshPolicy.NONE, bulkItemRequests);
        executeBulkShardRequest(shard, bulkShardRequest);
    }

    private List<String> executeBulkShardRequest(IndexShard shard, BulkShardRequest bulkShardRequest) throws Exception {
        final PlainActionFuture<List<String>> future = PlainActionFuture.newFuture();
        threadPool.executor(ThreadPool.Names.WRITE).submit(() -> {
            TransportShardBulkAction.performOnPrimary(
                bulkShardRequest,
                shard,
                mock(UpdateHelper.class),
                () -> 0L,
                (update, shardId, listener) -> listener.onFailure(new RuntimeException("Unexpected mapping update")),
                (listener) -> listener.onFailure(new RuntimeException("Unexpected mapping update")),
                new ActionListener<>() {
                    @Override
                    public void onResponse(TransportReplicationAction.PrimaryResult<BulkShardRequest, BulkShardResponse> primaryResult) {
                        final var docIds = Arrays.stream(primaryResult.finalResponseIfSuccessful.getResponses())
                            .map(BulkItemResponse::getId)
                            .toList();
                        primaryResult.runPostReplicationActions(future.delegateFailure((delegate, unused) -> delegate.onResponse(docIds)));
                    }

                    @Override
                    public void onFailure(Exception e) {
                        future.onFailure(e);
                    }
                },
                threadPool,
                ThreadPool.Names.WRITE
            );
        });

        return future.get();
    }

    /**
    * Creates a fake clock that advances the given {@param tickTime} in every
    * call to the clock
    */
    private LongSupplier fakeClock(TimeValue tickTime) {
        final AtomicLong fakeClock = new AtomicLong();
        return () -> fakeClock.getAndAdd(tickTime.nanos());
    }
}
