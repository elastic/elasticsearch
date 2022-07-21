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
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.Consumer;
import java.util.function.Function;
import java.util.function.LongSupplier;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import static org.hamcrest.Matchers.closeTo;
import static org.hamcrest.Matchers.empty;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.greaterThan;
import static org.hamcrest.Matchers.hasSize;
import static org.hamcrest.Matchers.is;
import static org.hamcrest.Matchers.lessThanOrEqualTo;
import static org.hamcrest.Matchers.notNullValue;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

public class IndicesWriteLoadStatsCollectorTests extends IndexShardTestCase {
    public void testRegularIndicesLoadIsNotTracked() throws Exception {
        try (var shardRef = createRegularIndexShard()) {
            final var shard = shardRef.shard();

            final var samplingFrequency = TimeValue.timeValueSeconds(1);
            final var indicesWriteLoadStatsCollector = createIndicesWriteLoadStatsCollector(shard, samplingFrequency);

            for (int i = 0; i < 2; i++) {
                indexDocs(shard, randomIntBetween(1, 10));
                indicesWriteLoadStatsCollector.collectWriteLoadStats();
                assertThat(shard.getTotalIndexingTimeInNanos(), is(equalTo(0L)));
            }
            final var shardWriteLoadHistogramSnapshots = indicesWriteLoadStatsCollector.getWriteLoadHistogramSnapshotsAndReset();
            assertThat(shardWriteLoadHistogramSnapshots, is(empty()));
        }
    }

    public void testIndexingLoadTracking() throws Exception {
        final AtomicReference<Runnable> sleepTimePerDocumentToken = new AtomicReference<>(() -> {});
        // Use a custom analyzer that executes the given runnable
        // during the analysis phase, this allows simulating long
        // indexing operations
        final var analyzer = new SleepingAnalyzer(sleepTimePerDocumentToken);

        try (var shardRef = createDataStreamShardWithAnalyzer(analyzer)) {
            final var shard = shardRef.shard();

            final var samplingFrequency = TimeValue.timeValueMillis(500);
            final var indicesWriteLoadStatsCollector = createIndicesWriteLoadStatsCollector(shard, samplingFrequency);

            long previousTotalIndexingTimeNanos = 0;
            int numberOfProcessors = randomIntBetween(2, 4);
            final int maxCPUsUsed = randomIntBetween(1, numberOfProcessors);

            for (int i = 0; i < 2; i++) {
                int samplingFrequencyInMillis = (int) samplingFrequency.millis();
                int remainingIndexingTime = maxCPUsUsed * (samplingFrequencyInMillis - 40);
                final List<Integer> indexTime = new ArrayList<>();
                while (remainingIndexingTime > 0) {
                    int opTime = randomIntBetween(1, remainingIndexingTime);
                    indexTime.add(opTime);
                    remainingIndexingTime -= opTime;
                }

                final var iter = indexTime.iterator();

                sleepTimePerDocumentToken.set(() -> {
                    if (iter.hasNext()) {
                        sleep(iter.next());
                    }
                });

                int numDocs = indexTime.size();

                indexDocs(shard, numDocs);
                indicesWriteLoadStatsCollector.collectWriteLoadStats();
                final long totalIndexingTimeInNanos = shard.getTotalIndexingTimeInNanos();
                assertThat(totalIndexingTimeInNanos, is(greaterThan(previousTotalIndexingTimeNanos)));
                previousTotalIndexingTimeNanos = totalIndexingTimeInNanos;
            }

            // Run a few fast operations to bring down the median
            for (int i = 0; i < 4; i++) {
                sleepTimePerDocumentToken.set(() -> {});
                indexDocs(shard, randomIntBetween(10, 30));
                indicesWriteLoadStatsCollector.collectWriteLoadStats();
            }

            {
                final var shardLoadHistogramSnapshots = indicesWriteLoadStatsCollector.getWriteLoadHistogramSnapshotsAndReset();

                assertThat(shardLoadHistogramSnapshots, hasSize(1));
                final var shardWriteLoadHistogramSnapshot = shardLoadHistogramSnapshots.get(0);
                final var indexingLoadHistogramSnapshot = shardWriteLoadHistogramSnapshot.indexLoadHistogramSnapshot();

                assertMaxIsCloseTo(indexingLoadHistogramSnapshot, maxCPUsUsed, 0.3);
                assertThat(indexingLoadHistogramSnapshot.p50(), is(closeTo(0.0, 0.1)));
                // ensure that we never measure more than the number of processors
                assertThat(indexingLoadHistogramSnapshot.max(), is(lessThanOrEqualTo((double) numberOfProcessors)));
            }

            {
                // We didn't have any readings after the previous reset
                final var shardLoadHistogramSnapshots = getWriteLoadHistogram(indicesWriteLoadStatsCollector);
                final var shardWriteLoadHistogramSnapshot = shardLoadHistogramSnapshots.get(shard.shardId().getIndexName());
                final var indexingLoadHistogramSnapshot = shardWriteLoadHistogramSnapshot.indexLoadHistogramSnapshot();

                assertThat(indexingLoadHistogramSnapshot.max(), is(equalTo(0.0)));
                assertThat(indexingLoadHistogramSnapshot.p99(), is(equalTo(0.0)));
                assertThat(indexingLoadHistogramSnapshot.p90(), is(equalTo(0.0)));
                assertThat(indexingLoadHistogramSnapshot.p50(), is(equalTo(0.0)));
            }
        }
    }

    public void testFsyncIsAccountedInIndexingWriteLoad() throws Exception {
        final AtomicLong timeToSleepBeforeFsync = new AtomicLong(0);
        final Runnable runBeforeFsync = () -> sleep(timeToSleepBeforeFsync.get());
        try (var shardRef = createDataStreamShardWithFSyncNotifier(runBeforeFsync)) {
            final var shard = shardRef.shard();

            final var samplingFrequency = TimeValue.timeValueMillis(500);
            final var indicesWriteLoadStatsCollector = createIndicesWriteLoadStatsCollector(shard, samplingFrequency);

            for (int i = 0; i < 5; i++) {
                indexDocs(shard, randomIntBetween(20, 30));
                indicesWriteLoadStatsCollector.collectWriteLoadStats();
            }

            // Ensure that the max write load with fast fsyncs is really low
            {
                final var shardLoadHistogramSnapshots = indicesWriteLoadStatsCollector.getWriteLoadHistogramSnapshotsAndReset();

                assertThat(shardLoadHistogramSnapshots, hasSize(1));
                final var shardWriteLoadHistogramSnapshot = shardLoadHistogramSnapshots.get(0);
                final var indexingLoadHistogramSnapshot = shardWriteLoadHistogramSnapshot.indexLoadHistogramSnapshot();

                assertMaxIsCloseTo(indexingLoadHistogramSnapshot, 0.1, 0.1);
            }

            timeToSleepBeforeFsync.set(samplingFrequency.millis());

            // Run a few bulk operations with a slow fsync that would increase the max cpu usage
            for (int i = 0; i < 2; i++) {
                indexDocs(shard, randomIntBetween(1, 20));
                indicesWriteLoadStatsCollector.collectWriteLoadStats();
            }

            {
                final var shardLoadHistogramSnapshots = indicesWriteLoadStatsCollector.getWriteLoadHistogramSnapshotsAndReset();

                assertThat(shardLoadHistogramSnapshots, hasSize(1));
                final var shardWriteLoadHistogramSnapshot = shardLoadHistogramSnapshots.get(0);
                final var indexingLoadHistogramSnapshot = shardWriteLoadHistogramSnapshot.indexLoadHistogramSnapshot();

                assertMaxIsCloseTo(indexingLoadHistogramSnapshot, 1, 0.1);
            }
        }
    }

    public void testRefreshLoadTracking() throws Exception {
        final AtomicReference<Consumer<String>> runBeforeCreateOutput = new AtomicReference<>((fileName) -> {});
        final Function<Directory, Directory> directoryWrapper = (directory -> new FilterDirectory(directory) {
            @Override
            public IndexOutput createOutput(String name, IOContext context) throws IOException {
                runBeforeCreateOutput.get().accept(name);
                return super.createOutput(name, context);
            }
        });

        try (var shardRef = createDataStreamShardWithDirectoryWrapper(directoryWrapper)) {
            final var shard = shardRef.shard();

            final var samplingFrequency = TimeValue.timeValueMillis(500);
            final var indicesWriteLoadStatsCollector = createIndicesWriteLoadStatsCollector(shard, samplingFrequency);

            final long initialTotalRefreshTime = shard.getTotalRefreshTimeInNanos();

            indexDocs(shard, randomIntBetween(10, 20));
            indicesWriteLoadStatsCollector.collectWriteLoadStats();

            runBeforeCreateOutput.set((fileName) -> {
                if (fileName.endsWith(".si")) {
                    sleep(samplingFrequency.millis() - 40);
                }
            });
            shard.refresh("test");

            assertThat(shard.getTotalRefreshTimeInNanos(), is(greaterThan(initialTotalRefreshTime)));

            indicesWriteLoadStatsCollector.collectWriteLoadStats();

            final var shardLoadHistograms = indicesWriteLoadStatsCollector.getWriteLoadHistogramSnapshotsAndReset();

            assertThat(shardLoadHistograms, hasSize(1));
            final var shardWriteLoadDistribution = shardLoadHistograms.get(0);

            final HistogramSnapshot refreshLoadHistogramSnapshot = shardWriteLoadDistribution.refreshLoadHistogramSnapshot();
            assertMaxIsCloseTo(refreshLoadHistogramSnapshot, 1.0, 0.2);
        }
    }

    public void testMergeLoadTracking() throws Exception {
        final AtomicReference<Consumer<String>> runBeforeCreateOutput = new AtomicReference<>((fileName) -> {});
        final Function<Directory, Directory> directoryWrapper = (directory -> new FilterDirectory(directory) {
            @Override
            public IndexOutput createOutput(String name, IOContext context) throws IOException {
                runBeforeCreateOutput.get().accept(name);
                return super.createOutput(name, context);
            }
        });
        try (var shardRef = createDataStreamShardWithDirectoryWrapper(directoryWrapper)) {
            final var shard = shardRef.shard();

            final var samplingFrequency = TimeValue.timeValueMillis(500);
            final var indicesWriteLoadStatsCollector = createIndicesWriteLoadStatsCollector(shard, samplingFrequency);

            final int numberOfSegments = randomIntBetween(2, 4);
            for (int i = 0; i < numberOfSegments; i++) {
                indexDocs(shard, randomIntBetween(1, 20));
                shard.refresh("test");

                indicesWriteLoadStatsCollector.collectWriteLoadStats();
            }
            assertThat(shard.getTotalMergeTimeInNanos(), is(equalTo(0L)));

            runBeforeCreateOutput.set(fileName -> {
                if (fileName.endsWith(".si")) {
                    sleep(samplingFrequency.millis() - 20);
                }
            });
            shard.forceMerge(new ForceMergeRequest().maxNumSegments(1).flush(false));

            assertThat(shard.getTotalMergeTimeInNanos(), is(greaterThan(0L)));

            indicesWriteLoadStatsCollector.collectWriteLoadStats();

            final var shardLoadHistograms = indicesWriteLoadStatsCollector.getWriteLoadHistogramSnapshotsAndReset();

            assertThat(shardLoadHistograms, hasSize(1));
            final var shardWriteLoadDistribution = shardLoadHistograms.get(0);

            assertThat(shardWriteLoadDistribution.mergeLoadHistogramSnapshot().max(), is(closeTo(1.0, 0.1)));
        }
    }

    public void testDeleteTimeIsTracked() throws Exception {
        try (var shardRef = createDataStreamShard()) {
            final var shard = shardRef.shard();

            final var docIds = indexDocs(shard, 1000);

            final long totalIndexingTimeBeforeDeleteDocs = shard.getTotalIndexingTimeInNanos();

            deleteDocs(shard, docIds);

            assertThat(shard.getTotalIndexingTimeInNanos(), is(greaterThan(totalIndexingTimeBeforeDeleteDocs)));
        }
    }

    public void testShardLoadDistributionInfoIsClearedAfterDeletion() throws Exception {
        try (var shardRef = createDataStreamShard()) {
            final var shard = shardRef.shard();

            final var samplingFrequency = TimeValue.timeValueSeconds(1);
            final var indicesWriteLoadStatsCollector = createIndicesWriteLoadStatsCollector(shard, samplingFrequency);

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

            final var indicesWriteLoadStatsCollector = new IndicesWriteLoadStatsCollector(
                clusterService,
                "nodeId",
                fakeClock(samplingFrequency)
            );
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

            final Map<String, ShardWriteLoadHistogramSnapshot> writeLoadHistogramAfterRollOver = indicesWriteLoadStatsCollector
                .getWriteLoadHistogramSnapshotsAndReset()
                .stream()
                .collect(Collectors.toMap(histogramSnapshot -> histogramSnapshot.shardId().getIndexName(), Function.identity()));

            assertThat(writeLoadHistogramAfterRollOver.entrySet(), hasSize(2));
            assertThat(writeLoadHistogramAfterRollOver.get(firstDataStreamIndex.getName()), is(notNullValue()));
            assertThat(writeLoadHistogramAfterRollOver.get(rolledOverIndex.getName()), is(notNullValue()));

            final Map<String, ShardWriteLoadHistogramSnapshot> writeLoadAfterRolloverCleanup = indicesWriteLoadStatsCollector
                .getWriteLoadHistogramSnapshotsAndReset()
                .stream()
                .collect(Collectors.toMap(histogramSnapshot -> histogramSnapshot.shardId().getIndexName(), Function.identity()));

            assertThat(writeLoadAfterRolloverCleanup.entrySet(), hasSize(1));

            final ShardWriteLoadHistogramSnapshot rolledOverIndexHistogram = writeLoadHistogramAfterRollOver.get(rolledOverIndex.getName());
            assertThat(rolledOverIndexHistogram, is(notNullValue()));
            assertThat(rolledOverIndexHistogram.shardId(), is(equalTo(rolledOverShard.shardId())));
        }
    }

    public void testWriteLoadIsAggregatedAtNodeLevel() throws Exception {
        final AtomicReference<Runnable> sleepTimePerDocumentToken = new AtomicReference<>(() -> {});
        // Use a custom analyzer that executes the given runnable
        // during the analysis phase, this allows simulating long
        // indexing operations
        final var analyzer = new SleepingAnalyzer(sleepTimePerDocumentToken);

        try (var shard1Ref = createDataStreamShardWithAnalyzer(analyzer); var shard2Ref = createDataStreamShardWithAnalyzer(analyzer)) {
            final var shard1 = shard1Ref.shard();
            final var index1 = shard1.shardId().getIndex();

            final var shard2 = shard2Ref.shard();
            final var index2 = shard2.shardId().getIndex();

            final var clusterService = mock(ClusterService.class);
            final var clusterState = ClusterState.builder(new ClusterName("cluster"))
                .metadata(
                    Metadata.builder()
                        .put(getDataStream(index1))
                        .put(getDataStream(index2))
                        .put(
                            IndexMetadata.builder(index1.getName())
                                .settings(settings(Version.CURRENT))
                                .numberOfShards(1)
                                .numberOfReplicas(0)
                                .build(),
                            false
                        )
                        .put(
                            IndexMetadata.builder(index2.getName())
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

            final var samplingFrequency = TimeValue.timeValueMillis(500);
            final var indicesWriteLoadStatsCollector = new IndicesWriteLoadStatsCollector(
                clusterService,
                "nodeId",
                fakeClock(samplingFrequency)
            );
            indicesWriteLoadStatsCollector.afterIndexShardStarted(shard1);
            indicesWriteLoadStatsCollector.afterIndexShardStarted(shard2);

            sleepTimePerDocumentToken.set(() -> sleep(10));

            for (int i = 0; i < 2; i++) {
                indexDocs(shard1, randomIntBetween(10, 20));
                indexDocs(shard2, randomIntBetween(10, 20));
                indicesWriteLoadStatsCollector.collectWriteLoadStats();
            }

            final var indicesWriteLoadHistogramSnapshots = indicesWriteLoadStatsCollector.getWriteLoadHistogramSnapshotsAndReset();
            final var totalMaxIndexingWriteLoad = indicesWriteLoadHistogramSnapshots.stream()
                .mapToDouble(writeLoad -> writeLoad.indexLoadHistogramSnapshot().max())
                .sum();
            assertThat(totalMaxIndexingWriteLoad, is(greaterThan(0.0)));
            final var nodeWriteLoadHistogramSnapshot = indicesWriteLoadStatsCollector.getNodeWriteLoadHistogramSnapshotAndReset();

            assertThat(nodeWriteLoadHistogramSnapshot.indexLoadHistogramSnapshot().max(), is(closeTo(totalMaxIndexingWriteLoad, 0.1)));
        }
    }

    public void testLongRunningOperation() throws Exception {
        final AtomicReference<Runnable> sleepTimePerDocumentToken = new AtomicReference<>(() -> {});
        // Use a custom analyzer that executes the given runnable
        // during the analysis phase, this allows simulating long
        // indexing operations
        final var analyzer = new SleepingAnalyzer(sleepTimePerDocumentToken);
        try (var shardRef = createDataStreamShardWithAnalyzer(analyzer)) {
            var shard = shardRef.shard();

            final var samplingFrequency = TimeValue.timeValueMillis(100);
            final var indicesWriteLoadStatsCollector = createIndicesWriteLoadStatsCollector(shard, samplingFrequency);

            sleepTimePerDocumentToken.set(() -> sleep(500));

            final AtomicBoolean run = new AtomicBoolean(true);

            final var samplingThread = new Thread(() -> {
                while (run.get()) {
                    indicesWriteLoadStatsCollector.collectWriteLoadStats();
                    sleep(95);
                }
            });

            samplingThread.start();
            indexDocs(shard, 1);
            run.set(false);
            samplingThread.join();

            final var nodeWriteLoadHistogramSnapshot = indicesWriteLoadStatsCollector.getNodeWriteLoadHistogramSnapshotAndReset();
            assertThat(nodeWriteLoadHistogramSnapshot.indexLoadHistogramSnapshot().max(), is(closeTo(1.0, 0.1)));
        }
    }

    private Map<String, ShardWriteLoadHistogramSnapshot> getWriteLoadHistogram(IndicesWriteLoadStatsCollector collector) {
        return collector.getWriteLoadHistogramSnapshotsAndReset()
            .stream()
            .collect(Collectors.toMap(histogram -> histogram.shardId().getIndexName(), Function.identity()));
    }

    private void assertMaxIsCloseTo(HistogramSnapshot histogramSnapshot, double expected, double error) {
        assertThat(histogramSnapshot.max(), is(closeTo(expected, error)));
        assertThat(histogramSnapshot.p99(), is(lessThanOrEqualTo(histogramSnapshot.max())));
        assertThat(histogramSnapshot.p90(), is(lessThanOrEqualTo(histogramSnapshot.p99())));
        assertThat(histogramSnapshot.p50(), is(lessThanOrEqualTo(histogramSnapshot.p90())));
    }

    private IndicesWriteLoadStatsCollector createIndicesWriteLoadStatsCollector(IndexShard shard, TimeValue samplingFrequency) {
        final var clusterService = getClusterServiceWithClusterStateIncludingShard(shard);
        final var indicesWriteLoadStatsCollector = new IndicesWriteLoadStatsCollector(
            clusterService,
            "nodeId",
            fakeClock(samplingFrequency)
        );
        indicesWriteLoadStatsCollector.afterIndexShardStarted(shard);
        return indicesWriteLoadStatsCollector;
    }

    private ClusterService getClusterServiceWithClusterStateIncludingShard(IndexShard shard) {
        final var clusterService = mock(ClusterService.class);
        final var clusterState = clusterStateWithShardIndex(shard);
        when(clusterService.state()).thenReturn(clusterState);
        return clusterService;
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

    private ClusterState clusterStateWithShardIndex(IndexShard shard) {
        final var index = shard.shardId().getIndex();
        final Metadata.Builder metadata = Metadata.builder()
            .put(
                IndexMetadata.builder(index.getName()).settings(settings(Version.CURRENT)).numberOfShards(1).numberOfReplicas(0).build(),
                false
            );

        if (shard.isDataStreamIndex()) {
            metadata.put(getDataStream(index));
        }

        return ClusterState.builder(new ClusterName("cluster")).metadata(metadata).build();
    }

    record ShardRef(IndexShard shard) implements AutoCloseable {
        @Override
        public void close() throws IOException {
            IOUtils.close(shard.store(), () -> shard.close("test", false));
        }
    }

    private ShardRef createDataStreamShard() throws Exception {
        return createShard(true, null, () -> {}, Function.identity());
    }

    private ShardRef createDataStreamShardWithAnalyzer(Analyzer analyzer) throws Exception {
        return createShard(true, analyzer, () -> {}, Function.identity());
    }

    private ShardRef createDataStreamShardWithFSyncNotifier(Runnable fsync) throws Exception {
        return createShard(true, null, fsync, Function.identity());
    }

    private ShardRef createDataStreamShardWithDirectoryWrapper(Function<Directory, Directory> directoryWrapper) throws Exception {
        return createShard(true, null, () -> {}, directoryWrapper);
    }

    private ShardRef createRegularIndexShard() throws Exception {
        return createShard(false, null, () -> {}, Function.identity());
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

    private static void sleep(long time) {
        try {
            Thread.sleep(time);
        } catch (InterruptedException e) {
            throw new RuntimeException(e);
        }
    }

    static class SleepingAnalyzer extends Analyzer {
        private final AtomicReference<Runnable> perTokenOp;

        SleepingAnalyzer(AtomicReference<Runnable> perTokenOp) {
            this.perTokenOp = perTokenOp;
        }

        @Override
        protected TokenStreamComponents createComponents(String fieldName) {
            Tokenizer tokenizer = new StandardTokenizer();
            TokenFilter filter = new TokenFilter(tokenizer) {
                @Override
                public boolean incrementToken() throws IOException {
                    boolean hasMoreTokens = input.incrementToken();

                    if (hasMoreTokens) {
                        perTokenOp.get().run();
                    }

                    return hasMoreTokens;
                }
            };
            return new TokenStreamComponents(tokenizer, filter);
        }
    }

    private List<String> indexDocs(IndexShard shard, int numDocs) throws Exception {
        BulkItemRequest[] bulkItemRequests = new BulkItemRequest[numDocs];
        for (int i = 0; i < numDocs; i++) {
            String source = "{\"@timestamp\": \"2020-12-12\", \"test\": \"test\"}";
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
