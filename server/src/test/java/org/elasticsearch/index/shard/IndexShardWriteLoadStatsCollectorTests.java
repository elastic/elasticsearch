/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.index.shard;

import org.apache.lucene.analysis.Analyzer;
import org.apache.lucene.analysis.TokenFilter;
import org.apache.lucene.analysis.Tokenizer;
import org.apache.lucene.analysis.standard.StandardTokenizer;
import org.apache.lucene.store.ByteBuffersDirectory;
import org.elasticsearch.Version;
import org.elasticsearch.action.ActionListener;
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
import org.elasticsearch.cluster.metadata.IndexMetadata;
import org.elasticsearch.cluster.routing.RecoverySource;
import org.elasticsearch.cluster.routing.ShardRouting;
import org.elasticsearch.cluster.routing.ShardRoutingState;
import org.elasticsearch.cluster.routing.TestShardRouting;
import org.elasticsearch.common.UUIDs;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.util.BigArrays;
import org.elasticsearch.core.IOUtils;
import org.elasticsearch.core.Nullable;
import org.elasticsearch.core.TimeValue;
import org.elasticsearch.env.NodeEnvironment;
import org.elasticsearch.index.Index;
import org.elasticsearch.index.IndexModule;
import org.elasticsearch.index.codec.CodecService;
import org.elasticsearch.index.engine.EngineConfig;
import org.elasticsearch.index.engine.EngineFactory;
import org.elasticsearch.index.engine.InternalEngine;
import org.elasticsearch.index.seqno.RetentionLeaseSyncer;
import org.elasticsearch.index.store.Store;
import org.elasticsearch.index.translog.Translog;
import org.elasticsearch.test.DummyShardLock;
import org.elasticsearch.threadpool.ThreadPool;
import org.elasticsearch.xcontent.XContentType;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.Future;
import java.util.concurrent.atomic.AtomicReference;
import java.util.stream.Stream;

import static org.hamcrest.Matchers.closeTo;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.greaterThan;
import static org.hamcrest.Matchers.greaterThanOrEqualTo;
import static org.hamcrest.Matchers.is;
import static org.hamcrest.Matchers.lessThanOrEqualTo;
import static org.mockito.Mockito.mock;

public class IndexShardWriteLoadStatsCollectorTests extends IndexShardTestCase {
    public void testRegularIndicesWriteLoadIsNotTracked() throws Exception {
        try (var shardRef = createRegularIndexShard()) {
            final var shard = shardRef.shard();
            final var samplingFrequency = TimeValue.timeValueSeconds(1);

            for (int i = 0; i < 2; i++) {
                indexDocsWithTextField(shard, randomIntBetween(1, 10));
                shard.recordWriteLoad(samplingFrequency.nanos());
            }

            assertThat(shard.getTotalIndexingTimeInNanos(), is(equalTo(0L)));
            assertThat(shard.writeLoadStats().indexingLoadAvg(), is(equalTo(0.0)));
        }
    }

    public void testDataStreamsIndexingLoadTracking() throws Exception {
        try (var shardRef = createDataStreamShard()) {
            final var shard = shardRef.shard();
            final var samplingFrequency = TimeValue.timeValueMillis(500);

            assertThat(shard.writeLoadStats().indexingLoadAvg(), is(equalTo(0.0)));

            final int numberOfBulks = randomIntBetween(5, 10);

            for (int i = 0; i < numberOfBulks; i++) {
                indexDocsWithTextField(shard, randomIntBetween(15, 30));
                shard.recordWriteLoad(samplingFrequency.nanos());
            }

            assertThat(shard.writeLoadStats().indexingLoadAvg(), is(greaterThan(0.0)));
        }
    }

    public void testSlowIndexingOperationsTracking() throws Exception {
        // Use a custom analyzer that executes the given runnable
        // during the analysis phase, this allows simulating long
        // indexing operations. The provided runnable will run once
        // per text token in the indexed doc. #indexDocs index a
        // document with a single token text field, effectively
        // running this once per doc.
        final var analyzer = new TestAnalyzer();

        try (var shardRef = createDataStreamShardWithAnalyzer(analyzer)) {
            final var shard = shardRef.shard();
            final var samplingFrequency = TimeValue.timeValueMillis(500);

            final int numberOfProcessors = randomIntBetween(2, 4);
            final int usedProcessors = randomIntBetween(1, numberOfProcessors);

            final int numberOfSlowBulks = randomIntBetween(1, 6);

            long previousTotalIndexingTimeNanos = 0;
            for (int i = 0; i < numberOfSlowBulks; i++) {
                // We want to simulate that usedProcessors are used during indexing.
                // To achieve that goal, we should consume samplingFrequency * usedProcessors CPU time during indexing.
                // Since we're running real indexing we should account for that real work (which should be fast) and
                // subtract around 10% of the total time to be consumed sleeping during indexing.
                final long totalIndexingTimeInMillis = usedProcessors * (samplingFrequency.millis() - (samplingFrequency.millis() / 10));

                final List<Long> sleepTimePerDoc = distributeTimeIntoMultipleOperations(totalIndexingTimeInMillis);
                final var sleepTimePerDocIter = sleepTimePerDoc.iterator();
                analyzer.setRunDuringDocAnalysis(() -> {
                    long indexOpTime = 0;
                    // Each bulk operation is dispatched into a WRITE thread,
                    // hence we should synchronize the iterator.
                    synchronized (sleepTimePerDocIter) {
                        if (sleepTimePerDocIter.hasNext()) {
                            indexOpTime = sleepTimePerDocIter.next();
                        }
                    }
                    if (indexOpTime > 0) {
                        sleep(indexOpTime);
                    }
                });

                indexDocsWithTextField(shard, sleepTimePerDoc.size());
                shard.recordWriteLoad(samplingFrequency.nanos());

                final long totalIndexingTimeInNanos = shard.getTotalIndexingTimeInNanos();
                assertThat(totalIndexingTimeInNanos, is(greaterThan(previousTotalIndexingTimeNanos)));
                previousTotalIndexingTimeNanos = totalIndexingTimeInNanos;
            }

            {
                final var writeLoadStats = shard.writeLoadStats();

                // ensure that we never measure more than the number of processors
                assertThat(writeLoadStats.indexingLoadAvg(), is(lessThanOrEqualTo((double) usedProcessors)));
                assertThat(writeLoadStats.indexingLoadAvg(), is(closeTo(usedProcessors, 0.4)));
            }
        }
    }

    public void testFsyncIsAccountedInIndexingWriteLoad() throws Exception {
        final AtomicReference<Runnable> runBeforeFsync = new AtomicReference<>(() -> {});
        try (var shardRef = createDataStreamShardWithFSyncNotifier(runBeforeFsync)) {
            final var shard = shardRef.shard();
            final var samplingFrequency = TimeValue.timeValueMillis(500);

            // Run a few bulk operations with a slow fsync, increasing the max cpu usage.
            // Technically the thread is blocked waiting for IO, but the thread is blocked anyway...
            runBeforeFsync.set(() -> sleep(samplingFrequency.millis()));
            final int numberOfBulks = randomIntBetween(2, 4);
            for (int i = 0; i < numberOfBulks; i++) {
                indexDocsWithTextField(shard, randomIntBetween(1, 20));
                shard.recordWriteLoad(samplingFrequency.nanos());
            }

            {
                final var writeLoadStats = shard.writeLoadStats();
                // Fsync takes at least the sampling frequency time to complete, therefore the load should be >= 1.0
                assertThat(writeLoadStats.indexingLoadAvg(), is(greaterThanOrEqualTo(1.0)));
            }
        }
    }

    public void testDeleteTimeIsTracked() throws Exception {
        try (var shardRef = createDataStreamShard()) {
            final var shard = shardRef.shard();

            final var docIds = indexDocsWithTextField(shard, 1000);

            final long totalIndexingTimeBeforeDeletingDocs = shard.getTotalIndexingTimeInNanos();

            deleteDocs(shard, docIds);

            assertThat(shard.getTotalIndexingTimeInNanos(), is(greaterThan(totalIndexingTimeBeforeDeletingDocs)));
        }
    }

    public void testLongRunningIndexingOperationReportsPartialProgress() throws Exception {
        final var analyzer = new TestAnalyzer();
        try (var shardRef = createDataStreamShardWithAnalyzer(analyzer)) {
            var shard = shardRef.shard();

            final var samplingFrequency = TimeValue.timeValueMillis(100);

            analyzer.setRunDuringDocAnalysis(() -> sleep(1000));

            final var indexingFuture = indexDocsWithTextFieldAsync(shard, 1);

            // Take samples while the long-running indexing operation is not completed
            while (indexingFuture.isDone() == false) {
                sleep(samplingFrequency.millis() - 5);
                shard.recordWriteLoad(samplingFrequency.nanos());
            }

            indexingFuture.get();
            assertThat(shard.writeLoadStats().indexingLoadAvg(), is(closeTo(1.0, 0.2)));
        }
    }

    private List<Long> distributeTimeIntoMultipleOperations(long totalTimeInMillis) {
        long remainingTimeInMillis = totalTimeInMillis;
        final List<Long> operationsTime = new ArrayList<>();
        while (remainingTimeInMillis > 0) {
            long operationTime = randomLongBetween(1, remainingTimeInMillis);
            operationsTime.add(operationTime);
            remainingTimeInMillis -= operationTime;
        }
        return List.copyOf(operationsTime);
    }

    record ShardRef(IndexShard shard) implements AutoCloseable {
        @Override
        public void close() throws IOException {
            IOUtils.close(shard.store(), () -> shard.close("test", false));
        }
    }

    private ShardRef createDataStreamShard() throws Exception {
        return createShard(true, null, null);
    }

    private ShardRef createDataStreamShardWithAnalyzer(Analyzer analyzer) throws Exception {
        return createShard(true, analyzer, null);
    }

    private ShardRef createDataStreamShardWithFSyncNotifier(AtomicReference<Runnable> runBeforeTranslogFSync) throws Exception {
        return createShard(true, null, runBeforeTranslogFSync);
    }

    private ShardRef createRegularIndexShard() throws Exception {
        return createShard(false, null, null);
    }

    private ShardRef createShard(
        boolean createAsDataStream,
        @Nullable Analyzer analyzer,
        @Nullable AtomicReference<Runnable> runBeforeTranslogFsync
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
                new CodecService(null, BigArrays.NON_RECYCLING_INSTANCE),
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
                config.getLeafSorter()
            );
            return new InternalEngine(engineConfigWithCustomAnalyzer) {
                @Override
                public boolean ensureTranslogSynced(Stream<Translog.Location> locations) throws IOException {
                    if (runBeforeTranslogFsync != null) {
                        runBeforeTranslogFsync.get().run();
                    }
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
                // Use an in-memory directory to avoid timings variations
                // while we run the test suite in slower machines (i.e. in CI).
                new ByteBuffersDirectory(),
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

    private static void sleep(long timeInMillis) {
        try {
            Thread.sleep(timeInMillis);
        } catch (InterruptedException e) {
            throw new RuntimeException(e);
        }
    }

    static class TestAnalyzer extends Analyzer {
        final AtomicReference<Runnable> runDuringDocAnalysis = new AtomicReference<>(() -> {});

        @Override
        protected TokenStreamComponents createComponents(String fieldName) {
            Tokenizer tokenizer = new StandardTokenizer();
            TokenFilter filter = new TokenFilter(tokenizer) {
                @Override
                public boolean incrementToken() throws IOException {
                    boolean hasMoreTokens = input.incrementToken();

                    if (hasMoreTokens) {
                        runDuringDocAnalysis.get().run();
                    }

                    return hasMoreTokens;
                }
            };
            return new TokenStreamComponents(tokenizer, filter);
        }

        void setRunDuringDocAnalysis(Runnable runDuringDocAnalysis) {
            this.runDuringDocAnalysis.set(runDuringDocAnalysis);
        }
    }

    private List<String> indexDocsWithTextField(IndexShard shard, int numDocs) throws Exception {
        return indexDocsWithTextFieldAsync(shard, numDocs).get();
    }

    private Future<List<String>> indexDocsWithTextFieldAsync(IndexShard shard, int numDocs) throws Exception {
        BulkItemRequest[] bulkItemRequests = new BulkItemRequest[numDocs];
        for (int i = 0; i < numDocs; i++) {
            String source = "{\"@timestamp\": \"2022-12-12\", \"test\": \"test\"}";
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
        executeBulkShardRequest(shard, bulkShardRequest).get();
    }

    private Future<List<String>> executeBulkShardRequest(IndexShard shard, BulkShardRequest bulkShardRequest) throws Exception {
        final PlainActionFuture<List<String>> future = PlainActionFuture.newFuture();
        final UpdateHelper updateHelper = mock(UpdateHelper.class);
        final CountDownLatch bulkDispatched = new CountDownLatch(1);
        threadPool.executor(ThreadPool.Names.WRITE).submit(() -> {
            bulkDispatched.countDown();
            TransportShardBulkAction.performOnPrimary(
                bulkShardRequest,
                shard,
                updateHelper,
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

        bulkDispatched.await();
        return future;
    }
}
