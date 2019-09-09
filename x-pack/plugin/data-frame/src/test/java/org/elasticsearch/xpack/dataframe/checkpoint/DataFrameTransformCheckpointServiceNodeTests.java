/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */

package org.elasticsearch.xpack.dataframe.checkpoint;

import org.elasticsearch.action.ActionListener;
import org.elasticsearch.action.ActionRequest;
import org.elasticsearch.action.ActionResponse;
import org.elasticsearch.action.ActionType;
import org.elasticsearch.action.admin.indices.get.GetIndexRequest;
import org.elasticsearch.action.admin.indices.get.GetIndexResponse;
import org.elasticsearch.action.admin.indices.stats.CommonStats;
import org.elasticsearch.action.admin.indices.stats.IndicesStatsRequest;
import org.elasticsearch.action.admin.indices.stats.IndicesStatsResponse;
import org.elasticsearch.action.admin.indices.stats.ShardStats;
import org.elasticsearch.cluster.routing.RecoverySource;
import org.elasticsearch.cluster.routing.ShardRouting;
import org.elasticsearch.cluster.routing.UnassignedInfo;
import org.elasticsearch.common.UUIDs;
import org.elasticsearch.index.Index;
import org.elasticsearch.index.cache.query.QueryCacheStats;
import org.elasticsearch.index.cache.request.RequestCacheStats;
import org.elasticsearch.index.engine.SegmentsStats;
import org.elasticsearch.index.fielddata.FieldDataStats;
import org.elasticsearch.index.flush.FlushStats;
import org.elasticsearch.index.get.GetStats;
import org.elasticsearch.index.merge.MergeStats;
import org.elasticsearch.index.refresh.RefreshStats;
import org.elasticsearch.index.search.stats.SearchStats;
import org.elasticsearch.index.seqno.SeqNoStats;
import org.elasticsearch.index.shard.DocsStats;
import org.elasticsearch.index.shard.IndexingStats;
import org.elasticsearch.index.shard.ShardId;
import org.elasticsearch.index.shard.ShardPath;
import org.elasticsearch.index.store.StoreStats;
import org.elasticsearch.index.warmer.WarmerStats;
import org.elasticsearch.search.suggest.completion.CompletionStats;
import org.elasticsearch.test.client.NoOpClient;
import org.elasticsearch.xpack.core.dataframe.transforms.DataFrameIndexerPosition;
import org.elasticsearch.xpack.core.dataframe.transforms.DataFrameIndexerPositionTests;
import org.elasticsearch.xpack.core.dataframe.transforms.DataFrameTransformCheckpoint;
import org.elasticsearch.xpack.core.dataframe.transforms.DataFrameTransformCheckpointStats;
import org.elasticsearch.xpack.core.dataframe.transforms.DataFrameTransformCheckpointingInfo;
import org.elasticsearch.xpack.core.dataframe.transforms.DataFrameTransformConfigTests;
import org.elasticsearch.xpack.core.dataframe.transforms.DataFrameTransformProgress;
import org.elasticsearch.xpack.core.dataframe.transforms.DataFrameTransformProgressTests;
import org.elasticsearch.xpack.dataframe.DataFrameSingleNodeTestCase;
import org.elasticsearch.xpack.dataframe.notifications.DataFrameAuditor;
import org.elasticsearch.xpack.dataframe.persistence.DataFrameTransformsConfigManager;
import org.junit.AfterClass;
import org.junit.Before;

import java.nio.file.Path;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Set;

import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

public class DataFrameTransformCheckpointServiceNodeTests extends DataFrameSingleNodeTestCase {

    // re-use the mock client for the whole test suite as the underlying thread pool and the
    // corresponding context if recreated cause unreliable test execution
    // see https://github.com/elastic/elasticsearch/issues/45238 and https://github.com/elastic/elasticsearch/issues/42577
    private static MockClientForCheckpointing mockClientForCheckpointing = null;

    private DataFrameTransformsConfigManager transformsConfigManager;
    private DataFrameTransformsCheckpointService transformsCheckpointService;

    private class MockClientForCheckpointing extends NoOpClient {

        private volatile ShardStats[] shardStats;
        private volatile String[] indices;

        MockClientForCheckpointing(String testName) {
            super(testName);
        }

        void setShardStats(ShardStats[] shardStats) {
            this.shardStats = shardStats;

            Set<String> indices = new HashSet<>();
            for (ShardStats s:shardStats) {
                indices.add(s.getShardRouting().getIndexName());
            }

            this.indices = indices.toArray(new String[0]);
        }

        @SuppressWarnings("unchecked")
        @Override
        protected <Request extends ActionRequest, Response extends ActionResponse>
        void doExecute(ActionType<Response> action, Request request, ActionListener<Response> listener) {

            if (request instanceof GetIndexRequest) {
                // for this test we only need the indices
                assert(indices != null);
                final GetIndexResponse indexResponse = new GetIndexResponse(indices, null, null, null, null);

                listener.onResponse((Response) indexResponse);
                return;
            } else if (request instanceof IndicesStatsRequest) {

                // IndicesStatsResponse is package private, therefore using a mock
                final IndicesStatsResponse indicesStatsResponse = mock(IndicesStatsResponse.class);
                when(indicesStatsResponse.getShards()).thenReturn(shardStats);
                when(indicesStatsResponse.getFailedShards()).thenReturn(0);


                listener.onResponse((Response) indicesStatsResponse);
                return;
            }

            super.doExecute(action, request, listener);
        }
    }

    @Before
    public void createComponents() {
        // it's not possible to run it as @BeforeClass as clients aren't initialized
        if (mockClientForCheckpointing == null) {
            mockClientForCheckpointing = new MockClientForCheckpointing("DataFrameTransformCheckpointServiceNodeTests");
        }

        transformsConfigManager = new DataFrameTransformsConfigManager(client(), xContentRegistry());

        // use a mock for the checkpoint service
        DataFrameAuditor mockAuditor = mock(DataFrameAuditor.class);
        transformsCheckpointService = new DataFrameTransformsCheckpointService(mockClientForCheckpointing,
                                                                               transformsConfigManager,
                                                                               mockAuditor);
    }

    @AfterClass
    public static void tearDownClient() {
        mockClientForCheckpointing.close();
        mockClientForCheckpointing = null;
    }

    public void testCreateReadDeleteCheckpoint() throws InterruptedException {
        String transformId = randomAlphaOfLengthBetween(3, 10);
        long timestamp = 1000;

        DataFrameTransformCheckpoint checkpoint = new DataFrameTransformCheckpoint(transformId, timestamp, 1L,
                createCheckPointMap(transformId, 10, 10, 10), null);

        // create transform
        assertAsync(
                listener -> transformsConfigManager
                        .putTransformConfiguration(DataFrameTransformConfigTests.randomDataFrameTransformConfig(transformId), listener),
                true, null, null);

        // by design no exception is thrown but an empty checkpoint is returned
        assertAsync(listener -> transformsConfigManager.getTransformCheckpoint(transformId, 1L, listener),
                DataFrameTransformCheckpoint.EMPTY, null, null);

        assertAsync(listener -> transformsConfigManager.putTransformCheckpoint(checkpoint, listener), true, null, null);

        assertAsync(listener -> transformsConfigManager.getTransformCheckpoint(transformId, 1L, listener), checkpoint, null, null);

        // add a 2nd checkpoint
        DataFrameTransformCheckpoint checkpoint2 = new DataFrameTransformCheckpoint(transformId, timestamp + 100L, 2L,
                createCheckPointMap(transformId, 20, 20, 20), null);

        assertAsync(listener -> transformsConfigManager.putTransformCheckpoint(checkpoint2, listener), true, null, null);

        // both checkpoints should be there
        assertAsync(listener -> transformsConfigManager.getTransformCheckpoint(transformId, 1L, listener), checkpoint, null, null);
        assertAsync(listener -> transformsConfigManager.getTransformCheckpoint(transformId, 2L, listener), checkpoint2, null, null);

        // delete transform
        assertAsync(listener -> transformsConfigManager.deleteTransform(transformId, listener), true, null, null);

        // checkpoints should be empty again
        assertAsync(listener -> transformsConfigManager.getTransformCheckpoint(transformId, 1L, listener),
                DataFrameTransformCheckpoint.EMPTY, null, null);

        assertAsync(listener -> transformsConfigManager.getTransformCheckpoint(transformId, 2L, listener),
                DataFrameTransformCheckpoint.EMPTY, null, null);
    }

    public void testGetCheckpointStats() throws InterruptedException {
        String transformId = randomAlphaOfLengthBetween(3, 10);
        long timestamp = 1000;
        DataFrameIndexerPosition position = DataFrameIndexerPositionTests.randomDataFrameIndexerPosition();
        DataFrameTransformProgress progress = DataFrameTransformProgressTests.randomDataFrameTransformProgress();

        // create transform
        assertAsync(
                listener -> transformsConfigManager
                        .putTransformConfiguration(DataFrameTransformConfigTests.randomDataFrameTransformConfig(transformId), listener),
                true, null, null);

        DataFrameTransformCheckpoint checkpoint = new DataFrameTransformCheckpoint(transformId, timestamp, 1L,
                createCheckPointMap(transformId, 10, 10, 10), null);

        assertAsync(listener -> transformsConfigManager.putTransformCheckpoint(checkpoint, listener), true, null, null);

        DataFrameTransformCheckpoint checkpoint2 = new DataFrameTransformCheckpoint(transformId, timestamp + 100L, 2L,
                createCheckPointMap(transformId, 20, 20, 20), null);

        assertAsync(listener -> transformsConfigManager.putTransformCheckpoint(checkpoint2, listener), true, null, null);

        mockClientForCheckpointing.setShardStats(createShardStats(createCheckPointMap(transformId, 20, 20, 20)));
        DataFrameTransformCheckpointingInfo checkpointInfo = new DataFrameTransformCheckpointingInfo(
                new DataFrameTransformCheckpointStats(1, null, null, timestamp, 0L),
                new DataFrameTransformCheckpointStats(2, position, progress, timestamp + 100L, 0L),
                30L);

        assertAsync(listener ->
                transformsCheckpointService.getCheckpointingInfo(transformId, 1, position, progress, listener),
            checkpointInfo, null, null);

        mockClientForCheckpointing.setShardStats(createShardStats(createCheckPointMap(transformId, 10, 50, 33)));
        checkpointInfo = new DataFrameTransformCheckpointingInfo(
                new DataFrameTransformCheckpointStats(1, null, null, timestamp, 0L),
                new DataFrameTransformCheckpointStats(2, position, progress, timestamp + 100L, 0L),
                63L);
        assertAsync(listener ->
                transformsCheckpointService.getCheckpointingInfo(transformId, 1, position, progress, listener),
            checkpointInfo, null, null);

        // same as current
        mockClientForCheckpointing.setShardStats(createShardStats(createCheckPointMap(transformId, 10, 10, 10)));
        checkpointInfo = new DataFrameTransformCheckpointingInfo(
                new DataFrameTransformCheckpointStats(1, null, null, timestamp, 0L),
                new DataFrameTransformCheckpointStats(2, position, progress, timestamp + 100L, 0L),
                0L);
        assertAsync(listener ->
                transformsCheckpointService.getCheckpointingInfo(transformId, 1, position, progress, listener),
            checkpointInfo, null, null);
    }

    private static Map<String, long[]> createCheckPointMap(String index, long checkpointShard1, long checkpointShard2,
            long checkpointShard3) {
        return Collections.singletonMap(index, new long[] { checkpointShard1, checkpointShard2, checkpointShard3 });
    }

    private static ShardStats[] createShardStats(Map<String, long[]> checkpoints) {
        List<ShardStats> shardStats = new ArrayList<>();

        for (Entry<String, long[]> entry : checkpoints.entrySet()) {

            for (int i = 0; i < entry.getValue().length; ++i) {
                long checkpoint = entry.getValue()[i];
                CommonStats stats = new CommonStats();
                stats.fieldData = new FieldDataStats();
                stats.queryCache = new QueryCacheStats();
                stats.docs = new DocsStats();
                stats.store = new StoreStats();
                stats.indexing = new IndexingStats();
                stats.search = new SearchStats();
                stats.segments = new SegmentsStats();
                stats.merge = new MergeStats();
                stats.refresh = new RefreshStats();
                stats.completion = new CompletionStats();
                stats.requestCache = new RequestCacheStats();
                stats.get = new GetStats();
                stats.flush = new FlushStats();
                stats.warmer = new WarmerStats();

                SeqNoStats seqNoStats = new SeqNoStats(checkpoint, checkpoint, checkpoint);
                Index index = new Index(entry.getKey(), UUIDs.randomBase64UUID(random()));
                ShardId shardId = new ShardId(index, i);
                ShardRouting shardRouting = ShardRouting.newUnassigned(shardId, true, RecoverySource.EmptyStoreRecoverySource.INSTANCE,
                        new UnassignedInfo(UnassignedInfo.Reason.INDEX_CREATED, null));
                Path path = createTempDir().resolve("indices").resolve(index.getUUID()).resolve(String.valueOf(i));

                shardStats.add(new ShardStats(shardRouting, new ShardPath(false, path, path, shardId), stats, null, seqNoStats, null));
            }

        }
        return shardStats.toArray(new ShardStats[0]);
    }

}
