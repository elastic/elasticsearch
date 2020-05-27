/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */

package org.elasticsearch.xpack.transform.checkpoint;

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
import org.elasticsearch.cluster.service.ClusterService;
import org.elasticsearch.common.UUIDs;
import org.elasticsearch.common.settings.ClusterSettings;
import org.elasticsearch.common.settings.Settings;
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
import org.elasticsearch.xpack.core.transform.transforms.TransformCheckpoint;
import org.elasticsearch.xpack.core.transform.transforms.TransformCheckpointStats;
import org.elasticsearch.xpack.core.transform.transforms.TransformCheckpointingInfo;
import org.elasticsearch.xpack.core.transform.transforms.TransformCheckpointingInfo.TransformCheckpointingInfoBuilder;
import org.elasticsearch.xpack.core.transform.transforms.TransformConfigTests;
import org.elasticsearch.xpack.core.transform.transforms.TransformIndexerPosition;
import org.elasticsearch.xpack.core.transform.transforms.TransformIndexerPositionTests;
import org.elasticsearch.xpack.core.transform.transforms.TransformProgress;
import org.elasticsearch.xpack.core.transform.transforms.TransformProgressTests;
import org.elasticsearch.xpack.transform.TransformSingleNodeTestCase;
import org.elasticsearch.xpack.transform.notifications.TransformAuditor;
import org.elasticsearch.xpack.transform.persistence.IndexBasedTransformConfigManager;
import org.junit.AfterClass;
import org.junit.Before;

import java.nio.file.Path;
import java.time.Instant;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Set;

import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

public class TransformCheckpointServiceNodeTests extends TransformSingleNodeTestCase {

    // re-use the mock client for the whole test suite as the underlying thread pool and the
    // corresponding context if recreated cause unreliable test execution
    // see https://github.com/elastic/elasticsearch/issues/45238 and https://github.com/elastic/elasticsearch/issues/42577
    private static MockClientForCheckpointing mockClientForCheckpointing = null;

    private IndexBasedTransformConfigManager transformsConfigManager;
    private TransformCheckpointService transformCheckpointService;

    private class MockClientForCheckpointing extends NoOpClient {

        private volatile ShardStats[] shardStats;
        private volatile String[] indices;

        MockClientForCheckpointing(String testName) {
            super(testName);
        }

        void setShardStats(ShardStats[] shardStats) {
            this.shardStats = shardStats;

            Set<String> indices = new HashSet<>();
            for (ShardStats s : shardStats) {
                indices.add(s.getShardRouting().getIndexName());
            }

            this.indices = indices.toArray(new String[0]);
        }

        @SuppressWarnings("unchecked")
        @Override
        protected <Request extends ActionRequest, Response extends ActionResponse> void doExecute(
            ActionType<Response> action,
            Request request,
            ActionListener<Response> listener
        ) {

            if (request instanceof GetIndexRequest) {
                // for this test we only need the indices
                assert (indices != null);
                final GetIndexResponse indexResponse = new GetIndexResponse(indices, null, null, null, null, null);

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
            mockClientForCheckpointing = new MockClientForCheckpointing("TransformCheckpointServiceNodeTests");
        }

        transformsConfigManager = new IndexBasedTransformConfigManager(client(), xContentRegistry());

        // use a mock for the checkpoint service
        TransformAuditor mockAuditor = mock(TransformAuditor.class);
        transformCheckpointService = new TransformCheckpointService(
            Settings.EMPTY,
            new ClusterService(Settings.EMPTY, new ClusterSettings(Settings.EMPTY, ClusterSettings.BUILT_IN_CLUSTER_SETTINGS), null),
            transformsConfigManager,
            mockAuditor
        );
    }

    @AfterClass
    public static void tearDownClient() {
        mockClientForCheckpointing.close();
        mockClientForCheckpointing = null;
    }

    public void testCreateReadDeleteCheckpoint() throws InterruptedException {
        String transformId = randomAlphaOfLengthBetween(3, 10);
        long timestamp = 1000;

        TransformCheckpoint checkpoint = new TransformCheckpoint(
            transformId,
            timestamp,
            1L,
            createCheckPointMap(transformId, 10, 10, 10),
            null
        );

        // create transform
        assertAsync(
            listener -> transformsConfigManager.putTransformConfiguration(
                TransformConfigTests.randomTransformConfig(transformId),
                listener
            ),
            true,
            null,
            null
        );

        // by design no exception is thrown but an empty checkpoint is returned
        assertAsync(
            listener -> transformsConfigManager.getTransformCheckpoint(transformId, 1L, listener),
            TransformCheckpoint.EMPTY,
            null,
            null
        );

        assertAsync(listener -> transformsConfigManager.putTransformCheckpoint(checkpoint, listener), true, null, null);

        assertAsync(listener -> transformsConfigManager.getTransformCheckpoint(transformId, 1L, listener), checkpoint, null, null);

        // add a 2nd checkpoint
        TransformCheckpoint checkpoint2 = new TransformCheckpoint(
            transformId,
            timestamp + 100L,
            2L,
            createCheckPointMap(transformId, 20, 20, 20),
            null
        );

        assertAsync(listener -> transformsConfigManager.putTransformCheckpoint(checkpoint2, listener), true, null, null);

        // both checkpoints should be there
        assertAsync(listener -> transformsConfigManager.getTransformCheckpoint(transformId, 1L, listener), checkpoint, null, null);
        assertAsync(listener -> transformsConfigManager.getTransformCheckpoint(transformId, 2L, listener), checkpoint2, null, null);

        // delete transform
        assertAsync(listener -> transformsConfigManager.deleteTransform(transformId, listener), true, null, null);

        // checkpoints should be empty again
        assertAsync(
            listener -> transformsConfigManager.getTransformCheckpoint(transformId, 1L, listener),
            TransformCheckpoint.EMPTY,
            null,
            null
        );

        assertAsync(
            listener -> transformsConfigManager.getTransformCheckpoint(transformId, 2L, listener),
            TransformCheckpoint.EMPTY,
            null,
            null
        );
    }

    public void testGetCheckpointStats() throws InterruptedException {
        String transformId = randomAlphaOfLengthBetween(3, 10);
        long timestamp = 1000;
        TransformIndexerPosition position = TransformIndexerPositionTests.randomTransformIndexerPosition();
        TransformProgress progress = TransformProgressTests.randomTransformProgress();

        // create transform
        assertAsync(
            listener -> transformsConfigManager.putTransformConfiguration(
                TransformConfigTests.randomTransformConfig(transformId),
                listener
            ),
            true,
            null,
            null
        );

        TransformCheckpoint checkpoint = new TransformCheckpoint(
            transformId,
            timestamp,
            1L,
            createCheckPointMap(transformId, 10, 10, 10),
            null
        );

        assertAsync(listener -> transformsConfigManager.putTransformCheckpoint(checkpoint, listener), true, null, null);

        TransformCheckpoint checkpoint2 = new TransformCheckpoint(
            transformId,
            timestamp + 100L,
            2L,
            createCheckPointMap(transformId, 20, 20, 20),
            null
        );

        assertAsync(listener -> transformsConfigManager.putTransformCheckpoint(checkpoint2, listener), true, null, null);

        mockClientForCheckpointing.setShardStats(createShardStats(createCheckPointMap(transformId, 20, 20, 20)));
        TransformCheckpointingInfo checkpointInfo = new TransformCheckpointingInfo(
            new TransformCheckpointStats(1, null, null, timestamp, 0L),
            new TransformCheckpointStats(2, position, progress, timestamp + 100L, 0L),
            30L,
            Instant.ofEpochMilli(timestamp)
        );

        assertAsync(
            listener -> getCheckpoint(transformCheckpointService, transformId, 1, position, progress, listener),
            checkpointInfo,
            null,
            null
        );

        mockClientForCheckpointing.setShardStats(createShardStats(createCheckPointMap(transformId, 10, 50, 33)));
        checkpointInfo = new TransformCheckpointingInfo(
            new TransformCheckpointStats(1, null, null, timestamp, 0L),
            new TransformCheckpointStats(2, position, progress, timestamp + 100L, 0L),
            63L,
            Instant.ofEpochMilli(timestamp)
        );
        assertAsync(
            listener -> getCheckpoint(transformCheckpointService, transformId, 1, position, progress, listener),
            checkpointInfo,
            null,
            null
        );

        // same as current
        mockClientForCheckpointing.setShardStats(createShardStats(createCheckPointMap(transformId, 10, 10, 10)));
        checkpointInfo = new TransformCheckpointingInfo(
            new TransformCheckpointStats(1, null, null, timestamp, 0L),
            new TransformCheckpointStats(2, position, progress, timestamp + 100L, 0L),
            0L,
            Instant.ofEpochMilli(timestamp)
        );
        assertAsync(
            listener -> getCheckpoint(transformCheckpointService, transformId, 1, position, progress, listener),
            checkpointInfo,
            null,
            null
        );
    }

    private static Map<String, long[]> createCheckPointMap(
        String index,
        long checkpointShard1,
        long checkpointShard2,
        long checkpointShard3
    ) {
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
                ShardRouting shardRouting = ShardRouting.newUnassigned(
                    shardId,
                    true,
                    RecoverySource.EmptyStoreRecoverySource.INSTANCE,
                    new UnassignedInfo(UnassignedInfo.Reason.INDEX_CREATED, null)
                );
                Path path = createTempDir().resolve("indices").resolve(index.getUUID()).resolve(String.valueOf(i));

                shardStats.add(new ShardStats(shardRouting, new ShardPath(false, path, path, shardId), stats, null, seqNoStats, null));
            }

        }
        return shardStats.toArray(new ShardStats[0]);
    }

    private static void getCheckpoint(
        TransformCheckpointService transformCheckpointService,
        String transformId,
        long lastCheckpointNumber,
        TransformIndexerPosition nextCheckpointPosition,
        TransformProgress nextCheckpointProgress,
        ActionListener<TransformCheckpointingInfo> listener
    ) {
        ActionListener<TransformCheckpointingInfoBuilder> checkPointInfoListener = ActionListener.wrap(
            infoBuilder -> { listener.onResponse(infoBuilder.build()); },
            listener::onFailure
        );
        transformCheckpointService.getCheckpointingInfo(
            mockClientForCheckpointing,
            transformId,
            lastCheckpointNumber,
            nextCheckpointPosition,
            nextCheckpointProgress,
            checkPointInfoListener
        );
    }

}
