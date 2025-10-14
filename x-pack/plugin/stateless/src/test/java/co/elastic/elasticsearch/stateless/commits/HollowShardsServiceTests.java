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

package co.elastic.elasticsearch.stateless.commits;

import co.elastic.elasticsearch.stateless.IndexShardCacheWarmer;
import co.elastic.elasticsearch.stateless.engine.HollowShardsMetrics;
import co.elastic.elasticsearch.stateless.engine.IndexEngine;

import org.elasticsearch.Version;
import org.elasticsearch.cluster.ClusterState;
import org.elasticsearch.cluster.metadata.DataStream;
import org.elasticsearch.cluster.metadata.DataStreamLifecycleTests;
import org.elasticsearch.cluster.metadata.DataStreamOptions;
import org.elasticsearch.cluster.metadata.IndexAbstraction;
import org.elasticsearch.cluster.metadata.IndexMetadata;
import org.elasticsearch.cluster.metadata.Metadata;
import org.elasticsearch.cluster.metadata.ProjectMetadata;
import org.elasticsearch.cluster.node.DiscoveryNodeRole;
import org.elasticsearch.cluster.service.ClusterService;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.core.TimeValue;
import org.elasticsearch.index.IndexMode;
import org.elasticsearch.index.engine.Engine;
import org.elasticsearch.index.shard.IndexShard;
import org.elasticsearch.index.shard.ShardId;
import org.elasticsearch.indices.IndicesService;
import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.threadpool.ThreadPool;

import java.util.List;
import java.util.TreeMap;
import java.util.function.Function;
import java.util.function.LongSupplier;

import static co.elastic.elasticsearch.stateless.commits.HollowShardsService.SETTING_HOLLOW_INGESTION_DS_NON_WRITE_TTL;
import static co.elastic.elasticsearch.stateless.commits.HollowShardsService.SETTING_HOLLOW_INGESTION_TTL;
import static co.elastic.elasticsearch.stateless.commits.HollowShardsService.STATELESS_HOLLOW_INDEX_SHARDS_ENABLED;
import static co.elastic.elasticsearch.stateless.commits.StatelessCompoundCommit.COMPOUND_COMMITS_WITH_EXTRA_CONTENT;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

public class HollowShardsServiceTests extends ESTestCase {

    public void testHollowSetting() throws Exception {
        var testHarness = TestHarness.create(
            true,
            false,
            randomNonNegativeLong(),
            randomNonNegativeLong(),
            () -> randomNonNegativeLong(),
            randomBoolean(),
            randomNonNegativeInt(),
            randomBoolean(),
            () -> randomNonNegativeLong(),
            randomBoolean()
        );
        assertFalse(testHarness.hollowShardsService.isHollowableIndexShard(testHarness.indexShard));
    }

    public void testSystemShardIsNotHollowable() throws Exception {
        var testHarness = TestHarness.create(
            true,
            true,
            randomNonNegativeLong(),
            randomNonNegativeLong(),
            () -> randomNonNegativeLong(),
            true,
            0,
            randomBoolean(),
            () -> randomNonNegativeLong(),
            randomBoolean()
        );
        assertFalse(testHarness.hollowShardsService.isHollowableIndexShard(testHarness.indexShard));
    }

    public void testShardWithoutIndexEngineIsNotHollowable() throws Exception {
        var testHarness = TestHarness.create(
            true,
            true,
            randomNonNegativeLong(),
            randomNonNegativeLong(),
            () -> randomNonNegativeLong(),
            false,
            0,
            false,
            () -> randomNonNegativeLong(),
            randomBoolean()
        );
        assertFalse(testHarness.hollowShardsService.isHollowableIndexShard(testHarness.indexShard));
    }

    public void testShardWithActiveOperationsIsNotHollowable() throws Exception {
        var testHarness = TestHarness.create(
            true,
            true,
            randomNonNegativeLong(),
            randomNonNegativeLong(),
            () -> randomNonNegativeLong(),
            false,
            randomIntBetween(1, Integer.MAX_VALUE),
            true,
            () -> randomNonNegativeLong(),
            randomBoolean()
        );
        assertFalse(testHarness.hollowShardsService.isHollowableIndexShard(testHarness.indexShard));
    }

    public void testDataStreamNonWriteIndexWithIngestionIsNotHollowable() throws Exception {
        int lastIngestionTime = randomNonNegativeInt();
        int ttl = randomNonNegativeInt();
        long now = lastIngestionTime + ttl - randomIntBetween(1, ttl);
        var testHarness = TestHarness.create(
            true,
            true,
            randomNonNegativeLong(),
            ttl,
            () -> now,
            false,
            0,
            true,
            () -> TimeValue.NSEC_PER_MSEC * lastIngestionTime,
            true
        );
        assertFalse(testHarness.hollowShardsService.isHollowableIndexShard(testHarness.indexShard));
    }

    public void testDataStreamNonWriteIndexWithoutIngestionIsHollowable() throws Exception {
        long lastIngestionTime = randomNonNegativeInt();
        long ttl = randomNonNegativeInt();
        long now = lastIngestionTime + ttl + randomIntBetween(1, Integer.MAX_VALUE);
        var testHarness = TestHarness.create(
            true,
            true,
            randomNonNegativeLong(),
            ttl,
            () -> now,
            false,
            0,
            true,
            () -> TimeValue.NSEC_PER_MSEC * lastIngestionTime,
            true
        );
        assertTrue(testHarness.hollowShardsService.isHollowableIndexShard(testHarness.indexShard));
    }

    public void testRegularOrDsWriteIndexWithIngestionIsNotHollowable() throws Exception {
        int lastIngestionTime = randomNonNegativeInt();
        int ttl = randomNonNegativeInt();
        long now = lastIngestionTime + ttl - randomIntBetween(1, ttl);
        var testHarness = TestHarness.create(
            true,
            true,
            ttl,
            randomNonNegativeLong(),
            () -> now,
            false,
            0,
            true,
            () -> TimeValue.NSEC_PER_MSEC * lastIngestionTime,
            false
        );
        assertFalse(testHarness.hollowShardsService.isHollowableIndexShard(testHarness.indexShard));
    }

    public void testRegularOrDsWriteIndexWithoutIngestionIsHollowable() throws Exception {
        long lastIngestionTime = randomNonNegativeInt();
        long ttl = randomNonNegativeInt();
        long now = lastIngestionTime + ttl + randomIntBetween(1, Integer.MAX_VALUE);
        var testHarness = TestHarness.create(
            true,
            true,
            ttl,
            randomNonNegativeLong(),
            () -> now,
            false,
            0,
            true,
            () -> TimeValue.NSEC_PER_MSEC * lastIngestionTime,
            false
        );
        assertTrue(testHarness.hollowShardsService.isHollowableIndexShard(testHarness.indexShard));
    }

    private record TestHarness(HollowShardsService hollowShardsService, IndexShard indexShard) {
        public static TestHarness create(
            boolean hasNodeIndexRole,
            boolean enableHollowShards,
            long ingestionTtlMillis,
            long ingestionTtlMillisDsNonWrite,
            LongSupplier relativeTimeSupplierInMillis,
            boolean shardIsSystem,
            int shardActiveOperationsCount,
            boolean shardIndexEngine,
            LongSupplier shardLastWriteTimeSupplierInNanos,
            boolean shardDsNonWriteIndex
        ) {
            var indexShard = mock(IndexShard.class);
            when(indexShard.isSystem()).thenReturn(shardIsSystem);
            when(indexShard.getActiveOperationsCount()).thenReturn(shardActiveOperationsCount);

            final Engine engine = shardIndexEngine ? mock(IndexEngine.class) : randomBoolean() ? null : mock(Engine.class);
            if (engine != null) {
                when(engine.getLastWriteNanos()).thenAnswer(invocation -> shardLastWriteTimeSupplierInNanos.getAsLong());
            }
            when(indexShard.getEngineOrNull()).thenReturn(engine);

            final Function<String, IndexMetadata> createIndexMetadata = indexName -> IndexMetadata.builder(indexName)
                .settings(
                    Settings.builder()
                        .put(IndexMetadata.INDEX_NUMBER_OF_SHARDS_SETTING.getKey(), 1)
                        .put(IndexMetadata.INDEX_NUMBER_OF_REPLICAS_SETTING.getKey(), 1)
                        .put(IndexMetadata.SETTING_INDEX_VERSION_CREATED.getKey(), Version.CURRENT)
                )
                .build();
            final var indexMetadata = createIndexMetadata.apply("index");
            final var shardId = new ShardId(indexMetadata.getIndex(), 0);

            final var indicesLookup = new TreeMap<String, IndexAbstraction>();
            DataStream dataStream = new DataStream(
                randomAlphaOfLength(10),
                List.of(shardDsNonWriteIndex ? createIndexMetadata.apply("writeindex").getIndex() : indexMetadata.getIndex()),
                randomNonNegativeInt(),
                null,
                shardIsSystem,
                false,
                shardIsSystem,
                randomBoolean(),
                randomBoolean() ? IndexMode.STANDARD : IndexMode.TIME_SERIES,
                DataStreamLifecycleTests.randomDataLifecycle(),
                DataStreamOptions.FAILURE_STORE_DISABLED,
                List.of(),
                randomBoolean(),
                null
            );

            final var indexAbstraction = new IndexAbstraction.ConcreteIndex(
                indexMetadata,
                shardDsNonWriteIndex ? dataStream : randomBoolean() ? null : dataStream
            );
            indicesLookup.put(indexMetadata.getIndex().getName(), indexAbstraction);

            final var metadata = mock(Metadata.class);
            final ProjectMetadata projectMetadata = mock(ProjectMetadata.class);
            when(metadata.getProject()).thenReturn(projectMetadata);
            when(projectMetadata.getIndicesLookup()).thenReturn(indicesLookup);
            final var clusterState = mock(ClusterState.class);
            when(clusterState.metadata()).thenReturn(metadata);
            when(clusterState.getMinTransportVersion()).thenReturn(COMPOUND_COMMITS_WITH_EXTRA_CONTENT);
            final var clusterService = mock(ClusterService.class);
            when(clusterService.state()).thenReturn(clusterState);

            when(indexShard.shardId()).thenReturn(shardId);
            when(indexShard.getEngineOrNull()).thenReturn(engine);

            Settings settings = Settings.builder()
                .put("node.roles", hasNodeIndexRole ? DiscoveryNodeRole.INDEX_ROLE.roleName() : DiscoveryNodeRole.DATA_ROLE.roleName())
                .put(STATELESS_HOLLOW_INDEX_SHARDS_ENABLED.getKey(), enableHollowShards)
                .put(SETTING_HOLLOW_INGESTION_DS_NON_WRITE_TTL.getKey(), TimeValue.timeValueMillis(ingestionTtlMillisDsNonWrite))
                .put(SETTING_HOLLOW_INGESTION_TTL.getKey(), TimeValue.timeValueMillis(ingestionTtlMillis))
                .build();
            return new TestHarness(
                new HollowShardsService(
                    settings,
                    clusterService,
                    mock(IndicesService.class),
                    mock(IndexShardCacheWarmer.class),
                    mock(ThreadPool.class),
                    HollowShardsMetrics.NOOP,
                    relativeTimeSupplierInMillis
                ),
                indexShard
            );
        }
    }

}
