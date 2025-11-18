/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */
package org.elasticsearch.indices;

import org.apache.lucene.search.similarities.BM25Similarity;
import org.apache.lucene.search.similarities.Similarity;
import org.apache.lucene.store.AlreadyClosedException;
import org.elasticsearch.action.ActionListener;
import org.elasticsearch.action.admin.indices.stats.CommonStatsFlags;
import org.elasticsearch.action.admin.indices.stats.IndexShardStats;
import org.elasticsearch.action.support.ActionTestUtils;
import org.elasticsearch.cluster.ClusterName;
import org.elasticsearch.cluster.ClusterState;
import org.elasticsearch.cluster.ProjectState;
import org.elasticsearch.cluster.metadata.AliasMetadata;
import org.elasticsearch.cluster.metadata.DataStreamTestHelper;
import org.elasticsearch.cluster.metadata.IndexGraveyard;
import org.elasticsearch.cluster.metadata.IndexMetadata;
import org.elasticsearch.cluster.metadata.IndexNameExpressionResolver.ResolvedExpression;
import org.elasticsearch.cluster.metadata.Metadata;
import org.elasticsearch.cluster.metadata.ProjectId;
import org.elasticsearch.cluster.metadata.ProjectMetadata;
import org.elasticsearch.cluster.service.ClusterService;
import org.elasticsearch.common.Strings;
import org.elasticsearch.common.UUIDs;
import org.elasticsearch.common.io.FileSystemUtils;
import org.elasticsearch.common.settings.Setting;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.util.concurrent.EsExecutors;
import org.elasticsearch.core.FixForMultiProject;
import org.elasticsearch.core.TimeValue;
import org.elasticsearch.env.NodeEnvironment;
import org.elasticsearch.env.ShardLockObtainFailedException;
import org.elasticsearch.gateway.GatewayMetaState;
import org.elasticsearch.gateway.LocalAllocateDangledIndices;
import org.elasticsearch.gateway.MetaStateWriterUtils;
import org.elasticsearch.health.node.selection.HealthNodeTaskExecutor;
import org.elasticsearch.index.Index;
import org.elasticsearch.index.IndexModule;
import org.elasticsearch.index.IndexService;
import org.elasticsearch.index.IndexSettings;
import org.elasticsearch.index.IndexVersion;
import org.elasticsearch.index.IndexVersions;
import org.elasticsearch.index.SlowLogFieldProvider;
import org.elasticsearch.index.SlowLogFields;
import org.elasticsearch.index.engine.Engine;
import org.elasticsearch.index.engine.EngineConfig;
import org.elasticsearch.index.engine.EngineFactory;
import org.elasticsearch.index.engine.InternalEngine;
import org.elasticsearch.index.engine.InternalEngineFactory;
import org.elasticsearch.index.mapper.KeywordFieldMapper;
import org.elasticsearch.index.mapper.Mapper;
import org.elasticsearch.index.mapper.MapperService;
import org.elasticsearch.index.query.BoolQueryBuilder;
import org.elasticsearch.index.query.QueryBuilders;
import org.elasticsearch.index.shard.IllegalIndexShardStateException;
import org.elasticsearch.index.shard.IndexShard;
import org.elasticsearch.index.shard.IndexShardState;
import org.elasticsearch.index.shard.ShardId;
import org.elasticsearch.index.shard.ShardPath;
import org.elasticsearch.index.similarity.NonNegativeScoresSimilarity;
import org.elasticsearch.indices.IndicesService.ShardDeletionCheckResult;
import org.elasticsearch.indices.cluster.IndexRemovalReason;
import org.elasticsearch.plugins.EnginePlugin;
import org.elasticsearch.plugins.MapperPlugin;
import org.elasticsearch.plugins.Plugin;
import org.elasticsearch.search.internal.AliasFilter;
import org.elasticsearch.test.ESSingleNodeTestCase;
import org.elasticsearch.test.IndexSettingsModule;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.CountDownLatch;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import static org.elasticsearch.action.support.WriteRequest.RefreshPolicy.IMMEDIATE;
import static org.elasticsearch.cluster.metadata.DataStreamTestHelper.createBackingIndex;
import static org.elasticsearch.cluster.metadata.IndexMetadata.SETTING_INDEX_UUID;
import static org.elasticsearch.cluster.metadata.IndexNameExpressionResolverTests.indexBuilder;
import static org.elasticsearch.test.hamcrest.ElasticsearchAssertions.assertAcked;
import static org.elasticsearch.test.hamcrest.ElasticsearchAssertions.assertHitCount;
import static org.hamcrest.Matchers.arrayContainingInAnyOrder;
import static org.hamcrest.Matchers.containsInAnyOrder;
import static org.hamcrest.Matchers.containsString;
import static org.hamcrest.Matchers.empty;
import static org.hamcrest.Matchers.emptyArray;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.hasToString;
import static org.hamcrest.Matchers.instanceOf;
import static org.hamcrest.Matchers.is;
import static org.hamcrest.Matchers.matchesRegex;
import static org.hamcrest.Matchers.not;
import static org.hamcrest.Matchers.nullValue;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

public class IndicesServiceTests extends ESSingleNodeTestCase {

    public IndicesService getIndicesService() {
        return getInstanceFromNode(IndicesService.class);
    }

    public NodeEnvironment getNodeEnvironment() {
        return getInstanceFromNode(NodeEnvironment.class);
    }

    @Override
    protected Collection<Class<? extends Plugin>> getPlugins() {
        return Stream.concat(super.getPlugins().stream(), Stream.of(TestPlugin.class, FooEnginePlugin.class, BarEnginePlugin.class))
            .toList();
    }

    public static class FooEnginePlugin extends Plugin implements EnginePlugin {

        static class FooEngineFactory implements EngineFactory {

            @Override
            public Engine newReadWriteEngine(final EngineConfig config) {
                return new InternalEngine(config);
            }

        }

        private static final Setting<Boolean> FOO_INDEX_SETTING = Setting.boolSetting(
            "index.foo_index",
            false,
            Setting.Property.IndexScope
        );

        @Override
        public List<Setting<?>> getSettings() {
            return Collections.singletonList(FOO_INDEX_SETTING);
        }

        @Override
        public Optional<EngineFactory> getEngineFactory(final IndexSettings indexSettings) {
            if (FOO_INDEX_SETTING.get(indexSettings.getSettings())) {
                return Optional.of(new FooEngineFactory());
            } else {
                return Optional.empty();
            }
        }

    }

    public static class BarEnginePlugin extends Plugin implements EnginePlugin {

        static class BarEngineFactory implements EngineFactory {

            @Override
            public Engine newReadWriteEngine(final EngineConfig config) {
                return new InternalEngine(config);
            }

        }

        private static final Setting<Boolean> BAR_INDEX_SETTING = Setting.boolSetting(
            "index.bar_index",
            false,
            Setting.Property.IndexScope
        );

        @Override
        public List<Setting<?>> getSettings() {
            return Collections.singletonList(BAR_INDEX_SETTING);
        }

        @Override
        public Optional<EngineFactory> getEngineFactory(final IndexSettings indexSettings) {
            if (BAR_INDEX_SETTING.get(indexSettings.getSettings())) {
                return Optional.of(new BarEngineFactory());
            } else {
                return Optional.empty();
            }
        }

    }

    public static class TestPlugin extends Plugin implements MapperPlugin {

        public TestPlugin() {}

        @Override
        public Map<String, Mapper.TypeParser> getMappers() {
            return Collections.singletonMap("fake-mapper", KeywordFieldMapper.PARSER);
        }

        @Override
        public void onIndexModule(IndexModule indexModule) {
            super.onIndexModule(indexModule);
            indexModule.addSimilarity("fake-similarity", (settings, indexCreatedVersion, scriptService) -> new BM25Similarity());
        }
    }

    public static class TestSlowLogFieldProvider implements SlowLogFieldProvider {

        private static Map<String, String> fields = Map.of();

        static void setFields(Map<String, String> fields) {
            TestSlowLogFieldProvider.fields = fields;
        }

        @Override
        public SlowLogFields create() {
            return new SlowLogFields() {
                @Override
                public Map<String, String> indexFields() {
                    return fields;
                }

                @Override
                public Map<String, String> searchFields() {
                    return fields;
                }
            };
        }

        @Override
        public SlowLogFields create(IndexSettings indexSettings) {
            return create();
        }

    }

    public static class TestAnotherSlowLogFieldProvider implements SlowLogFieldProvider {

        private static Map<String, String> fields = Map.of();

        static void setFields(Map<String, String> fields) {
            TestAnotherSlowLogFieldProvider.fields = fields;
        }

        @Override
        public SlowLogFields create() {
            return new SlowLogFields() {
                @Override
                public Map<String, String> indexFields() {
                    return fields;
                }

                @Override
                public Map<String, String> searchFields() {
                    return fields;
                }
            };
        }

        @Override
        public SlowLogFields create(IndexSettings indexSettings) {
            return create();
        }
    }

    @Override
    protected boolean resetNodeAfterTest() {
        return true;
    }

    @Override
    protected Settings nodeSettings() {
        // Disable the health node selection so the task assignment does not interfere with the cluster state during the test
        return Settings.builder().put(HealthNodeTaskExecutor.ENABLED_SETTING.getKey(), false).build();
    }

    public void testCanDeleteShardContent() {
        IndicesService indicesService = getIndicesService();
        IndexMetadata meta = IndexMetadata.builder("test")
            .settings(settings(IndexVersion.current()))
            .numberOfShards(1)
            .numberOfReplicas(1)
            .build();
        IndexSettings indexSettings = IndexSettingsModule.newIndexSettings("test", meta.getSettings());
        ShardId shardId = new ShardId(meta.getIndex(), 0);
        assertEquals(
            "no shard location",
            indicesService.canDeleteShardContent(shardId, indexSettings),
            ShardDeletionCheckResult.NO_FOLDER_FOUND
        );
        IndexService test = createIndex("test");
        shardId = new ShardId(test.index(), 0);
        assertTrue(test.hasShard(0));
        assertEquals(
            "shard is allocated",
            indicesService.canDeleteShardContent(shardId, test.getIndexSettings()),
            ShardDeletionCheckResult.STILL_ALLOCATED
        );
        test.removeShard(0, "boom", EsExecutors.DIRECT_EXECUTOR_SERVICE, ActionTestUtils.assertNoFailureListener(v -> {}));
        assertEquals(
            "shard is removed",
            indicesService.canDeleteShardContent(shardId, test.getIndexSettings()),
            ShardDeletionCheckResult.FOLDER_FOUND_CAN_DELETE
        );
        ShardId notAllocated = new ShardId(test.index(), 100);
        assertEquals(
            "shard that was never on this node should NOT be deletable",
            indicesService.canDeleteShardContent(notAllocated, test.getIndexSettings()),
            ShardDeletionCheckResult.NO_FOLDER_FOUND
        );
    }

    public void testDeleteIndexStore() throws Exception {
        IndicesService indicesService = getIndicesService();
        IndexService test = createIndex("test");
        ClusterService clusterService = getInstanceFromNode(ClusterService.class);
        IndexMetadata firstMetadata = clusterService.state().metadata().getProject().index("test");
        assertTrue(test.hasShard(0));
        ShardPath firstPath = ShardPath.loadShardPath(
            logger,
            getNodeEnvironment(),
            new ShardId(test.index(), 0),
            test.getIndexSettings().customDataPath()
        );

        expectThrows(IllegalStateException.class, () -> indicesService.deleteIndexStore("boom", firstMetadata, randomReason()));
        assertTrue(firstPath.exists());

        GatewayMetaState gwMetaState = getInstanceFromNode(GatewayMetaState.class);
        Metadata meta = gwMetaState.getMetadata();
        assertNotNull(meta);
        assertNotNull(meta.getProject().index("test"));
        assertAcked(client().admin().indices().prepareDelete("test"));
        awaitIndexShardCloseAsyncTasks();

        assertFalse(firstPath.exists());

        meta = gwMetaState.getMetadata();
        assertNotNull(meta);
        assertNull(meta.getProject().index("test"));

        test = createIndex("test");
        prepareIndex("test").setId("1").setSource("field", "value").setRefreshPolicy(IMMEDIATE).get();
        client().admin().indices().prepareFlush("test").get();
        assertHitCount(client().prepareSearch("test"), 1);
        IndexMetadata secondMetadata = clusterService.state().metadata().getProject().index("test");
        assertAcked(client().admin().indices().prepareClose("test"));
        ShardPath secondPath = ShardPath.loadShardPath(
            logger,
            getNodeEnvironment(),
            new ShardId(test.index(), 0),
            test.getIndexSettings().customDataPath()
        );
        assertTrue(secondPath.exists());

        expectThrows(IllegalStateException.class, () -> indicesService.deleteIndexStore("boom", secondMetadata, randomReason()));
        assertTrue(secondPath.exists());

        assertAcked(client().admin().indices().prepareOpen("test"));
        ensureGreen("test");
    }

    public void testPendingTasks() throws Exception {
        final IndexService indexService = createIndex("test");
        final Index index = indexService.index();
        final IndexSettings indexSettings = indexService.getIndexSettings();

        final IndexShard indexShard = indexService.getShardOrNull(0);
        assertNotNull(indexShard);
        assertTrue(indexShard.routingEntry().started());

        final ShardPath shardPath = indexShard.shardPath();
        assertEquals(
            ShardPath.loadShardPath(logger, getNodeEnvironment(), indexShard.shardId(), indexSettings.customDataPath()),
            shardPath
        );

        final IndicesService indicesService = getIndicesService();
        expectThrows(
            ShardLockObtainFailedException.class,
            () -> indicesService.processPendingDeletes(index, indexSettings, TimeValue.timeValueMillis(0))
        );
        assertTrue(shardPath.exists());

        int numPending = 1;
        if (randomBoolean()) {
            indicesService.addPendingDelete(indexShard.shardId(), indexSettings, randomReason());
        } else {
            if (randomBoolean()) {
                numPending++;
                indicesService.addPendingDelete(indexShard.shardId(), indexSettings, randomReason());
            }
            indicesService.addPendingDelete(index, indexSettings, randomReason());
        }

        assertAcked(client().admin().indices().prepareClose("test"));
        assertTrue(shardPath.exists());
        ensureGreen("test");

        assertEquals(indicesService.numPendingDeletes(index), numPending);
        assertTrue(indicesService.hasUncompletedPendingDeletes());

        expectThrows(
            ShardLockObtainFailedException.class,
            () -> indicesService.processPendingDeletes(index, indexSettings, TimeValue.timeValueMillis(0))
        );

        assertEquals(indicesService.numPendingDeletes(index), numPending);
        assertTrue(indicesService.hasUncompletedPendingDeletes());

        final boolean hasBogus = randomBoolean();
        if (hasBogus) {
            indicesService.addPendingDelete(new ShardId(index, 0), indexSettings, randomReason());
            indicesService.addPendingDelete(new ShardId(index, 1), indexSettings, randomReason());
            indicesService.addPendingDelete(new ShardId("bogus", "_na_", 1), indexSettings, randomReason());
            assertEquals(indicesService.numPendingDeletes(index), numPending + 2);
            assertTrue(indicesService.hasUncompletedPendingDeletes());
        }

        assertAcked(client().admin().indices().prepareDelete("test"));
        assertBusy(() -> {
            try {
                indicesService.processPendingDeletes(index, indexSettings, TimeValue.timeValueMillis(0));
                assertEquals(indicesService.numPendingDeletes(index), 0);
            } catch (final Exception e) {
                fail(e.getMessage());
            }
        });
        assertBusy(() -> {
            assertThat(indicesService.hasUncompletedPendingDeletes(), equalTo(hasBogus)); // "bogus" index has not been removed
            assertFalse(shardPath.exists());
        });
    }

    public void testVerifyIfIndexContentDeleted() throws Exception {
        final Index index = new Index("test", UUIDs.randomBase64UUID());
        final IndicesService indicesService = getIndicesService();
        final NodeEnvironment nodeEnv = getNodeEnvironment();

        final ClusterService clusterService = getInstanceFromNode(ClusterService.class);
        final Settings idxSettings = Settings.builder()
            .put(IndexMetadata.SETTING_VERSION_CREATED, IndexVersion.current())
            .put(SETTING_INDEX_UUID, index.getUUID())
            .build();
        final IndexMetadata indexMetadata = new IndexMetadata.Builder(index.getName()).settings(idxSettings)
            .numberOfShards(1)
            .numberOfReplicas(0)
            .build();

        MetaStateWriterUtils.writeIndex(nodeEnv, "test index being created", indexMetadata);
        final Metadata metadata = Metadata.builder(clusterService.state().metadata()).put(indexMetadata, true).build();
        final ClusterState csWithIndex = new ClusterState.Builder(clusterService.state()).metadata(metadata).build();
        try {
            indicesService.verifyIndexIsDeleted(index, csWithIndex);
            fail("Should not be able to delete index contents when the index is part of the cluster state.");
        } catch (IllegalStateException e) {
            assertThat(e.getMessage(), containsString("Cannot delete index"));
        }

        final ClusterState withoutIndex = new ClusterState.Builder(csWithIndex).metadata(
            Metadata.builder(csWithIndex.metadata()).remove(index.getName())
        ).build();
        indicesService.verifyIndexIsDeleted(index, withoutIndex);
        assertFalse("index files should be deleted", FileSystemUtils.exists(nodeEnv.indexPaths(index)));
    }

    public void testDanglingIndicesWithAliasConflict() throws Exception {
        final String indexName = "test-idx1";
        final String alias = "test-alias";
        final ClusterService clusterService = getInstanceFromNode(ClusterService.class);
        createIndex(indexName);

        // create the alias for the index
        client().admin().indices().prepareAliases(TEST_REQUEST_TIMEOUT, TEST_REQUEST_TIMEOUT).addAlias(indexName, alias).get();
        final ClusterState originalState = clusterService.state();

        // try to import a dangling index with the same name as the alias, it should fail
        final LocalAllocateDangledIndices dangling = getInstanceFromNode(LocalAllocateDangledIndices.class);
        final Settings idxSettings = Settings.builder()
            .put(IndexMetadata.SETTING_VERSION_CREATED, IndexVersion.current())
            .put(SETTING_INDEX_UUID, UUIDs.randomBase64UUID())
            .build();
        final IndexMetadata indexMetadata = new IndexMetadata.Builder(alias).settings(idxSettings)
            .numberOfShards(1)
            .numberOfReplicas(0)
            .build();
        CountDownLatch latch = new CountDownLatch(1);
        dangling.allocateDangled(Arrays.asList(indexMetadata), ActionListener.running(latch::countDown));
        latch.await();
        assertThat(clusterService.state(), equalTo(originalState));

        // remove the alias
        client().admin().indices().prepareAliases(TEST_REQUEST_TIMEOUT, TEST_REQUEST_TIMEOUT).removeAlias(indexName, alias).get();

        // now try importing a dangling index with the same name as the alias, it should succeed.
        latch = new CountDownLatch(1);
        dangling.allocateDangled(Arrays.asList(indexMetadata), ActionListener.running(latch::countDown));
        latch.await();
        assertThat(clusterService.state(), not(originalState));
        assertNotNull(clusterService.state().getMetadata().getProject().index(alias));
    }

    public void testDanglingIndicesWithLaterVersion() throws Exception {
        final String indexNameLater = "test-idxnewer";
        final ClusterService clusterService = getInstanceFromNode(ClusterService.class);
        final ClusterState originalState = clusterService.state();

        // import an index with minor version incremented by one over cluster master version, it should be ignored
        final LocalAllocateDangledIndices dangling = getInstanceFromNode(LocalAllocateDangledIndices.class);
        final Settings idxSettingsLater = Settings.builder()
            .put(IndexMetadata.SETTING_VERSION_CREATED, IndexVersion.fromId(IndexVersion.current().id() + 10000))
            .put(SETTING_INDEX_UUID, UUIDs.randomBase64UUID())
            .build();
        final IndexMetadata indexMetadataLater = new IndexMetadata.Builder(indexNameLater).settings(idxSettingsLater)
            .numberOfShards(1)
            .numberOfReplicas(0)
            .build();
        CountDownLatch latch = new CountDownLatch(1);
        dangling.allocateDangled(Arrays.asList(indexMetadataLater), ActionListener.running(latch::countDown));
        latch.await();
        assertThat(clusterService.state(), equalTo(originalState));
    }

    /**
     * This test checks an edge case where, if a node had an index (lets call it A with UUID 1), then
     * deleted it (so a tombstone entry for A will exist in the cluster state), then created
     * a new index A with UUID 2, then shutdown, when the node comes back online, it will look at the
     * tombstones for deletions, and it should proceed with trying to delete A with UUID 1 and not
     * throw any errors that the index still exists in the cluster state.  This is a case of ensuring
     * that tombstones that have the same name as current valid indices don't cause confusion by
     * trying to delete an index that exists.
     * See https://github.com/elastic/elasticsearch/issues/18054
     */
    public void testIndexAndTombstoneWithSameNameOnStartup() throws Exception {
        final String indexName = "test";
        final Index index = new Index(indexName, UUIDs.randomBase64UUID());
        final IndicesService indicesService = getIndicesService();
        final Settings idxSettings = Settings.builder()
            .put(IndexMetadata.SETTING_VERSION_CREATED, IndexVersion.current())
            .put(SETTING_INDEX_UUID, index.getUUID())
            .build();
        final IndexMetadata indexMetadata = new IndexMetadata.Builder(index.getName()).settings(idxSettings)
            .numberOfShards(1)
            .numberOfReplicas(0)
            .build();
        final Index tombstonedIndex = new Index(indexName, UUIDs.randomBase64UUID());
        final IndexGraveyard graveyard = IndexGraveyard.builder().addTombstone(tombstonedIndex).build();
        @FixForMultiProject // Use random project-id
        final var project = ProjectMetadata.builder(ProjectId.DEFAULT).put(indexMetadata, true).indexGraveyard(graveyard).build();
        final ClusterState clusterState = new ClusterState.Builder(new ClusterName("testCluster")).putProjectMetadata(project).build();
        // if all goes well, this won't throw an exception, otherwise, it will throw an IllegalStateException
        indicesService.verifyIndexIsDeleted(tombstonedIndex, clusterState);
    }

    /**
     * Tests that teh {@link MapperService} created by {@link IndicesService#createIndexMapperServiceForValidation(IndexMetadata)} contains
     * custom types and similarities registered by plugins
     */
    public void testStandAloneMapperServiceWithPlugins() throws IOException {
        final String indexName = "test";
        final Index index = new Index(indexName, UUIDs.randomBase64UUID());
        final IndicesService indicesService = getIndicesService();
        final Settings idxSettings = Settings.builder()
            .put(IndexMetadata.SETTING_VERSION_CREATED, IndexVersion.current())
            .put(SETTING_INDEX_UUID, index.getUUID())
            .put(IndexModule.SIMILARITY_SETTINGS_PREFIX + ".test.type", "fake-similarity")
            .build();
        final IndexMetadata indexMetadata = new IndexMetadata.Builder(index.getName()).settings(idxSettings)
            .numberOfShards(1)
            .numberOfReplicas(0)
            .build();
        MapperService mapperService = indicesService.createIndexMapperServiceForValidation(indexMetadata);
        assertNotNull(mapperService.parserContext().typeParser("fake-mapper"));
        Similarity sim = mapperService.parserContext().getSimilarity("test").get();
        assertThat(sim, instanceOf(NonNegativeScoresSimilarity.class));
        sim = ((NonNegativeScoresSimilarity) sim).getDelegate();
        assertThat(sim, instanceOf(BM25Similarity.class));
    }

    public void testStatsByShardDoesNotDieFromExpectedExceptions() {
        final int shardCount = randomIntBetween(2, 5);
        final int failedShardId = randomIntBetween(0, shardCount - 1);

        final Index index = new Index("test-index", "abc123");
        // the shard that is going to fail
        final ShardId shardId = new ShardId(index, failedShardId);

        final List<IndexShard> shards = new ArrayList<>(shardCount);
        final List<IndexShardStats> shardStats = new ArrayList<>(shardCount - 1);

        final IndexShardState state = randomFrom(IndexShardState.values());
        final String message = "TEST - expected";

        final RuntimeException expectedException = randomFrom(
            new IllegalIndexShardStateException(shardId, state, message),
            new AlreadyClosedException(message)
        );

        // this allows us to control the indices that exist
        final IndicesService mockIndicesService = mock(IndicesService.class);
        final IndexService indexService = mock(IndexService.class);

        // generate fake shards and their responses
        for (int i = 0; i < shardCount; ++i) {
            final IndexShard shard = mock(IndexShard.class);

            shards.add(shard);

            if (failedShardId != i) {
                final IndexShardStats successfulShardStats = mock(IndexShardStats.class);

                shardStats.add(successfulShardStats);

                when(mockIndicesService.indexShardStats(mockIndicesService, shard, CommonStatsFlags.ALL)).thenReturn(successfulShardStats);
            } else {
                when(mockIndicesService.indexShardStats(mockIndicesService, shard, CommonStatsFlags.ALL)).thenThrow(expectedException);
            }
        }

        when(mockIndicesService.iterator()).thenReturn(Collections.singleton(indexService).iterator());
        when(indexService.iterator()).thenReturn(shards.iterator());
        when(indexService.index()).thenReturn(index);

        // real one, which has a logger defined
        final IndicesService indicesService = getIndicesService();

        final Map<Index, List<IndexShardStats>> indexStats = IndicesService.statsByShard(mockIndicesService, CommonStatsFlags.ALL);

        assertThat(indexStats.isEmpty(), equalTo(false));
        assertThat("index not defined", indexStats.containsKey(index), equalTo(true));
        assertThat("unexpected shard stats", indexStats.get(index), equalTo(shardStats));
    }

    public void testGetEngineFactory() throws IOException {
        final IndicesService indicesService = getIndicesService();

        final Boolean[] values = new Boolean[] { true, false, null };
        for (final Boolean value : values) {
            final String indexName = "foo-" + value;
            final Index index = new Index(indexName, UUIDs.randomBase64UUID());
            final Settings.Builder builder = Settings.builder()
                .put(IndexMetadata.SETTING_VERSION_CREATED, IndexVersion.current())
                .put(SETTING_INDEX_UUID, index.getUUID());
            if (value != null) {
                builder.put(FooEnginePlugin.FOO_INDEX_SETTING.getKey(), value);
            }

            final IndexMetadata indexMetadata = new IndexMetadata.Builder(index.getName()).settings(builder.build())
                .numberOfShards(1)
                .numberOfReplicas(0)
                .build();
            final IndexService indexService = indicesService.createIndex(indexMetadata, Collections.emptyList(), false);
            if (value != null && value) {
                assertThat(indexService.getEngineFactory(), instanceOf(FooEnginePlugin.FooEngineFactory.class));
            } else {
                assertThat(indexService.getEngineFactory(), instanceOf(InternalEngineFactory.class));
            }
        }
    }

    public void testConflictingEngineFactories() {
        final String indexName = "foobar";
        final Index index = new Index(indexName, UUIDs.randomBase64UUID());
        final Settings settings = Settings.builder()
            .put(IndexMetadata.SETTING_VERSION_CREATED, IndexVersion.current())
            .put(SETTING_INDEX_UUID, index.getUUID())
            .put(FooEnginePlugin.FOO_INDEX_SETTING.getKey(), true)
            .put(BarEnginePlugin.BAR_INDEX_SETTING.getKey(), true)
            .build();
        final IndexMetadata indexMetadata = new IndexMetadata.Builder(index.getName()).settings(settings)
            .numberOfShards(1)
            .numberOfReplicas(0)
            .build();

        final IndicesService indicesService = getIndicesService();
        final IllegalStateException e = expectThrows(
            IllegalStateException.class,
            () -> indicesService.createIndex(indexMetadata, Collections.emptyList(), false)
        );
        final String pattern =
            ".*multiple engine factories provided for \\[foobar/.*\\]: \\[.*FooEngineFactory\\],\\[.*BarEngineFactory\\].*";
        assertThat(e, hasToString(matchesRegex(pattern)));
    }

    public void testBuildAliasFilter() {
        var indicesService = getIndicesService();
        final ProjectId projectId = randomProjectIdOrDefault();
        final ProjectMetadata.Builder projBuilder = ProjectMetadata.builder(projectId)
            .put(
                indexBuilder("test-0").state(IndexMetadata.State.OPEN)
                    .putAlias(AliasMetadata.builder("test-alias-0").filter(Strings.toString(QueryBuilders.termQuery("foo", "bar"))))
                    .putAlias(AliasMetadata.builder("test-alias-1").filter(Strings.toString(QueryBuilders.termQuery("foo", "baz"))))
                    .putAlias(AliasMetadata.builder("test-alias-non-filtering"))
            )
            .put(
                indexBuilder("test-1").state(IndexMetadata.State.OPEN)
                    .putAlias(AliasMetadata.builder("test-alias-0").filter(Strings.toString(QueryBuilders.termQuery("foo", "bar"))))
                    .putAlias(AliasMetadata.builder("test-alias-1").filter(Strings.toString(QueryBuilders.termQuery("foo", "bax"))))
                    .putAlias(AliasMetadata.builder("test-alias-non-filtering"))
            );
        final ClusterState clusterState = ClusterState.builder(new ClusterName("_name")).putProjectMetadata(projBuilder).build();
        final ProjectState projectState = clusterState.projectState(projectId);
        {
            AliasFilter result = indicesService.buildAliasFilter(projectState, "test-0", resolvedExpressions("test-alias-0"));
            assertThat(result.getAliases(), arrayContainingInAnyOrder("test-alias-0"));
            assertThat(result.getQueryBuilder(), equalTo(QueryBuilders.termQuery("foo", "bar")));
        }
        {
            AliasFilter result = indicesService.buildAliasFilter(projectState, "test-1", resolvedExpressions("test-alias-0"));
            assertThat(result.getAliases(), arrayContainingInAnyOrder("test-alias-0"));
            assertThat(result.getQueryBuilder(), equalTo(QueryBuilders.termQuery("foo", "bar")));
        }
        {
            AliasFilter result = indicesService.buildAliasFilter(projectState, "test-0", resolvedExpressions("test-alias-1"));
            assertThat(result.getAliases(), arrayContainingInAnyOrder("test-alias-1"));
            assertThat(result.getQueryBuilder(), equalTo(QueryBuilders.termQuery("foo", "baz")));
        }
        {
            AliasFilter result = indicesService.buildAliasFilter(projectState, "test-1", resolvedExpressions("test-alias-1"));
            assertThat(result.getAliases(), arrayContainingInAnyOrder("test-alias-1"));
            assertThat(result.getQueryBuilder(), equalTo(QueryBuilders.termQuery("foo", "bax")));
        }
        {
            AliasFilter result = indicesService.buildAliasFilter(
                projectState,
                "test-0",
                resolvedExpressions("test-alias-0", "test-alias-1")
            );
            assertThat(result.getAliases(), arrayContainingInAnyOrder("test-alias-0", "test-alias-1"));
            BoolQueryBuilder filter = (BoolQueryBuilder) result.getQueryBuilder();
            assertThat(filter.filter(), empty());
            assertThat(filter.must(), empty());
            assertThat(filter.mustNot(), empty());
            assertThat(filter.should(), containsInAnyOrder(QueryBuilders.termQuery("foo", "baz"), QueryBuilders.termQuery("foo", "bar")));
        }
        {
            AliasFilter result = indicesService.buildAliasFilter(
                projectState,
                "test-1",
                resolvedExpressions("test-alias-0", "test-alias-1")
            );
            assertThat(result.getAliases(), arrayContainingInAnyOrder("test-alias-0", "test-alias-1"));
            BoolQueryBuilder filter = (BoolQueryBuilder) result.getQueryBuilder();
            assertThat(filter.filter(), empty());
            assertThat(filter.must(), empty());
            assertThat(filter.mustNot(), empty());
            assertThat(filter.should(), containsInAnyOrder(QueryBuilders.termQuery("foo", "bax"), QueryBuilders.termQuery("foo", "bar")));
        }
        {
            AliasFilter result = indicesService.buildAliasFilter(
                projectState,
                "test-0",
                resolvedExpressions("test-alias-0", "test-alias-1", "test-alias-non-filtering")
            );
            assertThat(result.getAliases(), emptyArray());
            assertThat(result.getQueryBuilder(), nullValue());
        }
        {
            AliasFilter result = indicesService.buildAliasFilter(
                projectState,
                "test-1",
                resolvedExpressions("test-alias-0", "test-alias-1", "test-alias-non-filtering")
            );
            assertThat(result.getAliases(), emptyArray());
            assertThat(result.getQueryBuilder(), nullValue());
        }
    }

    public void testBuildAliasFilterDataStreamAliases() {
        var indicesService = getIndicesService();

        final String dataStreamName1 = "logs-foobar";
        final String dataStreamName2 = "logs-foobaz";
        IndexMetadata backingIndex1 = createBackingIndex(dataStreamName1, 1).build();
        IndexMetadata backingIndex2 = createBackingIndex(dataStreamName2, 1).build();
        final ProjectId projectId = randomProjectIdOrDefault();
        final ProjectMetadata.Builder projBuilder = ProjectMetadata.builder(projectId)
            .put(backingIndex1, false)
            .put(backingIndex2, false)
            .put(DataStreamTestHelper.newInstance(dataStreamName1, List.of(backingIndex1.getIndex())))
            .put(DataStreamTestHelper.newInstance(dataStreamName2, List.of(backingIndex2.getIndex())));
        projBuilder.put("logs_foo", dataStreamName1, null, Strings.toString(QueryBuilders.termQuery("foo", "bar")));
        projBuilder.put("logs_foo", dataStreamName2, null, Strings.toString(QueryBuilders.termQuery("foo", "baz")));
        projBuilder.put("logs", dataStreamName1, null, Strings.toString(QueryBuilders.termQuery("foo", "baz")));
        projBuilder.put("logs", dataStreamName2, null, Strings.toString(QueryBuilders.termQuery("foo", "bax")));
        projBuilder.put("logs_bar", dataStreamName1, null, null);
        ClusterState clusterState = ClusterState.builder(new ClusterName("_name")).putProjectMetadata(projBuilder).build();
        ProjectState projectState = clusterState.projectState(projectId);
        {
            String index = backingIndex1.getIndex().getName();
            AliasFilter result = indicesService.buildAliasFilter(projectState, index, resolvedExpressions("logs_foo"));
            assertThat(result.getAliases(), arrayContainingInAnyOrder("logs_foo"));
            assertThat(result.getQueryBuilder(), equalTo(QueryBuilders.termQuery("foo", "bar")));
        }
        {
            String index = backingIndex2.getIndex().getName();
            AliasFilter result = indicesService.buildAliasFilter(projectState, index, resolvedExpressions("logs_foo"));
            assertThat(result.getAliases(), arrayContainingInAnyOrder("logs_foo"));
            assertThat(result.getQueryBuilder(), equalTo(QueryBuilders.termQuery("foo", "baz")));
        }
        {
            String index = backingIndex1.getIndex().getName();
            AliasFilter result = indicesService.buildAliasFilter(projectState, index, resolvedExpressions("logs_foo", "logs"));
            assertThat(result.getAliases(), arrayContainingInAnyOrder("logs_foo", "logs"));
            BoolQueryBuilder filter = (BoolQueryBuilder) result.getQueryBuilder();
            assertThat(filter.filter(), empty());
            assertThat(filter.must(), empty());
            assertThat(filter.mustNot(), empty());
            assertThat(filter.should(), containsInAnyOrder(QueryBuilders.termQuery("foo", "baz"), QueryBuilders.termQuery("foo", "bar")));
        }
        {
            String index = backingIndex2.getIndex().getName();
            AliasFilter result = indicesService.buildAliasFilter(projectState, index, resolvedExpressions("logs_foo", "logs"));
            assertThat(result.getAliases(), arrayContainingInAnyOrder("logs_foo", "logs"));
            BoolQueryBuilder filter = (BoolQueryBuilder) result.getQueryBuilder();
            assertThat(filter.filter(), empty());
            assertThat(filter.must(), empty());
            assertThat(filter.mustNot(), empty());
            assertThat(filter.should(), containsInAnyOrder(QueryBuilders.termQuery("foo", "baz"), QueryBuilders.termQuery("foo", "bax")));
        }
        {
            // querying an unfiltered and a filtered alias for the same data stream should drop the filters
            String index = backingIndex1.getIndex().getName();
            AliasFilter result = indicesService.buildAliasFilter(projectState, index, resolvedExpressions("logs_foo", "logs", "logs_bar"));
            assertThat(result, is(AliasFilter.EMPTY));
        }
        {
            // similarly, querying the data stream name and a filtered alias should drop the filter
            String index = backingIndex1.getIndex().getName();
            AliasFilter result = indicesService.buildAliasFilter(projectState, index, resolvedExpressions("logs", dataStreamName1));
            assertThat(result, is(AliasFilter.EMPTY));
        }
    }

    public void testLoadSlowLogFieldProvider() {
        TestSlowLogFieldProvider.setFields(Map.of("key1", "value1"));
        TestAnotherSlowLogFieldProvider.setFields(Map.of("key2", "value2"));

        var indicesService = getIndicesService();
        SlowLogFieldProvider fieldProvider = indicesService.slowLogFieldProvider;
        SlowLogFields fields = fieldProvider.create(null);

        // The map of fields from the two providers are merged to a single map of fields
        assertEquals(Map.of("key1", "value1", "key2", "value2"), fields.searchFields());
        assertEquals(Map.of("key1", "value1", "key2", "value2"), fields.indexFields());

        TestSlowLogFieldProvider.setFields(Map.of("key1", "value1"));
        TestAnotherSlowLogFieldProvider.setFields(Map.of("key1", "value2"));

        // There is an overlap of field names, since this isn't deterministic and probably a
        // programming error (two providers provide the same field) throw an exception
        assertThrows(IllegalStateException.class, fields::searchFields);
        assertThrows(IllegalStateException.class, fields::indexFields);

        TestSlowLogFieldProvider.setFields(Map.of("key1", "value1"));
        TestAnotherSlowLogFieldProvider.setFields(Map.of());

        // One provider has no fields
        assertEquals(Map.of("key1", "value1"), fields.searchFields());
        assertEquals(Map.of("key1", "value1"), fields.indexFields());

        TestSlowLogFieldProvider.setFields(Map.of());
        TestAnotherSlowLogFieldProvider.setFields(Map.of());

        // Both providers have no fields
        assertEquals(Map.of(), fields.searchFields());
        assertEquals(Map.of(), fields.indexFields());
    }

    public void testWithTempIndexServiceHandlesExistingIndex() throws Exception {
        // This test makes sure that we can run withTempIndexService even if the index already exists
        IndicesService indicesService = getIndicesService();
        IndexMetadata indexMetadata = new IndexMetadata.Builder("test").settings(
            indexSettings(randomIntBetween(1, 5), randomIntBetween(0, 5)).put("index.version.created", IndexVersions.V_8_10_0)
                .put(SETTING_INDEX_UUID, randomUUID())
        ).build();
        IndexService createdIndexService = indicesService.createIndex(indexMetadata, List.of(), true);
        indicesService.withTempIndexService(indexMetadata, indexService -> {
            assertNotEquals(createdIndexService, indexService);
            assertEquals(createdIndexService.index(), indexService.index());
            return null;
        });
    }

    private Set<ResolvedExpression> resolvedExpressions(String... expressions) {
        return Arrays.stream(expressions).map(ResolvedExpression::new).collect(Collectors.toSet());
    }

    private IndexRemovalReason randomReason() {
        return randomFrom(IndexRemovalReason.values());
    }
}
