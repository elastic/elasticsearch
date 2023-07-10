/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.cluster.metadata;

import org.elasticsearch.ExceptionsHelper;
import org.elasticsearch.ResourceAlreadyExistsException;
import org.elasticsearch.Version;
import org.elasticsearch.action.ActionListener;
import org.elasticsearch.action.admin.indices.alias.Alias;
import org.elasticsearch.action.admin.indices.create.CreateIndexClusterStateUpdateRequest;
import org.elasticsearch.action.admin.indices.shrink.ResizeType;
import org.elasticsearch.cluster.ClusterName;
import org.elasticsearch.cluster.ClusterState;
import org.elasticsearch.cluster.ESAllocationTestCase;
import org.elasticsearch.cluster.EmptyClusterInfoService;
import org.elasticsearch.cluster.TestShardRoutingRoleStrategies;
import org.elasticsearch.cluster.block.ClusterBlocks;
import org.elasticsearch.cluster.node.DiscoveryNode;
import org.elasticsearch.cluster.node.DiscoveryNodeRole;
import org.elasticsearch.cluster.node.DiscoveryNodeUtils;
import org.elasticsearch.cluster.node.DiscoveryNodes;
import org.elasticsearch.cluster.routing.RoutingTable;
import org.elasticsearch.cluster.routing.allocation.AllocationService;
import org.elasticsearch.cluster.routing.allocation.DataTier;
import org.elasticsearch.cluster.routing.allocation.allocator.BalancedShardsAllocator;
import org.elasticsearch.cluster.routing.allocation.decider.AllocationDeciders;
import org.elasticsearch.cluster.routing.allocation.decider.MaxRetryAllocationDecider;
import org.elasticsearch.cluster.service.ClusterService;
import org.elasticsearch.common.Strings;
import org.elasticsearch.common.compress.CompressedXContent;
import org.elasticsearch.common.settings.IndexScopedSettings;
import org.elasticsearch.common.settings.Setting;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.core.TimeValue;
import org.elasticsearch.index.Index;
import org.elasticsearch.index.IndexModule;
import org.elasticsearch.index.IndexNotFoundException;
import org.elasticsearch.index.IndexSettingProviders;
import org.elasticsearch.index.IndexSettings;
import org.elasticsearch.index.IndexVersion;
import org.elasticsearch.index.mapper.MapperService;
import org.elasticsearch.index.query.SearchExecutionContext;
import org.elasticsearch.index.query.SearchExecutionContextHelper;
import org.elasticsearch.indices.EmptySystemIndices;
import org.elasticsearch.indices.InvalidAliasNameException;
import org.elasticsearch.indices.InvalidIndexNameException;
import org.elasticsearch.indices.ShardLimitValidator;
import org.elasticsearch.indices.SystemIndexDescriptor;
import org.elasticsearch.indices.SystemIndexDescriptorUtils;
import org.elasticsearch.indices.SystemIndices;
import org.elasticsearch.snapshots.EmptySnapshotsInfoService;
import org.elasticsearch.test.ClusterServiceUtils;
import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.test.VersionUtils;
import org.elasticsearch.test.gateway.TestGatewayAllocator;
import org.elasticsearch.test.index.IndexVersionUtils;
import org.elasticsearch.threadpool.TestThreadPool;
import org.elasticsearch.threadpool.ThreadPool;
import org.elasticsearch.xcontent.NamedXContentRegistry;
import org.elasticsearch.xcontent.XContentFactory;
import org.hamcrest.Matchers;
import org.junit.Before;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.UUID;
import java.util.function.BiConsumer;
import java.util.function.Consumer;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import static org.elasticsearch.cluster.metadata.IndexMetadata.INDEX_NUMBER_OF_ROUTING_SHARDS_SETTING;
import static org.elasticsearch.cluster.metadata.IndexMetadata.INDEX_NUMBER_OF_SHARDS_SETTING;
import static org.elasticsearch.cluster.metadata.IndexMetadata.INDEX_READ_ONLY_BLOCK;
import static org.elasticsearch.cluster.metadata.IndexMetadata.SETTING_NUMBER_OF_SHARDS;
import static org.elasticsearch.cluster.metadata.IndexMetadata.SETTING_READ_ONLY;
import static org.elasticsearch.cluster.metadata.IndexMetadata.SETTING_VERSION_CREATED;
import static org.elasticsearch.cluster.metadata.MetadataCreateIndexService.aggregateIndexSettings;
import static org.elasticsearch.cluster.metadata.MetadataCreateIndexService.buildIndexMetadata;
import static org.elasticsearch.cluster.metadata.MetadataCreateIndexService.clusterStateCreateIndex;
import static org.elasticsearch.cluster.metadata.MetadataCreateIndexService.getIndexNumberOfRoutingShards;
import static org.elasticsearch.cluster.metadata.MetadataCreateIndexService.parseV1Mappings;
import static org.elasticsearch.cluster.metadata.MetadataCreateIndexService.resolveAndValidateAliases;
import static org.elasticsearch.index.IndexSettings.INDEX_SOFT_DELETES_SETTING;
import static org.elasticsearch.indices.ShardLimitValidatorTests.createTestShardLimitService;
import static org.hamcrest.Matchers.contains;
import static org.hamcrest.Matchers.endsWith;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.hasKey;
import static org.hamcrest.Matchers.hasValue;
import static org.hamcrest.Matchers.is;
import static org.hamcrest.Matchers.notNullValue;
import static org.hamcrest.Matchers.nullValue;
import static org.hamcrest.Matchers.startsWith;

public class MetadataCreateIndexServiceTests extends ESTestCase {

    private CreateIndexClusterStateUpdateRequest request;
    private SearchExecutionContext searchExecutionContext;

    @Before
    public void setupCreateIndexRequestAndAliasValidator() {
        request = new CreateIndexClusterStateUpdateRequest("create index", "test", "test");
        Settings indexSettings = indexSettings(Version.CURRENT, 1, 1).build();
        searchExecutionContext = SearchExecutionContextHelper.createSimple(
            new IndexSettings(IndexMetadata.builder("test").settings(indexSettings).build(), indexSettings),
            parserConfig(),
            writableRegistry()
        );
    }

    private ClusterState createClusterState(String name, int numShards, int numReplicas, Settings settings) {
        int numRoutingShards = settings.getAsInt(IndexMetadata.INDEX_NUMBER_OF_ROUTING_SHARDS_SETTING.getKey(), numShards);
        Metadata.Builder metaBuilder = Metadata.builder();
        IndexMetadata indexMetadata = IndexMetadata.builder(name)
            .settings(settings(Version.CURRENT).put(settings))
            .numberOfShards(numShards)
            .numberOfReplicas(numReplicas)
            .setRoutingNumShards(numRoutingShards)
            .build();
        metaBuilder.put(indexMetadata, false);
        Metadata metadata = metaBuilder.build();
        RoutingTable.Builder routingTableBuilder = RoutingTable.builder(TestShardRoutingRoleStrategies.DEFAULT_ROLE_ONLY);
        routingTableBuilder.addAsNew(metadata.index(name));

        RoutingTable routingTable = routingTableBuilder.build();
        ClusterState clusterState = ClusterState.builder(ClusterName.DEFAULT)
            .metadata(metadata)
            .routingTable(routingTable)
            .blocks(ClusterBlocks.builder().addBlocks(indexMetadata))
            .build();
        return clusterState;
    }

    public static boolean isShrinkable(int source, int target) {
        int x = source / target;
        assert source > target : source + " <= " + target;
        return target * x == source;
    }

    public static boolean isSplitable(int source, int target) {
        int x = target / source;
        assert source < target : source + " >= " + target;
        return source * x == target;
    }

    public void testValidateShrinkIndex() {
        int numShards = randomIntBetween(2, 42);
        ClusterState state = createClusterState(
            "source",
            numShards,
            randomIntBetween(0, 10),
            Settings.builder().put("index.blocks.write", true).build()
        );

        assertEquals(
            "index [source] already exists",
            expectThrows(
                ResourceAlreadyExistsException.class,
                () -> MetadataCreateIndexService.validateShrinkIndex(state, "target", "source", Settings.EMPTY)
            ).getMessage()
        );

        assertEquals(
            "no such index [no_such_index]",
            expectThrows(
                IndexNotFoundException.class,
                () -> MetadataCreateIndexService.validateShrinkIndex(state, "no_such_index", "target", Settings.EMPTY)
            ).getMessage()
        );

        Settings targetSettings = Settings.builder().put("index.number_of_shards", 1).build();
        assertEquals(
            "can't shrink an index with only one shard",
            expectThrows(
                IllegalArgumentException.class,
                () -> MetadataCreateIndexService.validateShrinkIndex(
                    createClusterState("source", 1, 0, Settings.builder().put("index.blocks.write", true).build()),
                    "source",
                    "target",
                    targetSettings
                )
            ).getMessage()
        );

        assertEquals(
            "the number of target shards [10] must be less that the number of source shards [5]",
            expectThrows(
                IllegalArgumentException.class,
                () -> MetadataCreateIndexService.validateShrinkIndex(
                    createClusterState("source", 5, 0, Settings.builder().put("index.blocks.write", true).build()),
                    "source",
                    "target",
                    Settings.builder().put("index.number_of_shards", 10).build()
                )
            ).getMessage()
        );

        assertEquals(
            "index source must be read-only to resize index. use \"index.blocks.write=true\"",
            expectThrows(
                IllegalStateException.class,
                () -> MetadataCreateIndexService.validateShrinkIndex(
                    createClusterState("source", randomIntBetween(2, 100), randomIntBetween(0, 10), Settings.EMPTY),
                    "source",
                    "target",
                    targetSettings
                )
            ).getMessage()
        );

        assertEquals(
            "index source must have all shards allocated on the same node to shrink index",
            expectThrows(
                IllegalStateException.class,
                () -> MetadataCreateIndexService.validateShrinkIndex(state, "source", "target", targetSettings)

            ).getMessage()
        );
        assertEquals(
            "the number of source shards [8] must be a multiple of [3]",
            expectThrows(
                IllegalArgumentException.class,
                () -> MetadataCreateIndexService.validateShrinkIndex(
                    createClusterState("source", 8, randomIntBetween(0, 10), Settings.builder().put("index.blocks.write", true).build()),
                    "source",
                    "target",
                    Settings.builder().put("index.number_of_shards", 3).build()
                )
            ).getMessage()
        );

        // create one that won't fail
        ClusterState clusterState = ClusterState.builder(
            createClusterState("source", numShards, 0, Settings.builder().put("index.blocks.write", true).build())
        ).nodes(DiscoveryNodes.builder().add(newNode("node1"))).build();
        AllocationService service = new AllocationService(
            new AllocationDeciders(Collections.singleton(new MaxRetryAllocationDecider())),
            new TestGatewayAllocator(),
            new BalancedShardsAllocator(Settings.EMPTY),
            EmptyClusterInfoService.INSTANCE,
            EmptySnapshotsInfoService.INSTANCE,
            TestShardRoutingRoleStrategies.DEFAULT_ROLE_ONLY
        );

        RoutingTable routingTable = service.reroute(clusterState, "reroute", ActionListener.noop()).routingTable();
        clusterState = ClusterState.builder(clusterState).routingTable(routingTable).build();
        // now we start the shard
        routingTable = ESAllocationTestCase.startInitializingShardsAndReroute(service, clusterState, "source").routingTable();
        clusterState = ClusterState.builder(clusterState).routingTable(routingTable).build();
        int targetShards;
        do {
            targetShards = randomIntBetween(1, numShards / 2);
        } while (isShrinkable(numShards, targetShards) == false);
        MetadataCreateIndexService.validateShrinkIndex(
            clusterState,
            "source",
            "target",
            Settings.builder().put("index.number_of_shards", targetShards).build()
        );
    }

    public void testValidateSplitIndex() {
        int numShards = randomIntBetween(1, 42);
        Settings targetSettings = Settings.builder().put("index.number_of_shards", numShards * 2).build();
        ClusterState state = createClusterState(
            "source",
            numShards,
            randomIntBetween(0, 10),
            Settings.builder().put("index.blocks.write", true).build()
        );

        assertEquals(
            "index [source] already exists",
            expectThrows(
                ResourceAlreadyExistsException.class,
                () -> MetadataCreateIndexService.validateSplitIndex(state, "target", "source", targetSettings)
            ).getMessage()
        );

        assertEquals(
            "no such index [no_such_index]",
            expectThrows(
                IndexNotFoundException.class,
                () -> MetadataCreateIndexService.validateSplitIndex(state, "no_such_index", "target", targetSettings)
            ).getMessage()
        );

        assertEquals(
            "the number of source shards [10] must be less that the number of target shards [5]",
            expectThrows(
                IllegalArgumentException.class,
                () -> MetadataCreateIndexService.validateSplitIndex(
                    createClusterState("source", 10, 0, Settings.builder().put("index.blocks.write", true).build()),
                    "source",
                    "target",
                    Settings.builder().put("index.number_of_shards", 5).build()
                )
            ).getMessage()
        );

        assertEquals(
            "index source must be read-only to resize index. use \"index.blocks.write=true\"",
            expectThrows(
                IllegalStateException.class,
                () -> MetadataCreateIndexService.validateSplitIndex(
                    createClusterState("source", randomIntBetween(2, 100), randomIntBetween(0, 10), Settings.EMPTY),
                    "source",
                    "target",
                    targetSettings
                )
            ).getMessage()
        );

        assertEquals(
            "the number of source shards [3] must be a factor of [4]",
            expectThrows(
                IllegalArgumentException.class,
                () -> MetadataCreateIndexService.validateSplitIndex(
                    createClusterState("source", 3, randomIntBetween(0, 10), Settings.builder().put("index.blocks.write", true).build()),
                    "source",
                    "target",
                    Settings.builder().put("index.number_of_shards", 4).build()
                )
            ).getMessage()
        );

        int targetShards;
        do {
            targetShards = randomIntBetween(numShards + 1, 100);
        } while (isSplitable(numShards, targetShards) == false);
        ClusterState clusterState = ClusterState.builder(
            createClusterState(
                "source",
                numShards,
                0,
                Settings.builder().put("index.blocks.write", true).put("index.number_of_routing_shards", targetShards).build()
            )
        ).nodes(DiscoveryNodes.builder().add(newNode("node1"))).build();
        AllocationService service = new AllocationService(
            new AllocationDeciders(Collections.singleton(new MaxRetryAllocationDecider())),
            new TestGatewayAllocator(),
            new BalancedShardsAllocator(Settings.EMPTY),
            EmptyClusterInfoService.INSTANCE,
            EmptySnapshotsInfoService.INSTANCE,
            TestShardRoutingRoleStrategies.DEFAULT_ROLE_ONLY
        );

        RoutingTable routingTable = service.reroute(clusterState, "reroute", ActionListener.noop()).routingTable();
        clusterState = ClusterState.builder(clusterState).routingTable(routingTable).build();
        // now we start the shard
        routingTable = ESAllocationTestCase.startInitializingShardsAndReroute(service, clusterState, "source").routingTable();
        clusterState = ClusterState.builder(clusterState).routingTable(routingTable).build();

        MetadataCreateIndexService.validateSplitIndex(
            clusterState,
            "source",
            "target",
            Settings.builder().put("index.number_of_shards", targetShards).build()
        );
    }

    public void testPrepareResizeIndexSettings() {
        final List<IndexVersion> versions = Stream.of(IndexVersionUtils.randomVersion(random()), IndexVersionUtils.randomVersion(random()))
            .sorted()
            .toList();
        final IndexVersion version = versions.get(0);
        final IndexVersion upgraded = versions.get(1);
        final Settings.Builder indexSettingsBuilder = Settings.builder()
            .put("index.version.created", version)
            .put("index.similarity.default.type", "BM25")
            .put("index.analysis.analyzer.default.tokenizer", "keyword")
            .put("index.soft_deletes.enabled", "true");
        if (randomBoolean()) {
            indexSettingsBuilder.put("index.allocation.max_retries", randomIntBetween(1, 1000));
        }
        runPrepareResizeIndexSettingsTest(
            indexSettingsBuilder.build(),
            Settings.EMPTY,
            Collections.emptyList(),
            randomBoolean(),
            settings -> {
                assertThat("similarity settings must be copied", settings.get("index.similarity.default.type"), equalTo("BM25"));
                assertThat(
                    "analysis settings must be copied",
                    settings.get("index.analysis.analyzer.default.tokenizer"),
                    equalTo("keyword")
                );
                assertThat(settings.get("index.routing.allocation.initial_recovery._id"), equalTo("node1"));
                assertThat(settings.get("index.allocation.max_retries"), nullValue());
                assertThat(settings.getAsVersionId("index.version.created", IndexVersion::fromId), equalTo(version));
                assertThat(settings.get("index.soft_deletes.enabled"), equalTo("true"));
            }
        );
    }

    public void testPrepareResizeIndexSettingsCopySettings() {
        final int maxMergeCount = randomIntBetween(1, 16);
        final int maxThreadCount = randomIntBetween(1, 16);
        final Setting<String> nonCopyableExistingIndexSetting = Setting.simpleString(
            "index.non_copyable.existing",
            Setting.Property.IndexScope,
            Setting.Property.NotCopyableOnResize
        );
        final Setting<String> nonCopyableRequestIndexSetting = Setting.simpleString(
            "index.non_copyable.request",
            Setting.Property.IndexScope,
            Setting.Property.NotCopyableOnResize
        );
        runPrepareResizeIndexSettingsTest(
            Settings.builder()
                .put("index.merge.scheduler.max_merge_count", maxMergeCount)
                .put("index.non_copyable.existing", "existing")
                .build(),
            Settings.builder()
                .put("index.blocks.write", (String) null)
                .put("index.merge.scheduler.max_thread_count", maxThreadCount)
                .put("index.non_copyable.request", "request")
                .build(),
            Arrays.asList(nonCopyableExistingIndexSetting, nonCopyableRequestIndexSetting),
            true,
            settings -> {
                assertNull(settings.getAsBoolean("index.blocks.write", null));
                assertThat(settings.get("index.routing.allocation.require._name"), equalTo("node1"));
                assertThat(settings.getAsInt("index.merge.scheduler.max_merge_count", null), equalTo(maxMergeCount));
                assertThat(settings.getAsInt("index.merge.scheduler.max_thread_count", null), equalTo(maxThreadCount));
                assertNull(settings.get("index.non_copyable.existing"));
                assertThat(settings.get("index.non_copyable.request"), equalTo("request"));
            }
        );
    }

    public void testPrepareResizeIndexSettingsAnalysisSettings() {
        // analysis settings from the request are not overwritten
        runPrepareResizeIndexSettingsTest(
            Settings.EMPTY,
            Settings.builder().put("index.analysis.analyzer.default.tokenizer", "whitespace").build(),
            Collections.emptyList(),
            randomBoolean(),
            settings -> assertThat(
                "analysis settings are not overwritten",
                settings.get("index.analysis.analyzer.default.tokenizer"),
                equalTo("whitespace")
            )
        );

    }

    public void testPrepareResizeIndexSettingsSimilaritySettings() {
        // similarity settings from the request are not overwritten
        runPrepareResizeIndexSettingsTest(
            Settings.EMPTY,
            Settings.builder().put("index.similarity.sim.type", "DFR").build(),
            Collections.emptyList(),
            randomBoolean(),
            settings -> assertThat("similarity settings are not overwritten", settings.get("index.similarity.sim.type"), equalTo("DFR"))
        );

    }

    public void testDoNotOverrideSoftDeletesSettingOnResize() {
        runPrepareResizeIndexSettingsTest(
            Settings.builder().put("index.soft_deletes.enabled", "false").build(),
            Settings.builder().put("index.soft_deletes.enabled", "true").build(),
            Collections.emptyList(),
            randomBoolean(),
            settings -> assertThat(settings.get("index.soft_deletes.enabled"), equalTo("true"))
        );
    }

    private void runPrepareResizeIndexSettingsTest(
        final Settings sourceSettings,
        final Settings requestSettings,
        final Collection<Setting<?>> additionalIndexScopedSettings,
        final boolean copySettings,
        final Consumer<Settings> consumer
    ) {
        final String indexName = randomAlphaOfLength(10);

        final Settings indexSettings = Settings.builder()
            .put("index.blocks.write", true)
            .put("index.routing.allocation.require._name", "node1")
            .put(sourceSettings)
            .build();

        final ClusterState initialClusterState = ClusterState.builder(
            createClusterState(indexName, randomIntBetween(2, 10), 0, indexSettings)
        ).nodes(DiscoveryNodes.builder().add(newNode("node1"))).build();

        final AllocationService service = new AllocationService(
            new AllocationDeciders(Collections.singleton(new MaxRetryAllocationDecider())),
            new TestGatewayAllocator(),
            new BalancedShardsAllocator(Settings.EMPTY),
            EmptyClusterInfoService.INSTANCE,
            EmptySnapshotsInfoService.INSTANCE,
            TestShardRoutingRoleStrategies.DEFAULT_ROLE_ONLY
        );

        final RoutingTable initialRoutingTable = service.reroute(initialClusterState, "reroute", ActionListener.noop()).routingTable();
        final ClusterState routingTableClusterState = ClusterState.builder(initialClusterState).routingTable(initialRoutingTable).build();

        // now we start the shard
        final RoutingTable routingTable = ESAllocationTestCase.startInitializingShardsAndReroute(
            service,
            routingTableClusterState,
            indexName
        ).routingTable();
        final ClusterState clusterState = ClusterState.builder(routingTableClusterState).routingTable(routingTable).build();

        final Settings.Builder indexSettingsBuilder = Settings.builder().put("index.number_of_shards", 1).put(requestSettings);
        final Set<Setting<?>> settingsSet = Stream.concat(
            IndexScopedSettings.BUILT_IN_INDEX_SETTINGS.stream(),
            additionalIndexScopedSettings.stream()
        ).collect(Collectors.toSet());
        MetadataCreateIndexService.prepareResizeIndexSettings(
            clusterState,
            indexSettingsBuilder,
            clusterState.metadata().index(indexName).getIndex(),
            "target",
            ResizeType.SHRINK,
            copySettings,
            new IndexScopedSettings(Settings.EMPTY, settingsSet)
        );
        consumer.accept(indexSettingsBuilder.build());
    }

    private DiscoveryNode newNode(String nodeId) {
        return DiscoveryNodeUtils.builder(nodeId).roles(Set.of(DiscoveryNodeRole.MASTER_ROLE, DiscoveryNodeRole.DATA_ROLE)).build();
    }

    public void testValidateIndexName() throws Exception {
        withTemporaryClusterService(((clusterService, threadPool) -> {
            MetadataCreateIndexService checkerService = new MetadataCreateIndexService(
                Settings.EMPTY,
                clusterService,
                null,
                null,
                createTestShardLimitService(randomIntBetween(1, 1000), clusterService),
                null,
                null,
                threadPool,
                null,
                EmptySystemIndices.INSTANCE,
                false,
                new IndexSettingProviders(Set.of())
            );
            validateIndexName(checkerService, "index?name", "must not contain the following characters " + Strings.INVALID_FILENAME_CHARS);

            validateIndexName(checkerService, "index#name", "must not contain '#'");

            validateIndexName(checkerService, "_indexname", "must not start with '_', '-', or '+'");
            validateIndexName(checkerService, "-indexname", "must not start with '_', '-', or '+'");
            validateIndexName(checkerService, "+indexname", "must not start with '_', '-', or '+'");

            validateIndexName(checkerService, "INDEXNAME", "must be lowercase");

            validateIndexName(checkerService, "..", "must not be '.' or '..'");

            validateIndexName(checkerService, "foo:bar", "must not contain ':'");
        }));
    }

    private static void validateIndexName(MetadataCreateIndexService metadataCreateIndexService, String indexName, String errorMessage) {
        InvalidIndexNameException e = expectThrows(
            InvalidIndexNameException.class,
            () -> MetadataCreateIndexService.validateIndexName(indexName, ClusterState.builder(ClusterName.DEFAULT).build())
        );
        assertThat(e.getMessage(), endsWith(errorMessage));
    }

    public void testCalculateNumRoutingShards() {
        assertEquals(1024, MetadataCreateIndexService.calculateNumRoutingShards(1, IndexVersion.current()));
        assertEquals(1024, MetadataCreateIndexService.calculateNumRoutingShards(2, IndexVersion.current()));
        assertEquals(768, MetadataCreateIndexService.calculateNumRoutingShards(3, IndexVersion.current()));
        assertEquals(576, MetadataCreateIndexService.calculateNumRoutingShards(9, IndexVersion.current()));
        assertEquals(1024, MetadataCreateIndexService.calculateNumRoutingShards(512, IndexVersion.current()));
        assertEquals(2048, MetadataCreateIndexService.calculateNumRoutingShards(1024, IndexVersion.current()));
        assertEquals(4096, MetadataCreateIndexService.calculateNumRoutingShards(2048, IndexVersion.current()));

        for (int i = 0; i < 1000; i++) {
            int randomNumShards = randomIntBetween(1, 10000);
            int numRoutingShards = MetadataCreateIndexService.calculateNumRoutingShards(randomNumShards, IndexVersion.current());
            if (numRoutingShards <= 1024) {
                assertTrue("numShards: " + randomNumShards, randomNumShards < 513);
                assertTrue("numRoutingShards: " + numRoutingShards, numRoutingShards > 512);
            } else {
                assertEquals("numShards: " + randomNumShards, numRoutingShards / 2, randomNumShards);
            }

            double ratio = numRoutingShards / randomNumShards;
            int intRatio = (int) ratio;
            assertEquals(ratio, intRatio, 0.0d);
            assertTrue(1 < ratio);
            assertTrue(ratio <= 1024);
            assertEquals(0, intRatio % 2);
            assertEquals("ratio is not a power of two", intRatio, Integer.highestOneBit(intRatio));
        }
    }

    public void testValidateDotIndex() {
        List<SystemIndexDescriptor> systemIndexDescriptors = new ArrayList<>();
        systemIndexDescriptors.add(SystemIndexDescriptorUtils.createUnmanaged(".test-one*", "test"));
        systemIndexDescriptors.add(SystemIndexDescriptorUtils.createUnmanaged(".test-~(one*)", "test"));
        systemIndexDescriptors.add(SystemIndexDescriptorUtils.createUnmanaged(".pattern-test*", "test-1"));

        withTemporaryClusterService(((clusterService, threadPool) -> {
            MetadataCreateIndexService checkerService = new MetadataCreateIndexService(
                Settings.EMPTY,
                clusterService,
                null,
                null,
                createTestShardLimitService(randomIntBetween(1, 1000), clusterService),
                null,
                null,
                threadPool,
                null,
                new SystemIndices(Collections.singletonList(new SystemIndices.Feature("foo", "test feature", systemIndexDescriptors))),
                false,
                new IndexSettingProviders(Set.of())
            );
            // Check deprecations
            assertFalse(checkerService.validateDotIndex(".test2", false));
            assertWarnings(
                "index name [.test2] starts with a dot '.', in the next major version, index "
                    + "names starting with a dot are reserved for hidden indices and system indices"
            );

            // Check non-system hidden indices don't trigger a warning
            assertFalse(checkerService.validateDotIndex(".test2", true));

            // Check NO deprecation warnings if we give the index name
            assertTrue(checkerService.validateDotIndex(".test-one", false));
            assertTrue(checkerService.validateDotIndex(".test-3", false));

            // Check that patterns with wildcards work
            assertTrue(checkerService.validateDotIndex(".pattern-test", false));
            assertTrue(checkerService.validateDotIndex(".pattern-test-with-suffix", false));
            assertTrue(checkerService.validateDotIndex(".pattern-test-other-suffix", false));
        }));
    }

    @SuppressWarnings("unchecked")
    public void testParseMappingsAppliesDataFromTemplateAndRequest() throws Exception {
        IndexTemplateMetadata templateMetadata = addMatchingTemplate(templateBuilder -> {
            templateBuilder.putAlias(AliasMetadata.builder("alias1"));
            templateBuilder.putMapping("_doc", createMapping("mapping_from_template", "text"));
        });
        request.mappings(createMapping("mapping_from_request", "text").string());

        Map<String, Object> parsedMappings = MetadataCreateIndexService.parseV1Mappings(
            request.mappings(),
            List.of(templateMetadata.getMappings()),
            NamedXContentRegistry.EMPTY
        );

        assertThat(parsedMappings, hasKey("_doc"));
        Map<String, Object> doc = (Map<String, Object>) parsedMappings.get("_doc");
        assertThat(doc, hasKey("properties"));
        Map<String, Object> mappingsProperties = (Map<String, Object>) doc.get("properties");
        assertThat(mappingsProperties, hasKey("mapping_from_request"));
        assertThat(mappingsProperties, hasKey("mapping_from_template"));
    }

    public void testAggregateSettingsAppliesSettingsFromTemplatesAndRequest() {
        IndexTemplateMetadata templateMetadata = addMatchingTemplate(builder -> {
            builder.settings(Settings.builder().put("template_setting", "value1"));
        });
        Metadata metadata = new Metadata.Builder().templates(Map.of("template_1", templateMetadata)).build();
        ClusterState clusterState = ClusterState.builder(ClusterName.DEFAULT).metadata(metadata).build();
        request.settings(Settings.builder().put("request_setting", "value2").build());

        Settings aggregatedIndexSettings = aggregateIndexSettings(
            clusterState,
            request,
            templateMetadata.settings(),
            null,
            null,
            Settings.EMPTY,
            IndexScopedSettings.DEFAULT_SCOPED_SETTINGS,
            randomShardLimitService(),
            Collections.emptySet()
        );

        assertThat(aggregatedIndexSettings.get("template_setting"), equalTo("value1"));
        assertThat(aggregatedIndexSettings.get("request_setting"), equalTo("value2"));
    }

    public void testInvalidAliasName() {
        final String[] invalidAliasNames = new String[] { "-alias1", "+alias2", "_alias3", "a#lias", "al:ias", ".", ".." };
        String aliasName = randomFrom(invalidAliasNames);
        request.aliases(Set.of(new Alias(aliasName)));

        expectThrows(
            InvalidAliasNameException.class,
            () -> resolveAndValidateAliases(
                request.index(),
                request.aliases(),
                List.of(),
                Metadata.builder().build(),
                xContentRegistry(),
                searchExecutionContext,
                IndexNameExpressionResolver::resolveDateMathExpression,
                m -> false
            )
        );
    }

    public void testAliasNameWithMathExpression() {
        final String aliasName = "<date-math-based-{2021-01-19||/M{yyyy-MM-dd}}>";

        request.aliases(Set.of(new Alias(aliasName)));

        List<AliasMetadata> aliasMetadata = resolveAndValidateAliases(
            request.index(),
            request.aliases(),
            List.of(),
            Metadata.builder().build(),
            xContentRegistry(),
            searchExecutionContext,
            IndexNameExpressionResolver::resolveDateMathExpression,
            m -> false
        );

        assertEquals("date-math-based-2021-01-01", aliasMetadata.get(0).alias());
    }

    @SuppressWarnings("unchecked")
    public void testRequestDataHavePriorityOverTemplateData() throws Exception {
        CompressedXContent templateMapping = createMapping("test", "text");
        CompressedXContent reqMapping = createMapping("test", "keyword");

        IndexTemplateMetadata templateMetadata = addMatchingTemplate(
            builder -> builder.putAlias(AliasMetadata.builder("alias").searchRouting("fromTemplate").build())
                .putMapping("_doc", templateMapping)
                .settings(Settings.builder().put("key1", "templateValue"))
        );

        request.mappings(reqMapping.string());
        request.aliases(Set.of(new Alias("alias").searchRouting("fromRequest")));
        request.settings(Settings.builder().put("key1", "requestValue").build());

        Map<String, Object> parsedMappings = MetadataCreateIndexService.parseV1Mappings(
            request.mappings(),
            List.of(templateMetadata.mappings()),
            xContentRegistry()
        );
        List<AliasMetadata> resolvedAliases = resolveAndValidateAliases(
            request.index(),
            request.aliases(),
            MetadataIndexTemplateService.resolveAliases(List.of(templateMetadata)),
            Metadata.builder().build(),
            xContentRegistry(),
            searchExecutionContext,
            IndexNameExpressionResolver::resolveDateMathExpression,
            m -> false
        );

        Settings aggregatedIndexSettings = aggregateIndexSettings(
            ClusterState.EMPTY_STATE,
            request,
            templateMetadata.settings(),
            null,
            null,
            Settings.EMPTY,
            IndexScopedSettings.DEFAULT_SCOPED_SETTINGS,
            randomShardLimitService(),
            Collections.emptySet()
        );

        assertThat(resolvedAliases.get(0).getSearchRouting(), equalTo("fromRequest"));
        assertThat(aggregatedIndexSettings.get("key1"), equalTo("requestValue"));
        assertThat(parsedMappings, hasKey("_doc"));
        Map<String, Object> doc = (Map<String, Object>) parsedMappings.get("_doc");
        assertThat(doc, hasKey("properties"));
        Map<String, Object> mappingsProperties = (Map<String, Object>) doc.get("properties");
        assertThat(mappingsProperties, hasKey("test"));
        assertThat((Map<String, Object>) mappingsProperties.get("test"), hasValue("keyword"));
    }

    public void testDefaultSettings() {
        Settings aggregatedIndexSettings = aggregateIndexSettings(
            ClusterState.EMPTY_STATE,
            request,
            Settings.EMPTY,
            null,
            null,
            Settings.EMPTY,
            IndexScopedSettings.DEFAULT_SCOPED_SETTINGS,
            randomShardLimitService(),
            Collections.emptySet()
        );

        assertThat(aggregatedIndexSettings.get(SETTING_NUMBER_OF_SHARDS), equalTo("1"));
    }

    public void testSettingsFromClusterState() {
        Settings aggregatedIndexSettings = aggregateIndexSettings(
            ClusterState.EMPTY_STATE,
            request,
            Settings.EMPTY,
            null,
            null,
            Settings.builder().put(SETTING_NUMBER_OF_SHARDS, 15).build(),
            IndexScopedSettings.DEFAULT_SCOPED_SETTINGS,
            randomShardLimitService(),
            Collections.emptySet()
        );

        assertThat(aggregatedIndexSettings.get(SETTING_NUMBER_OF_SHARDS), equalTo("15"));
    }

    public void testTemplateOrder() throws Exception {
        List<IndexTemplateMetadata> templates = new ArrayList<>(3);
        templates.add(
            addMatchingTemplate(
                builder -> builder.order(3)
                    .settings(Settings.builder().put(SETTING_NUMBER_OF_SHARDS, 12))
                    .putAlias(AliasMetadata.builder("alias1").writeIndex(true).searchRouting("3").build())
            )
        );
        templates.add(
            addMatchingTemplate(
                builder -> builder.order(2)
                    .settings(Settings.builder().put(SETTING_NUMBER_OF_SHARDS, 11))
                    .putAlias(AliasMetadata.builder("alias1").searchRouting("2").build())
            )
        );
        templates.add(
            addMatchingTemplate(
                builder -> builder.order(1)
                    .settings(Settings.builder().put(SETTING_NUMBER_OF_SHARDS, 10))
                    .putAlias(AliasMetadata.builder("alias1").searchRouting("1").build())
            )
        );

        Settings aggregatedIndexSettings = aggregateIndexSettings(
            ClusterState.EMPTY_STATE,
            request,
            MetadataIndexTemplateService.resolveSettings(templates),
            null,
            null,
            Settings.EMPTY,
            IndexScopedSettings.DEFAULT_SCOPED_SETTINGS,
            randomShardLimitService(),
            Collections.emptySet()
        );
        List<AliasMetadata> resolvedAliases = resolveAndValidateAliases(
            request.index(),
            request.aliases(),
            MetadataIndexTemplateService.resolveAliases(templates),
            Metadata.builder().build(),
            xContentRegistry(),
            searchExecutionContext,
            IndexNameExpressionResolver::resolveDateMathExpression,
            m -> false
        );

        assertThat(aggregatedIndexSettings.get(SETTING_NUMBER_OF_SHARDS), equalTo("12"));
        AliasMetadata alias = resolvedAliases.get(0);
        assertThat(alias.getSearchRouting(), equalTo("3"));
        assertThat(alias.writeIndex(), is(true));
    }

    public void testResolvedAliasInTemplate() {
        List<IndexTemplateMetadata> templates = new ArrayList<>(3);
        templates.add(
            addMatchingTemplate(
                builder -> builder.order(3)
                    .settings(Settings.builder().put(SETTING_NUMBER_OF_SHARDS, 1))
                    .putAlias(AliasMetadata.builder("<jan-{2021-01-07||/M{yyyy-MM-dd}}>").build())
            )
        );
        templates.add(
            addMatchingTemplate(
                builder -> builder.order(2)
                    .settings(Settings.builder().put(SETTING_NUMBER_OF_SHARDS, 1))
                    .putAlias(AliasMetadata.builder("<feb-{2021-02-28||/M{yyyy-MM-dd}}>").build())
            )
        );
        templates.add(
            addMatchingTemplate(
                builder -> builder.order(1)
                    .settings(Settings.builder().put(SETTING_NUMBER_OF_SHARDS, 1))
                    .putAlias(AliasMetadata.builder("<mar-{2021-03-07||/M{yyyy-MM-dd}}>").build())
            )
        );

        List<AliasMetadata> resolvedAliases = resolveAndValidateAliases(
            request.index(),
            request.aliases(),
            MetadataIndexTemplateService.resolveAliases(templates),
            Metadata.builder().build(),
            xContentRegistry(),
            searchExecutionContext,
            IndexNameExpressionResolver::resolveDateMathExpression,
            m -> false
        );

        assertThat(resolvedAliases.get(0).alias(), equalTo("jan-2021-01-01"));
        assertThat(resolvedAliases.get(1).alias(), equalTo("feb-2021-02-01"));
        assertThat(resolvedAliases.get(2).alias(), equalTo("mar-2021-03-01"));
    }

    public void testAggregateIndexSettingsIgnoresTemplatesOnCreateFromSourceIndex() throws Exception {
        CompressedXContent templateMapping = createMapping("test", "text");

        IndexTemplateMetadata templateMetadata = addMatchingTemplate(
            builder -> builder.putAlias(AliasMetadata.builder("alias").searchRouting("fromTemplate").build())
                .putMapping("_doc", templateMapping)
                .settings(Settings.builder().put("templateSetting", "templateValue"))
        );

        request.settings(Settings.builder().put("requestSetting", "requestValue").build());
        request.resizeType(ResizeType.SPLIT);
        request.recoverFrom(new Index("sourceIndex", UUID.randomUUID().toString()));
        ClusterState clusterState = createClusterState("sourceIndex", 1, 0, Settings.builder().put("index.blocks.write", true).build());

        Settings aggregatedIndexSettings = aggregateIndexSettings(
            clusterState,
            request,
            templateMetadata.settings(),
            List.of(templateMetadata.getMappings()),
            clusterState.metadata().index("sourceIndex"),
            Settings.EMPTY,
            IndexScopedSettings.DEFAULT_SCOPED_SETTINGS,
            randomShardLimitService(),
            Collections.emptySet()
        );

        assertThat(aggregatedIndexSettings.get("templateSetting"), is(nullValue()));
        assertThat(aggregatedIndexSettings.get("requestSetting"), is("requestValue"));
    }

    public void testClusterStateCreateIndexThrowsWriteIndexValidationException() throws Exception {
        IndexMetadata existingWriteIndex = IndexMetadata.builder("test2")
            .settings(settings(Version.CURRENT))
            .putAlias(AliasMetadata.builder("alias1").writeIndex(true).build())
            .numberOfShards(1)
            .numberOfReplicas(0)
            .build();
        ClusterState currentClusterState = ClusterState.builder(ClusterState.EMPTY_STATE)
            .metadata(Metadata.builder().put(existingWriteIndex, false).build())
            .build();

        IndexMetadata newIndex = IndexMetadata.builder("test")
            .settings(settings(Version.CURRENT))
            .numberOfShards(1)
            .numberOfReplicas(0)
            .putAlias(AliasMetadata.builder("alias1").writeIndex(true).build())
            .build();

        assertThat(
            expectThrows(
                IllegalStateException.class,
                () -> clusterStateCreateIndex(
                    currentClusterState,
                    Set.of(),
                    newIndex,
                    null,
                    TestShardRoutingRoleStrategies.DEFAULT_ROLE_ONLY
                )
            ).getMessage(),
            startsWith("alias [alias1] has more than one write index [")
        );
    }

    public void testClusterStateCreateIndex() {
        ClusterState currentClusterState = ClusterState.builder(ClusterState.EMPTY_STATE).build();

        IndexMetadata newIndexMetadata = IndexMetadata.builder("test")
            .settings(settings(Version.CURRENT).put(SETTING_READ_ONLY, true))
            .numberOfShards(1)
            .numberOfReplicas(0)
            .putAlias(AliasMetadata.builder("alias1").writeIndex(true).build())
            .build();

        ClusterState updatedClusterState = clusterStateCreateIndex(
            currentClusterState,
            Set.of(INDEX_READ_ONLY_BLOCK),
            newIndexMetadata,
            null,
            TestShardRoutingRoleStrategies.DEFAULT_ROLE_ONLY
        );
        assertThat(updatedClusterState.blocks().getIndexBlockWithId("test", INDEX_READ_ONLY_BLOCK.id()), is(INDEX_READ_ONLY_BLOCK));
        assertThat(updatedClusterState.routingTable().index("test"), is(notNullValue()));

        Metadata metadata = updatedClusterState.metadata();
        IndexAbstraction alias = metadata.getIndicesLookup().get("alias1");
        assertNotNull(alias);
        assertThat(alias.getType(), equalTo(IndexAbstraction.Type.ALIAS));
        Index index = metadata.index("test").getIndex();
        assertThat(alias.getIndices(), contains(index));
        assertThat(metadata.aliasedIndices("alias1"), contains(index));
    }

    public void testClusterStateCreateIndexWithMetadataTransaction() {
        ClusterState currentClusterState = ClusterState.builder(ClusterState.EMPTY_STATE)
            .metadata(
                Metadata.builder()
                    .put(
                        IndexMetadata.builder("my-index")
                            .settings(settings(Version.CURRENT).put(SETTING_READ_ONLY, true))
                            .numberOfShards(1)
                            .numberOfReplicas(0)
                    )
            )
            .build();

        IndexMetadata newIndexMetadata = IndexMetadata.builder("test")
            .settings(settings(Version.CURRENT).put(SETTING_READ_ONLY, true))
            .numberOfShards(1)
            .numberOfReplicas(0)
            .putAlias(AliasMetadata.builder("alias1").writeIndex(true).build())
            .build();

        // adds alias from new index to existing index
        BiConsumer<Metadata.Builder, IndexMetadata> metadataTransformer = (builder, indexMetadata) -> {
            AliasMetadata newAlias = indexMetadata.getAliases().values().iterator().next();
            IndexMetadata myIndex = builder.get("my-index");
            builder.put(IndexMetadata.builder(myIndex).putAlias(AliasMetadata.builder(newAlias.getAlias()).build()));
        };

        ClusterState updatedClusterState = clusterStateCreateIndex(
            currentClusterState,
            Set.of(INDEX_READ_ONLY_BLOCK),
            newIndexMetadata,
            metadataTransformer,
            TestShardRoutingRoleStrategies.DEFAULT_ROLE_ONLY
        );
        assertTrue(updatedClusterState.metadata().findAllAliases(new String[] { "my-index" }).containsKey("my-index"));
        assertNotNull(updatedClusterState.metadata().findAllAliases(new String[] { "my-index" }).get("my-index"));
        assertNotNull(
            updatedClusterState.metadata().findAllAliases(new String[] { "my-index" }).get("my-index").get(0).alias(),
            equalTo("alias1")
        );
    }

    public void testParseMappingsWithTypedTemplateAndTypelessIndexMapping() throws Exception {
        IndexTemplateMetadata templateMetadata = addMatchingTemplate(builder -> {
            try {
                builder.putMapping("type", "{\"type\": {}}");
            } catch (IOException e) {
                ExceptionsHelper.reThrowIfNotNull(e);
            }
        });

        Map<String, Object> mappings = parseV1Mappings("{\"_doc\":{}}", List.of(templateMetadata.mappings()), xContentRegistry());
        assertThat(mappings, Matchers.hasKey(MapperService.SINGLE_MAPPING_NAME));
    }

    public void testParseMappingsWithTypedTemplate() throws Exception {
        IndexTemplateMetadata templateMetadata = addMatchingTemplate(builder -> {
            try {
                builder.putMapping("type", """
                    {"type":{"properties":{"field":{"type":"keyword"}}}}
                    """);
            } catch (IOException e) {
                ExceptionsHelper.reThrowIfNotNull(e);
            }
        });
        Map<String, Object> mappings = parseV1Mappings("", List.of(templateMetadata.mappings()), xContentRegistry());
        assertThat(mappings, Matchers.hasKey(MapperService.SINGLE_MAPPING_NAME));
    }

    public void testParseMappingsWithTypelessTemplate() throws Exception {
        IndexTemplateMetadata templateMetadata = addMatchingTemplate(builder -> {
            try {
                builder.putMapping(MapperService.SINGLE_MAPPING_NAME, "{\"_doc\": {}}");
            } catch (IOException e) {
                ExceptionsHelper.reThrowIfNotNull(e);
            }
        });
        Map<String, Object> mappings = parseV1Mappings("", List.of(templateMetadata.mappings()), xContentRegistry());
        assertThat(mappings, Matchers.hasKey(MapperService.SINGLE_MAPPING_NAME));
    }

    public void testBuildIndexMetadata() {
        IndexMetadata sourceIndexMetadata = IndexMetadata.builder("parent")
            .settings(Settings.builder().put("index.version.created", Version.CURRENT).build())
            .numberOfShards(1)
            .numberOfReplicas(0)
            .primaryTerm(0, 3L)
            .build();

        Settings indexSettings = indexSettings(Version.CURRENT, 1, 0).build();
        List<AliasMetadata> aliases = List.of(AliasMetadata.builder("alias1").build());
        IndexMetadata indexMetadata = buildIndexMetadata("test", aliases, () -> null, indexSettings, 4, sourceIndexMetadata, false);

        assertThat(indexMetadata.getAliases().size(), is(1));
        assertThat(indexMetadata.getAliases().keySet().iterator().next(), is("alias1"));
        assertThat("The source index primary term must be used", indexMetadata.primaryTerm(0), is(3L));
    }

    public void testGetIndexNumberOfRoutingShardsWithNullSourceIndex() {
        Settings indexSettings = Settings.builder()
            .put("index.version.created", Version.CURRENT)
            .put(INDEX_NUMBER_OF_SHARDS_SETTING.getKey(), 3)
            .build();
        int targetRoutingNumberOfShards = getIndexNumberOfRoutingShards(indexSettings, null);
        assertThat(
            "When the target routing number of shards is not specified the expected value is the configured number of shards "
                + "multiplied by 2 at most ten times (ie. 3 * 2^8)",
            targetRoutingNumberOfShards,
            is(768)
        );
    }

    public void testGetIndexNumberOfRoutingShardsWhenExplicitlyConfigured() {
        Settings indexSettings = Settings.builder()
            .put(INDEX_NUMBER_OF_ROUTING_SHARDS_SETTING.getKey(), 9)
            .put(INDEX_NUMBER_OF_SHARDS_SETTING.getKey(), 3)
            .build();
        int targetRoutingNumberOfShards = getIndexNumberOfRoutingShards(indexSettings, null);
        assertThat(targetRoutingNumberOfShards, is(9));
    }

    public void testGetIndexNumberOfRoutingShardsNullVsNotDefined() {
        int numberOfPrimaryShards = randomIntBetween(1, 16);
        Settings indexSettings = settings(Version.CURRENT).put(INDEX_NUMBER_OF_ROUTING_SHARDS_SETTING.getKey(), (String) null)
            .put(INDEX_NUMBER_OF_SHARDS_SETTING.getKey(), numberOfPrimaryShards)
            .build();
        int targetRoutingNumberOfShardsWithNull = getIndexNumberOfRoutingShards(indexSettings, null);
        indexSettings = settings(Version.CURRENT).put(INDEX_NUMBER_OF_SHARDS_SETTING.getKey(), numberOfPrimaryShards).build();
        int targetRoutingNumberOfShardsWithNotDefined = getIndexNumberOfRoutingShards(indexSettings, null);
        assertThat(targetRoutingNumberOfShardsWithNull, is(targetRoutingNumberOfShardsWithNotDefined));
    }

    public void testGetIndexNumberOfRoutingShardsNull() {
        Settings indexSettings = settings(Version.CURRENT).put(INDEX_NUMBER_OF_ROUTING_SHARDS_SETTING.getKey(), (String) null)
            .put(INDEX_NUMBER_OF_SHARDS_SETTING.getKey(), 2)
            .build();
        int targetRoutingNumberOfShardsWithNull = getIndexNumberOfRoutingShards(indexSettings, null);
        assertThat(targetRoutingNumberOfShardsWithNull, is(1024));
    }

    public void testGetIndexNumberOfRoutingShardsYieldsSourceNumberOfShards() {
        Settings indexSettings = Settings.builder().put(INDEX_NUMBER_OF_SHARDS_SETTING.getKey(), 3).build();

        IndexMetadata sourceIndexMetadata = IndexMetadata.builder("parent")
            .settings(Settings.builder().put("index.version.created", Version.CURRENT).build())
            .numberOfShards(6)
            .numberOfReplicas(0)
            .build();

        int targetRoutingNumberOfShards = getIndexNumberOfRoutingShards(indexSettings, sourceIndexMetadata);
        assertThat(targetRoutingNumberOfShards, is(6));
    }

    private Optional<String> aggregatedTierPreference(Settings settings, boolean isDataStream) {
        Settings templateSettings = Settings.EMPTY;
        request.settings(Settings.EMPTY);

        if (randomBoolean()) {
            templateSettings = settings;
        } else {
            request.settings(settings);
        }

        if (isDataStream) {
            request.dataStreamName(randomAlphaOfLength(10));
        } else {
            request.dataStreamName(null);
        }
        Settings aggregatedIndexSettings = aggregateIndexSettings(
            ClusterState.EMPTY_STATE,
            request,
            templateSettings,
            null,
            null,
            Settings.EMPTY,
            IndexScopedSettings.DEFAULT_SCOPED_SETTINGS,
            randomShardLimitService(),
            Set.of(new DataTier.DefaultHotAllocationSettingProvider())
        );

        if (aggregatedIndexSettings.keySet().contains(DataTier.TIER_PREFERENCE)) {
            return Optional.of(aggregatedIndexSettings.get(DataTier.TIER_PREFERENCE));
        } else {
            return Optional.empty();
        }
    }

    public void testEnforceDefaultTierPreference() {
        Settings settings;
        Optional<String> tier;

        // empty settings gets the appropriate tier
        settings = Settings.EMPTY;
        tier = aggregatedTierPreference(settings, false);
        assertEquals(DataTier.DATA_CONTENT, tier.get());

        settings = Settings.EMPTY;
        tier = aggregatedTierPreference(settings, true);
        assertEquals(DataTier.DATA_HOT, tier.get());

        // an explicit tier is respected
        settings = Settings.builder().put(DataTier.TIER_PREFERENCE, DataTier.DATA_COLD).build();
        tier = aggregatedTierPreference(settings, randomBoolean());
        assertEquals(DataTier.DATA_COLD, tier.get());

        // any of the INDEX_ROUTING_.*_GROUP_PREFIX settings still result in a default
        String includeRoutingSetting = randomFrom(
            IndexMetadata.INDEX_ROUTING_REQUIRE_GROUP_PREFIX,
            IndexMetadata.INDEX_ROUTING_EXCLUDE_GROUP_PREFIX,
            IndexMetadata.INDEX_ROUTING_INCLUDE_GROUP_PREFIX
        ) + "." + randomAlphaOfLength(10);
        settings = Settings.builder().put(includeRoutingSetting, randomAlphaOfLength(10)).build();
        tier = aggregatedTierPreference(settings, false);
        assertEquals(DataTier.DATA_CONTENT, tier.get());

        // an explicit null gets the appropriate tier
        settings = Settings.builder().putNull(DataTier.TIER_PREFERENCE).build();
        tier = aggregatedTierPreference(settings, false);
        assertEquals(DataTier.DATA_CONTENT, tier.get());

        settings = Settings.builder().putNull(DataTier.TIER_PREFERENCE).build();
        tier = aggregatedTierPreference(settings, true);
        assertEquals(DataTier.DATA_HOT, tier.get());
    }

    public void testRejectWithSoftDeletesDisabled() {
        final IllegalArgumentException error = expectThrows(IllegalArgumentException.class, () -> {
            request = new CreateIndexClusterStateUpdateRequest("create index", "test", "test");
            request.settings(Settings.builder().put(INDEX_SOFT_DELETES_SETTING.getKey(), false).build());
            aggregateIndexSettings(
                ClusterState.EMPTY_STATE,
                request,
                Settings.EMPTY,
                null,
                null,
                Settings.EMPTY,
                IndexScopedSettings.DEFAULT_SCOPED_SETTINGS,
                randomShardLimitService(),
                Collections.emptySet()
            );
        });
        assertThat(
            error.getMessage(),
            equalTo(
                "Creating indices with soft-deletes disabled is no longer supported. "
                    + "Please do not specify a value for setting [index.soft_deletes.enabled]."
            )
        );
    }

    public void testRejectTranslogRetentionSettings() {
        request = new CreateIndexClusterStateUpdateRequest("create index", "test", "test");
        final Settings.Builder settings = Settings.builder();
        if (randomBoolean()) {
            settings.put(IndexSettings.INDEX_TRANSLOG_RETENTION_AGE_SETTING.getKey(), TimeValue.timeValueMillis(between(1, 120)));
        } else {
            settings.put(IndexSettings.INDEX_TRANSLOG_RETENTION_SIZE_SETTING.getKey(), between(1, 128) + "mb");
        }
        if (randomBoolean()) {
            settings.put(SETTING_VERSION_CREATED, VersionUtils.randomVersionBetween(random(), Version.V_8_0_0, Version.CURRENT));
        }
        request.settings(settings.build());
        IllegalArgumentException error = expectThrows(
            IllegalArgumentException.class,
            () -> aggregateIndexSettings(
                ClusterState.EMPTY_STATE,
                request,
                Settings.EMPTY,
                null,
                null,
                Settings.EMPTY,
                IndexScopedSettings.DEFAULT_SCOPED_SETTINGS,
                randomShardLimitService(),
                Collections.emptySet()
            )
        );
        assertThat(
            error.getMessage(),
            equalTo(
                "Translog retention settings [index.translog.retention.age] "
                    + "and [index.translog.retention.size] are no longer supported. Please do not specify values for these settings"
            )
        );
    }

    public void testDeprecateTranslogRetentionSettings() {
        request = new CreateIndexClusterStateUpdateRequest("create index", "test", "test");
        final Settings.Builder settings = Settings.builder();
        if (randomBoolean()) {
            settings.put(IndexSettings.INDEX_TRANSLOG_RETENTION_AGE_SETTING.getKey(), TimeValue.timeValueMillis(between(1, 120)));
        } else {
            settings.put(IndexSettings.INDEX_TRANSLOG_RETENTION_SIZE_SETTING.getKey(), between(1, 128) + "mb");
        }
        settings.put(SETTING_VERSION_CREATED, IndexVersionUtils.randomPreviousCompatibleVersion(random(), IndexVersion.V_8_0_0));
        request.settings(settings.build());
        aggregateIndexSettings(
            ClusterState.EMPTY_STATE,
            request,
            Settings.EMPTY,
            null,
            null,
            Settings.EMPTY,
            IndexScopedSettings.DEFAULT_SCOPED_SETTINGS,
            randomShardLimitService(),
            Collections.emptySet()
        );
        assertWarnings(
            "Translog retention settings [index.translog.retention.age] "
                + "and [index.translog.retention.size] are deprecated and effectively ignored. They will be removed in a future version."
        );
    }

    public void testDeprecateSimpleFS() {
        request = new CreateIndexClusterStateUpdateRequest("create index", "test", "test");
        final Settings.Builder settings = Settings.builder();
        settings.put(IndexModule.INDEX_STORE_TYPE_SETTING.getKey(), IndexModule.Type.SIMPLEFS.getSettingsKey());

        request.settings(settings.build());
        aggregateIndexSettings(
            ClusterState.EMPTY_STATE,
            request,
            Settings.EMPTY,
            null,
            null,
            Settings.EMPTY,
            IndexScopedSettings.DEFAULT_SCOPED_SETTINGS,
            randomShardLimitService(),
            Collections.emptySet()
        );
        assertWarnings(
            "[simplefs] is deprecated and will be removed in 8.0. Use [niofs] or other file systems instead. "
                + "Elasticsearch 7.15 or later uses [niofs] for the [simplefs] store type "
                + "as it offers superior or equivalent performance to [simplefs]."
        );
    }

    private IndexTemplateMetadata addMatchingTemplate(Consumer<IndexTemplateMetadata.Builder> configurator) {
        IndexTemplateMetadata.Builder builder = templateMetadataBuilder("template1", "te*");
        configurator.accept(builder);
        return builder.build();
    }

    private IndexTemplateMetadata.Builder templateMetadataBuilder(String name, String pattern) {
        return IndexTemplateMetadata.builder(name).patterns(Collections.singletonList(pattern));
    }

    private CompressedXContent createMapping(String fieldName, String fieldType) {
        try {
            final String mapping = Strings.toString(
                XContentFactory.jsonBuilder()
                    .startObject()
                    .startObject("_doc")
                    .startObject("properties")
                    .startObject(fieldName)
                    .field("type", fieldType)
                    .endObject()
                    .endObject()
                    .endObject()
                    .endObject()
            );

            return new CompressedXContent(mapping);
        } catch (IOException e) {
            throw ExceptionsHelper.convertToRuntime(e);
        }
    }

    private ShardLimitValidator randomShardLimitService() {
        return createTestShardLimitService(randomIntBetween(10, 10000));
    }

    private void withTemporaryClusterService(BiConsumer<ClusterService, ThreadPool> consumer) {
        ThreadPool threadPool = new TestThreadPool(getTestName());
        try {
            final ClusterService clusterService = ClusterServiceUtils.createClusterService(threadPool);
            consumer.accept(clusterService, threadPool);
        } finally {
            threadPool.shutdown();
        }
    }
}
