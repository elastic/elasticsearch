/*
 * Licensed to Elasticsearch under one or more contributor
 * license agreements. See the NOTICE file distributed with
 * this work for additional information regarding copyright
 * ownership. Elasticsearch licenses this file to you under
 * the Apache License, Version 2.0 (the "License"); you may
 * not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package org.elasticsearch.cluster.metadata;

import org.elasticsearch.ResourceAlreadyExistsException;
import org.elasticsearch.Version;
import org.elasticsearch.action.admin.indices.shrink.ResizeType;
import org.elasticsearch.cluster.ClusterName;
import org.elasticsearch.cluster.ClusterState;
import org.elasticsearch.cluster.ESAllocationTestCase;
import org.elasticsearch.cluster.EmptyClusterInfoService;
import org.elasticsearch.cluster.block.ClusterBlocks;
import org.elasticsearch.cluster.node.DiscoveryNode;
import org.elasticsearch.cluster.node.DiscoveryNodeRole;
import org.elasticsearch.cluster.node.DiscoveryNodes;
import org.elasticsearch.cluster.routing.RoutingTable;
import org.elasticsearch.cluster.routing.allocation.AllocationService;
import org.elasticsearch.cluster.routing.allocation.allocator.BalancedShardsAllocator;
import org.elasticsearch.cluster.routing.allocation.decider.AllocationDeciders;
import org.elasticsearch.cluster.routing.allocation.decider.MaxRetryAllocationDecider;
import org.elasticsearch.cluster.shards.ClusterShardLimitIT;
import org.elasticsearch.common.Strings;
import org.elasticsearch.common.ValidationException;
import org.elasticsearch.common.settings.IndexScopedSettings;
import org.elasticsearch.common.settings.Setting;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.index.IndexNotFoundException;
import org.elasticsearch.indices.InvalidIndexNameException;
import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.test.VersionUtils;
import org.elasticsearch.test.gateway.TestGatewayAllocator;

import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.Comparator;
import java.util.List;
import java.util.Locale;
import java.util.Set;
import java.util.function.Consumer;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import static java.util.Collections.emptyMap;
import static org.elasticsearch.cluster.metadata.IndexMetaData.SETTING_NUMBER_OF_REPLICAS;
import static org.elasticsearch.cluster.metadata.IndexMetaData.SETTING_NUMBER_OF_SHARDS;
import static org.elasticsearch.cluster.metadata.IndexMetaData.SETTING_VERSION_CREATED;
import static org.elasticsearch.cluster.shards.ClusterShardLimitIT.ShardCounts.forDataNodeCount;
import static org.elasticsearch.indices.IndicesServiceTests.createClusterForShardLimitTest;
import static org.hamcrest.Matchers.containsString;
import static org.hamcrest.Matchers.endsWith;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.hasToString;

public class MetaDataCreateIndexServiceTests extends ESTestCase {

    private ClusterState createClusterState(String name, int numShards, int numReplicas, Settings settings) {
        int numRoutingShards = settings.getAsInt(IndexMetaData.INDEX_NUMBER_OF_ROUTING_SHARDS_SETTING.getKey(), numShards);
        MetaData.Builder metaBuilder = MetaData.builder();
        IndexMetaData indexMetaData = IndexMetaData.builder(name).settings(settings(Version.CURRENT)
            .put(settings))
            .numberOfShards(numShards).numberOfReplicas(numReplicas)
            .setRoutingNumShards(numRoutingShards).build();
        metaBuilder.put(indexMetaData, false);
        MetaData metaData = metaBuilder.build();
        RoutingTable.Builder routingTableBuilder = RoutingTable.builder();
        routingTableBuilder.addAsNew(metaData.index(name));

        RoutingTable routingTable = routingTableBuilder.build();
        ClusterState clusterState = ClusterState.builder(org.elasticsearch.cluster.ClusterName.CLUSTER_NAME_SETTING
            .getDefault(Settings.EMPTY))
            .metaData(metaData).routingTable(routingTable).blocks(ClusterBlocks.builder().addBlocks(indexMetaData)).build();
        return clusterState;
    }

    public static boolean isShrinkable(int source, int target) {
        int x = source / target;
        assert source > target : source  + " <= " + target;
        return target * x == source;
    }

    public static boolean isSplitable(int source, int target) {
        int x = target / source;
        assert source < target : source  + " >= " + target;
        return source * x == target;
    }

    public void testValidateShrinkIndex() {
        int numShards = randomIntBetween(2, 42);
        ClusterState state = createClusterState("source", numShards, randomIntBetween(0, 10),
            Settings.builder().put("index.blocks.write", true).build());

        assertEquals("index [source] already exists",
            expectThrows(ResourceAlreadyExistsException.class, () ->
                MetaDataCreateIndexService.validateShrinkIndex(state, "target", Collections.emptySet(), "source", Settings.EMPTY)
            ).getMessage());

        assertEquals("no such index [no_such_index]",
            expectThrows(IndexNotFoundException.class, () ->
                MetaDataCreateIndexService.validateShrinkIndex(state, "no_such_index", Collections.emptySet(), "target", Settings.EMPTY)
            ).getMessage());

        Settings targetSettings = Settings.builder().put("index.number_of_shards", 1).build();
        assertEquals("can't shrink an index with only one shard",
            expectThrows(IllegalArgumentException.class, () -> MetaDataCreateIndexService.validateShrinkIndex(createClusterState("source",
                1, 0, Settings.builder().put("index.blocks.write", true).build()), "source",
                Collections.emptySet(), "target", targetSettings)).getMessage());

        assertEquals("the number of target shards [10] must be less that the number of source shards [5]",
            expectThrows(IllegalArgumentException.class, () -> MetaDataCreateIndexService.validateShrinkIndex(createClusterState("source",
                5, 0, Settings.builder().put("index.blocks.write", true).build()), "source",
                Collections.emptySet(), "target", Settings.builder().put("index.number_of_shards", 10).build())).getMessage());


        assertEquals("index source must be read-only to resize index. use \"index.blocks.write=true\"",
            expectThrows(IllegalStateException.class, () ->
                    MetaDataCreateIndexService.validateShrinkIndex(
                        createClusterState("source", randomIntBetween(2, 100), randomIntBetween(0, 10), Settings.EMPTY)
                        , "source", Collections.emptySet(), "target", targetSettings)
            ).getMessage());

        assertEquals("index source must have all shards allocated on the same node to shrink index",
            expectThrows(IllegalStateException.class, () ->
                MetaDataCreateIndexService.validateShrinkIndex(state, "source", Collections.emptySet(), "target", targetSettings)

            ).getMessage());
        assertEquals("the number of source shards [8] must be a multiple of [3]",
            expectThrows(IllegalArgumentException.class, () ->
                    MetaDataCreateIndexService.validateShrinkIndex(createClusterState("source", 8, randomIntBetween(0, 10),
                        Settings.builder().put("index.blocks.write", true).build()), "source", Collections.emptySet(), "target",
                        Settings.builder().put("index.number_of_shards", 3).build())
            ).getMessage());

        assertEquals("mappings are not allowed when resizing indices, all mappings are copied from the source index",
            expectThrows(IllegalArgumentException.class, () -> {
                MetaDataCreateIndexService.validateShrinkIndex(state, "source", Collections.singleton("foo"),
                    "target", targetSettings);
                }
            ).getMessage());

        // create one that won't fail
        ClusterState clusterState = ClusterState.builder(createClusterState("source", numShards, 0,
            Settings.builder().put("index.blocks.write", true).build())).nodes(DiscoveryNodes.builder().add(newNode("node1")))
            .build();
        AllocationService service = new AllocationService(new AllocationDeciders(
            Collections.singleton(new MaxRetryAllocationDecider())),
            new TestGatewayAllocator(), new BalancedShardsAllocator(Settings.EMPTY), EmptyClusterInfoService.INSTANCE);

        RoutingTable routingTable = service.reroute(clusterState, "reroute").routingTable();
        clusterState = ClusterState.builder(clusterState).routingTable(routingTable).build();
        // now we start the shard
        routingTable = ESAllocationTestCase.startInitializingShardsAndReroute(service, clusterState, "source").routingTable();
        clusterState = ClusterState.builder(clusterState).routingTable(routingTable).build();
        int targetShards;
        do {
            targetShards = randomIntBetween(1, numShards/2);
        } while (isShrinkable(numShards, targetShards) == false);
        MetaDataCreateIndexService.validateShrinkIndex(clusterState, "source", Collections.emptySet(), "target",
            Settings.builder().put("index.number_of_shards", targetShards).build());
    }

    public void testValidateSplitIndex() {
        int numShards = randomIntBetween(1, 42);
        Settings targetSettings = Settings.builder().put("index.number_of_shards", numShards * 2).build();
        ClusterState state = createClusterState("source", numShards, randomIntBetween(0, 10),
            Settings.builder().put("index.blocks.write", true).build());

        assertEquals("index [source] already exists",
            expectThrows(ResourceAlreadyExistsException.class, () ->
                MetaDataCreateIndexService.validateSplitIndex(state, "target", Collections.emptySet(), "source", targetSettings)
            ).getMessage());

        assertEquals("no such index [no_such_index]",
            expectThrows(IndexNotFoundException.class, () ->
                MetaDataCreateIndexService.validateSplitIndex(state, "no_such_index", Collections.emptySet(), "target", targetSettings)
            ).getMessage());

        assertEquals("the number of source shards [10] must be less that the number of target shards [5]",
            expectThrows(IllegalArgumentException.class, () -> MetaDataCreateIndexService.validateSplitIndex(createClusterState("source",
                10, 0, Settings.builder().put("index.blocks.write", true).build()), "source", Collections.emptySet(),
                "target", Settings.builder().put("index.number_of_shards", 5).build())
            ).getMessage());


        assertEquals("index source must be read-only to resize index. use \"index.blocks.write=true\"",
            expectThrows(IllegalStateException.class, () ->
                MetaDataCreateIndexService.validateSplitIndex(
                    createClusterState("source", randomIntBetween(2, 100), randomIntBetween(0, 10), Settings.EMPTY)
                    , "source", Collections.emptySet(), "target", targetSettings)
            ).getMessage());


        assertEquals("the number of source shards [3] must be a factor of [4]",
            expectThrows(IllegalArgumentException.class, () ->
                MetaDataCreateIndexService.validateSplitIndex(createClusterState("source", 3, randomIntBetween(0, 10),
                    Settings.builder().put("index.blocks.write", true).build()), "source", Collections.emptySet(), "target",
                    Settings.builder().put("index.number_of_shards", 4).build())
            ).getMessage());

        assertEquals("mappings are not allowed when resizing indices, all mappings are copied from the source index",
            expectThrows(IllegalArgumentException.class, () -> {
                    MetaDataCreateIndexService.validateSplitIndex(state, "source", Collections.singleton("foo"),
                        "target", targetSettings);
                }
            ).getMessage());

        int targetShards;
        do {
            targetShards = randomIntBetween(numShards+1, 100);
        } while (isSplitable(numShards, targetShards) == false);
        ClusterState clusterState = ClusterState.builder(createClusterState("source", numShards, 0,
            Settings.builder().put("index.blocks.write", true).put("index.number_of_routing_shards", targetShards).build()))
            .nodes(DiscoveryNodes.builder().add(newNode("node1"))).build();
        AllocationService service = new AllocationService(new AllocationDeciders(
            Collections.singleton(new MaxRetryAllocationDecider())),
            new TestGatewayAllocator(), new BalancedShardsAllocator(Settings.EMPTY), EmptyClusterInfoService.INSTANCE);

        RoutingTable routingTable = service.reroute(clusterState, "reroute").routingTable();
        clusterState = ClusterState.builder(clusterState).routingTable(routingTable).build();
        // now we start the shard
        routingTable = ESAllocationTestCase.startInitializingShardsAndReroute(service, clusterState, "source").routingTable();
        clusterState = ClusterState.builder(clusterState).routingTable(routingTable).build();

        MetaDataCreateIndexService.validateSplitIndex(clusterState, "source", Collections.emptySet(), "target",
            Settings.builder().put("index.number_of_shards", targetShards).build());
    }

    public void testPrepareResizeIndexSettings() {
        final List<Version> versions = Arrays.asList(VersionUtils.randomVersion(random()), VersionUtils.randomVersion(random()));
        versions.sort(Comparator.comparingLong(l -> l.id));
        final Version version = versions.get(0);
        final Version upgraded = versions.get(1);
        final Settings indexSettings =
                Settings.builder()
                        .put("index.version.created", version)
                        .put("index.version.upgraded", upgraded)
                        .put("index.similarity.default.type", "BM25")
                        .put("index.analysis.analyzer.default.tokenizer", "keyword")
                        .put("index.soft_deletes.enabled", "true")
                        .build();
        runPrepareResizeIndexSettingsTest(
                indexSettings,
                Settings.EMPTY,
                Collections.emptyList(),
                randomBoolean(),
                settings -> {
                    assertThat("similarity settings must be copied", settings.get("index.similarity.default.type"), equalTo("BM25"));
                    assertThat(
                            "analysis settings must be copied",
                            settings.get("index.analysis.analyzer.default.tokenizer"),
                            equalTo("keyword"));
                    assertThat(settings.get("index.routing.allocation.initial_recovery._id"), equalTo("node1"));
                    assertThat(settings.get("index.allocation.max_retries"), equalTo("1"));
                    assertThat(settings.getAsVersion("index.version.created", null), equalTo(version));
                    assertThat(settings.getAsVersion("index.version.upgraded", null), equalTo(upgraded));
                    assertThat(settings.get("index.soft_deletes.enabled"), equalTo("true"));
                });
    }

    public void testPrepareResizeIndexSettingsCopySettings() {
        final int maxMergeCount = randomIntBetween(1, 16);
        final int maxThreadCount = randomIntBetween(1, 16);
        final Setting<String> nonCopyableExistingIndexSetting =
                Setting.simpleString("index.non_copyable.existing", Setting.Property.IndexScope, Setting.Property.NotCopyableOnResize);
        final Setting<String> nonCopyableRequestIndexSetting =
                Setting.simpleString("index.non_copyable.request", Setting.Property.IndexScope, Setting.Property.NotCopyableOnResize);
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
                });
    }

    public void testPrepareResizeIndexSettingsAnalysisSettings() {
        // analysis settings from the request are not overwritten
        runPrepareResizeIndexSettingsTest(
                Settings.EMPTY,
                Settings.builder().put("index.analysis.analyzer.default.tokenizer", "whitespace").build(),
                Collections.emptyList(),
                randomBoolean(),
                settings ->
                    assertThat(
                            "analysis settings are not overwritten",
                            settings.get("index.analysis.analyzer.default.tokenizer"),
                            equalTo("whitespace"))
                );

    }

    public void testPrepareResizeIndexSettingsSimilaritySettings() {
        // similarity settings from the request are not overwritten
        runPrepareResizeIndexSettingsTest(
                Settings.EMPTY,
                Settings.builder().put("index.similarity.sim.type", "DFR").build(),
                Collections.emptyList(),
                randomBoolean(),
                settings ->
                        assertThat("similarity settings are not overwritten", settings.get("index.similarity.sim.type"), equalTo("DFR")));

    }

    public void testDoNotOverrideSoftDeletesSettingOnResize() {
        runPrepareResizeIndexSettingsTest(
            Settings.builder().put("index.soft_deletes.enabled", "false").build(),
            Settings.builder().put("index.soft_deletes.enabled", "true").build(),
            Collections.emptyList(),
            randomBoolean(),
            settings -> assertThat(settings.get("index.soft_deletes.enabled"), equalTo("true")));
    }

    private void runPrepareResizeIndexSettingsTest(
            final Settings sourceSettings,
            final Settings requestSettings,
            final Collection<Setting<?>> additionalIndexScopedSettings,
            final boolean copySettings,
            final Consumer<Settings> consumer) {
        final String indexName = randomAlphaOfLength(10);

        final Settings indexSettings = Settings.builder()
                .put("index.blocks.write", true)
                .put("index.routing.allocation.require._name", "node1")
                .put(sourceSettings)
                .build();

        final ClusterState initialClusterState =
                ClusterState
                        .builder(createClusterState(indexName, randomIntBetween(2, 10), 0, indexSettings))
                        .nodes(DiscoveryNodes.builder().add(newNode("node1")))
                        .build();

        final AllocationService service = new AllocationService(
                new AllocationDeciders(Collections.singleton(new MaxRetryAllocationDecider())),
                new TestGatewayAllocator(),
                new BalancedShardsAllocator(Settings.EMPTY),
                EmptyClusterInfoService.INSTANCE);

        final RoutingTable initialRoutingTable = service.reroute(initialClusterState, "reroute").routingTable();
        final ClusterState routingTableClusterState = ClusterState.builder(initialClusterState).routingTable(initialRoutingTable).build();

        // now we start the shard
        final RoutingTable routingTable
            = ESAllocationTestCase.startInitializingShardsAndReroute(service, routingTableClusterState, indexName).routingTable();
        final ClusterState clusterState = ClusterState.builder(routingTableClusterState).routingTable(routingTable).build();

        final Settings.Builder indexSettingsBuilder = Settings.builder().put("index.number_of_shards", 1).put(requestSettings);
        final Set<Setting<?>> settingsSet =
                Stream.concat(
                        IndexScopedSettings.BUILT_IN_INDEX_SETTINGS.stream(),
                        additionalIndexScopedSettings.stream())
                        .collect(Collectors.toSet());
        MetaDataCreateIndexService.prepareResizeIndexSettings(
                clusterState,
                Collections.emptySet(),
                indexSettingsBuilder,
                clusterState.metaData().index(indexName).getIndex(),
                "target",
                ResizeType.SHRINK,
                copySettings,
                new IndexScopedSettings(Settings.EMPTY, settingsSet));
        consumer.accept(indexSettingsBuilder.build());
    }

    private DiscoveryNode newNode(String nodeId) {
        return new DiscoveryNode(
                nodeId,
                buildNewFakeTransportAddress(),
                emptyMap(),
                Set.of(DiscoveryNodeRole.MASTER_ROLE, DiscoveryNodeRole.DATA_ROLE), Version.CURRENT);
    }

    public void testValidateIndexName() throws Exception {

        validateIndexName("index?name", "must not contain the following characters " + Strings.INVALID_FILENAME_CHARS);

        validateIndexName("index#name", "must not contain '#'");

        validateIndexName("_indexname", "must not start with '_', '-', or '+'");
        validateIndexName("-indexname", "must not start with '_', '-', or '+'");
        validateIndexName("+indexname", "must not start with '_', '-', or '+'");

        validateIndexName("INDEXNAME", "must be lowercase");

        validateIndexName("..", "must not be '.' or '..'");

        validateIndexName("foo:bar", "must not contain ':'");
    }

    private void validateIndexName(String indexName, String errorMessage) {
        InvalidIndexNameException e = expectThrows(InvalidIndexNameException.class,
            () -> MetaDataCreateIndexService.validateIndexName(indexName, ClusterState.builder(ClusterName.CLUSTER_NAME_SETTING
                .getDefault(Settings.EMPTY)).build()));
        assertThat(e.getMessage(), endsWith(errorMessage));
    }

    public void testCalculateNumRoutingShards() {
        assertEquals(1024, MetaDataCreateIndexService.calculateNumRoutingShards(1, Version.CURRENT));
        assertEquals(1024, MetaDataCreateIndexService.calculateNumRoutingShards(2, Version.CURRENT));
        assertEquals(768, MetaDataCreateIndexService.calculateNumRoutingShards(3, Version.CURRENT));
        assertEquals(576, MetaDataCreateIndexService.calculateNumRoutingShards(9, Version.CURRENT));
        assertEquals(1024, MetaDataCreateIndexService.calculateNumRoutingShards(512, Version.CURRENT));
        assertEquals(2048, MetaDataCreateIndexService.calculateNumRoutingShards(1024, Version.CURRENT));
        assertEquals(4096, MetaDataCreateIndexService.calculateNumRoutingShards(2048, Version.CURRENT));

        for (int i = 0; i < 1000; i++) {
            int randomNumShards = randomIntBetween(1, 10000);
            int numRoutingShards = MetaDataCreateIndexService.calculateNumRoutingShards(randomNumShards, Version.CURRENT);
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

    public void testShardLimit() {
        int nodesInCluster = randomIntBetween(2,100);
        ClusterShardLimitIT.ShardCounts counts = forDataNodeCount(nodesInCluster);
        Settings clusterSettings = Settings.builder()
            .put(MetaData.SETTING_CLUSTER_MAX_SHARDS_PER_NODE.getKey(), counts.getShardsPerNode())
            .build();
        ClusterState state = createClusterForShardLimitTest(nodesInCluster, counts.getFirstIndexShards(), counts.getFirstIndexReplicas(),
            clusterSettings);

        Settings indexSettings = Settings.builder()
            .put(SETTING_VERSION_CREATED, Version.CURRENT)
            .put(SETTING_NUMBER_OF_SHARDS, counts.getFailingIndexShards())
            .put(SETTING_NUMBER_OF_REPLICAS, counts.getFailingIndexReplicas())
            .build();

        final ValidationException e = expectThrows(
            ValidationException.class,
            () -> MetaDataCreateIndexService.checkShardLimit(indexSettings, state));
        int totalShards = counts.getFailingIndexShards() * (1 + counts.getFailingIndexReplicas());
        int currentShards = counts.getFirstIndexShards() * (1 + counts.getFirstIndexReplicas());
        int maxShards = counts.getShardsPerNode() * nodesInCluster;
        final String expectedMessage = String.format(
            Locale.ROOT,
            "this action would add [%d] total shards, but this cluster currently has [%d]/[%d] maximum shards open",
            totalShards,
            currentShards,
            maxShards);
        assertThat(e, hasToString(containsString(expectedMessage)));
    }

}
