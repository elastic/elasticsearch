/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.autoscaling.storage;

import org.elasticsearch.action.admin.indices.settings.put.UpdateSettingsRequest;
import org.elasticsearch.action.admin.indices.shrink.ResizeType;
import org.elasticsearch.action.admin.indices.stats.IndicesStatsResponse;
import org.elasticsearch.action.index.IndexRequestBuilder;
import org.elasticsearch.action.support.ActiveShardCount;
import org.elasticsearch.cluster.ClusterInfo;
import org.elasticsearch.cluster.ClusterInfoService;
import org.elasticsearch.cluster.metadata.IndexMetadata;
import org.elasticsearch.cluster.node.DiscoveryNodeRole;
import org.elasticsearch.cluster.routing.allocation.DataTier;
import org.elasticsearch.common.settings.ClusterSettings;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.core.Tuple;
import org.elasticsearch.node.Node;
import org.elasticsearch.test.ESIntegTestCase;
import org.elasticsearch.test.NodeRoles;
import org.elasticsearch.transport.TransportService;
import org.elasticsearch.xpack.autoscaling.action.GetAutoscalingCapacityAction;
import org.elasticsearch.xpack.autoscaling.action.PutAutoscalingPolicyAction;
import org.hamcrest.Matchers;

import java.util.Arrays;
import java.util.Locale;
import java.util.Map;
import java.util.Set;
import java.util.TreeMap;
import java.util.TreeSet;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

import static org.elasticsearch.index.store.Store.INDEX_STORE_STATS_REFRESH_INTERVAL_SETTING;
import static org.elasticsearch.test.hamcrest.ElasticsearchAssertions.assertAcked;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.is;
import static org.hamcrest.Matchers.notNullValue;

@ESIntegTestCase.ClusterScope(scope = ESIntegTestCase.Scope.TEST, numDataNodes = 0)
public class ReactiveStorageIT extends AutoscalingStorageIntegTestCase {

    public void testScaleUp() throws InterruptedException {
        internalCluster().startMasterOnlyNode();
        final String dataNodeName = internalCluster().startDataOnlyNode();
        final String policyName = "test";
        putAutoscalingPolicy(policyName, "data");

        final String indexName = randomAlphaOfLength(10).toLowerCase(Locale.ROOT);
        createIndex(
            indexName,
            Settings.builder()
                .put(IndexMetadata.SETTING_NUMBER_OF_REPLICAS, 0)
                .put(IndexMetadata.SETTING_NUMBER_OF_SHARDS, 6)
                .put(INDEX_STORE_STATS_REFRESH_INTERVAL_SETTING.getKey(), "0ms")
                .build()
        );
        indexRandom(
            true,
            IntStream.range(1, 100)
                .mapToObj(i -> client().prepareIndex(indexName).setSource("field", randomAlphaOfLength(50)))
                .toArray(IndexRequestBuilder[]::new)
        );
        forceMerge();
        refresh();

        // just check it does not throw when not refreshed.
        capacity();

        IndicesStatsResponse stats = client().admin().indices().prepareStats(indexName).clear().setStore(true).get();
        long used = stats.getTotal().getStore().getSizeInBytes();
        long minShardSize = Arrays.stream(stats.getShards()).mapToLong(s -> s.getStats().getStore().sizeInBytes()).min().orElseThrow();
        long maxShardSize = Arrays.stream(stats.getShards()).mapToLong(s -> s.getStats().getStore().sizeInBytes()).max().orElseThrow();
        long enoughSpace = used + HIGH_WATERMARK_BYTES + 1;

        setTotalSpace(dataNodeName, enoughSpace);
        GetAutoscalingCapacityAction.Response response = capacity();
        assertThat(response.results().keySet(), equalTo(Set.of(policyName)));
        assertThat(response.results().get(policyName).currentCapacity().total().storage().getBytes(), equalTo(enoughSpace));
        assertThat(response.results().get(policyName).requiredCapacity().total().storage().getBytes(), equalTo(enoughSpace));
        assertThat(
            response.results().get(policyName).requiredCapacity().node().storage().getBytes(),
            equalTo(maxShardSize + ReactiveStorageDeciderService.NODE_DISK_OVERHEAD + LOW_WATERMARK_BYTES)
        );

        setTotalSpace(dataNodeName, enoughSpace - 2);
        response = capacity();
        assertThat(response.results().keySet(), equalTo(Set.of(policyName)));
        assertThat(response.results().get(policyName).currentCapacity().total().storage().getBytes(), equalTo(enoughSpace - 2));
        assertThat(
            response.results().get(policyName).requiredCapacity().total().storage().getBytes(),
            Matchers.greaterThan(enoughSpace - 2)
        );
        assertThat(
            response.results().get(policyName).requiredCapacity().total().storage().getBytes(),
            Matchers.lessThanOrEqualTo(enoughSpace + minShardSize)
        );
        assertThat(
            response.results().get(policyName).requiredCapacity().node().storage().getBytes(),
            equalTo(maxShardSize + ReactiveStorageDeciderService.NODE_DISK_OVERHEAD + LOW_WATERMARK_BYTES)
        );
    }

    public void testScaleFromEmptyWarmMove() throws Exception {
        testScaleFromEmptyWarm(true);
    }

    public void testScaleFromEmptyWarmUnassigned() throws Exception {
        testScaleFromEmptyWarm(false);
    }

    private void testScaleFromEmptyWarm(boolean allocatable) throws Exception {
        internalCluster().startMasterOnlyNode();
        internalCluster().startNode(NodeRoles.onlyRole(DiscoveryNodeRole.DATA_HOT_NODE_ROLE));
        putAutoscalingPolicy("hot", DataTier.DATA_HOT);
        putAutoscalingPolicy("warm", DataTier.DATA_WARM);

        final String indexName = randomAlphaOfLength(10).toLowerCase(Locale.ROOT);
        assertAcked(
            prepareCreate(indexName).setSettings(
                Settings.builder()
                    .put(IndexMetadata.SETTING_NUMBER_OF_REPLICAS, 0)
                    .put(IndexMetadata.SETTING_NUMBER_OF_SHARDS, 6)
                    .put(INDEX_STORE_STATS_REFRESH_INTERVAL_SETTING.getKey(), "0ms")
                    .put(DataTier.TIER_PREFERENCE, allocatable ? "data_hot" : "data_content")
                    .build()
            ).setWaitForActiveShards(allocatable ? ActiveShardCount.DEFAULT : ActiveShardCount.NONE)
        );
        if (allocatable) {
            refresh();
        }
        assertThat(capacity().results().get("warm").requiredCapacity().total().storage().getBytes(), equalTo(0L));
        assertThat(capacity().results().get("warm").requiredCapacity().node().storage().getBytes(), equalTo(0L));

        assertAcked(
            client().admin()
                .indices()
                .updateSettings(
                    new UpdateSettingsRequest(indexName).settings(Settings.builder().put(DataTier.TIER_PREFERENCE, "data_warm,data_hot"))
                )
                .actionGet()
        );
        if (allocatable == false) {
            refresh();
        }

        assertThat(capacity().results().get("warm").requiredCapacity().total().storage().getBytes(), Matchers.greaterThan(0L));
        assertThat(
            capacity().results().get("warm").requiredCapacity().node().storage().getBytes(),
            Matchers.greaterThan(ReactiveStorageDeciderService.NODE_DISK_OVERHEAD)
        );

    }

    public void testScaleFromEmptyLegacy() {
        internalCluster().startMasterOnlyNode();
        internalCluster().startNode(
            NodeRoles.onlyRole(
                Settings.builder().put(Node.NODE_ATTRIBUTES.getKey() + "data_tier", "hot").build(),
                DiscoveryNodeRole.DATA_HOT_NODE_ROLE
            )
        );

        putAutoscalingPolicy("hot", DataTier.DATA_HOT);
        putAutoscalingPolicy("warm", DataTier.DATA_WARM);
        putAutoscalingPolicy("cold", DataTier.DATA_COLD);

        // add an index using `_id` allocation to check that it does not trigger spinning up the tier.
        assertAcked(
            prepareCreate(randomAlphaOfLength(10).toLowerCase(Locale.ROOT)).setSettings(
                Settings.builder()
                    // more than 0 replica provokes the same shard decider to say no.
                    .put(IndexMetadata.SETTING_NUMBER_OF_REPLICAS, between(0, 5))
                    .put(IndexMetadata.SETTING_NUMBER_OF_SHARDS, 6)
                    .put(INDEX_STORE_STATS_REFRESH_INTERVAL_SETTING.getKey(), "0ms")
                    .put(IndexMetadata.INDEX_ROUTING_INCLUDE_GROUP_SETTING.getKey() + "_id", randomAlphaOfLength(5))
                    .build()
            ).setWaitForActiveShards(ActiveShardCount.NONE)
        );

        final String indexName = randomAlphaOfLength(10).toLowerCase(Locale.ROOT);
        assertAcked(
            prepareCreate(indexName).setSettings(
                Settings.builder()
                    // more than 0 replica provokes the same shard decider to say no.
                    .put(IndexMetadata.SETTING_NUMBER_OF_REPLICAS, between(0, 5))
                    .put(IndexMetadata.SETTING_NUMBER_OF_SHARDS, 6)
                    .put(INDEX_STORE_STATS_REFRESH_INTERVAL_SETTING.getKey(), "0ms")
                    .put(IndexMetadata.INDEX_ROUTING_INCLUDE_GROUP_SETTING.getKey() + "data_tier", "hot")
                    .build()
            ).setWaitForActiveShards(ActiveShardCount.NONE)
        );

        // the tier preference will have defaulted to data_content, set it back to null
        updateIndexSettings(indexName, Settings.builder().putNull(DataTier.TIER_PREFERENCE));

        refresh(indexName);
        assertThat(capacity().results().get("warm").requiredCapacity().total().storage().getBytes(), equalTo(0L));
        assertThat(capacity().results().get("warm").requiredCapacity().node().storage().getBytes(), equalTo(0L));
        assertThat(capacity().results().get("cold").requiredCapacity().total().storage().getBytes(), equalTo(0L));
        assertThat(capacity().results().get("cold").requiredCapacity().node().storage().getBytes(), equalTo(0L));

        assertAcked(
            client().admin()
                .indices()
                .updateSettings(
                    new UpdateSettingsRequest(indexName).settings(
                        Settings.builder().put(IndexMetadata.INDEX_ROUTING_INCLUDE_GROUP_SETTING.getKey() + "data_tier", "warm")
                    )
                )
                .actionGet()
        );

        assertThat(capacity().results().get("warm").requiredCapacity().total().storage().getBytes(), Matchers.greaterThan(0L));
        assertThat(
            capacity().results().get("warm").requiredCapacity().node().storage().getBytes(),
            Matchers.greaterThan(ReactiveStorageDeciderService.NODE_DISK_OVERHEAD)
        );
        // this is not desirable, but one of the caveats of not using data tiers in the ILM policy.
        assertThat(capacity().results().get("cold").requiredCapacity().total().storage().getBytes(), Matchers.greaterThan(0L));
        assertThat(
            capacity().results().get("cold").requiredCapacity().node().storage().getBytes(),
            Matchers.greaterThan(ReactiveStorageDeciderService.NODE_DISK_OVERHEAD)
        );
    }

    @AwaitsFix(bugUrl = "https://github.com/elastic/elasticsearch/issues/88842")
    public void testScaleWhileShrinking() throws Exception {
        internalCluster().startMasterOnlyNode();
        final String dataNode1Name = internalCluster().startDataOnlyNode();
        final String dataNode2Name = internalCluster().startDataOnlyNode();

        final String dataNode1Id = internalCluster().getInstance(TransportService.class, dataNode1Name).getLocalNode().getId();
        final String dataNode2Id = internalCluster().getInstance(TransportService.class, dataNode2Name).getLocalNode().getId();
        final String policyName = "test";
        putAutoscalingPolicy(policyName, "data");

        final String indexName = randomAlphaOfLength(10).toLowerCase(Locale.ROOT);
        createIndex(
            indexName,
            Settings.builder()
                .put(IndexMetadata.SETTING_NUMBER_OF_REPLICAS, 0)
                .put(IndexMetadata.SETTING_NUMBER_OF_SHARDS, 6)
                .put(INDEX_STORE_STATS_REFRESH_INTERVAL_SETTING.getKey(), "0ms")
                .build()
        );
        indexRandom(
            true,
            IntStream.range(1, 100)
                .mapToObj(i -> client().prepareIndex(indexName).setSource("field", randomAlphaOfLength(50)))
                .toArray(IndexRequestBuilder[]::new)
        );
        forceMerge();
        refresh();

        IndicesStatsResponse stats = client().admin().indices().prepareStats(indexName).clear().setStore(true).get();
        long used = stats.getTotal().getStore().getSizeInBytes();
        long maxShardSize = Arrays.stream(stats.getShards()).mapToLong(s -> s.getStats().getStore().sizeInBytes()).max().orElseThrow();

        Map<String, Long> byNode = Arrays.stream(stats.getShards())
            .collect(
                Collectors.groupingBy(
                    s -> s.getShardRouting().currentNodeId(),
                    Collectors.summingLong(s -> s.getStats().getStore().getSizeInBytes())
                )
            );

        long enoughSpace1 = byNode.get(dataNode1Id).longValue() + HIGH_WATERMARK_BYTES + 1;
        long enoughSpace2 = byNode.get(dataNode2Id).longValue() + HIGH_WATERMARK_BYTES + 1;
        long enoughSpace = enoughSpace1 + enoughSpace2;

        setTotalSpace(dataNode1Name, enoughSpace1);
        setTotalSpace(dataNode2Name, enoughSpace2);

        GetAutoscalingCapacityAction.Response response = capacity();
        assertThat(response.results().keySet(), equalTo(Set.of(policyName)));
        assertThat(response.results().get(policyName).currentCapacity().total().storage().getBytes(), equalTo(enoughSpace));
        assertThat(response.results().get(policyName).requiredCapacity().total().storage().getBytes(), equalTo(enoughSpace));
        assertThat(
            response.results().get(policyName).requiredCapacity().node().storage().getBytes(),
            equalTo(maxShardSize + LOW_WATERMARK_BYTES + ReactiveStorageDeciderService.NODE_DISK_OVERHEAD)
        );

        Tuple<String, String> filter = switch (between(0, 2)) {
            case 0 -> Tuple.tuple("_id", dataNode1Id);
            case 1 -> Tuple.tuple("_name", dataNode1Name);
            case 2 -> Tuple.tuple("name", dataNode1Name);
            default -> throw new IllegalArgumentException();
        };

        String filterKey = randomFrom(IndexMetadata.INDEX_ROUTING_INCLUDE_GROUP_SETTING, IndexMetadata.INDEX_ROUTING_REQUIRE_GROUP_SETTING)
            .getKey() + filter.v1();

        assertAcked(
            client().admin()
                .indices()
                .updateSettings(
                    new UpdateSettingsRequest(indexName).settings(
                        Settings.builder().put(filterKey, filter.v2()).put("index.blocks.write", true)
                    )
                )
                .actionGet()
        );

        long shrinkSpace = used + LOW_WATERMARK_BYTES;

        response = capacity();
        assertThat(response.results().keySet(), equalTo(Set.of(policyName)));
        assertThat(response.results().get(policyName).currentCapacity().total().storage().getBytes(), equalTo(enoughSpace));
        assertThat(response.results().get(policyName).requiredCapacity().total().storage().getBytes(), equalTo(enoughSpace));
        assertThat(
            response.results().get(policyName).requiredCapacity().node().storage().getBytes(),
            equalTo(shrinkSpace + ReactiveStorageDeciderService.NODE_DISK_OVERHEAD)
        );

        long enoughSpaceForColocation = used + LOW_WATERMARK_BYTES;
        setTotalSpace(dataNode1Name, enoughSpaceForColocation);
        setTotalSpace(dataNode2Name, enoughSpaceForColocation);
        assertAcked(client().admin().cluster().prepareReroute());
        waitForRelocation();

        // Ensure that the relocated shard index files are removed from the data 2 node,
        // this is done asynchronously, therefore we might need to wait a bit until the files
        // are removed. This is necessary, otherwise the shrunk index won't fit in any node
        // and the autoscaling decider ends up requesting more disk space.
        assertBusy(() -> {
            refreshClusterInfo();
            final ClusterInfo clusterInfo = getClusterInfo();
            final long freeBytes = clusterInfo.getNodeMostAvailableDiskUsages().get(dataNode2Id).getFreeBytes();
            assertThat(freeBytes, is(equalTo(enoughSpaceForColocation)));
        });

        String shrinkName = "shrink-" + indexName;
        assertAcked(
            client().admin()
                .indices()
                .prepareResizeIndex(indexName, shrinkName)
                .setSettings(
                    Settings.builder()
                        .put(IndexMetadata.SETTING_NUMBER_OF_SHARDS, 1)
                        .put(IndexMetadata.SETTING_NUMBER_OF_REPLICAS, 0)
                        .build()
                )
                .setWaitForActiveShards(ActiveShardCount.NONE)
                .get()
        );

        // * 2 since worst case is no hard links, see DiskThresholdDecider.getExpectedShardSize.
        long requiredSpaceForShrink = used * 2 + LOW_WATERMARK_BYTES;

        response = capacity();
        assertThat(response.results().keySet(), equalTo(Set.of(policyName)));
        assertThat(
            response.results().get(policyName).currentCapacity().total().storage().getBytes(),
            equalTo(enoughSpaceForColocation * 2)
        );
        // test that even when the shard cannot allocate due to disk space, we do not request a "total" scale up, only a node-level.
        assertThat(
            response.results().get(policyName).requiredCapacity().total().storage().getBytes(),
            equalTo(enoughSpaceForColocation * 2)
        );
        assertThat(
            response.results().get(policyName).requiredCapacity().node().storage().getBytes(),
            equalTo(requiredSpaceForShrink + ReactiveStorageDeciderService.NODE_DISK_OVERHEAD)
        );

        assertThat(client().admin().cluster().prepareHealth(shrinkName).get().getUnassignedShards(), equalTo(1));

        // test that the required amount is enough.
        // Adjust the amount since autoscaling calculates a node size to stay below low watermark though the shard can be
        // allocated to a node as long as the node is below low watermark and allocating the shard does not exceed high watermark.
        long tooLittleSpaceForShrink = requiredSpaceForShrink - Math.min(LOW_WATERMARK_BYTES - HIGH_WATERMARK_BYTES, used) - 1;
        assert tooLittleSpaceForShrink <= requiredSpaceForShrink;
        setTotalSpace(dataNode1Name, tooLittleSpaceForShrink);
        assertAcked(client().admin().cluster().prepareReroute());
        assertThat(client().admin().cluster().prepareHealth(shrinkName).get().getUnassignedShards(), equalTo(1));
        setTotalSpace(dataNode1Name, tooLittleSpaceForShrink + 1);
        assertAcked(client().admin().cluster().prepareReroute());
        ensureGreen();

        client().admin().indices().prepareDelete(indexName).get();
        response = capacity();
        assertThat(
            response.results().get(policyName).requiredCapacity().total().storage(),
            equalTo(response.results().get(policyName).currentCapacity().total().storage())
        );
    }

    public void testScaleDuringSplitOrClone() throws Exception {
        internalCluster().startMasterOnlyNode();
        final String dataNode1Name = internalCluster().startDataOnlyNode();

        final String id1 = internalCluster().getInstance(TransportService.class, dataNode1Name).getLocalNode().getId();
        final String policyName = "test";
        putAutoscalingPolicy(policyName, "data");

        final String indexName = randomAlphaOfLength(10).toLowerCase(Locale.ROOT);
        createIndex(
            indexName,
            Settings.builder()
                .put(IndexMetadata.SETTING_NUMBER_OF_REPLICAS, 0)
                .put(IndexMetadata.SETTING_NUMBER_OF_SHARDS, 1)
                .put(INDEX_STORE_STATS_REFRESH_INTERVAL_SETTING.getKey(), "0ms")
                .build()
        );
        indexRandom(
            true,
            IntStream.range(1, 100)
                .mapToObj(i -> client().prepareIndex(indexName).setSource("field", randomAlphaOfLength(50)))
                .toArray(IndexRequestBuilder[]::new)
        );
        forceMerge();
        refresh();

        IndicesStatsResponse stats = client().admin().indices().prepareStats(indexName).clear().setStore(true).get();
        long used = stats.getTotal().getStore().getSizeInBytes();

        long enoughSpace = used + HIGH_WATERMARK_BYTES + 1;

        final String dataNode2Name = internalCluster().startDataOnlyNode();
        setTotalSpace(dataNode1Name, enoughSpace);
        setTotalSpace(dataNode2Name, enoughSpace);

        // It might take a while until the autoscaling polls the node information of dataNode2 and
        // provides a complete autoscaling capacity response
        assertBusy(() -> {
            GetAutoscalingCapacityAction.Response response = capacity();
            assertThat(response.results().keySet(), equalTo(Set.of(policyName)));
            assertThat(response.results().get(policyName).requiredCapacity(), is(notNullValue()));
        });

        // validate initial state looks good
        GetAutoscalingCapacityAction.Response response = capacity();
        assertThat(response.results().keySet(), equalTo(Set.of(policyName)));
        assertThat(response.results().get(policyName).currentCapacity().total().storage().getBytes(), equalTo(enoughSpace * 2));
        assertThat(response.results().get(policyName).requiredCapacity().total().storage().getBytes(), equalTo(enoughSpace * 2));
        assertThat(
            response.results().get(policyName).requiredCapacity().node().storage().getBytes(),
            equalTo(used + LOW_WATERMARK_BYTES + ReactiveStorageDeciderService.NODE_DISK_OVERHEAD)
        );

        assertAcked(
            client().admin()
                .indices()
                .updateSettings(new UpdateSettingsRequest(indexName).settings(Settings.builder().put("index.blocks.write", true)))
                .actionGet()
        );

        ResizeType resizeType = randomFrom(ResizeType.CLONE, ResizeType.SPLIT);
        String cloneName = "clone-" + indexName;
        int resizedShardCount = resizeType == ResizeType.CLONE ? 1 : between(2, 10);
        assertAcked(
            client().admin()
                .indices()
                .prepareResizeIndex(indexName, cloneName)
                .setSettings(
                    Settings.builder()
                        .put(IndexMetadata.SETTING_NUMBER_OF_SHARDS, resizedShardCount)
                        .put(IndexMetadata.SETTING_NUMBER_OF_REPLICAS, 0)
                        .build()
                )
                .setWaitForActiveShards(ActiveShardCount.NONE)
                .setResizeType(resizeType)
                .get()
        );

        // * 2 since worst case is no hard links, see DiskThresholdDecider.getExpectedShardSize.
        long requiredSpaceForClone = used * 2 + LOW_WATERMARK_BYTES;

        response = capacity();
        assertThat(response.results().keySet(), equalTo(Set.of(policyName)));
        assertThat(response.results().get(policyName).currentCapacity().total().storage().getBytes(), equalTo(enoughSpace * 2));
        // test that even when the shard cannot allocate due to disk space, we do not request a "total" scale up, only a node-level.
        assertThat(response.results().get(policyName).requiredCapacity().total().storage().getBytes(), equalTo(enoughSpace * 2));
        assertThat(
            response.results().get(policyName).requiredCapacity().node().storage().getBytes(),
            equalTo(requiredSpaceForClone + ReactiveStorageDeciderService.NODE_DISK_OVERHEAD)
        );

        assertThat(client().admin().cluster().prepareHealth(cloneName).get().getUnassignedShards(), equalTo(resizedShardCount));

        // test that the required amount is enough.
        // Adjust the amount since autoscaling calculates a node size to stay below low watermark though the shard can be
        // allocated to a node as long as the node is below low watermark and allocating the shard does not exceed high watermark.
        long tooLittleSpaceForClone = requiredSpaceForClone - Math.min(LOW_WATERMARK_BYTES - HIGH_WATERMARK_BYTES, used) - 1;
        assert tooLittleSpaceForClone <= requiredSpaceForClone;
        setTotalSpace(dataNode1Name, tooLittleSpaceForClone);
        assertAcked(client().admin().cluster().prepareReroute());
        assertThat(client().admin().cluster().prepareHealth(cloneName).get().getUnassignedShards(), equalTo(resizedShardCount));
        setTotalSpace(dataNode1Name, requiredSpaceForClone);
        assertAcked(client().admin().cluster().prepareReroute());
        ensureGreen();

        client().admin().indices().prepareDelete(indexName).get();
        response = capacity();
        assertThat(
            response.results().get(policyName).requiredCapacity().total().storage().getBytes(),
            equalTo(requiredSpaceForClone + enoughSpace)
        );
    }

    /**
     * Verify that the list of roles includes all data roles except frozen to ensure we consider adding future data roles.
     */
    public void testRoles() {
        // this has to be an integration test to ensure roles are available.
        internalCluster().startMasterOnlyNode();
        ReactiveStorageDeciderService service = new ReactiveStorageDeciderService(
            Settings.EMPTY,
            new ClusterSettings(Settings.EMPTY, ClusterSettings.BUILT_IN_CLUSTER_SETTINGS),
            null
        );
        assertThat(
            service.roles().stream().sorted().collect(Collectors.toList()),
            equalTo(
                DiscoveryNodeRole.roles()
                    .stream()
                    .filter(DiscoveryNodeRole::canContainData)
                    .filter(r -> r != DiscoveryNodeRole.DATA_FROZEN_NODE_ROLE)
                    .sorted()
                    .collect(Collectors.toList())
            )
        );
    }

    public void setTotalSpace(String dataNodeName, long totalSpace) {
        getTestFileStore(dataNodeName).setTotalSpace(totalSpace);
        refreshClusterInfo();
    }

    public GetAutoscalingCapacityAction.Response capacity() {
        GetAutoscalingCapacityAction.Request request = new GetAutoscalingCapacityAction.Request();
        GetAutoscalingCapacityAction.Response response = client().execute(GetAutoscalingCapacityAction.INSTANCE, request).actionGet();
        return response;
    }

    private void putAutoscalingPolicy(String policyName, String role) {
        final PutAutoscalingPolicyAction.Request request = new PutAutoscalingPolicyAction.Request(
            policyName,
            new TreeSet<>(Set.of(role)),
            new TreeMap<>(Map.of("reactive_storage", Settings.EMPTY))
        );
        assertAcked(client().execute(PutAutoscalingPolicyAction.INSTANCE, request).actionGet());
    }

    private ClusterInfo getClusterInfo() {
        final ClusterInfoService clusterInfoService = internalCluster().getInstance(
            ClusterInfoService.class,
            internalCluster().getMasterName()
        );
        return clusterInfoService.getClusterInfo();
    }
}
