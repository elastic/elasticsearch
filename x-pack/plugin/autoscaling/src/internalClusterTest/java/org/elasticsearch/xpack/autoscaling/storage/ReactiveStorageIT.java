/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.autoscaling.storage;

import org.elasticsearch.action.admin.indices.settings.put.UpdateSettingsRequest;
import org.elasticsearch.action.admin.indices.stats.IndicesStatsResponse;
import org.elasticsearch.action.index.IndexRequestBuilder;
import org.elasticsearch.action.support.ActiveShardCount;
import org.elasticsearch.cluster.ClusterInfoService;
import org.elasticsearch.cluster.ClusterInfoServiceUtils;
import org.elasticsearch.cluster.InternalClusterInfoService;
import org.elasticsearch.cluster.metadata.IndexMetadata;
import org.elasticsearch.cluster.node.DiscoveryNodeRole;
import org.elasticsearch.cluster.routing.allocation.DataTier;
import org.elasticsearch.common.settings.ClusterSettings;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.node.Node;
import org.elasticsearch.test.ESIntegTestCase;
import org.elasticsearch.test.NodeRoles;
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
        assertThat(response.results().keySet(), Matchers.equalTo(Set.of(policyName)));
        assertThat(response.results().get(policyName).currentCapacity().total().storage().getBytes(), Matchers.equalTo(enoughSpace));
        assertThat(response.results().get(policyName).requiredCapacity().total().storage().getBytes(), Matchers.equalTo(enoughSpace));
        assertThat(response.results().get(policyName).requiredCapacity().node().storage().getBytes(), Matchers.equalTo(maxShardSize));

        setTotalSpace(dataNodeName, enoughSpace - 2);
        response = capacity();
        assertThat(response.results().keySet(), Matchers.equalTo(Set.of(policyName)));
        assertThat(response.results().get(policyName).currentCapacity().total().storage().getBytes(), Matchers.equalTo(enoughSpace - 2));
        assertThat(
            response.results().get(policyName).requiredCapacity().total().storage().getBytes(),
            Matchers.greaterThan(enoughSpace - 2)
        );
        assertThat(
            response.results().get(policyName).requiredCapacity().total().storage().getBytes(),
            Matchers.lessThanOrEqualTo(enoughSpace + minShardSize)
        );
        assertThat(response.results().get(policyName).requiredCapacity().node().storage().getBytes(), Matchers.equalTo(maxShardSize));
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
        assertThat(capacity().results().get("warm").requiredCapacity().total().storage().getBytes(), Matchers.equalTo(0L));

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
            )
        );

        // the tier preference will have defaulted to data_content, set it back to null
        updateIndexSettings(indexName, Settings.builder().putNull(DataTier.TIER_PREFERENCE));

        refresh(indexName);
        assertThat(capacity().results().get("warm").requiredCapacity().total().storage().getBytes(), Matchers.equalTo(0L));
        assertThat(capacity().results().get("cold").requiredCapacity().total().storage().getBytes(), Matchers.equalTo(0L));

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
        // this is not desirable, but one of the caveats of not using data tiers in the ILM policy.
        assertThat(capacity().results().get("cold").requiredCapacity().total().storage().getBytes(), Matchers.greaterThan(0L));
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
            Matchers.equalTo(
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
        final ClusterInfoService clusterInfoService = internalCluster().getCurrentMasterNodeInstance(ClusterInfoService.class);
        ClusterInfoServiceUtils.refresh(((InternalClusterInfoService) clusterInfoService));
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
}
