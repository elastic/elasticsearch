/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */

package org.elasticsearch.xpack.autoscaling.storage;

import org.elasticsearch.action.admin.indices.stats.IndicesStatsResponse;
import org.elasticsearch.action.index.IndexRequestBuilder;
import org.elasticsearch.cluster.ClusterInfoService;
import org.elasticsearch.cluster.InternalClusterInfoService;
import org.elasticsearch.cluster.metadata.IndexMetadata;
import org.elasticsearch.cluster.node.DiscoveryNode;
import org.elasticsearch.cluster.node.DiscoveryNodeRole;
import org.elasticsearch.common.settings.ClusterSettings;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.test.ESIntegTestCase;
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
        putAutoscalingPolicy(policyName);

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
        long enoughSpace = used + WATERMARK_BYTES + 1;

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

    /**
     * Verify that the list of roles includes all data roles to ensure we consider adding future data roles.
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
                DiscoveryNode.getPossibleRoles().stream().filter(DiscoveryNodeRole::canContainData).sorted().collect(Collectors.toList())
            )
        );
    }

    public void setTotalSpace(String dataNodeName, long totalSpace) {
        getTestFileStore(dataNodeName).setTotalSpace(totalSpace);
        final ClusterInfoService clusterInfoService = internalCluster().getCurrentMasterNodeInstance(ClusterInfoService.class);
        ((InternalClusterInfoService) clusterInfoService).refresh();
    }

    public GetAutoscalingCapacityAction.Response capacity() {
        GetAutoscalingCapacityAction.Request request = new GetAutoscalingCapacityAction.Request();
        GetAutoscalingCapacityAction.Response response = client().execute(GetAutoscalingCapacityAction.INSTANCE, request).actionGet();
        return response;
    }

    private void putAutoscalingPolicy(String policyName) {
        final PutAutoscalingPolicyAction.Request request = new PutAutoscalingPolicyAction.Request(
            policyName,
            new TreeSet<>(Set.of("data")),
            new TreeMap<>(Map.of("reactive_storage", Settings.EMPTY))
        );
        assertAcked(client().execute(PutAutoscalingPolicyAction.INSTANCE, request).actionGet());
    }
}
