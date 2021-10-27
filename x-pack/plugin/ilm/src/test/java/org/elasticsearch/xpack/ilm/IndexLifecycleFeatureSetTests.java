/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.ilm;

import org.elasticsearch.Version;
import org.elasticsearch.action.support.PlainActionFuture;
import org.elasticsearch.cluster.ClusterName;
import org.elasticsearch.cluster.ClusterState;
import org.elasticsearch.cluster.metadata.IndexMetadata;
import org.elasticsearch.cluster.metadata.Metadata;
import org.elasticsearch.cluster.service.ClusterService;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.xpack.core.ilm.IndexLifecycleFeatureSetUsage;
import org.elasticsearch.xpack.core.ilm.IndexLifecycleFeatureSetUsage.PolicyStats;
import org.elasticsearch.xpack.core.ilm.IndexLifecycleMetadata;
import org.elasticsearch.xpack.core.ilm.LifecyclePolicy;
import org.elasticsearch.xpack.core.ilm.LifecyclePolicyMetadata;
import org.elasticsearch.xpack.core.ilm.LifecycleSettings;
import org.elasticsearch.xpack.core.ilm.OperationMode;
import org.elasticsearch.xpack.core.ilm.Phase;
import org.junit.Before;
import org.mockito.Mockito;

import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.function.Function;
import java.util.stream.Collectors;

import static org.hamcrest.Matchers.equalTo;
import static org.mockito.Mockito.mock;

public class IndexLifecycleFeatureSetTests extends ESTestCase {

    private ClusterService clusterService;

    @Before
    public void init() throws Exception {
        clusterService = mock(ClusterService.class);
    }

    public void testAvailable() {
        IndexLifecycleFeatureSet featureSet = new IndexLifecycleFeatureSet(clusterService);
        assertThat(featureSet.available(), equalTo(true));
    }

    public void testName() {
        IndexLifecycleFeatureSet featureSet = new IndexLifecycleFeatureSet(clusterService);
        assertThat(featureSet.name(), equalTo("ilm"));
    }

    public void testNativeCodeInfo() {
        IndexLifecycleFeatureSet featureSet = new IndexLifecycleFeatureSet(clusterService);
        assertNull(featureSet.nativeCodeInfo());
    }

    public void testUsageStats() throws Exception {
        Map<String, String> indexPolicies = new HashMap<>();
        List<LifecyclePolicy> policies = new ArrayList<>();
        String policy1Name = randomAlphaOfLength(10);
        String policy2Name = randomAlphaOfLength(10);
        String policy3Name = randomAlphaOfLength(10);
        indexPolicies.put("index_1", policy1Name);
        indexPolicies.put("index_2", policy1Name);
        indexPolicies.put("index_3", policy1Name);
        indexPolicies.put("index_4", policy1Name);
        indexPolicies.put("index_5", policy3Name);
        LifecyclePolicy policy1 = new LifecyclePolicy(policy1Name, Collections.emptyMap());
        policies.add(policy1);
        PolicyStats policy1Stats = new PolicyStats(Collections.emptyMap(), 4);

        Map<String, Phase> phases1 = new HashMap<>();
        LifecyclePolicy policy2 = new LifecyclePolicy(policy2Name, phases1);
        policies.add(policy2);
        PolicyStats policy2Stats = new PolicyStats(Collections.emptyMap(), 0);

        LifecyclePolicy policy3 = new LifecyclePolicy(policy3Name, Collections.emptyMap());
        policies.add(policy3);
        PolicyStats policy3Stats = new PolicyStats(Collections.emptyMap(), 1);

        ClusterState clusterState = buildClusterState(policies, indexPolicies);
        Mockito.when(clusterService.state()).thenReturn(clusterState);

        PlainActionFuture<IndexLifecycleFeatureSet.Usage> future = new PlainActionFuture<>();
        IndexLifecycleFeatureSet ilmFeatureSet = new IndexLifecycleFeatureSet(clusterService);
        ilmFeatureSet.usage(future);
        IndexLifecycleFeatureSetUsage ilmUsage = (IndexLifecycleFeatureSetUsage) future.get();
        assertThat(ilmUsage.enabled(), equalTo(true));
        assertThat(ilmUsage.available(), equalTo(true));

        List<PolicyStats> policyStatsList = ilmUsage.getPolicyStats();
        assertThat(policyStatsList.size(), equalTo(policies.size()));
        assertTrue(policyStatsList.contains(policy1Stats));
        assertTrue(policyStatsList.contains(policy2Stats));
        assertTrue(policyStatsList.contains(policy3Stats));

    }

    private ClusterState buildClusterState(List<LifecyclePolicy> lifecyclePolicies, Map<String, String> indexPolicies) {
        Map<String, LifecyclePolicyMetadata> lifecyclePolicyMetadatasMap = lifecyclePolicies.stream()
            .map(p -> new LifecyclePolicyMetadata(p, Collections.emptyMap(), 1, 0L))
            .collect(Collectors.toMap(LifecyclePolicyMetadata::getName, Function.identity()));
        IndexLifecycleMetadata indexLifecycleMetadata = new IndexLifecycleMetadata(lifecyclePolicyMetadatasMap, OperationMode.RUNNING);

        Metadata.Builder metadata = Metadata.builder().putCustom(IndexLifecycleMetadata.TYPE, indexLifecycleMetadata);
        indexPolicies.forEach((indexName, policyName) -> {
            Settings indexSettings = Settings.builder()
                .put(LifecycleSettings.LIFECYCLE_NAME, policyName)
                .put(IndexMetadata.SETTING_NUMBER_OF_SHARDS, 1)
                .put(IndexMetadata.SETTING_NUMBER_OF_REPLICAS, 0)
                .put(IndexMetadata.SETTING_VERSION_CREATED, Version.CURRENT)
                .build();
            IndexMetadata.Builder indexMetadata = IndexMetadata.builder(indexName).settings(indexSettings);
            metadata.put(indexMetadata);
        });

        return ClusterState.builder(new ClusterName("my_cluster")).metadata(metadata).build();
    }
}
