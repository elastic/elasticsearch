/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */

package org.elasticsearch.xpack.ilm;

import org.elasticsearch.Version;
import org.elasticsearch.action.support.ActionFilters;
import org.elasticsearch.action.support.PlainActionFuture;
import org.elasticsearch.cluster.ClusterName;
import org.elasticsearch.cluster.ClusterState;
import org.elasticsearch.cluster.metadata.IndexMetadata;
import org.elasticsearch.cluster.metadata.Metadata;
import org.elasticsearch.cluster.service.ClusterService;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.license.XPackLicenseState;
import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.transport.TransportService;
import org.elasticsearch.xpack.core.action.XPackUsageFeatureResponse;
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
import static org.mockito.Mockito.when;

public class IndexLifecycleInfoTransportActionTests extends ESTestCase {

    private XPackLicenseState licenseState;
    private ClusterService clusterService;

    @Before
    public void init() throws Exception {
        licenseState = mock(XPackLicenseState.class);
        clusterService = mock(ClusterService.class);
    }

    public void testAvailable() {
        IndexLifecycleInfoTransportAction featureSet = new IndexLifecycleInfoTransportAction(
            mock(TransportService.class), mock(ActionFilters.class), licenseState);

        when(licenseState.isAllowed(XPackLicenseState.Feature.ILM)).thenReturn(false);
        assertThat(featureSet.available(), equalTo(false));

        when(licenseState.isAllowed(XPackLicenseState.Feature.ILM)).thenReturn(true);
        assertThat(featureSet.available(), equalTo(true));
    }

    public void testName() {
        IndexLifecycleInfoTransportAction featureSet = new IndexLifecycleInfoTransportAction(
            mock(TransportService.class), mock(ActionFilters.class), licenseState);
        assertThat(featureSet.name(), equalTo("ilm"));
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

        var usageAction = new IndexLifecycleUsageTransportAction(mock(TransportService.class), null, null,
            mock(ActionFilters.class), null, Settings.EMPTY, licenseState);
        PlainActionFuture<XPackUsageFeatureResponse> future = new PlainActionFuture<>();
        usageAction.masterOperation(null, null, clusterState, future);
        IndexLifecycleFeatureSetUsage ilmUsage = (IndexLifecycleFeatureSetUsage) future.get().getUsage();
        assertThat(ilmUsage.enabled(), equalTo(true));
        assertThat(ilmUsage.available(), equalTo(false));

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
            Settings indexSettings = Settings.builder().put(LifecycleSettings.LIFECYCLE_NAME, policyName)
                    .put(IndexMetadata.SETTING_NUMBER_OF_SHARDS, 1)
                    .put(IndexMetadata.SETTING_NUMBER_OF_REPLICAS, 0)
                    .put(IndexMetadata.SETTING_VERSION_CREATED, Version.CURRENT).build();
            IndexMetadata.Builder indexMetadata = IndexMetadata.builder(indexName).settings(indexSettings);
            metadata.put(indexMetadata);
        });

        return ClusterState.builder(new ClusterName("my_cluster")).metadata(metadata).build();
    }
}
