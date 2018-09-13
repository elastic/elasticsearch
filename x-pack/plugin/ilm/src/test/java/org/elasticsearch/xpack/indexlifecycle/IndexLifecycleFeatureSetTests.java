/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */

package org.elasticsearch.xpack.indexlifecycle;

import org.elasticsearch.action.support.PlainActionFuture;
import org.elasticsearch.cluster.ClusterName;
import org.elasticsearch.cluster.ClusterState;
import org.elasticsearch.cluster.metadata.MetaData;
import org.elasticsearch.cluster.service.ClusterService;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.license.XPackLicenseState;
import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.xpack.core.indexlifecycle.IndexLifecycleFeatureSetUsage;
import org.elasticsearch.xpack.core.indexlifecycle.IndexLifecycleFeatureSetUsage.PolicyStats;
import org.elasticsearch.xpack.core.indexlifecycle.IndexLifecycleMetadata;
import org.elasticsearch.xpack.core.indexlifecycle.LifecyclePolicy;
import org.elasticsearch.xpack.core.indexlifecycle.LifecyclePolicyMetadata;
import org.elasticsearch.xpack.core.indexlifecycle.OperationMode;
import org.elasticsearch.xpack.core.indexlifecycle.Phase;
import org.junit.Before;
import org.mockito.Mockito;

import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.function.Function;
import java.util.stream.Collectors;

import static org.hamcrest.Matchers.contains;
import static org.hamcrest.Matchers.equalTo;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

public class IndexLifecycleFeatureSetTests extends ESTestCase {

    private XPackLicenseState licenseState;
    private ClusterService clusterService;

    @Before
    public void init() throws Exception {
        licenseState = mock(XPackLicenseState.class);
        clusterService = mock(ClusterService.class);
    }

    public void testAvailable() {
        IndexLifecycleFeatureSet featureSet = new IndexLifecycleFeatureSet(Settings.EMPTY, licenseState, clusterService);

        when(licenseState.isIndexLifecycleAllowed()).thenReturn(false);
        assertThat(featureSet.available(), equalTo(false));

        when(licenseState.isIndexLifecycleAllowed()).thenReturn(true);
        assertThat(featureSet.available(), equalTo(true));

        featureSet = new IndexLifecycleFeatureSet(Settings.EMPTY, null, clusterService);
        assertThat(featureSet.available(), equalTo(false));
    }

    public void testEnabled() {
        Settings.Builder settings = Settings.builder().put("xpack.ilm.enabled", false);
        IndexLifecycleFeatureSet featureSet = new IndexLifecycleFeatureSet(settings.build(), licenseState, clusterService);
        assertThat(featureSet.enabled(), equalTo(false));

        settings = Settings.builder().put("xpack.ilm.enabled", true);
        featureSet = new IndexLifecycleFeatureSet(settings.build(), licenseState, clusterService);
        assertThat(featureSet.enabled(), equalTo(true));
    }

    public void testName() {
        IndexLifecycleFeatureSet featureSet = new IndexLifecycleFeatureSet(Settings.EMPTY, licenseState, clusterService);
        assertThat(featureSet.name(), equalTo("ilm"));
    }

    public void testNativeCodeInfo() {
        IndexLifecycleFeatureSet featureSet = new IndexLifecycleFeatureSet(Settings.EMPTY, licenseState, clusterService);
        assertNull(featureSet.nativeCodeInfo());
    }

    @SuppressWarnings("unchecked")
    public void testUsageStats() throws Exception {
        List<LifecyclePolicy> policies = new ArrayList<>();
        LifecyclePolicy policy1 = new LifecyclePolicy(randomAlphaOfLength(10), Collections.emptyMap());
        policies.add(policy1);
        PolicyStats policy1Stats = new PolicyStats(Collections.emptyMap());

        Map<String, Phase> phases1 = new HashMap<>();
        LifecyclePolicy policy2 = new LifecyclePolicy(randomAlphaOfLength(10), phases1);
        policies.add(policy2);
        PolicyStats policy2Stats = new PolicyStats(Collections.emptyMap());

        LifecyclePolicy policy3 = new LifecyclePolicy(randomAlphaOfLength(10), Collections.emptyMap());
        policies.add(policy3);
        PolicyStats policy3Stats = new PolicyStats(Collections.emptyMap());

        ClusterState clusterState = buildClusterState(policies);
        Mockito.when(clusterService.state()).thenReturn(clusterState);

        PlainActionFuture<IndexLifecycleFeatureSet.Usage> future = new PlainActionFuture<>();
        IndexLifecycleFeatureSet ilmFeatureSet = new IndexLifecycleFeatureSet(Settings.EMPTY, licenseState, clusterService);
        ilmFeatureSet.usage(future);
        IndexLifecycleFeatureSetUsage ilmUsage = (IndexLifecycleFeatureSetUsage) future.get();
        assertThat(ilmUsage.enabled(), equalTo(ilmFeatureSet.enabled()));
        assertThat(ilmUsage.available(), equalTo(ilmFeatureSet.available()));

        List<PolicyStats> policyStatsList = ilmUsage.getPolicyStats();
        assertThat(policyStatsList.size(), equalTo(policies.size()));
        assertThat(policyStatsList, contains(equalTo(policy1Stats), equalTo(policy2Stats), equalTo(policy3Stats)));

    }

    private ClusterState buildClusterState(List<LifecyclePolicy> lifecyclePolicies) {
        Map<String, LifecyclePolicyMetadata> lifecyclePolicyMetadatasMap = lifecyclePolicies.stream()
                .map(p -> new LifecyclePolicyMetadata(p, Collections.emptyMap(), 1, 0L))
                .collect(Collectors.toMap(LifecyclePolicyMetadata::getName, Function.identity()));
        IndexLifecycleMetadata indexLifecycleMetadata = new IndexLifecycleMetadata(lifecyclePolicyMetadatasMap, OperationMode.RUNNING);

        MetaData metadata = MetaData.builder().putCustom(IndexLifecycleMetadata.TYPE, indexLifecycleMetadata).build();
        return ClusterState.builder(new ClusterName("my_cluster")).metaData(metadata).build();
    }
}
