/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.features;

import org.elasticsearch.Version;
import org.elasticsearch.cluster.ClusterState;
import org.elasticsearch.cluster.service.ClusterService;
import org.elasticsearch.test.ESIntegTestCase;
import org.junit.BeforeClass;

import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.Set;

import static org.hamcrest.Matchers.is;

public class NodeFeaturesIT extends ESIntegTestCase {

    private static NodeFeature FEATURE;
    private static NodeFeature HISTORICAL_FEATURE;

    @BeforeClass
    public static void featureSetup() {
        FEATURE = new NodeFeature("f1");
        HISTORICAL_FEATURE = new NodeFeature("hf1");
    }

    private static class TestFeatures implements FeatureSpecification {
        @Override
        public Set<NodeFeature> getFeatures() {
            return Set.of(FEATURE);
        }

        @Override
        public Map<NodeFeature, Version> getHistoricalFeatures() {
            return Map.of(HISTORICAL_FEATURE, Version.V_7_0_0);
        }
    }

    @Override
    protected Collection<? extends FeatureSpecification> nodeFeatureSpecifications(String nodeId) {
        return List.of(new TestFeatures());
    }

    public void testFeaturesAvailable() throws Exception {
        internalCluster().startNodes(2);
        ensureGreen();

        ClusterState state = internalCluster().getCurrentMasterNodeInstance(ClusterService.class).state();

        assertThat(state.getNodes().allNodesHaveFeature(FEATURE), is(true));
        assertThat(state.getNodes().allNodesHaveFeature(HISTORICAL_FEATURE), is(true));
    }
}
