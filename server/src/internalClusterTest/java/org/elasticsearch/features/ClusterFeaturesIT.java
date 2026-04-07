/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.features;

import org.elasticsearch.action.admin.cluster.state.ClusterStateRequest;
import org.elasticsearch.core.SuppressForbidden;
import org.elasticsearch.test.ESIntegTestCase;

import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;

import static org.hamcrest.Matchers.empty;
import static org.hamcrest.Matchers.hasKey;

public class ClusterFeaturesIT extends ESIntegTestCase {

    @SuppressForbidden(reason = "Directly checking node features in cluster state")
    public void testClusterHasFeatures() {
        internalCluster().startNodes(2);
        ensureGreen();

        FeatureService service = internalCluster().getCurrentMasterNodeInstance(FeatureService.class);

        assertThat(service.getNodeFeatures(), hasKey(FeatureService.TEST_FEATURES_ENABLED.id()));

        // check the nodes all have a feature in their cluster state (there should always be features_supported)
        var response = clusterAdmin().state(new ClusterStateRequest(TEST_REQUEST_TIMEOUT).clear().nodes(true)).actionGet();
        var features = response.getState().clusterFeatures().nodeFeatures();
        Set<String> missing = features.entrySet()
            .stream()
            .filter(e -> e.getValue().contains(FeatureService.TEST_FEATURES_ENABLED.id()) == false)
            .map(Map.Entry::getKey)
            .collect(Collectors.toSet());
        assertThat(missing + " out of " + features.keySet() + " does not have the required feature", missing, empty());

        // check that all nodes have the same features
        var featureList = List.copyOf(response.getState().clusterFeatures().nodeFeatures().values());
        assertEquals("Nodes do not have the same features", featureList.get(0), featureList.get(1));
    }
}
