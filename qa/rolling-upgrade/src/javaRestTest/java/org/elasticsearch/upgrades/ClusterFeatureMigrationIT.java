/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.upgrades;

import com.carrotsearch.randomizedtesting.annotations.Name;

import org.elasticsearch.client.Request;
import org.elasticsearch.common.xcontent.support.XContentMapValues;
import org.elasticsearch.features.FeatureService;
import org.junit.BeforeClass;

import java.io.IOException;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;

import static org.hamcrest.Matchers.empty;
import static org.hamcrest.Matchers.hasSize;

public class ClusterFeatureMigrationIT extends ParameterizedRollingUpgradeTestCase {

    @BeforeClass
    public static void checkMigrationVersion() {
        assumeTrue(
            "This checks migrations from before cluster features were introduced",
            getOldClusterVersion().before(FeatureService.CLUSTER_FEATURES_ADDED_VERSION)
        );
    }

    public ClusterFeatureMigrationIT(@Name("upgradedNodes") int upgradedNodes) {
        super(upgradedNodes);
    }

    public void testClusterFeatureMigration() throws IOException {
        if (isUpgradedCluster()) {
            // check the nodes all have a feature in their cluster state (there should always be features_supported)
            var response = entityAsMap(adminClient().performRequest(new Request("GET", "/_cluster/state/nodes")));
            List<?> nodeFeatures = (List<?>) XContentMapValues.extractValue("nodes_features", response);
            assertThat(nodeFeatures, hasSize(adminClient().getNodes().size()));

            Map<String, List<?>> features = nodeFeatures.stream()
                .map(o -> (Map<?, ?>) o)
                .collect(Collectors.toMap(m -> (String) m.get("node_id"), m -> (List<?>) m.get("features")));

            Set<String> missing = features.entrySet()
                .stream()
                .filter(e -> e.getValue().contains(FeatureService.FEATURES_SUPPORTED.id()) == false)
                .map(Map.Entry::getKey)
                .collect(Collectors.toSet());
            assertThat(missing + " out of " + features.keySet() + " does not have the required feature", missing, empty());
        }
    }
}
