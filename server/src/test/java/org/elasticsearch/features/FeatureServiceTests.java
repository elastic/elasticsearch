/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.features;

import org.elasticsearch.Version;
import org.elasticsearch.cluster.ClusterName;
import org.elasticsearch.cluster.ClusterState;
import org.elasticsearch.cluster.node.DiscoveryNodeUtils;
import org.elasticsearch.cluster.node.DiscoveryNodes;
import org.elasticsearch.index.IndexVersion;
import org.elasticsearch.index.IndexVersions;
import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.test.VersionUtils;

import java.util.List;
import java.util.Map;
import java.util.Set;

import static org.hamcrest.Matchers.containsInAnyOrder;
import static org.hamcrest.Matchers.containsString;

public class FeatureServiceTests extends ESTestCase {

    private static class TestFeatureSpecification implements FeatureSpecification {
        private final Set<NodeFeature> features;
        private final Map<NodeFeature, Version> historicalFeatures;

        private TestFeatureSpecification(Set<NodeFeature> features, Map<NodeFeature, Version> historicalFeatures) {
            this.features = features;
            this.historicalFeatures = historicalFeatures;
        }

        @Override
        public Set<NodeFeature> getFeatures() {
            return features;
        }

        @Override
        public Map<NodeFeature, Version> getHistoricalFeatures() {
            return historicalFeatures;
        }
    }

    public void testFailsDuplicateFeatures() {
        // these all need to be separate classes to trigger the exception
        FeatureSpecification fs1 = new TestFeatureSpecification(Set.of(new NodeFeature("f1")), Map.of()) {
        };
        FeatureSpecification fs2 = new TestFeatureSpecification(Set.of(new NodeFeature("f1")), Map.of()) {
        };
        FeatureSpecification hfs1 = new TestFeatureSpecification(Set.of(), Map.of(new NodeFeature("f1"), Version.V_8_11_0)) {
        };
        FeatureSpecification hfs2 = new TestFeatureSpecification(Set.of(), Map.of(new NodeFeature("f1"), Version.V_8_11_0)) {
        };

        assertThat(
            expectThrows(IllegalArgumentException.class, () -> new FeatureService(List.of(fs1, fs2))).getMessage(),
            containsString("Duplicate feature")
        );
        assertThat(
            expectThrows(IllegalArgumentException.class, () -> new FeatureService(List.of(hfs1, hfs2))).getMessage(),
            containsString("Duplicate feature")
        );
        assertThat(
            expectThrows(IllegalArgumentException.class, () -> new FeatureService(List.of(fs1, hfs1))).getMessage(),
            containsString("Duplicate feature")
        );
    }

    public void testFailsNonHistoricalVersion() {
        FeatureSpecification fs = new TestFeatureSpecification(
            Set.of(),
            Map.of(new NodeFeature("f1"), Version.fromId(FeatureService.CLUSTER_FEATURES_ADDED_VERSION.id + 1))
        );

        assertThat(
            expectThrows(IllegalArgumentException.class, () -> new FeatureService(List.of(fs))).getMessage(),
            containsString("not a historical version")
        );
    }

    public void testGetNodeFeaturesCombinesAllSpecs() {
        List<FeatureSpecification> specs = List.of(
            new TestFeatureSpecification(Set.of(new NodeFeature("f1"), new NodeFeature("f2")), Map.of()),
            new TestFeatureSpecification(Set.of(new NodeFeature("f3")), Map.of()),
            new TestFeatureSpecification(Set.of(new NodeFeature("f4"), new NodeFeature("f5")), Map.of()),
            new TestFeatureSpecification(Set.of(), Map.of())
        );

        FeatureService service = new FeatureService(specs);
        assertThat(service.getNodeFeatures().keySet(), containsInAnyOrder("f1", "f2", "f3", "f4", "f5"));
    }

    public void testStateHasFeatures() {
        List<FeatureSpecification> specs = List.of(
            new TestFeatureSpecification(Set.of(new NodeFeature("f1"), new NodeFeature("f2")), Map.of()),
            new TestFeatureSpecification(Set.of(new NodeFeature("f3")), Map.of()),
            new TestFeatureSpecification(Set.of(new NodeFeature("f4"), new NodeFeature("f5")), Map.of()),
            new TestFeatureSpecification(Set.of(), Map.of())
        );

        ClusterState state = ClusterState.builder(ClusterName.DEFAULT)
            .nodeFeatures(
                Map.of("node1", Set.of("f1", "f2", "nf1"), "node2", Set.of("f1", "f2", "nf2"), "node3", Set.of("f1", "f2", "nf1"))
            )
            .build();

        FeatureService service = new FeatureService(specs);
        assertTrue(service.clusterHasFeature(state, new NodeFeature("f1")));
        assertTrue(service.clusterHasFeature(state, new NodeFeature("f2")));
        assertFalse(service.clusterHasFeature(state, new NodeFeature("nf1")));
        assertFalse(service.clusterHasFeature(state, new NodeFeature("nf2")));
        assertFalse(service.clusterHasFeature(state, new NodeFeature("nf3")));
    }

    private static ClusterState stateWithMinVersion(Version version) {
        DiscoveryNodes.Builder nodes = DiscoveryNodes.builder();
        nodes.add(DiscoveryNodeUtils.builder("node").version(version, IndexVersions.ZERO, IndexVersion.current()).build());
        for (int n = randomInt(5); n >= 0; n--) {
            nodes.add(
                DiscoveryNodeUtils.builder("node" + n)
                    .version(
                        VersionUtils.randomVersionBetween(random(), version, Version.CURRENT),
                        IndexVersions.ZERO,
                        IndexVersion.current()
                    )
                    .build()
            );
        }

        return ClusterState.builder(ClusterName.DEFAULT).nodes(nodes).build();
    }

    public void testStateHasHistoricalFeatures() {
        NodeFeature v8_11_0 = new NodeFeature("hf_8.11.0");
        NodeFeature v8_10_0 = new NodeFeature("hf_8.10.0");
        NodeFeature v7_17_0 = new NodeFeature("hf_7.17.0");
        List<FeatureSpecification> specs = List.of(
            new TestFeatureSpecification(Set.of(), Map.of(v8_11_0, Version.V_8_11_0)),
            new TestFeatureSpecification(Set.of(), Map.of(v8_10_0, Version.V_8_10_0)),
            new TestFeatureSpecification(Set.of(), Map.of(v7_17_0, Version.V_7_17_0))
        );

        FeatureService service = new FeatureService(specs);
        assertTrue(service.clusterHasFeature(stateWithMinVersion(Version.V_8_11_0), v8_11_0));
        assertTrue(service.clusterHasFeature(stateWithMinVersion(Version.V_8_11_0), v8_10_0));
        assertTrue(service.clusterHasFeature(stateWithMinVersion(Version.V_8_11_0), v7_17_0));

        assertFalse(service.clusterHasFeature(stateWithMinVersion(Version.V_8_10_0), v8_11_0));
        assertTrue(service.clusterHasFeature(stateWithMinVersion(Version.V_8_10_0), v8_10_0));
        assertTrue(service.clusterHasFeature(stateWithMinVersion(Version.V_8_10_0), v7_17_0));

        assertFalse(service.clusterHasFeature(stateWithMinVersion(Version.V_7_17_0), v8_11_0));
        assertFalse(service.clusterHasFeature(stateWithMinVersion(Version.V_7_17_0), v8_10_0));
        assertTrue(service.clusterHasFeature(stateWithMinVersion(Version.V_7_17_0), v7_17_0));

        assertFalse(service.clusterHasFeature(stateWithMinVersion(Version.V_7_16_0), v8_11_0));
        assertFalse(service.clusterHasFeature(stateWithMinVersion(Version.V_7_16_0), v8_10_0));
        assertFalse(service.clusterHasFeature(stateWithMinVersion(Version.V_7_16_0), v7_17_0));
    }
}
