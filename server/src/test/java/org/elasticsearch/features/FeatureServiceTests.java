/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.features;

import org.elasticsearch.Version;
import org.elasticsearch.cluster.ClusterName;
import org.elasticsearch.cluster.ClusterState;
import org.elasticsearch.cluster.node.DiscoveryNodeUtils;
import org.elasticsearch.cluster.node.DiscoveryNodes;
import org.elasticsearch.cluster.node.VersionInformation;
import org.elasticsearch.index.IndexVersion;
import org.elasticsearch.index.IndexVersions;
import org.elasticsearch.test.ESTestCase;

import java.util.List;
import java.util.Map;
import java.util.Set;

import static org.hamcrest.Matchers.containsInAnyOrder;
import static org.hamcrest.Matchers.containsString;

public class FeatureServiceTests extends ESTestCase {

    private static class TestFeatureSpecification implements FeatureSpecification {
        private final Set<NodeFeature> features;

        private TestFeatureSpecification(Set<NodeFeature> features) {
            this.features = features;
        }

        @Override
        public Set<NodeFeature> getFeatures() {
            return features;
        }
    }

    public void testFailsDuplicateFeatures() {
        // these all need to be separate classes to trigger the exception
        FeatureSpecification fs1 = new TestFeatureSpecification(Set.of(new NodeFeature("f1"))) {
        };
        FeatureSpecification fs2 = new TestFeatureSpecification(Set.of(new NodeFeature("f1"))) {
        };

        assertThat(
            expectThrows(IllegalArgumentException.class, () -> new FeatureService(List.of(fs1, fs2))).getMessage(),
            containsString("Duplicate feature")
        );
    }

    public void testGetNodeFeaturesCombinesAllSpecs() {
        List<FeatureSpecification> specs = List.of(
            new TestFeatureSpecification(Set.of(new NodeFeature("f1"), new NodeFeature("f2"))),
            new TestFeatureSpecification(Set.of(new NodeFeature("f3"))),
            new TestFeatureSpecification(Set.of(new NodeFeature("f4"), new NodeFeature("f5"))),
            new TestFeatureSpecification(Set.of())
        );

        FeatureService service = new FeatureService(specs);
        assertThat(service.getNodeFeatures().keySet(), containsInAnyOrder("f1", "f2", "f3", "f4", "f5"));
    }

    public void testStateHasFeatures() {
        List<FeatureSpecification> specs = List.of(
            new TestFeatureSpecification(Set.of(new NodeFeature("f1"), new NodeFeature("f2"))),
            new TestFeatureSpecification(Set.of(new NodeFeature("f3"))),
            new TestFeatureSpecification(Set.of(new NodeFeature("f4"), new NodeFeature("f5"))),
            new TestFeatureSpecification(Set.of())
        );

        ClusterState state = ClusterState.builder(ClusterName.DEFAULT)
            .nodes(
                DiscoveryNodes.builder()
                    .add(DiscoveryNodeUtils.create("node1"))
                    .add(DiscoveryNodeUtils.create("node2"))
                    .add(DiscoveryNodeUtils.create("node3"))
            )
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

    private static Version nextMajor() {
        return Version.fromId((Version.CURRENT.major + 1) * 1_000_000 + 99);
    }

    public void testStateHasAssumedFeatures() {
        List<FeatureSpecification> specs = List.of(
            new TestFeatureSpecification(Set.of(new NodeFeature("f1"), new NodeFeature("f2"), new NodeFeature("af1", true)))
        );

        ClusterState state = ClusterState.builder(ClusterName.DEFAULT)
            .nodes(
                DiscoveryNodes.builder()
                    .add(DiscoveryNodeUtils.create("node1"))
                    .add(DiscoveryNodeUtils.create("node2"))
                    .add(
                        DiscoveryNodeUtils.builder("node3")
                            .version(new VersionInformation(nextMajor(), IndexVersions.MINIMUM_COMPATIBLE, IndexVersion.current()))
                            .build()
                    )
            )
            .nodeFeatures(Map.of("node1", Set.of("f1", "af1"), "node2", Set.of("f1", "f2", "af1"), "node3", Set.of("f1", "f2")))
            .build();

        FeatureService service = new FeatureService(specs);
        assertTrue(service.clusterHasFeature(state, new NodeFeature("f1")));
        assertFalse(service.clusterHasFeature(state, new NodeFeature("f2")));
        assertTrue(service.clusterHasFeature(state, new NodeFeature("af1", true)));
    }
}
