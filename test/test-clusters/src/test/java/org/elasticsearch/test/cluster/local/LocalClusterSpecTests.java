/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.test.cluster.local;

import org.elasticsearch.test.cluster.ElasticsearchCluster;
import org.elasticsearch.test.cluster.local.LocalClusterSpec.LocalNodeSpec;
import org.junit.Test;

import java.util.function.Consumer;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.equalTo;

/**
 * Pins the precedence rules of {@link LocalNodeSpec#resolveSystemProperties()}. Test cluster definitions rely on these
 * to layer a suite-specific value over a shared base configuration — for instance the ES|QL federation kill switch,
 * where a base cluster factory opts in and an individual suite overrides that. Because the layering is expressed only
 * through the order in which builder calls happen, it is easy to break silently: the affected suite still runs, just
 * against a node configured the other way round. These tests make such a regression a compile-or-test failure.
 */
public class LocalClusterSpecTests {

    private static final String KEY = "es.test.property";

    /**
     * A plain value always wins over a provider for the same key, whichever was registered first: providers are
     * resolved into the map before the explicit values are put over them.
     */
    @Test
    public void testPlainValueOverridesProviderRegardlessOfOrder() {
        assertThat(resolveSingleNode(builder -> {
            builder.systemProperty(KEY, () -> "provider");
            builder.systemProperty(KEY, "plain");
        }), equalTo("plain"));

        assertThat(resolveSingleNode(builder -> {
            builder.systemProperty(KEY, "plain");
            builder.systemProperty(KEY, () -> "provider");
        }), equalTo("plain"));
    }

    /** Among providers for the same key the last one registered wins, since they are applied in registration order. */
    @Test
    public void testLaterProviderOverridesEarlier() {
        assertThat(resolveSingleNode(builder -> {
            builder.systemProperty(KEY, () -> "first");
            builder.systemProperty(KEY, () -> "second");
        }), equalTo("second"));
    }

    /**
     * A predicated provider contributes nothing for the nodes it does not match, so a cluster-level provider it is
     * registered after remains in effect for those.
     */
    @Test
    public void testNodePredicatedProviderOverridesClusterLevelForMatchingNodesOnly() {
        SpecOnlyBuilder builder = new SpecOnlyBuilder();
        builder.nodes(2);
        builder.systemProperty(KEY, () -> "cluster-wide");
        builder.systemProperty(KEY, () -> "first-node-only", node -> node.getName().endsWith("-0"));

        LocalClusterSpec spec = builder.spec();
        assertThat(spec.getNodes().get(0).resolveSystemProperties().get(KEY), equalTo("first-node-only"));
        assertThat(spec.getNodes().get(1).resolveSystemProperties().get(KEY), equalTo("cluster-wide"));
    }

    /** A provider registered on a node wins over one registered on the cluster: node providers are inherited last. */
    @Test
    public void testNodeProviderOverridesClusterProvider() {
        SpecOnlyBuilder builder = new SpecOnlyBuilder();
        builder.systemProperty(KEY, () -> "cluster-wide");
        builder.withNode(node -> node.systemProperty(KEY, () -> "node-local"));

        assertThat(builder.spec().getNodes().get(0).resolveSystemProperties().get(KEY), equalTo("node-local"));
    }

    private static String resolveSingleNode(Consumer<SpecOnlyBuilder> configuration) {
        SpecOnlyBuilder builder = new SpecOnlyBuilder();
        configuration.accept(builder);
        return builder.spec().getNodes().get(0).resolveSystemProperties().get(KEY);
    }

    /**
     * A builder that stops at the spec, so no distribution is resolved and no node is ever started. Unlike
     * {@link DefaultLocalClusterSpecBuilder} it registers none of the default providers, keeping the resolved map to
     * exactly what a test configures.
     */
    private static class SpecOnlyBuilder extends AbstractLocalClusterSpecBuilder<ElasticsearchCluster> {

        LocalClusterSpec spec() {
            return buildClusterSpec();
        }

        @Override
        public ElasticsearchCluster build() {
            throw new UnsupportedOperationException("this builder only produces a spec");
        }
    }
}
