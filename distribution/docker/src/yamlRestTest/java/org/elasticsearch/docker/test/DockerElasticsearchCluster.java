/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */
package org.elasticsearch.docker.test;

import org.elasticsearch.test.fixtures.testcontainers.AbstractDockerClusterRule;
import org.elasticsearch.test.fixtures.testcontainers.DockerEnvironmentAwareTestContainer;
import org.elasticsearch.test.fixtures.testcontainers.ElasticsearchNode;
import org.elasticsearch.test.fixtures.testcontainers.Junit4NetworkRule;
import org.junit.rules.RuleChain;
import org.testcontainers.containers.Network;

import java.nio.file.Path;
import java.util.Map;

/**
 * Boots a two-node Elasticsearch cluster against the locally built {@code elasticsearch:test} Docker image.
 */
public class DockerElasticsearchCluster extends AbstractDockerClusterRule {

    private static final String CLUSTER_NAME = "elasticsearch-default";
    private static final String NODE_1 = "elasticsearch-default-1";
    private static final String NODE_2 = "elasticsearch-default-2";

    // Defer to ElasticsearchNode so credentials are defined once across all cluster fixtures.
    public static final String USER = ElasticsearchNode.USER;
    public static final String PASS = ElasticsearchNode.PASS;

    private DockerEnvironmentAwareTestContainer node1;
    private DockerEnvironmentAwareTestContainer node2;

    @Override
    protected RuleChain buildRuleChain(Network network, Path repoDir) {
        node1 = ElasticsearchNode.create(
            network,
            repoDir,
            NODE_1,
            Map.of(
                "node.name",
                NODE_1,
                "cluster.name",
                CLUSTER_NAME,
                "cluster.initial_master_nodes",
                NODE_1 + "," + NODE_2,
                "discovery.seed_hosts",
                NODE_2 + ":9300"
            )
        );
        node2 = ElasticsearchNode.create(
            network,
            repoDir,
            NODE_2,
            Map.of(
                "node.name",
                NODE_2,
                "cluster.name",
                CLUSTER_NAME,
                "cluster.initial_master_nodes",
                NODE_1 + "," + NODE_2,
                "discovery.seed_hosts",
                NODE_1 + ":9300"
            )
        );
        return RuleChain.outerRule(Junit4NetworkRule.from(network)).around(asRule(node1)).around(asRule(node2));
    }

    @Override
    protected void clearContainerReferences() {
        node1 = null;
        node2 = null;
    }

    public String getHttpAddresses() {
        // Fail loudly if a caller (e.g. an overridden getTestRestCluster()) reads addresses
        // before the @ClassRule has started the containers, instead of returning a misleading "null:0,null:0".
        if (node1 == null || node2 == null) {
            throw new IllegalStateException("Cluster has not been started; getHttpAddresses() called outside @ClassRule scope");
        }
        return node1.getHost()
            + ":"
            + node1.getMappedPort(ElasticsearchNode.HTTP_PORT)
            + ","
            + node2.getHost()
            + ":"
            + node2.getMappedPort(ElasticsearchNode.HTTP_PORT);
    }
}
