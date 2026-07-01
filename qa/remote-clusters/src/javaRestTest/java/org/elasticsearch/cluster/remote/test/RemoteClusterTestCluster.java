/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */
package org.elasticsearch.cluster.remote.test;

import org.elasticsearch.test.fixtures.testcontainers.AbstractDockerClusterRule;
import org.elasticsearch.test.fixtures.testcontainers.DockerEnvironmentAwareTestContainer;
import org.elasticsearch.test.fixtures.testcontainers.ElasticsearchNode;
import org.elasticsearch.test.fixtures.testcontainers.Junit4NetworkRule;
import org.junit.rules.RuleChain;
import org.testcontainers.containers.GenericContainer;
import org.testcontainers.containers.Network;
import org.testcontainers.containers.wait.strategy.Wait;
import org.testcontainers.utility.MountableFile;

import java.nio.file.Path;
import java.time.Duration;
import java.util.Map;

/**
 * Boots two independent single-node Elasticsearch clusters and one HAProxy container against the
 * locally built {@code elasticsearch:test} Docker image. All three containers share a Docker network
 * so that inter-cluster transport connections ({@code elasticsearch-default-2:9300}) and
 * SNI-routed HAProxy connections ({@code haproxy:9600}) resolve correctly inside each container.
 */
public class RemoteClusterTestCluster extends AbstractDockerClusterRule {

    static final String CLUSTER_1_NAME = "elasticsearch-default-1";
    static final String CLUSTER_2_NAME = "elasticsearch-default-2";

    // Defer to ElasticsearchNode so credentials are defined once across all cluster fixtures.
    public static final String USER = ElasticsearchNode.USER;
    public static final String PASS = ElasticsearchNode.PASS;

    private DockerEnvironmentAwareTestContainer node1;
    private DockerEnvironmentAwareTestContainer node2;
    private GenericContainer<?> haproxy;

    @Override
    protected RuleChain buildRuleChain(Network network, Path repoDir) {
        node1 = ElasticsearchNode.create(
            network,
            repoDir,
            CLUSTER_1_NAME,
            Map.of(
                "node.name",
                CLUSTER_1_NAME,
                "cluster.name",
                CLUSTER_1_NAME,
                "cluster.initial_master_nodes",
                CLUSTER_1_NAME,
                // publish loopback so sniff-mode cross-cluster attempts get back 127.0.0.1:9300,
                // which is unreachable from the remote cluster — keeping testSniffModeConnectionFails honest
                "network.publish_host",
                "127.0.0.1"
            )
        );
        node2 = ElasticsearchNode.create(
            network,
            repoDir,
            CLUSTER_2_NAME,
            Map.of(
                "node.name",
                CLUSTER_2_NAME,
                "cluster.name",
                CLUSTER_2_NAME,
                "cluster.initial_master_nodes",
                CLUSTER_2_NAME,
                "network.publish_host",
                "127.0.0.1"
            )
        );
        haproxy = createHaproxy(network);
        return RuleChain.outerRule(Junit4NetworkRule.from(network)).around(asRule(node1)).around(asRule(node2)).around(asRule(haproxy));
    }

    @Override
    protected void clearContainerReferences() {
        node1 = null;
        node2 = null;
        haproxy = null;
    }

    private static GenericContainer<?> createHaproxy(Network network) {
        return new GenericContainer<>("haproxy:2.1.2").withNetwork(network)
            .withNetworkAliases("haproxy")
            // HAProxy config references elasticsearch-default-{1,2}:9300 via Docker network DNS
            .withCopyFileToContainer(MountableFile.forClasspathResource("/haproxy-default.cfg"), "/usr/local/etc/haproxy/haproxy.cfg")
            .withExposedPorts(9600)
            .waitingFor(Wait.forListeningPort().withStartupTimeout(Duration.ofMinutes(1)));
    }

    /**
     * Returns the host-mapped HTTP address ({@code host:port}) of cluster 1.
     * Must only be called within the scope of the {@code @ClassRule}.
     */
    public String getCluster1HttpAddress() {
        checkStarted();
        return node1.getHost() + ":" + node1.getMappedPort(ElasticsearchNode.HTTP_PORT);
    }

    /**
     * Returns the host-mapped HTTP address ({@code host:port}) of cluster 2.
     * Must only be called within the scope of the {@code @ClassRule}.
     */
    public String getCluster2HttpAddress() {
        checkStarted();
        return node2.getHost() + ":" + node2.getMappedPort(ElasticsearchNode.HTTP_PORT);
    }

    private void checkStarted() {
        if (node1 == null || node2 == null) {
            throw new IllegalStateException(
                "RemoteClusterTestCluster has not been started; address methods called outside @ClassRule scope"
            );
        }
    }
}
