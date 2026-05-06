/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */
package org.elasticsearch.docker.test;

import com.github.dockerjava.api.model.Ulimit;

import org.elasticsearch.test.fixtures.testcontainers.DockerAvailability;
import org.junit.Assume;
import org.junit.rules.RuleChain;
import org.junit.rules.TestRule;
import org.junit.runner.Description;
import org.junit.runners.model.Statement;
import org.testcontainers.containers.GenericContainer;
import org.testcontainers.containers.Network;
import org.testcontainers.containers.wait.strategy.Wait;
import org.testcontainers.utility.MountableFile;

import java.io.IOException;
import java.nio.file.FileVisitResult;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.SimpleFileVisitor;
import java.nio.file.attribute.BasicFileAttributes;
import java.time.Duration;

/**
 * Boots a two-node Elasticsearch cluster against the locally built {@code elasticsearch:test} Docker image.
 * Replaces the previous {@code docker-compose.yml}-based fixture so the suite can be wired via a JUnit
 * rule rather than the legacy {@code elasticsearch.test.fixtures} Gradle plugin.
 */
public class DockerElasticsearchCluster implements TestRule {

    private static final String IMAGE = "elasticsearch:test";
    private static final String CLUSTER_NAME = "elasticsearch-default";
    private static final String NODE_1 = "elasticsearch-default-1";
    private static final String NODE_2 = "elasticsearch-default-2";
    private static final int HTTP_PORT = 9200;

    private final Network network = Network.newNetwork();
    private final Path repoDir = createRepoDir();
    private final GenericContainer<?> node1 = createNode(NODE_1, NODE_2);
    private final GenericContainer<?> node2 = createNode(NODE_2, NODE_1);

    private static Path createRepoDir() {
        try {
            return Files.createTempDirectory("es-docker-repo");
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }

    private GenericContainer<?> createNode(String nodeName, String otherNodeName) {
        return new GenericContainer<>(IMAGE).withNetwork(network)
            .withNetworkAliases(nodeName)
            .withEnv("node.name", nodeName)
            .withEnv("cluster.name", CLUSTER_NAME)
            .withEnv("cluster.initial_master_nodes", NODE_1 + "," + NODE_2)
            .withEnv("discovery.seed_hosts", otherNodeName + ":9300")
            .withEnv("bootstrap.memory_lock", "true")
            .withEnv("ES_JAVA_OPTS", "-Xms512m -Xmx512m")
            .withEnv("path.repo", "/tmp/es-repo")
            .withEnv("node.attr.testattr", "test")
            .withEnv("cluster.routing.allocation.disk.watermark.low", "1b")
            .withEnv("cluster.routing.allocation.disk.watermark.high", "1b")
            .withEnv("cluster.routing.allocation.disk.watermark.flood_stage", "1b")
            .withEnv("node.store.allow_mmap", "false")
            .withEnv("ingest.geoip.downloader.enabled", "false")
            .withEnv("xpack.security.enabled", "true")
            .withEnv("xpack.security.transport.ssl.enabled", "true")
            .withEnv("xpack.security.http.ssl.enabled", "true")
            .withEnv("xpack.security.authc.token.enabled", "true")
            .withEnv("xpack.security.audit.enabled", "true")
            .withEnv("xpack.security.authc.realms.file.file1.order", "0")
            .withEnv("xpack.security.authc.realms.native.native1.order", "1")
            .withEnv("xpack.security.transport.ssl.key", "/usr/share/elasticsearch/config/testnode.pem")
            .withEnv("xpack.security.transport.ssl.certificate", "/usr/share/elasticsearch/config/testnode.crt")
            .withEnv("xpack.security.http.ssl.key", "/usr/share/elasticsearch/config/testnode.pem")
            .withEnv("xpack.security.http.ssl.certificate", "/usr/share/elasticsearch/config/testnode.crt")
            .withEnv("xpack.http.ssl.verification_mode", "certificate")
            .withEnv("xpack.security.transport.ssl.verification_mode", "certificate")
            .withEnv("xpack.license.self_generated.type", "trial")
            .withEnv("action.destructive_requires_name", "false")
            .withEnv("cluster.deprecation_indexing.enabled", "false")
            .withCopyFileToContainer(MountableFile.forClasspathResource("/testnode.pem"), "/usr/share/elasticsearch/config/testnode.pem")
            .withCopyFileToContainer(MountableFile.forClasspathResource("/testnode.crt"), "/usr/share/elasticsearch/config/testnode.crt")
            .withCopyFileToContainer(MountableFile.forClasspathResource("/docker-test-entrypoint.sh", 0755), "/docker-test-entrypoint.sh")
            .withCreateContainerCmdModifier(cmd -> {
                cmd.withEntrypoint("/docker-test-entrypoint.sh");
                // bootstrap.memory_lock=true requires unlimited locked memory
                cmd.getHostConfig().withUlimits(new Ulimit[] { new Ulimit("memlock", -1L, -1L), new Ulimit("nofile", 65536L, 65536L) });
            })
            .withFileSystemBind(repoDir.toAbsolutePath().toString(), "/tmp/es-repo")
            .withExposedPorts(HTTP_PORT)
            .waitingFor(Wait.forListeningPort().withStartupTimeout(Duration.ofMinutes(3)));
    }

    public String getHttpAddresses() {
        return node1.getHost() + ":" + node1.getMappedPort(HTTP_PORT) + "," + node2.getHost() + ":" + node2.getMappedPort(HTTP_PORT);
    }

    @Override
    public Statement apply(Statement base, Description description) {
        Statement chained = RuleChain.outerRule(asRule(node1)).around(asRule(node2)).apply(base, description);
        return new Statement() {
            @Override
            public void evaluate() throws Throwable {
                Assume.assumeTrue("Docker is not available", DockerAvailability.isDockerAvailable());
                try {
                    chained.evaluate();
                } finally {
                    try {
                        network.close();
                    } finally {
                        deleteRecursively(repoDir);
                    }
                }
            }
        };
    }

    private static void deleteRecursively(Path root) throws IOException {
        if (Files.exists(root) == false) {
            return;
        }
        Files.walkFileTree(root, new SimpleFileVisitor<>() {
            @Override
            public FileVisitResult visitFile(Path file, BasicFileAttributes attrs) throws IOException {
                Files.deleteIfExists(file);
                return FileVisitResult.CONTINUE;
            }

            @Override
            public FileVisitResult postVisitDirectory(Path dir, IOException exc) throws IOException {
                Files.deleteIfExists(dir);
                return FileVisitResult.CONTINUE;
            }
        });
    }

    private static TestRule asRule(GenericContainer<?> container) {
        return (base, description) -> new Statement() {
            @Override
            public void evaluate() throws Throwable {
                container.start();
                try {
                    base.evaluate();
                } finally {
                    container.stop();
                }
            }
        };
    }
}
