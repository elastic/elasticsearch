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

import org.elasticsearch.test.fixtures.testcontainers.DockerEnvironmentAwareTestContainer;
import org.elasticsearch.test.fixtures.testcontainers.Junit4NetworkRule;
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
import java.util.Map;
import java.util.concurrent.CompletableFuture;

import static org.elasticsearch.test.fixtures.testcontainers.DockerAvailability.assumeDockerIsAvailable;

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

    // Review item 10: single source of truth for the superuser credentials. These are propagated to each
    // container via ES_TEST_USER/ES_TEST_PASS env vars so docker-test-entrypoint.sh creates a matching user;
    // DockerYmlTestSuiteIT reads them from here too, so renaming either constant updates everything.
    public static final String USER = "x_pack_rest_user";
    public static final String PASS = "x-pack-test-password";

    // Review item 2: container references must remain visible to test methods that call getHttpAddresses(),
    // but they are constructed lazily inside apply() so that nothing is allocated when Docker is unavailable
    // (which would otherwise leak a temp directory and a docker network on assumption failure).
    // Review item 5: we type these as DockerEnvironmentAwareTestContainer to gain Slf4jLogConsumer streaming
    // (so failed runs surface node logs in the test report) and ensureContainerFullyStopped() on shutdown,
    // matching the convention used by every other Testcontainer fixture in the repo.
    private DockerEnvironmentAwareTestContainer node1;
    private DockerEnvironmentAwareTestContainer node2;

    private DockerEnvironmentAwareTestContainer createNode(Network network, Path repoDir, String nodeName, String otherNodeName) {
        // Review nice-to-have: env vars grouped via Map.ofEntries so the security/TLS block is visually
        // distinct from the cluster-identity and disk-watermark blocks; the entries-list also reads as a
        // configuration table rather than a 25-line withEnv() chain.
        Map<String, String> env = Map.ofEntries(
            // cluster identity & discovery
            Map.entry("node.name", nodeName),
            Map.entry("cluster.name", CLUSTER_NAME),
            Map.entry("cluster.initial_master_nodes", NODE_1 + "," + NODE_2),
            Map.entry("discovery.seed_hosts", otherNodeName + ":9300"),

            // JVM & node behavior
            Map.entry("bootstrap.memory_lock", "true"),
            Map.entry("ES_JAVA_OPTS", "-Xms512m -Xmx512m"),
            Map.entry("path.repo", "/tmp/es-repo"),
            Map.entry("node.attr.testattr", "test"),
            Map.entry("node.store.allow_mmap", "false"),
            Map.entry("ingest.geoip.downloader.enabled", "false"),

            // disk watermarks: forced very low so tests run on small CI disks without hitting them
            Map.entry("cluster.routing.allocation.disk.watermark.low", "1b"),
            Map.entry("cluster.routing.allocation.disk.watermark.high", "1b"),
            Map.entry("cluster.routing.allocation.disk.watermark.flood_stage", "1b"),

            // security & TLS
            Map.entry("xpack.security.enabled", "true"),
            Map.entry("xpack.security.transport.ssl.enabled", "true"),
            Map.entry("xpack.security.http.ssl.enabled", "true"),
            Map.entry("xpack.security.authc.token.enabled", "true"),
            Map.entry("xpack.security.audit.enabled", "true"),
            Map.entry("xpack.security.authc.realms.file.file1.order", "0"),
            Map.entry("xpack.security.authc.realms.native.native1.order", "1"),
            Map.entry("xpack.security.transport.ssl.key", "/usr/share/elasticsearch/config/testnode.pem"),
            Map.entry("xpack.security.transport.ssl.certificate", "/usr/share/elasticsearch/config/testnode.crt"),
            Map.entry("xpack.security.http.ssl.key", "/usr/share/elasticsearch/config/testnode.pem"),
            Map.entry("xpack.security.http.ssl.certificate", "/usr/share/elasticsearch/config/testnode.crt"),
            Map.entry("xpack.http.ssl.verification_mode", "certificate"),
            Map.entry("xpack.security.transport.ssl.verification_mode", "certificate"),

            // license & misc
            Map.entry("xpack.license.self_generated.type", "trial"),
            Map.entry("action.destructive_requires_name", "false"),
            Map.entry("cluster.deprecation_indexing.enabled", "false"),

            // Review item 10: superuser credentials read by docker-test-entrypoint.sh; defined once on
            // DockerElasticsearchCluster so the IT and the script see the same values.
            Map.entry("ES_TEST_USER", USER),
            Map.entry("ES_TEST_PASS", PASS)
        );

        // Review item 5: anonymous subclass is required because DockerEnvironmentAwareTestContainer is abstract.
        // The image name resolves immediately (CompletableFuture.completedFuture) — there is no async pull.
        return new DockerEnvironmentAwareTestContainer(CompletableFuture.completedFuture(IMAGE)) {}.withNetwork(network)
            .withNetworkAliases(nodeName)
            .withEnv(env)
            .withCopyFileToContainer(MountableFile.forClasspathResource("/testnode.pem"), "/usr/share/elasticsearch/config/testnode.pem")
            .withCopyFileToContainer(MountableFile.forClasspathResource("/testnode.crt"), "/usr/share/elasticsearch/config/testnode.crt")
            // 0755: docker-entrypoint.sh must be executable in the container; the classpath copy preserves
            // mode bits as supplied here, not as set on the host file.
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
        // Review item 2/QA #5: fail loudly if a caller (e.g. an overridden getTestRestCluster()) reads addresses
        // before the @ClassRule has started the containers, instead of returning a misleading "null:0,null:0".
        if (node1 == null || node2 == null) {
            throw new IllegalStateException("Cluster has not been started; getHttpAddresses() called outside @ClassRule scope");
        }
        return node1.getHost() + ":" + node1.getMappedPort(HTTP_PORT) + "," + node2.getHost() + ":" + node2.getMappedPort(HTTP_PORT);
    }

    @Override
    public Statement apply(Statement base, Description description) {
        return new Statement() {
            @Override
            public void evaluate() throws Throwable {
                // Review item 1: use assumeDockerIsAvailable() (not a plain Assume.assumeTrue) so that the rule
                // also applies the dockerOnLinuxExclusions list and asserts hard on CI when probing fails.
                // Calling it BEFORE allocating the temp dir avoids leaking the dir on assumption failure
                // (review item 2).
                assumeDockerIsAvailable();
                Path repoDir = Files.createTempDirectory("es-docker-repo");
                try {
                    // Network is lazy in Testcontainers (no docker resource until first getId()), so allocating
                    // it here costs nothing if the rule chain below short-circuits.
                    Network network = Network.newNetwork();
                    node1 = createNode(network, repoDir, NODE_1, NODE_2);
                    node2 = createNode(network, repoDir, NODE_2, NODE_1);
                    // Review item 2: reuse Junit4NetworkRule for the network's lifecycle instead of
                    // duplicating its close-in-finally logic inline. asRule() handles each container's
                    // start/stop. Order: network -> node1 -> node2 -> base.
                    RuleChain.outerRule(Junit4NetworkRule.from(network))
                        .around(asRule(node1))
                        .around(asRule(node2))
                        .apply(base, description)
                        .evaluate();
                } finally {
                    node1 = null;
                    node2 = null;
                    deleteRecursively(repoDir);
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

            // Review item 4: do not abort the entire walk on a single file failure. Docker bind-mount
            // releases are asynchronous, so a file may briefly be unreadable right after the container
            // stops; aborting here would leak the rest of the tree under /tmp.
            @Override
            public FileVisitResult visitFileFailed(Path file, IOException exc) {
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
