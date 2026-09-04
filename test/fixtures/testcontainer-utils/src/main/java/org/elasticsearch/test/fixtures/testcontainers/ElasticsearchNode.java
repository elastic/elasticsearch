/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */
package org.elasticsearch.test.fixtures.testcontainers;

import com.github.dockerjava.api.model.Ulimit;

import org.testcontainers.containers.Network;
import org.testcontainers.containers.wait.strategy.Wait;
import org.testcontainers.utility.MountableFile;

import java.nio.file.Path;
import java.time.Duration;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.CompletableFuture;

/**
 * Factory for a single Elasticsearch Docker container pre-configured with full security/TLS,
 * disk-watermark, and license settings against the locally built {@code elasticsearch:test} image.
 *
 * <p>Callers supply only the cluster-topology env vars ({@code node.name},
 * {@code cluster.name}, {@code cluster.initial_master_nodes}, and optionally
 * {@code discovery.seed_hosts}) that differ per use-case. All other settings are
 * provided by this factory and are identical for every Elasticsearch container created
 * by test suites in this repository.
 *
 * <p>The factory expects the following classpath resources to be present on the consuming
 * project's test classpath (copied via {@code processTestResources} or equivalent):
 * <ul>
 *   <li>{@code /testnode.pem} — TLS private key</li>
 *   <li>{@code /testnode.crt} — TLS certificate</li>
 * </ul>
 * The {@code /docker-test-entrypoint.sh} script is bundled inside this library and
 * does not need to be provided by the consuming project.
 */
public final class ElasticsearchNode {

    /** Docker image tag produced by the local Elasticsearch build. */
    public static final String IMAGE = "elasticsearch:test";

    /** HTTP port exposed by each Elasticsearch container. */
    public static final int HTTP_PORT = 9200;

    /** Default REST user provisioned by {@code docker-test-entrypoint.sh}. */
    public static final String USER = "x_pack_rest_user";

    /** Default REST password provisioned by {@code docker-test-entrypoint.sh}. */
    public static final String PASS = "x-pack-test-password";

    private ElasticsearchNode() {}

    /**
     * Creates a single Elasticsearch container with the standard security/TLS configuration.
     *
     * <p>The {@code topologyEnv} map must contain at minimum {@code node.name},
     * {@code cluster.name}, and {@code cluster.initial_master_nodes}. Any key present
     * in {@code topologyEnv} overrides the corresponding standard default, so callers can
     * also use it to inject {@code discovery.seed_hosts} for multi-node clusters.
     *
     * @param network      the shared Docker network all containers in the test cluster use
     * @param repoDir      host-side directory bind-mounted as {@code /tmp/es-repo} inside the container
     * @param networkAlias Docker network alias under which this container is reachable by its peers
     * @param topologyEnv  cluster-topology env vars (node name, cluster name, master nodes, …)
     * @return a fully configured but not yet started container
     */
    public static DockerEnvironmentAwareTestContainer create(
        Network network,
        Path repoDir,
        String networkAlias,
        Map<String, String> topologyEnv
    ) {
        Map<String, String> env = buildEnv(topologyEnv);

        return new DockerEnvironmentAwareTestContainer(CompletableFuture.completedFuture(IMAGE)) {}.withNetwork(network)
            .withNetworkAliases(networkAlias)
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

    /**
     * Merges the caller's topology env vars into the standard security/TLS/watermark defaults.
     * Topology entries win on conflict, so callers can also override any default if needed.
     */
    private static Map<String, String> buildEnv(Map<String, String> topologyEnv) {
        Map<String, String> env = new HashMap<>();

        // JVM & node behavior
        env.put("bootstrap.memory_lock", "true");
        env.put("ES_JAVA_OPTS", "-Xms512m -Xmx512m");
        env.put("path.repo", "/tmp/es-repo");
        env.put("node.attr.testattr", "test");
        env.put("node.store.allow_mmap", "false");
        env.put("ingest.geoip.downloader.enabled", "false");

        // disk watermarks: forced very low so tests run on small CI disks without hitting them
        env.put("cluster.routing.allocation.disk.watermark.low", "1b");
        env.put("cluster.routing.allocation.disk.watermark.high", "1b");
        env.put("cluster.routing.allocation.disk.watermark.flood_stage", "1b");

        // security & TLS
        env.put("xpack.security.enabled", "true");
        env.put("xpack.security.transport.ssl.enabled", "true");
        env.put("xpack.security.http.ssl.enabled", "true");
        env.put("xpack.security.authc.token.enabled", "true");
        env.put("xpack.security.audit.enabled", "true");
        env.put("xpack.security.authc.realms.file.file1.order", "0");
        env.put("xpack.security.authc.realms.native.native1.order", "1");
        env.put("xpack.security.transport.ssl.key", "/usr/share/elasticsearch/config/testnode.pem");
        env.put("xpack.security.transport.ssl.certificate", "/usr/share/elasticsearch/config/testnode.crt");
        env.put("xpack.security.http.ssl.key", "/usr/share/elasticsearch/config/testnode.pem");
        env.put("xpack.security.http.ssl.certificate", "/usr/share/elasticsearch/config/testnode.crt");
        env.put("xpack.http.ssl.verification_mode", "certificate");
        env.put("xpack.security.transport.ssl.verification_mode", "certificate");

        // license & misc
        env.put("xpack.license.self_generated.type", "trial");
        env.put("action.destructive_requires_name", "false");
        env.put("cluster.deprecation_indexing.enabled", "false");

        // credentials propagated into the provisioning script
        env.put("ES_TEST_USER", USER);
        env.put("ES_TEST_PASS", PASS);

        // topology env wins over defaults
        env.putAll(topologyEnv);

        return env;
    }
}
