/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.test.fixtures.oldelasticsearch;

import org.elasticsearch.test.fixtures.testcontainers.DockerEnvironmentAwareTestContainer;
import org.elasticsearch.test.fixtures.testcontainers.PullOrBuildImage;
import org.junit.Assume;
import org.testcontainers.containers.wait.strategy.Wait;
import org.testcontainers.images.builder.ImageFromDockerfile;

import java.time.Duration;
import java.util.Map;

/**
 * Testcontainers fixture backed by prebaked Docker images for specific old Elasticsearch versions.
 *
 * <p>The images are built and published by the {@code elasticsearch.deploy-test-fixtures} plugin in
 * this project, with names like {@code docker.elastic.co/elasticsearch-dev/old-elasticsearch-6-8-20-fixture:1.0}.
 * The fixture bind-mounts the host repo directory at the same absolute path inside the container so
 * that the containerised old ES and the host-side current-version cluster can both use the same
 * repository location when registering an {@code fs} repository.
 */
@SuppressWarnings("this-escape")
public class OldElasticsearchContainer extends DockerEnvironmentAwareTestContainer {

    private static final int HTTP_PORT = 9200;
    // Keep in sync with `def fixtureVersion` in test/fixtures/old-elasticsearch/build.gradle.
    private static final String FIXTURE_IMAGE_VERSION = "1.4";
    private static final Map<String, String> IMAGES = Map.of(
        "0.90.13",
        "docker.elastic.co/elasticsearch-dev/old-elasticsearch-0-90-13-fixture:" + FIXTURE_IMAGE_VERSION,
        "1.7.6",
        "docker.elastic.co/elasticsearch-dev/old-elasticsearch-1-7-6-fixture:" + FIXTURE_IMAGE_VERSION,
        "2.4.5",
        "docker.elastic.co/elasticsearch-dev/old-elasticsearch-2-4-5-fixture:" + FIXTURE_IMAGE_VERSION,
        "5.0.0",
        "docker.elastic.co/elasticsearch-dev/old-elasticsearch-5-0-0-fixture:" + FIXTURE_IMAGE_VERSION,
        "5.6.16",
        "docker.elastic.co/elasticsearch-dev/old-elasticsearch-5-6-16-fixture:" + FIXTURE_IMAGE_VERSION,
        "6.0.0",
        "docker.elastic.co/elasticsearch-dev/old-elasticsearch-6-0-0-fixture:" + FIXTURE_IMAGE_VERSION,
        "6.8.20",
        "docker.elastic.co/elasticsearch-dev/old-elasticsearch-6-8-20-fixture:" + FIXTURE_IMAGE_VERSION,
        "7.9.3",
        "docker.elastic.co/elasticsearch-dev/old-elasticsearch-7-9-3-fixture:" + FIXTURE_IMAGE_VERSION,
        "7.10.0",
        "docker.elastic.co/elasticsearch-dev/old-elasticsearch-7-10-0-fixture:" + FIXTURE_IMAGE_VERSION
    );

    // Version-specific elasticsearch.yml settings appended at runtime via ES_EXTRA_CONFIG.
    // 0.90.13, 1.7.6, 2.4.5, and 5.x versions need no extra settings beyond what is baked
    // into the shared Dockerfile.
    private static final Map<String, String> EXTRA_CONFIGS = Map.of(
        "6.0.0",
        "discovery.type: single-node",
        "6.8.20",
        "xpack.ml.enabled: false\nxpack.security.enabled: false\ndiscovery.type: single-node",
        "7.9.3",
        "xpack.ml.enabled: false\nxpack.security.enabled: false\ndiscovery.type: single-node",
        "7.10.0",
        "xpack.ml.enabled: false\nxpack.security.enabled: false\ndiscovery.type: single-node"
    );

    private final String version;

    public OldElasticsearchContainer(String version, String repoLocation) {
        super(new PullOrBuildImage(resolveImage(version), localImage(version)));
        this.version = version;
        addExposedPort(HTTP_PORT);
        withFileSystemBind(repoLocation, repoLocation);
        withEnv("ES_PATH_REPO", repoLocation);
        String extraConfig = EXTRA_CONFIGS.get(version);
        if (extraConfig != null) {
            withEnv("ES_EXTRA_CONFIG", extraConfig);
        }
        setWaitStrategy(Wait.forHttp("/_cluster/health").forPort(HTTP_PORT).forStatusCode(200).withStartupTimeout(Duration.ofMinutes(2)));
    }

    /**
     * Skips the fixture on aarch64 for Elasticsearch versions that predate native aarch64 support.
     *
     * <p>Elasticsearch first published {@code linux-aarch64} distributions in 7.8.0. Earlier majors
     * ship only x86-oriented distributions whose bundled JVM options and launch scripts do not run
     * under aarch64: the containerised old ES process exits immediately (code 1) and the fixture's
     * {@link Wait} strategy then times out waiting for {@code /_cluster/health}. Rather than let
     * these suites fail on the aarch64 periodic pipelines, we raise a JUnit assumption so the
     * dependent tests are skipped there while continuing to run on x86_64. Versions that do have a
     * native aarch64 distribution (7.8.0+, e.g. 7.9.3/7.10.0) still run on both architectures.
     */
    @Override
    public void start() {
        Assume.assumeTrue(
            "Elasticsearch [" + version + "] has no native aarch64 distribution and does not run on aarch64; skipping",
            isAarch64() == false || hasNativeAarch64Distribution(version)
        );
        super.start();
    }

    private static boolean isAarch64() {
        return "aarch64".equals(System.getProperty("os.arch"));
    }

    /**
     * Whether the given Elasticsearch version ships a native {@code linux-aarch64} distribution,
     * which was first available in 7.8.0.
     */
    static boolean hasNativeAarch64Distribution(String version) {
        String[] parts = version.split("\\.");
        int major = Integer.parseInt(parts[0]);
        int minor = parts.length > 1 ? Integer.parseInt(parts[1]) : 0;
        return major > 7 || (major == 7 && minor >= 8);
    }

    private static String resolveImage(String version) {
        String image = IMAGES.get(version);
        if (image == null) {
            throw new IllegalArgumentException("Unsupported old Elasticsearch fixture version [" + version + "]");
        }
        return image;
    }

    private static ImageFromDockerfile localImage(String version) {
        return new ImageFromDockerfile().withFileFromClasspath("Dockerfile", "docker/Dockerfile")
            .withFileFromClasspath("entrypoint.sh", "docker/entrypoint.sh")
            .withBuildArg("ES_VERSION", version)
            .withBuildArg("TARGETARCH", dockerArch());
    }

    /**
     * Docker's automatic {@code TARGETARCH} build-arg is only populated by BuildKit/buildx-driven
     * builds. Classic {@code docker build} (used by {@code ImageFromDockerfile} here) leaves it
     * unset, so it must be supplied explicitly for the versions ({@code 7.9.3}, {@code 7.10.0})
     * that resolve an architecture-specific download URL.
     */
    private static String dockerArch() {
        String arch = System.getProperty("os.arch");
        return switch (arch) {
            case "aarch64" -> "arm64";
            case "x86_64", "amd64" -> "amd64";
            default -> throw new IllegalStateException("Unsupported architecture [" + arch + "] for old Elasticsearch fixture");
        };
    }

    public int getHttpPort() {
        return getMappedPort(HTTP_PORT);
    }
}
