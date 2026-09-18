/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.backwards;

import org.elasticsearch.test.cluster.ElasticsearchCluster;
import org.elasticsearch.test.cluster.FeatureFlag;
import org.elasticsearch.test.cluster.local.LocalClusterSpecBuilder;
import org.elasticsearch.test.cluster.local.distribution.DistributionType;
import org.elasticsearch.test.cluster.util.Version;

import java.io.IOException;
import java.io.UncheckedIOException;
import java.nio.file.Files;
import java.nio.file.Path;

/**
 * Provides the single mixed-version cluster the {@code :qa:mixed-cluster} suites run against: a fixed four-node topology
 * where two nodes run the wire-compatible BWC version ({@code tests.old_cluster_version}) and two run the current version,
 * forming a mixed cluster that is asserted once rather than upgraded through phases. This mirrors the legacy Gradle
 * {@code testClusters} block, which started four nodes and upgraded two of them via {@code nextNodeToNextVersion} before
 * any test ran.
 * <p>
 * The cluster is declared {@code shared(true)} so it is started once and kept running across every test class, exactly as
 * the legacy {@code mixedClusterTest} task ran all suites against one cluster. Test isolation still holds because
 * {@code ESRestTestCase} (and the YAML runner built on it) wipes cluster content after every suite. Because the YAML runner
 * ({@code ESClientYamlSuiteTestCase}) and the plain REST ITs ({@code ESRestTestCase}) live in different type hierarchies,
 * each concrete suite references this same static {@link #CLUSTER} instance rather than inheriting it.
 * <p>
 * Sharing has two consequences, both wired up deliberately:
 * <ul>
 *     <li>Every class using this cluster must carry {@code @ThreadLeakFilters(filters = TestClustersThreadFilter.class)};
 *     the framework asserts this because a shared cluster outlives the suite that started it, so its threads would
 *     otherwise be reported as suite-level leaks.</li>
 *     <li>{@code build.gradle} pins {@code maxParallelForks = 1} on the versioned {@code yamlRestTest} tasks. Sharing is
 *     per-JVM, so parallel forks would each start their own four-node cluster and negate the saving.</li>
 * </ul>
 */
public final class Clusters {

    private static final String OLD_CLUSTER_VERSION = System.getProperty("tests.old_cluster_version");

    /**
     * Directory registered as {@code path.repo} on {@link #CLUSTER}. Created once for the shared cluster's lifetime so that
     * a test which registers an {@code fs} repository (see {@link IndexingIT#testUpdateSnapshotStatus}) can point the
     * repository at a location the cluster is allowed to use. It is created under the Gradle test-run temp directory and is
     * cleaned up with the build output.
     */
    static final Path REPO_DIR = createRepoDir();

    /**
     * The single four-node mixed-version cluster shared by every suite in this project.
     */
    static final ElasticsearchCluster CLUSTER = buildCluster();

    private Clusters() {}

    private static Path createRepoDir() {
        try {
            return Files.createTempDirectory("mixed-cluster-repo");
        } catch (IOException e) {
            throw new UncheckedIOException(e);
        }
    }

    private static ElasticsearchCluster buildCluster() {
        Version oldVersion = Version.fromString(OLD_CLUSTER_VERSION);
        // A detached build reports the version it will become but carries no guarantee about which commits it contains, so
        // version-gated settings below are keyed off the resolved BWC version rather than assuming feature availability.
        boolean isDetachedVersion = System.getProperty("tests.bwc.refspec.main") != null;

        LocalClusterSpecBuilder<ElasticsearchCluster> builder = ElasticsearchCluster.local()
            // BWC nodes run released distributions, which are only published as the default distribution.
            .distribution(DistributionType.DEFAULT)
            .shared(true)
            .withNode(node -> node.version(OLD_CLUSTER_VERSION, isDetachedVersion))
            .withNode(node -> node.version(OLD_CLUSTER_VERSION, isDetachedVersion))
            .withNode(node -> node.version(Version.CURRENT))
            .withNode(node -> node.version(Version.CURRENT))
            .setting("path.repo", () -> REPO_DIR.toString())
            .setting("xpack.security.enabled", "false")
            .setting("xpack.license.self_generated.type", "trial")
            .feature(FeatureFlag.TIME_SERIES_MODE);

        // There is a chance we have more master changes than "normal", so to avoid this test from failing we increase the
        // threshold (as the purpose of this test isn't to test that specific indicator). The setting was introduced in 8.4.0.
        if (oldVersion.onOrAfter(Version.fromString("8.4.0"))) {
            builder.setting("health.master_history.no_master_transitions_threshold", "10");
        }

        // Assertions on these mapper classes trip when a current-version node talks to a pre-8.18 node, so disable them.
        if (oldVersion.before(Version.fromString("8.18.0"))) {
            builder.jvmArg("-da:org.elasticsearch.index.mapper.DocumentMapper");
            builder.jvmArg("-da:org.elasticsearch.index.mapper.MapperService");
        }

        return builder.build();
    }
}
