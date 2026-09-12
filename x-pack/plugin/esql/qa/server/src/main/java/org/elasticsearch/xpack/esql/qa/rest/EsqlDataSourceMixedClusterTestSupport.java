/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.qa.rest;

import org.elasticsearch.Version;
import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.test.cluster.ElasticsearchCluster;
import org.elasticsearch.test.cluster.local.LocalClusterConfigProvider;
import org.elasticsearch.test.cluster.local.LocalClusterSpecBuilder;
import org.elasticsearch.test.cluster.local.LocalNodeSpecBuilder;
import org.elasticsearch.test.cluster.local.distribution.DistributionType;
import org.elasticsearch.test.rest.ObjectPath;
import org.elasticsearch.xpack.esql.datasources.Federation;
import org.junit.Assume;
import org.junit.rules.TestRule;
import org.junit.runners.model.Statement;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.function.Supplier;

/**
 * Format-neutral mixed-version cluster and coordinator support for data-source QA suites.
 *
 * <p>The owning suite supplies modules, feature flags, fixture settings, and optional
 * per-node customization. This class owns only version-sensitive behavior shared by all
 * formats: node versions, federation, the local-path setting rename, coordinator selection,
 * explicit build modes, and the outer FIPS/version guard.
 */
public final class EsqlDataSourceMixedClusterTestSupport {

    public static final String BWC_TEST_PROPERTY = "tests.esql.datasource.bwc";
    public static final String COORDINATOR_PROPERTY = "tests.esql.datasource.coordinator";
    public static final String CURRENT_SNAPSHOT_PROPERTY = "tests.esql.datasource.current_snapshot";
    public static final String OLD_SNAPSHOT_PROPERTY = "tests.esql.datasource.old_snapshot";
    public static final String DETACHED_BWC_REFSPEC_PROPERTY = "tests.bwc.refspec.main";

    private static final String OLD_CLUSTER_VERSION_PROPERTY = "tests.old_cluster_version";
    private static final String SERVERLESS_BWC_VERSION_PROPERTY = "tests.serverless.bwc_stack_version";
    private static final String FEDERATION_ENABLED_SETTING = Federation.FEDERATION_ENABLED.getKey();

    private EsqlDataSourceMixedClusterTestSupport() {}

    /**
     * Additional per-node configuration supplied by the owning format suite.
     *
     * <p>Every suite passes a no-op today: modules, settings, and JVM arguments are the same on
     * both sides and go through the cluster-level {@link LocalClusterConfigProvider} instead. The
     * hook exists for the settings and feature flags a format cannot apply cluster-wide because
     * only some node versions accept them — the first one being gRPC, whose connector flag has a
     * narrower version range than the suite's minimum BWC version.
     */
    @FunctionalInterface
    public interface VersionedNodeConfig {
        /**
         * Customizes one node after shared version-sensitive settings have been applied.
         */
        void configure(LocalNodeSpecBuilder node, Version version, boolean current);
    }

    /**
     * Whether this JVM was launched by the data-source BWC convention.
     */
    public static boolean isBwcTest() {
        return Boolean.parseBoolean(System.getProperty(BWC_TEST_PROPERTY, "false"));
    }

    /**
     * Whether the old distribution was built from a git ref rather than resolved as a release.
     */
    public static boolean isDetachedBwcBuild() {
        return System.getProperty(DETACHED_BWC_REFSPEC_PROPERTY) != null;
    }

    /**
     * Builds the four-node old/current cluster used by owner-module BWC tasks.
     */
    public static ElasticsearchCluster mixedCluster(
        Supplier<String> localAllowedPath,
        LocalClusterConfigProvider clusterConfig,
        VersionedNodeConfig nodeConfig
    ) {
        Version oldVersion = bwcVersion();
        String oldVersionString = requiredProperty(OLD_CLUSTER_VERSION_PROPERTY);
        boolean detached = isDetachedBwcBuild();

        LocalClusterSpecBuilder<ElasticsearchCluster> cluster = ElasticsearchCluster.local()
            .distribution(DistributionType.DEFAULT)
            .shared(true)
            .apply(clusterConfig)
            .withNode(node -> configureOldNode(node, "old-node-0", oldVersionString, oldVersion, detached, localAllowedPath, nodeConfig))
            .withNode(node -> configureCurrentNode(node, "current-node-0", localAllowedPath, nodeConfig))
            .withNode(node -> configureOldNode(node, "old-node-1", oldVersionString, oldVersion, detached, localAllowedPath, nodeConfig))
            .withNode(node -> configureCurrentNode(node, "current-node-1", localAllowedPath, nodeConfig));

        if (supportsRetryOnShardFailures(oldVersion) == false) {
            cluster.setting("cluster.routing.rebalance.enable", "none");
        }
        if (oldVersion.before(Version.V_8_18_0)) {
            cluster.jvmArg("-da:org.elasticsearch.index.mapper.DocumentMapper");
            cluster.jvmArg("-da:org.elasticsearch.index.mapper.MapperService");
        }
        return cluster.build();
    }

    /**
     * Applies the shared version-sensitive settings to a current node. A current node always knows
     * every setting this class writes, so nothing is version-gated away here.
     */
    public static void configureCurrentNode(
        LocalNodeSpecBuilder node,
        String nodeName,
        Supplier<String> localAllowedPath,
        VersionedNodeConfig nodeConfig
    ) {
        configureNode(
            node.name(nodeName).version(org.elasticsearch.test.cluster.util.Version.CURRENT),
            Version.CURRENT,
            false,
            localAllowedPath
        );
        nodeConfig.configure(node, Version.CURRENT, true);
    }

    /**
     * Applies the shared version-sensitive settings to an old node.
     *
     * @param detached whether the node is built from a git ref rather than resolved from a release. Such a build reports
     *        the version it will become, which says nothing about which commits it contains, so version-gated settings
     *        are left off entirely: the ref can point at a commit from before the setting was introduced.
     */
    public static void configureOldNode(
        LocalNodeSpecBuilder node,
        String nodeName,
        String versionString,
        Version version,
        boolean detached,
        Supplier<String> localAllowedPath,
        VersionedNodeConfig nodeConfig
    ) {
        configureNode(node.name(nodeName).version(versionString, detached), version, detached, localAllowedPath);
        nodeConfig.configure(node, version, false);
    }

    /**
     * Writes the settings that do not exist on every version in a mixed cluster. A node that does not know a setting
     * rejects it and fails to start, so the federation gate and the local-disk allowlist are set per node rather than
     * cluster-wide. Any node can coordinate a query and federation has to agree across the cluster, so every node that
     * knows the setting gets it; a build that predates it registers federation unconditionally and has it on without one.
     */
    private static void configureNode(LocalNodeSpecBuilder node, Version version, boolean detached, Supplier<String> localAllowedPath) {
        if (detached == false && version.onOrAfter(Version.V_9_5_0)) {
            node.setting(FEDERATION_ENABLED_SETTING, "true");
        }
        if (detached == false && localAllowedPath != null && version.onOrAfter(Version.V_9_5_0)) {
            node.setting(localAllowedPathsSetting(version), localAllowedPath);
        }
    }

    /**
     * Returns the old version supplied by Gradle.
     */
    public static Version bwcVersion() {
        String value = System.getProperty(OLD_CLUSTER_VERSION_PROPERTY);
        if (value == null) {
            value = System.getProperty(SERVERLESS_BWC_VERSION_PROPERTY);
        }
        if (value == null) {
            throw new IllegalStateException("BWC version system property is not set");
        }
        return Version.fromString(value.replace("-SNAPSHOT", ""));
    }

    /**
     * Snapshot mode of the current build, supplied explicitly by Gradle.
     */
    public static boolean currentBuildSnapshot() {
        return requiredBooleanProperty(CURRENT_SNAPSHOT_PROPERTY);
    }

    /**
     * Snapshot mode of the old distribution, supplied explicitly by Gradle.
     */
    public static boolean oldBuildSnapshot() {
        return requiredBooleanProperty(OLD_SNAPSHOT_PROPERTY);
    }

    /**
     * Whether the versioned task pins requests to old nodes.
     */
    public static boolean oldCoordinator() {
        return switch (requiredProperty(COORDINATOR_PROPERTY)) {
            case "old" -> true;
            case "current" -> false;
            default -> throw new IllegalArgumentException(
                "Unknown data-source BWC coordinator [" + System.getProperty(COORDINATOR_PROPERTY) + "]"
            );
        };
    }

    /**
     * Selects live nodes matching either the old or current version.
     */
    public static List<Node> nodesForCoordinator(ObjectPath nodesInfo, boolean oldCoordinator) throws IOException {
        Map<String, Object> nodes = nodesInfo.evaluate("nodes");
        List<Node> selected = new ArrayList<>();
        for (String id : nodes.keySet()) {
            Version version = Version.fromString(((String) nodesInfo.evaluate("nodes." + id + ".version")).replace("-SNAPSHOT", ""));
            if (version.equals(bwcVersion()) == oldCoordinator) {
                selected.add(
                    new Node(
                        nodesInfo.evaluate("nodes." + id + ".name"),
                        version,
                        nodesInfo.evaluate("nodes." + id + ".http.publish_address")
                    )
                );
            }
        }
        if (selected.isEmpty()) {
            throw new IllegalStateException(
                "No " + (oldCoordinator ? "old" : "current") + " nodes found in mixed cluster for BWC version [" + bwcVersion() + "]"
            );
        }
        return List.copyOf(selected);
    }

    /**
     * Returns the comma-separated HTTP addresses for the coordinator selected by Gradle.
     */
    public static String httpAddressesForCoordinator(ObjectPath nodesInfo) throws IOException {
        return httpAddressesForCoordinator(nodesInfo, oldCoordinator());
    }

    /**
     * Returns the comma-separated HTTP addresses for the requested version.
     */
    public static String httpAddressesForCoordinator(ObjectPath nodesInfo, boolean oldCoordinator) throws IOException {
        return String.join(",", nodesForCoordinator(nodesInfo, oldCoordinator).stream().map(Node::httpAddress).toList());
    }

    /**
     * Guard that skips unsupported/FIPS BWC executions before fixtures and clusters start.
     */
    public static TestRule outerBwcGuard(Version minimumVersion) {
        return (base, description) -> new Statement() {
            @Override
            public void evaluate() throws Throwable {
                if (isBwcTest()) {
                    Assume.assumeFalse(
                        "FIPS mode requires security enabled; data-source BWC fixtures use plain HTTP",
                        ESTestCase.inFipsJvm()
                    );
                    Assume.assumeTrue("data-source BWC coverage starts at " + minimumVersion, bwcVersion().onOrAfter(minimumVersion));
                    Assume.assumeTrue("requires distinguishable old and current nodes", bwcVersion().before(Version.CURRENT));
                }
                base.evaluate();
            }
        };
    }

    /**
     * The local-disk allowlist setting under the name a node of this version knows: it shipped in 9.5.0 as
     * {@code esql.datasource.local_allowed_paths} and was renamed to {@code esql.external.local_allowed_paths} in 9.6.0,
     * where the old name survives only as the deprecated fallback key of {@code ExternalSourceSettings}.
     */
    public static String localAllowedPathsSetting(Version version) {
        return version.onOrAfter(Version.V_9_6_0) ? "esql.external.local_allowed_paths" : "esql.datasource.local_allowed_paths";
    }

    private static boolean supportsRetryOnShardFailures(Version version) {
        return version.onOrAfter(Version.V_9_1_0) || (version.onOrAfter(Version.V_8_19_0) && version.before(Version.V_9_0_0));
    }

    private static boolean requiredBooleanProperty(String name) {
        String value = requiredProperty(name);
        if ("true".equals(value) == false && "false".equals(value) == false) {
            throw new IllegalArgumentException("System property [" + name + "] must be true or false but was [" + value + "]");
        }
        return Boolean.parseBoolean(value);
    }

    private static String requiredProperty(String name) {
        String value = System.getProperty(name);
        if (value == null || value.isBlank()) {
            throw new IllegalStateException("Required system property [" + name + "] is not set");
        }
        return value;
    }

    /**
     * One node returned by {@code GET /_nodes}.
     */
    public record Node(String name, Version version, String httpAddress) {}
}
