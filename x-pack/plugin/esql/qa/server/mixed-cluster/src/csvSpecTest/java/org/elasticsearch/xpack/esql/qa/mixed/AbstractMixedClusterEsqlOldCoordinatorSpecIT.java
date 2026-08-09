/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.qa.mixed;

import com.carrotsearch.randomizedtesting.annotations.ThreadLeakFilters;

import org.elasticsearch.Version;
import org.elasticsearch.test.TestClustersThreadFilter;
import org.elasticsearch.test.cluster.ElasticsearchCluster;
import org.elasticsearch.xpack.esql.CsvSpecReader.CsvTestCase;
import org.elasticsearch.xpack.esql.CsvTestUtils;
import org.elasticsearch.xpack.esql.action.EsqlCapabilities;
import org.elasticsearch.xpack.esql.qa.rest.EsqlSpecTestCase;
import org.junit.ClassRule;

import java.io.IOException;
import java.nio.file.Path;
import java.util.List;

import static org.elasticsearch.xpack.esql.action.EsqlCapabilities.Cap.JOIN_LOOKUP_V12;
import static org.elasticsearch.xpack.esql.qa.rest.RestEsqlTestCase.doesntHaveCapabilities;
import static org.elasticsearch.xpack.esql.qa.rest.RestEsqlTestCase.hasCapabilities;
import static org.junit.Assume.assumeTrue;

/**
 * Runs csv-spec tests in a mixed-version cluster with the old node pinned as coordinator.
 * <p>
 * {@link AbstractMixedClusterEsqlSpecIT} routes queries to all four nodes so the coordinator
 * is random (~50% old, ~50% new). This suite routes every query to node 0, which runs the
 * old version, making the coordinator deterministically old. This catches regressions where
 * a new-node data node sends a capability-gated response fragment to an old coordinator that
 * cannot deserialize it.
 * <p>
 * Because the coordinator is deterministically old, {@code missing_capability_coordinator}
 * directives in csv-spec files are supported here: the test runs only when the old node is
 * genuinely missing the listed capabilities, and is skipped when it already has them.
 */
@ThreadLeakFilters(filters = TestClustersThreadFilter.class)
public abstract class AbstractMixedClusterEsqlOldCoordinatorSpecIT extends EsqlSpecTestCase {
    private static final Path CSV_DATA_PATH = CsvTestUtils.createCsvDataDirectory();

    @ClassRule
    public static ElasticsearchCluster cluster = Clusters.mixedVersionCluster(CSV_DATA_PATH, true);

    static final Version bwcVersion = Version.fromString(
        System.getProperty("tests.old_cluster_version") != null
            ? System.getProperty("tests.old_cluster_version").replace("-SNAPSHOT", "")
            : null
    );

    protected AbstractMixedClusterEsqlOldCoordinatorSpecIT(
        String fileName,
        String groupName,
        String testName,
        Integer lineNumber,
        CsvTestCase testCase,
        String instructions
    ) {
        super(fileName, groupName, testName, lineNumber, testCase, instructions);
    }

    @Override
    protected Path getCsvDataPath() {
        return CSV_DATA_PATH;
    }

    /**
     * Routes all queries through old node 0, making it the deterministic coordinator.
     */
    @Override
    protected String getTestRestCluster() {
        return cluster.getHttpAddress(0);
    }

    // Slice indexing is unreleased and renamed its request parameter (_slice -> slice), so the
    // older bwc node cannot ingest data via the current parameter. Treat slice as unsupported
    // whenever an older node is present.
    private static boolean slicesSupportedOnBwcNode() {
        return bwcVersion == null || bwcVersion.before(Version.CURRENT) == false;
    }

    @Override
    protected boolean clusterHasCapability(EsqlCapabilities.Cap capability) {
        if (capability == EsqlCapabilities.Cap.METADATA_SLICE && slicesSupportedOnBwcNode() == false) {
            return false;
        }
        return super.clusterHasCapability(capability);
    }

    @Override
    protected void shouldSkipTest(String testName) throws IOException {
        // Delegates to EsqlSpecTestCase: checks requiredCapabilities and requiredCapabilitiesLocalCluster
        // both against the old node (because getTestRestCluster() returns only old-node addresses,
        // adminClient() routes there exclusively).
        super.shouldSkipTest(testName);
        CsvTestUtils.assumeTrueLogging(
            "Slice indexing is unreleased and unsupported on the older mixed-cluster node",
            slicesSupportedOnBwcNode()
                || testCase.requiredCapabilities.contains(EsqlCapabilities.Cap.METADATA_SLICE.capabilityName()) == false
        );
        // With the coordinator pinned to the old node, missing_capability_coordinator directives are
        // meaningful. Run the test when the old coordinator is missing all listed capabilities; skip
        // if it already supports any of them (the test's precondition is not met).
        CsvTestUtils.assumeTrueLogging(
            "Old coordinator already supports capabilities that should be absent: " + testCase.missingCapabilitiesLocalCluster,
            doesntHaveCapabilities(adminClient(), testCase.missingCapabilitiesLocalCluster)
        );
        // This suite has no remote cluster, so remote-cluster capability requirements cannot be evaluated.
        CsvTestUtils.assumeTrueLogging(
            "Old-coordinator mixed-cluster suite has no remote cluster",
            testCase.missingCapabilitiesRemoteCluster.isEmpty()
        );
        assumeTrue("Test " + testName + " is skipped on " + bwcVersion, CsvTestUtils.isEnabled(testName, instructions, bwcVersion));
    }

    @Override
    protected boolean enableRoundingDoubleValuesOnAsserting() {
        return true;
    }

    @Override
    protected boolean supportsSemanticTextInference() {
        return false;
    }

    @Override
    protected boolean supportsInferenceTestServiceOnLocalCluster() {
        return false;
    }

    @Override
    protected boolean supportsIndexModeLookup() {
        return hasCapabilities(adminClient(), List.of(JOIN_LOOKUP_V12.capabilityName()));
    }

    @Override
    protected boolean supportsSourceFieldMapping() {
        return false;
    }

    @Override
    protected boolean deduplicateExactWarnings() {
        return true;
    }
}
