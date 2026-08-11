/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.qa.mixed;

import com.carrotsearch.randomizedtesting.annotations.ThreadLeakFilters;

import org.elasticsearch.Version;
import org.elasticsearch.client.Request;
import org.elasticsearch.test.TestClustersThreadFilter;
import org.elasticsearch.test.cluster.ElasticsearchCluster;
import org.elasticsearch.xpack.esql.CsvSpecReader.CsvTestCase;
import org.elasticsearch.xpack.esql.CsvTestUtils;
import org.elasticsearch.xpack.esql.action.EsqlCapabilities;
import org.elasticsearch.xpack.esql.qa.rest.EsqlSpecTestCase;
import org.junit.Before;
import org.junit.ClassRule;

import java.io.IOException;
import java.nio.file.Path;
import java.util.List;
import java.util.Map;

import static org.elasticsearch.xpack.esql.action.EsqlCapabilities.Cap.JOIN_LOOKUP_V12;
import static org.elasticsearch.xpack.esql.qa.rest.RestEsqlTestCase.hasCapabilities;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.not;
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
 * The cluster is declared with {@code shared(true)}, so it starts once per JVM and is reused by
 * every generated subclass; test data is ingested exactly once, guarded by the {@code INGEST}
 * lock in {@link EsqlSpecTestCase}.
 */
@ThreadLeakFilters(filters = TestClustersThreadFilter.class)
public abstract class AbstractMixedClusterEsqlOldCoordinatorSpecIT extends EsqlSpecTestCase {
    private static final Path CSV_DATA_PATH = CsvTestUtils.createCsvDataDirectory();

    @ClassRule
    public static ElasticsearchCluster cluster = Clusters.mixedVersionCluster(CSV_DATA_PATH, true);

    private static boolean coordinatorVersionChecked = false;

    private static final Version bwcVersion = Version.fromString(
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
     * Checks that the pinned coordinator really is an old node. Node 0 is old only because
     * {@link Clusters#mixedVersionCluster} declares its nodes as old, current, old, current, and that
     * ordering lives in a shared file. Reordering those {@code withNode} calls, or returning more than
     * one address from {@link #getTestRestCluster()}, would otherwise reduce this suite to a copy of
     * {@link AbstractMixedClusterEsqlSpecIT} with every test still passing.
     * <p>
     * Runs once per JVM, and the flag is set before the assertion so a broken invariant surfaces once
     * rather than repeating for every test sharing the cluster. Skipped for detached builds: a node built
     * from a git ref reports the version it will become, which can equal {@link Version#CURRENT} even
     * though it is the older node.
     */
    @Before
    public void assertCoordinatorIsOldNode() throws IOException {
        if (coordinatorVersionChecked) {
            return;
        }
        coordinatorVersionChecked = true;
        Map<String, Object> info = entityAsMap(client().performRequest(new Request("GET", "/")));
        String coordinatorVersion = (String) ((Map<?, ?>) info.get("version")).get("number");
        logger.info("old-coordinator suite pinned to node [{}] version [{}]", info.get("name"), coordinatorVersion);
        if (System.getProperty("tests.bwc.refspec.main") != null) {
            return;
        }
        assertThat(
            "coordinator must be an old node; check the withNode order in Clusters.mixedVersionCluster",
            coordinatorVersion,
            not(equalTo(Version.CURRENT.toString()))
        );
    }

    /**
     * Routes all queries through old node 0, making it the deterministic coordinator. This works because
     * {@code TransportEsqlQueryAction} is a {@link org.elasticsearch.action.support.HandledTransportAction},
     * so the node that receives {@code POST /_query} plans and coordinates it. Returning more than one
     * address here would hand coordination back to a random node and silently make this suite a duplicate
     * of {@link AbstractMixedClusterEsqlSpecIT}.
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
        super.shouldSkipTest(testName);
        CsvTestUtils.assumeTrueLogging(
            "Slice indexing is unreleased and unsupported on the older mixed-cluster node",
            slicesSupportedOnBwcNode()
                || testCase.requiredCapabilities.contains(EsqlCapabilities.Cap.METADATA_SLICE.capabilityName()) == false
        );
        // missing_capability_coordinator is deliberately not evaluated here, even though pinning the coordinator would
        // allow it: such a test only runs while a wired BWC version falls between the two capabilities it names, so the
        // coverage is narrow and lapses without any signal once that version rotates out.
        CsvTestUtils.assumeTrueLogging(
            "Mixed-cluster tests don't support local cluster capability requirements",
            testCase.missingCapabilitiesLocalCluster.isEmpty()
        );
        CsvTestUtils.assumeTrueLogging(
            "Mixed-cluster tests don't support remote cluster capability requirements",
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

    /**
     * A mixed-version cluster can emit the same warning from more than one node, and this suite targets
     * old-coordinator behaviour rather than warning counts, so exact-match warning checks would fail for
     * reasons unrelated to what it tests.
     */
    @Override
    protected boolean deduplicateExactWarnings() {
        return true;
    }
}
