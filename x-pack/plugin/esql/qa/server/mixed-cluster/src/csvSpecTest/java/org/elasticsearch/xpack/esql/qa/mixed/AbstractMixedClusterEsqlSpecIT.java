/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.qa.mixed;

import com.carrotsearch.randomizedtesting.annotations.ThreadLeakFilters;

import org.apache.http.HttpHost;
import org.elasticsearch.Version;
import org.elasticsearch.client.Request;
import org.elasticsearch.client.RestClient;
import org.elasticsearch.test.TestClustersThreadFilter;
import org.elasticsearch.test.cluster.ElasticsearchCluster;
import org.elasticsearch.test.rest.ObjectPath;
import org.elasticsearch.xpack.esql.CsvSpecReader.CsvTestCase;
import org.elasticsearch.xpack.esql.CsvTestUtils;
import org.elasticsearch.xpack.esql.action.EsqlCapabilities;
import org.elasticsearch.xpack.esql.qa.rest.EsqlSpecTestCase;
import org.junit.ClassRule;

import java.io.IOException;
import java.io.UncheckedIOException;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;

import static org.elasticsearch.xpack.esql.CsvTestUtils.isEnabled;
import static org.elasticsearch.xpack.esql.action.EsqlCapabilities.Cap.JOIN_LOOKUP_V12;
import static org.elasticsearch.xpack.esql.qa.rest.RestEsqlTestCase.hasCapabilities;

/**
 * Shared behaviour for the mixed-version csv-spec suites. Subclasses differ only in which nodes they route
 * queries to, and therefore which version coordinates:
 * {@link AbstractMixedClusterEsqlCurrCoordSpecIT} takes the current-version nodes and
 * {@link AbstractMixedClusterEsqlOldCoordSpecIT} the old ones. Between them both coordinator versions
 * are exercised on every run.
 * <p>
 * Pinning by version works because {@code TransportEsqlQueryAction} is a
 * {@link org.elasticsearch.action.support.HandledTransportAction}: the node that receives
 * {@code POST /_query} plans and coordinates it. Each subclass resolves hosts via
 * {@link #httpAddressesForCoordinator(boolean)} from {@code GET /_nodes} (not from
 * {@code withNode} declaration order), so both nodes of that version take a turn while rotation stays
 * within one version and the client keeps a spare host.
 * <p>
 * The cluster is declared with {@code shared(true)}, so it starts once per JVM and is reused by every
 * generated subclass; test data is ingested exactly once, guarded by the {@code INGEST} lock in
 * {@link EsqlSpecTestCase}. The two suites run under separate Gradle tasks, hence separate JVMs, so each
 * gets its own cluster from this one {@code @ClassRule}.
 */
// Each generated class covers one csv-spec file and should complete in a few minutes at most
@ThreadLeakFilters(filters = TestClustersThreadFilter.class)
public abstract class AbstractMixedClusterEsqlSpecIT extends EsqlSpecTestCase {
    private static final Path CSV_DATA_PATH = CsvTestUtils.createCsvDataDirectory();

    @ClassRule
    public static ElasticsearchCluster cluster = Clusters.mixedVersionCluster(CSV_DATA_PATH, true);

    static final Version bwcVersion = Version.fromString(
        System.getProperty("tests.old_cluster_version") != null
            ? System.getProperty("tests.old_cluster_version").replace("-SNAPSHOT", "")
            : null
    );

    protected AbstractMixedClusterEsqlSpecIT(
        String fileName,
        String groupName,
        String testName,
        Integer lineNumber,
        CsvTestCase testCase,
        String instructions
    ) {
        super(fileName, groupName, testName, lineNumber, maybeOptionalWarnings(testCase), instructions);
    }

    private static CsvTestCase maybeOptionalWarnings(CsvTestCase testCase) {
        // To make warnings optional for some version, uncomment this. Tests might also
        // need changing, but that's fine.
        // if (bwcVersion.before(Version.V_9_6_0)) {
        // testCase.makeWarningsOptional();
        // }
        return testCase;
    }

    /**
     * Re-declared abstract so every subclass must state which nodes it coordinates on, rather than silently
     * inheriting a default.
     */
    @Override
    protected abstract String getTestRestCluster();

    /**
     * HTTP hosts for one coordinator version, resolved from live {@code GET /_nodes} rather than
     * {@link Clusters#mixedVersionCluster} {@code withNode} order. Old hosts are those whose version matches
     * {@code tests.old_cluster_version}; current hosts are the rest. Snapshot suffixes are stripped so the
     * comparison works on PR builds.
     * <p>
     * Cannot use {@link #adminClient()}: {@link #getTestRestCluster()} runs from {@code initClient} before
     * that client exists, so this opens a short-lived probe against every node address.
     */
    protected final String httpAddressesForCoordinator(boolean oldCoordinator) {
        HttpHost[] allHosts = parseClusterHosts(cluster.getHttpAddresses()).toArray(HttpHost[]::new);
        try (RestClient probe = buildClient(restAdminSettings(), allHosts)) {
            ObjectPath nodes = ObjectPath.createFromResponse(probe.performRequest(new Request("GET", "/_nodes")));
            Map<String, Object> nodesMap = nodes.evaluate("nodes");
            List<String> selected = new ArrayList<>();
            for (String id : nodesMap.keySet()) {
                String version = nodes.evaluate("nodes." + id + ".version");
                boolean isOld = Version.fromString(version.replace("-SNAPSHOT", "")).equals(bwcVersion);
                if (isOld == oldCoordinator) {
                    HttpHost host = HttpHost.create(nodes.evaluate("nodes." + id + ".http.publish_address"));
                    selected.add(host.getHostName() + ":" + host.getPort());
                }
            }
            if (selected.isEmpty()) {
                throw new IllegalStateException(
                    "No " + (oldCoordinator ? "old" : "current") + " nodes found in mixed cluster for BWC version [" + bwcVersion + "]"
                );
            }
            return String.join(",", selected);
        } catch (IOException e) {
            throw new UncheckedIOException("Failed to resolve coordinator addresses from /_nodes", e);
        }
    }

    @Override
    protected Path getCsvDataPath() {
        return CSV_DATA_PATH;
    }

    // Slice indexing is unreleased and renamed its request parameter (`_slice` -> `slice`), so the older bwc node
    // cannot ingest data via the current parameter. Treat slice as unsupported whenever an older node is present.
    private static boolean slicesSupportedOnBwcNode() {
        return bwcVersion == null || bwcVersion.before(Version.CURRENT) == false;
    }

    @Override
    protected boolean clusterHasCapability(EsqlCapabilities.Cap capability) {
        // Skip loading the slice dataset in a mixed-version cluster: the older node cannot ingest it.
        if (capability == EsqlCapabilities.Cap.METADATA_SLICE && slicesSupportedOnBwcNode() == false) {
            return false;
        }
        return super.clusterHasCapability(capability);
    }

    @Override
    protected void shouldSkipTest(String testName) throws IOException {
        super.shouldSkipTest(testName);
        // Slice tests need the slice dataset, which is not loaded against an older node (see clusterHasCapability).
        CsvTestUtils.assumeTrueLogging(
            "Slice indexing is unreleased and unsupported on the older mixed-cluster node",
            slicesSupportedOnBwcNode()
                || testCase.requiredCapabilities.contains(EsqlCapabilities.Cap.METADATA_SLICE.capabilityName()) == false
        );
        // Stricter than the checkCapabilities call in super.shouldSkipTest, which falls back to testFeatureService when
        // a capability is not reported. adminClient() suffices regardless of which nodes a subclass targets, because
        // _capabilities fans out to every node and ANDs the result, so the answer does not depend on the target.
        CsvTestUtils.assumeTrueLogging(
            "Old mixed-cluster node does not support required capabilities for " + testName,
            testCase.requiredCapabilities.isEmpty() || hasCapabilities(adminClient(), testCase.requiredCapabilities)
        );
        // missing_capability_coordinator is deliberately not evaluated, even though pinning the coordinator would
        // allow it: such a test only runs while a wired BWC version falls between the two capabilities it names, so
        // the coverage is narrow and lapses without any signal once that version rotates out.
        CsvTestUtils.assumeTrueLogging(
            "Mixed-cluster tests don't support local cluster capability requirements",
            testCase.missingCapabilitiesLocalCluster.isEmpty()
        );
        // Populated by missing_capability_data_node. Shard placement across the old and new nodes is not controlled,
        // so which version serves the data cannot be pinned.
        CsvTestUtils.assumeTrueLogging(
            "Mixed-cluster tests don't support remote cluster capability requirements",
            testCase.missingCapabilitiesRemoteCluster.isEmpty()
        );
        assumeTrue("Test " + testName + " is skipped on " + bwcVersion, isEnabled(testName, instructions, bwcVersion));
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
     * A mixed-version cluster can emit the same warning from more than one node, and these suites target
     * coordinator behaviour rather than warning counts, so exact-match warning checks would fail for reasons
     * unrelated to what they test.
     */
    @Override
    protected boolean deduplicateExactWarnings() {
        return true;
    }
}
