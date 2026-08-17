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
import org.elasticsearch.common.util.concurrent.ThreadContext;
import org.elasticsearch.test.TestClustersThreadFilter;
import org.elasticsearch.test.cluster.ElasticsearchCluster;
import org.elasticsearch.test.rest.ObjectPath;
import org.elasticsearch.xpack.esql.CsvSpecReader.CsvTestCase;
import org.elasticsearch.xpack.esql.CsvTestUtils;
import org.elasticsearch.xpack.esql.EsqlTestUtils;
import org.elasticsearch.xpack.esql.action.EsqlCapabilities;
import org.elasticsearch.xpack.esql.expression.function.EsqlFunctionRegistry;
import org.elasticsearch.xpack.esql.qa.rest.EsqlSpecTestCase;
import org.junit.ClassRule;

import java.io.IOException;
import java.io.UncheckedIOException;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.stream.Collectors;

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

        failIfQueryUsesFunctionMissingOnOldNode(testName);
    }

    private void failIfQueryUsesFunctionMissingOnOldNode(String testName) {
        Set<String> functionCapabilities = functionCapabilitiesIgnoringParseWarnings(testCase.query);
        if (functionCapabilities.isEmpty()) {
            return;
        }
        try {
            if (oldNodeMissingAnyOf(List.copyOf(functionCapabilities)) == false) {
                // The older node has every function, or the answer was undeterminable: nothing to enforce.
                return;
            }
            if (oldNodeSupportsFunctionCapabilities() == false) {
                // Nodes before 9.4 advertise no fn_ capabilities, so a missing function is indistinguishable there
                // from a missing mechanism.
                return;
            }
            List<String> missing = new ArrayList<>();
            for (String capability : functionCapabilities) {
                if (oldNodeMissingAnyOf(List.of(capability))) {
                    missing.add(capability);
                }
            }
            if (missing.isEmpty() == false) {
                fail(
                    "BWC Test ["
                        + testName
                        + "] calls function(s) the older node ["
                        + bwcVersion
                        + "] does not have and is not gated to skip there, so it flakes with \"Unknown function\".\n"
                        + "Add to the test:\n"
                        + missing.stream().map(c -> "required_capability: " + c).collect(Collectors.joining("\n"))
                );
            }
        } catch (IOException e) {
            // Skip rather than introduce a new flake when the older node is unreachable.
        }
    }

    /**
     * {@link EsqlTestUtils#functionCapabilitiesUsedBy} parses the query in this JVM, and parsing a deprecated construct
     * (e.g. INLINESTATS) emits a {@code HeaderWarning} onto the test thread that {@code ensureNoWarnings()} would blame
     * on the test. Stash the context around the parse so that warning is discarded. A null {@code threadContext} means
     * the warnings check is disabled, so nothing is registered to leak into and a plain call is safe.
     */
    private Set<String> functionCapabilitiesIgnoringParseWarnings(String query) {
        if (threadContext == null) {
            return EsqlTestUtils.functionCapabilitiesUsedBy(query);
        }
        try (ThreadContext.StoredContext ignored = threadContext.stashContext()) {
            return EsqlTestUtils.functionCapabilitiesUsedBy(query);
        }
    }

    private boolean oldNodeSupportsFunctionCapabilities() throws IOException {
        // Nodes before 9.4 have no fn_ capability mechanism; probe a long-established function to detect that.
        return oldNodeHasAllOf(List.of(EsqlFunctionRegistry.functionCapabilityName("count")));
    }

    /**
     * {@code true} only if the older node definitively lacks at least one capability. An unknown answer counts as
     * not-missing, so the guard fails closed rather than introduce a new flake.
     */
    private boolean oldNodeMissingAnyOf(List<String> capabilities) throws IOException {
        return oldNodeAdvertises(capabilities).equals(Optional.of(false));
    }

    /** {@code true} only if the older node definitively advertises every capability. */
    private boolean oldNodeHasAllOf(List<String> capabilities) throws IOException {
        return oldNodeAdvertises(capabilities).orElse(false);
    }

    private Optional<Boolean> oldNodeAdvertises(List<String> capabilities) throws IOException {
        // adminClient() answers for the old node: _capabilities ANDs across all nodes and the current node has every fn_
        // capability, so a false can only come from the old one. Uncached, so a transient failure can't stick for the run.
        return clusterHasCapability(adminClient(), "POST", "/_query", List.of(), capabilities);
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
