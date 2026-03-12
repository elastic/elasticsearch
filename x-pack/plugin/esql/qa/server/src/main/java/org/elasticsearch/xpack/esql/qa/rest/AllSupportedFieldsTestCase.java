/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.qa.rest;

import com.carrotsearch.randomizedtesting.annotations.ParametersFactory;

import org.apache.http.HttpHost;
import org.apache.http.util.EntityUtils;
import org.elasticsearch.Build;
import org.elasticsearch.TransportVersion;
import org.elasticsearch.client.Request;
import org.elasticsearch.client.RequestOptions;
import org.elasticsearch.client.Response;
import org.elasticsearch.client.ResponseException;
import org.elasticsearch.client.RestClient;
import org.elasticsearch.common.Strings;
import org.elasticsearch.common.logging.LoggerMessageFormat;
import org.elasticsearch.common.xcontent.XContentHelper;
import org.elasticsearch.core.Tuple;
import org.elasticsearch.exponentialhistogram.ExponentialHistogram;
import org.elasticsearch.exponentialhistogram.ExponentialHistogramCircuitBreaker;
import org.elasticsearch.exponentialhistogram.ExponentialHistogramXContent;
import org.elasticsearch.index.IndexMode;
import org.elasticsearch.index.mapper.MappedFieldType;
import org.elasticsearch.test.ListMatcher;
import org.elasticsearch.test.MapMatcher;
import org.elasticsearch.test.rest.ESRestTestCase;
import org.elasticsearch.xcontent.XContentBuilder;
import org.elasticsearch.xcontent.json.JsonXContent;
import org.elasticsearch.xpack.esql.core.type.DataType;
import org.hamcrest.Matcher;
import org.junit.Before;
import org.junit.Rule;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.Comparator;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Set;
import java.util.TreeMap;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

import static org.elasticsearch.common.xcontent.support.XContentMapValues.extractValue;
import static org.elasticsearch.test.ListMatcher.matchesList;
import static org.elasticsearch.test.MapMatcher.assertMap;
import static org.elasticsearch.test.MapMatcher.matchesMap;
import static org.elasticsearch.xpack.esql.action.EsqlResolveFieldsResponse.RESOLVE_FIELDS_RESPONSE_CREATED_TV;
import static org.elasticsearch.xpack.esql.action.EsqlResolveFieldsResponse.RESOLVE_FIELDS_RESPONSE_USED_TV;
import static org.elasticsearch.xpack.esql.core.type.DataType.DATE_RANGE;
import static org.elasticsearch.xpack.esql.core.type.DataType.HISTOGRAM;
import static org.elasticsearch.xpack.esql.enrich.EnrichPolicyResolver.ESQL_USE_MINIMUM_VERSION_FOR_ENRICH_RESOLUTION;
import static org.hamcrest.Matchers.any;
import static org.hamcrest.Matchers.anyOf;
import static org.hamcrest.Matchers.containsString;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.instanceOf;
import static org.hamcrest.Matchers.nullValue;
import static org.hamcrest.Matchers.startsWith;

/**
 * Queries like {@code FROM * | KEEP *} can include columns of unsupported types,
 * and we can run into serialization and correctness issues in mixed version clusters/CCS
 * when support for a type is added in a later version.
 * <p>
 * This creates indices with all index-able fields and fetches values from them to
 * confirm that we correctly handle data types, even if they were introduced in later versions.
 * Generally, this means that a type is treated as unsupported if any older node is involved.
 * See {@link org.elasticsearch.xpack.esql.session.Versioned} for more details on how ESQL
 * handles planning for mixed version clusters.
 * <p>
 *     This suite is entirely skipped in snapshot builds; data types that are under
 *     construction are normally tested well enough in spec tests, skipping
 *     old versions via {@link org.elasticsearch.xpack.esql.action.EsqlCapabilities}.
 * <p>
 *     In a single cluster where all nodes are on a single version this is
 *     just an "is it plugged in" style smoke test. In a mixed version cluster
 *     this is testing the behavior of fetching potentially unsupported field
 *     types. The same is true for multi-cluster cases.
 * <p>
 *     This isn't trying to test complex interactions with field loading so we
 *     load constant field values and have simple mappings.
 */
public class AllSupportedFieldsTestCase extends ESRestTestCase {

    private static final RequestOptions DEPRECATED_DEFAULT_METRIC_WARNING_HANDLER = RequestOptions.DEFAULT.toBuilder()
        .setWarningsHandler(warnings -> {
            if (warnings.isEmpty()) {
                return false;
            } else {
                for (String warning : warnings) {
                    if ("Parameter [default_metric] is deprecated and will be removed in a future version".equals(warning) == false) {
                        return true;
                    }
                }
                return false;
            }
        })
        .build();

    @Rule(order = Integer.MIN_VALUE)
    public ProfileLogger profileLogger = new ProfileLogger();

    @ParametersFactory(argumentFormatting = "pref=%s mode=%s")
    public static List<Object[]> args() {
        List<Object[]> args = new ArrayList<>();
        for (MappedFieldType.FieldExtractPreference extractPreference : Arrays.asList(
            null,
            MappedFieldType.FieldExtractPreference.NONE,
            MappedFieldType.FieldExtractPreference.STORED
        )) {
            for (IndexMode indexMode : IndexMode.values()) {
                args.add(new Object[] { extractPreference, indexMode });
            }
        }
        return args;
    }

    private final MappedFieldType.FieldExtractPreference extractPreference;
    private final IndexMode indexMode;

    protected AllSupportedFieldsTestCase(MappedFieldType.FieldExtractPreference extractPreference, IndexMode indexMode) {
        this.extractPreference = extractPreference;
        this.indexMode = indexMode;
    }

    protected IndexMode indexMode() {
        return indexMode;
    }

    protected record NodeInfo(
        String cluster,
        String id,
        boolean snapshot,
        TransportVersion version,
        Set<String> roles,
        Set<HttpHost> boundAddress
    ) {}

    private static Map<String, NodeInfo> nodeToInfo;

    private Map<String, NodeInfo> localNodeToInfo() throws IOException {
        if (nodeToInfo == null) {
            nodeToInfo = fetchNodeToInfo(client(), null);
        }
        return nodeToInfo;
    }

    private static Boolean denseVectorAggMetricDoubleIfFns;

    private boolean denseVectorAggMetricDoubleIfFns() throws IOException {
        if (denseVectorAggMetricDoubleIfFns == null) {
            denseVectorAggMetricDoubleIfFns = fetchDenseVectorAggMetricDoubleIfFns();
        }
        return denseVectorAggMetricDoubleIfFns;
    }

    protected boolean fetchDenseVectorAggMetricDoubleIfFns() throws IOException {
        return clusterHasCapability("GET", "/_query", List.of(), List.of("DENSE_VECTOR_AGG_METRIC_DOUBLE_IF_FNS")).orElse(false);
    }

    private static Boolean denseVectorAggMetricDoubleIfVersion;

    private boolean denseVectorAggMetricDoubleIfVersion() throws IOException {
        if (denseVectorAggMetricDoubleIfVersion == null) {
            denseVectorAggMetricDoubleIfVersion = fetchDenseVectorAggMetricDoubleIfVersion();
        }
        return denseVectorAggMetricDoubleIfVersion;
    }

    protected boolean fetchDenseVectorAggMetricDoubleIfVersion() throws IOException {
        return clusterHasCapability("GET", "/_query", List.of(), List.of("DENSE_VECTOR_AGG_METRIC_DOUBLE_IF_VERSION")).orElse(false);
    }

    protected boolean lookupJoinOnAllIndicesSupported() throws IOException {
        return true;
    }

    private static Boolean supportsNodeAssignment;

    protected boolean supportsNodeAssignment() throws IOException {
        if (supportsNodeAssignment == null) {
            supportsNodeAssignment = allNodeToInfo().values()
                .stream()
                .allMatch(i -> (i.roles.contains("index") && i.roles.contains("search")) || (i.roles.contains("data")));
        }
        return supportsNodeAssignment;
    }

    /**
     * Map from node name to information about the node.
     */
    protected Map<String, NodeInfo> allNodeToInfo() throws IOException {
        return localNodeToInfo();
    }

    protected static Map<String, NodeInfo> fetchNodeToInfo(RestClient client, String cluster) throws IOException {
        Map<String, NodeInfo> nodeToInfo = new TreeMap<>();
        Request request = new Request("GET", "/_nodes");
        Map<String, Object> response = responseAsMap(client.performRequest(request));
        Map<?, ?> nodes = (Map<?, ?>) extractValue(response, "nodes");
        for (Map.Entry<?, ?> n : nodes.entrySet()) {
            String id = (String) n.getKey();
            Map<?, ?> nodeInfo = (Map<?, ?>) n.getValue();
            String nodeName = (String) extractValue(nodeInfo, "name");
            Map<?, ?> http = (Map<?, ?>) extractValue(nodeInfo, "http");
            List<?> unparsedBoundAddress = (List<?>) extractValue(http, "bound_address");
            // The bound address can actually be 2 addresses, one ipv4 and one ipv6; stuff 'em in a set.
            Set<HttpHost> boundAddress = unparsedBoundAddress.stream().map(s -> HttpHost.create((String) s)).collect(Collectors.toSet());

            /*
             * Figuring out if a node is a snapshot is kind of tricky. The main version
             * doesn't include -SNAPSHOT. But ${VERSION}-SNAPSHOT is in the node info
             * *somewhere*. So we do this silly toString here.
             */
            String version = (String) extractValue(nodeInfo, "version");
            boolean snapshot = nodeInfo.toString().contains(version + "-SNAPSHOT");

            TransportVersion transportVersion = TransportVersion.fromId((Integer) extractValue(nodeInfo, "transport_version"));
            List<?> roles = (List<?>) nodeInfo.get("roles");

            nodeToInfo.put(
                nodeName,
                new NodeInfo(
                    cluster,
                    id,
                    snapshot,
                    transportVersion,
                    roles.stream().map(Object::toString).collect(Collectors.toSet()),
                    boundAddress
                )
            );
        }

        return nodeToInfo;
    }

    protected static final String ENRICH_POLICY_NAME = "all_fields_policy";
    protected static final String LOOKUP_INDEX_NAME = "all_fields_lookup_index";

    @Before
    public void createIndices() throws IOException {
        if (supportsNodeAssignment()) {
            for (Map.Entry<String, NodeInfo> e : localNodeToInfo().entrySet()) {
                createIndexForNode(client(), minVersion(), e.getKey(), e.getValue().id(), indexMode);
            }
        } else {
            createIndexForNode(client(), minVersion(), null, null, indexMode);
        }

        // We need a single lookup index that has the same name across all clusters, as well as a single enrich policy per cluster.
        // We create both only when we're testing LOOKUP mode.
        if (indexExists(LOOKUP_INDEX_NAME) == false && indexMode == IndexMode.LOOKUP) {
            createAllTypesIndex(client(), minVersion(), LOOKUP_INDEX_NAME, null, indexMode);
            createAllTypesDoc(client(), minVersion(), LOOKUP_INDEX_NAME);
            createEnrichPolicy(client(), minVersion(), LOOKUP_INDEX_NAME, ENRICH_POLICY_NAME);
        }
    }

    /**
     * Make sure the test doesn't run on builds that have nodes in both snapshot and release builds at the same time, as snapshot builds
     * will assume support for new data types on earlier versions than release builds.
     * <p>
     * Our bwc tests in release mode may still use snapshot builds for older versions that just aren't released yet. E.g. if we run
     * bwc tests against 9.x.1 and this patch version is not yet released because 9.x.1 is going to be the next patch release, its build
     * will be in snapshot mode. But bwc tests with 9.x.0  will consistently use release builds for all nodes if 9.x.0 is already released.
     */
    @Before
    public void skipPartialSnapshots() throws IOException {
        boolean someNodesOnReleaseBuild = false;
        boolean someNodesOnSnapshotBuild = false;
        for (NodeInfo n : allNodeToInfo().values()) {
            if (n.snapshot) {
                someNodesOnSnapshotBuild = true;
            } else {
                someNodesOnReleaseBuild = true;
            }
        }
        assumeFalse(
            "Skipped due to having nodes from both snapshot and release builds",
            someNodesOnReleaseBuild && someNodesOnSnapshotBuild
        );
    }

    /**
     * Is the test running for a single node on snapshot version?
     * If so, we'll assert the profile of the field readers.
     */
    protected boolean isSingleNodeSnapshot() {
        return false;
    }

    public final void testFetchAll() throws IOException {
        doTestFetchAll(fromAllQuery("""
            , _id, _ignored, _index_mode, _score, _source, _version
            | LIMIT 1000
            """), allNodeToInfo(), allNodeToInfo(), isSingleNodeSnapshot());
    }

    public final void testFetchAllEnrich() throws IOException {
        assumeTrue("Test only requires the enrich policy (made from a lookup index)", indexMode == IndexMode.LOOKUP);
        assumeFalse("Test currently not working on snapshot because of range fields", Build.current().isSnapshot());
        // The ENRICH is a no-op because it overwrites columns with the same identical data (except that it messes with
        // the order of the columns, but we don't assert that).
        doTestFetchAll(fromAllQuery(LoggerMessageFormat.format(null, """
            , _id, _ignored, _index_mode, _score, _source, _version
            | ENRICH _remote:{} ON {}
            | LIMIT 1000
            """, ENRICH_POLICY_NAME, LOOKUP_ID_FIELD)), allNodeToInfo(), allNodeToInfo(), false);
    }

    public final void testFetchAllLookupJoin() throws IOException {
        assumeTrue("Test only requires lookup indices", indexMode == IndexMode.LOOKUP);
        assumeTrue("Skipped in CCS tests if old nodes don't support remote lookup joins", lookupJoinOnAllIndicesSupported());
        // The LOOKUP JOIN is a no-op because it overwrites columns with the same identical data (except that it messes with
        // the order of the columns, but we don't assert that).
        // We force the lookup join on to the remotes by having a SORT after it.
        doTestFetchAll(fromAllQuery(LoggerMessageFormat.format(null, """
            , _id, _ignored, _index_mode, _score, _source, _version
            | LOOKUP JOIN {} ON {}
            | SORT _id
            | LIMIT 1000
            """, LOOKUP_INDEX_NAME, LOOKUP_ID_FIELD)), allNodeToInfo(), allNodeToInfo(), false);
    }

    /**
     * Runs the query and expects 1 document per index on the contributing nodes as well as all the columns.
     */
    protected final void doTestFetchAll(
        String query,
        Map<String, NodeInfo> nodesContributingIndices,
        Map<String, NodeInfo> nodesInvolvedInExecution,
        boolean checkProfile
    ) throws IOException {
        var responseAndCoordinatorVersion = runQuery(query);

        Map<String, Object> response = responseAndCoordinatorVersion.v1();
        TransportVersion coordinatorVersion = responseAndCoordinatorVersion.v2();

        assertNoPartialResponse(response);

        List<?> columns = (List<?>) response.get("columns");
        List<?> values = (List<?>) response.get("values");

        MapMatcher expectedColumns = allTypesColumnsMatcher(coordinatorVersion, minVersion(), minVersion(), indexMode, true, true);
        assertMap(nameToType(columns), expectedColumns);

        MapMatcher expectedAllValues = matchesMap();
        for (Map.Entry<String, NodeInfo> e : expectedIndices(indexMode, nodesContributingIndices).entrySet()) {
            String indexName = e.getKey();
            MapMatcher expectedValues = allTypesValuesMatcher(
                coordinatorVersion,
                minVersion(),
                minVersion(),
                indexMode,
                extractPreference,
                true,
                true,
                indexName
            );
            expectedAllValues = expectedAllValues.entry(indexName, expectedValues);
        }
        assertMap(indexToRow(columns, values), expectedAllValues);

        assertMinimumVersion(minVersion(nodesInvolvedInExecution), responseAndCoordinatorVersion, true, fetchAllIsCrossCluster());
        if (checkProfile) {
            assertProfile((Map<?, ?>) response.get("profile"));
        }

        profileLogger.clearProfile();
    }

    protected boolean fetchAllIsCrossCluster() {
        return false;
    }

    protected static void assertNoPartialResponse(Map<String, Object> response) {
        if ((Boolean) response.get("is_partial")) {
            throw new AssertionError("partial results: " + response);
        }
    }

    protected MapMatcher allTypesColumnsMatcher(
        TransportVersion coordinatorVersion,
        TransportVersion minimumVersionAcrossInvolvedNodes,
        TransportVersion minimumVersionAcrossAllNodes,
        IndexMode indexMode,
        boolean expectMetadataFields,
        boolean expectNonEnrichableFields
    ) {
        MapMatcher expectedColumns = matchesMap().entry(LOOKUP_ID_FIELD, "integer");
        for (DataType type : DataType.values()) {
            if (supportedInIndex(type, minimumVersionAcrossAllNodes) == false) {
                continue;
            }
            if (expectNonEnrichableFields == false && supportedInEnrich(type) == false) {
                continue;
            }
            expectedColumns = expectedColumns.entry(
                fieldName(type),
                expectedType(type, coordinatorVersion, minimumVersionAcrossInvolvedNodes, indexMode)
            );
        }
        if (expectMetadataFields) {
            expectedColumns = expectedColumns.entry("_id", "keyword")
                .entry("_ignored", "keyword")
                .entry("_index", "keyword")
                .entry("_index_mode", "keyword")
                .entry("_score", "double")
                .entry("_source", "_source")
                .entry("_version", "long");
        }
        return expectedColumns;
    }

    protected MapMatcher allTypesValuesMatcher(
        TransportVersion coordinatorVersion,
        TransportVersion minimumVersionAcrossInvolvedNodes,
        TransportVersion minimumVersionAcrossAllNodes,
        IndexMode indexMode,
        MappedFieldType.FieldExtractPreference extractPreference,
        boolean expectMetadataFields,
        boolean expectNonEnrichableFields,
        String indexName
    ) {
        MapMatcher expectedValues = matchesMap();
        expectedValues = expectedValues.entry(LOOKUP_ID_FIELD, equalTo(123));
        for (DataType type : DataType.values()) {
            if (supportedInIndex(type, minimumVersionAcrossAllNodes) == false) {
                continue;
            }
            if (expectNonEnrichableFields == false && supportedInEnrich(type) == false) {
                continue;
            }
            expectedValues = expectedValues.entry(
                fieldName(type),
                expectedValue(type, coordinatorVersion, minimumVersionAcrossInvolvedNodes, indexMode, extractPreference)
            );
        }
        if (expectMetadataFields) {
            expectedValues = expectedValues.entry("_id", any(String.class))
                .entry("_ignored", nullValue())
                .entry("_index", indexName)
                .entry("_index_mode", indexMode.toString())
                .entry("_score", 0.0)
                .entry("_source", matchesMap().extraOk())
                .entry("_version", 1);
        }

        return expectedValues;
    }

    /**
     * Tests fetching {@code dense_vector} if possible. Uses the {@code dense_vector_agg_metric_double_if_fns}
     * work around if required.
     */
    public final void testFetchDenseVector() throws IOException {
        Map<String, Object> response;
        try {
            String request = """
                | KEEP _index, f_dense_vector
                | LIMIT 1000
                """;
            if (denseVectorAggMetricDoubleIfVersion() == false) {
                request = """
                    | EVAL k = v_l2_norm(f_dense_vector, [1])  // workaround to enable fetching dense_vector
                    """ + request;
            }
            var responseAndCoordinatorVersion = runQuery(fromAllQuery(request));
            assertMinimumVersionFromAllQueries(responseAndCoordinatorVersion);

            response = runQuery(fromAllQuery(request)).v1();
            if ((Boolean) response.get("is_partial")) {
                Map<?, ?> clusters = (Map<?, ?>) response.get("_clusters");
                Map<?, ?> details = (Map<?, ?>) clusters.get("details");

                boolean foundError = false;
                for (Map.Entry<?, ?> cluster : details.entrySet()) {
                    String failures = cluster.getValue().toString();
                    if (denseVectorAggMetricDoubleIfFns()) {
                        throw new AssertionError("should correctly fetch the dense_vector: " + failures);
                    }
                    foundError |= failures.contains("doesn't understand data type [DENSE_VECTOR]");
                }
                assertTrue("didn't find errors: " + details, foundError);
                return;
            }
        } catch (ResponseException e) {
            if (denseVectorAggMetricDoubleIfFns()) {
                throw new AssertionError("should correctly fetch the dense_vector", e);
            }
            assertThat(
                "old version should fail with this error",
                EntityUtils.toString(e.getResponse().getEntity()),
                anyOf(
                    containsString("Unknown function [v_l2_norm]"),
                    containsString("Cannot use field [f_dense_vector] with unsupported type"),
                    containsString("doesn't understand data type [DENSE_VECTOR]")
                )
            );
            // Failure is expected and fine
            return;
        }
        List<?> columns = (List<?>) response.get("columns");
        List<?> values = (List<?>) response.get("values");

        MapMatcher expectedColumns = matchesMap().entry("f_dense_vector", "dense_vector").entry("_index", "keyword");
        assertMap(nameToType(columns), expectedColumns);

        MapMatcher expectedAllValues = matchesMap();
        for (Map.Entry<String, NodeInfo> e : expectedIndices(indexMode).entrySet()) {
            String indexName = e.getKey();
            MapMatcher expectedValues = matchesMap();
            if (DataType.DENSE_VECTOR.supportedVersion().supportedOn(minVersion(), false)) {
                expectedValues = expectedValues.entry("f_dense_vector", matchesList().item(0.5).item(10.0).item(5.9999995));
            } else {
                // While dense_vector was under construction, we could've also encountered other values here, e.g. [0.04, 0.86, 0.51].
                // We'll ignore the exact value here.
                expectedValues = expectedValues.entry("f_dense_vector", instanceOf(List.class));
            }
            expectedValues = expectedValues.entry("_index", indexName);
            expectedAllValues = expectedAllValues.entry(indexName, expectedValues);
        }
        assertMap(indexToRow(columns, values), expectedAllValues);
    }

    /**
     * Tests fetching {@code aggregate_metric_double} if possible. Uses the {@code dense_vector_agg_metric_double_if_fns}
     * work around if required.
     */
    public final void testFetchAggregateMetricDouble() throws IOException {
        Map<String, Object> response;
        try {
            String request = """
                | EVAL strjunk = TO_STRING(f_aggregate_metric_double)
                | KEEP _index, f_aggregate_metric_double
                | LIMIT 1000
                """;
            if (denseVectorAggMetricDoubleIfVersion() == false) {
                request = """
                    | EVAL junk = TO_AGGREGATE_METRIC_DOUBLE(1)  // workaround to enable fetching aggregate_metric_double
                    """ + request;
            }
            var responseAndCoordinatorVersion = runQuery(fromAllQuery(request));
            assertMinimumVersionFromAllQueries(responseAndCoordinatorVersion);

            response = runQuery(fromAllQuery(request)).v1();
            if ((Boolean) response.get("is_partial")) {
                Map<?, ?> clusters = (Map<?, ?>) response.get("_clusters");
                Map<?, ?> details = (Map<?, ?>) clusters.get("details");

                boolean foundError = false;
                for (Map.Entry<?, ?> cluster : details.entrySet()) {
                    String failures = cluster.getValue().toString();
                    if (denseVectorAggMetricDoubleIfFns()) {
                        throw new AssertionError("should correctly fetch the aggregate_metric_double: " + failures);
                    }
                    foundError |= failures.contains("doesn't understand data type [AGGREGATE_METRIC_DOUBLE]");
                }
                assertTrue("didn't find errors: " + details, foundError);
                return;
            }
        } catch (ResponseException e) {
            if (denseVectorAggMetricDoubleIfFns()) {
                throw new AssertionError("should correctly fetch the aggregate_metric_double", e);
            }
            assertThat(
                "old version should fail with this error",
                EntityUtils.toString(e.getResponse().getEntity()),
                anyOf(
                    containsString("Unknown function [TO_AGGREGATE_METRIC_DOUBLE]"),
                    containsString("Cannot use field [f_aggregate_metric_double] with unsupported type"),
                    containsString("doesn't understand data type [AGGREGATE_METRIC_DOUBLE]")
                )
            );
            // Failure is expected and fine
            return;
        }
        List<?> columns = (List<?>) response.get("columns");
        List<?> values = (List<?>) response.get("values");

        MapMatcher expectedColumns = matchesMap().entry("f_aggregate_metric_double", "aggregate_metric_double").entry("_index", "keyword");
        assertMap(nameToType(columns), expectedColumns);

        MapMatcher expectedAllValues = matchesMap();
        for (Map.Entry<String, NodeInfo> e : expectedIndices(indexMode).entrySet()) {
            String indexName = e.getKey();
            MapMatcher expectedValues = matchesMap();
            expectedValues = expectedValues.entry(
                "f_aggregate_metric_double",
                "{\"min\":-302.5,\"max\":702.3,\"sum\":200.0,\"value_count\":25}"
            );
            expectedValues = expectedValues.entry("_index", indexName);
            expectedAllValues = expectedAllValues.entry(indexName, expectedValues);
        }
        assertMap(indexToRow(columns, values), expectedAllValues);
    }

    protected String fromAllQuery(String indexPattern, String restOfQuery) {
        return ("FROM " + indexPattern + " METADATA _index").replace("%mode%", indexMode.toString()) + restOfQuery;
    }

    protected String fromAllQuery(String restOfQuery) {
        return fromAllQuery(allIndexPattern(), restOfQuery);
    }

    protected String allIndexPattern() {
        return "%mode%*";
    }

    public void testRow() throws IOException {
        assumeTrue(
            "Test has to run only once, skip on other configurations",
            extractPreference == MappedFieldType.FieldExtractPreference.NONE && indexMode == IndexMode.STANDARD
        );
        String query = "ROW x = 1 | LIMIT 1";
        var responseAndCoordinatorVersion = runQuery(query);

        assertMinimumVersion(minVersion(localNodeToInfo()), responseAndCoordinatorVersion, false, false);
    }

    @SuppressWarnings("unchecked")
    public void testRowLookupJoin() throws IOException {
        assumeTrue("Test only requires the lookup index", indexMode == IndexMode.LOOKUP);
        String query = "ROW " + LOOKUP_ID_FIELD + " = 123 | LOOKUP JOIN " + LOOKUP_INDEX_NAME + " ON " + LOOKUP_ID_FIELD + " | LIMIT 1";
        var responseAndCoordinatorVersion = runQuery(query);
        TransportVersion expectedMinimumVersion = minVersion(localNodeToInfo());

        assertMinimumVersion(expectedMinimumVersion, responseAndCoordinatorVersion, false, false);

        Map<String, Object> response = responseAndCoordinatorVersion.v1();
        TransportVersion coordinatorVersion = responseAndCoordinatorVersion.v2();

        assertNoPartialResponse(response);

        List<?> columns = (List<?>) response.get("columns");
        List<?> values = (List<?>) response.get("values");

        MapMatcher expectedColumns = allTypesColumnsMatcher(
            coordinatorVersion,
            expectedMinimumVersion,
            minVersion(),
            indexMode,
            false,
            true
        );
        assertMap(nameToType(columns), expectedColumns);

        MapMatcher expectedValues = allTypesValuesMatcher(
            coordinatorVersion,
            expectedMinimumVersion,
            minVersion(),
            indexMode,
            extractPreference,
            false,
            true,
            null
        );
        assertMap(nameToValue(names(columns), (List<Object>) values.getFirst()), expectedValues);
    }

    @SuppressWarnings("unchecked")
    public void testRowEnrich() throws IOException {
        assumeTrue("Test only requires the enrich policy (made from a lookup index)", indexMode == IndexMode.LOOKUP);
        assumeFalse("Test currently not working on snapshot because of range fields", Build.current().isSnapshot());
        String query = "ROW " + LOOKUP_ID_FIELD + " = 123 | ENRICH " + ENRICH_POLICY_NAME + " ON " + LOOKUP_ID_FIELD + " | LIMIT 1";
        var responseAndCoordinatorVersion = runQuery(query);
        Map<String, Object> response = responseAndCoordinatorVersion.v1();
        TransportVersion coordinatorVersion = responseAndCoordinatorVersion.v2();
        TransportVersion expectedMinimumVersion = minVersion(localNodeToInfo());

        assertMinimumVersion(expectedMinimumVersion, responseAndCoordinatorVersion, false, false);

        assertNoPartialResponse(response);

        List<?> columns = (List<?>) response.get("columns");
        List<?> values = (List<?>) response.get("values");

        MapMatcher expectedColumns = allTypesColumnsMatcher(
            coordinatorVersion,
            expectedMinimumVersion,
            minVersion(),
            indexMode,
            false,
            false
        );
        assertMap(nameToType(columns), expectedColumns);

        MapMatcher expectedValues = allTypesValuesMatcher(
            coordinatorVersion,
            expectedMinimumVersion,
            minVersion(),
            indexMode,
            extractPreference,
            false,
            false,
            null
        );
        assertMap(nameToValue(names(columns), (List<Object>) values.getFirst()), expectedValues);
    }

    /**
     * Run the query and return the response and the version of the coordinator.
     * <p>
     * Fails if the response contains any warnings.
     */
    @SuppressWarnings("unchecked")
    private Tuple<Map<String, Object>, TransportVersion> runQuery(String query) throws IOException {
        Request request = new Request("POST", "_query");
        XContentBuilder body = JsonXContent.contentBuilder().startObject();
        body.field("query", query);
        {
            body.startObject("pragma");
            if (extractPreference != null) {
                body.field("field_extract_preference", extractPreference);
            }
            body.endObject();
        }
        body.field("accept_pragma_risks", "true");
        body.field("profile", true);
        body.field("include_ccs_metadata", true);
        body.endObject();
        request.setJsonEntity(Strings.toString(body));

        Response response = client().performRequest(request);
        Map<String, Object> responseMap = responseAsMap(response);
        HttpHost coordinatorHost = response.getHost();
        NodeInfo coordinator = allNodeToInfo().values().stream().filter(n -> n.boundAddress().contains(coordinatorHost)).findFirst().get();
        TransportVersion coordinatorVersion = coordinator.version();

        profileLogger.extractProfile(responseMap, true);
        return new Tuple<>(responseMap, coordinatorVersion);
    }

    protected void assertMinimumVersionFromAllQueries(Tuple<Map<String, Object>, TransportVersion> responseAndCoordinatorVersion)
        throws IOException {
        assertMinimumVersion(minVersion(), responseAndCoordinatorVersion, true, fetchAllIsCrossCluster());
    }

    /**
     * @param expectedMinimumVersion the minimum version of all clusters that participate in the query
     * @param performsMainFieldCapsRequest {@code true} for queries that have a {@code FROM} command, so we don't retrieve the minimum
     *                                     version from the main field caps response.
     */
    @SuppressWarnings("unchecked")
    protected void assertMinimumVersion(
        TransportVersion expectedMinimumVersion,
        Tuple<Map<String, Object>, TransportVersion> responseAndCoordinatorVersion,
        boolean performsMainFieldCapsRequest,
        boolean isCrossCluster
    ) {
        var responseMap = responseAndCoordinatorVersion.v1();
        var coordinatorVersion = responseAndCoordinatorVersion.v2();

        if (coordinatorVersion.supports(ESQL_USE_MINIMUM_VERSION_FOR_ENRICH_RESOLUTION)) {
            Map<String, Object> profile = (Map<String, Object>) responseMap.get("profile");
            Integer minimumVersion = (Integer) profile.get("minimumTransportVersion");
            assertNotNull(minimumVersion);
            int minimumVersionInt = minimumVersion;
            if (expectedMinimumVersion.supports(RESOLVE_FIELDS_RESPONSE_CREATED_TV)
                || (performsMainFieldCapsRequest == false)
                || (isCrossCluster == false)) {
                assertEquals(expectedMinimumVersion.id(), minimumVersionInt);
            } else {
                // If a remote cluster is old enough that it doesn't provide version information in the field caps response, the coordinator
                // HAS to assume the oldest compatible version.
                // This only applies to multi-cluster tests; if we're looking at a mixed cluster, the coordinator is new enough
                // that it's field caps response will include the min cluster version. (Apparently the field caps request is performed
                // directly on the coordinator.)
                assertEquals(TransportVersion.minimumCompatible().id(), minimumVersionInt);
            }
        }
    }

    protected static void createIndexForNode(
        RestClient client,
        TransportVersion minimumVersionAcrossAllNodes,
        String nodeName,
        String nodeId,
        IndexMode mode
    ) throws IOException {
        String indexName = indexName(mode, nodeName);
        if (false == indexExists(client, indexName)) {
            createAllTypesIndex(client, minimumVersionAcrossAllNodes, indexName, nodeId, mode);
            createAllTypesDoc(client, minimumVersionAcrossAllNodes, indexName);
        }
    }

    protected static String indexName(IndexMode mode, String nodeName) {
        String indexName = mode.toString();
        if (nodeName != null) {
            indexName += "_" + nodeName.toLowerCase(Locale.ROOT);
        }
        return indexName;
    }

    private static final String LOOKUP_ID_FIELD = "lookup_id";

    protected static void createAllTypesIndex(
        RestClient client,
        TransportVersion minimumVersionAcrossAllNodes,
        String indexName,
        String nodeId,
        IndexMode mode
    ) throws IOException {
        XContentBuilder config = JsonXContent.contentBuilder().startObject();
        {
            config.startObject("settings");
            config.startObject("index");
            config.field("mode", mode);
            if (mode == IndexMode.TIME_SERIES) {
                config.field("routing_path", "f_keyword");
            }
            if (nodeId != null) {
                config.field("routing.allocation.include._id", nodeId);
            }
            config.endObject();
            config.endObject();
        }
        {
            config.startObject("mappings").startObject("properties");

            config.startObject(LOOKUP_ID_FIELD);
            config.field("type", "integer");
            config.endObject();

            for (DataType type : DataType.values()) {
                if (supportedInIndex(type, minimumVersionAcrossAllNodes) == false) {
                    continue;
                }
                config.startObject(fieldName(type));
                typeMapping(mode, config, type);
                config.endObject();
            }

            config.endObject().endObject().endObject();
        }
        Request request = new Request("PUT", indexName);
        request.setOptions(DEPRECATED_DEFAULT_METRIC_WARNING_HANDLER);
        request.setJsonEntity(Strings.toString(config));
        client.performRequest(request);
    }

    private static String fieldName(DataType type) {
        return type == DataType.DATETIME ? "@timestamp" : "f_" + type.esType();
    }

    private static void typeMapping(IndexMode indexMode, XContentBuilder config, DataType type) throws IOException {
        switch (type) {
            case COUNTER_DOUBLE, COUNTER_INTEGER, COUNTER_LONG -> config.field("type", type.esType().replace("counter_", ""))
                .field("time_series_metric", "counter");
            case SCALED_FLOAT -> config.field("type", type.esType()).field("scaling_factor", 1);
            case AGGREGATE_METRIC_DOUBLE -> config.field("type", type.esType())
                .field("metrics", List.of("min", "max", "sum", "value_count"))
                .field("default_metric", "max");
            case NULL -> config.field("type", "keyword");
            case KEYWORD -> {
                config.field("type", type.esType());
                if (indexMode == IndexMode.TIME_SERIES) {
                    config.field("time_series_dimension", true);
                }
            }
            default -> config.field("type", type.esType());
        }
    }

    protected static void createAllTypesDoc(RestClient client, TransportVersion minimumVersionAcrossAllNodes, String indexName)
        throws IOException {
        XContentBuilder doc = JsonXContent.contentBuilder().startObject();
        doc.field(LOOKUP_ID_FIELD);
        doc.value(123);
        for (DataType type : DataType.values()) {
            if (supportedInIndex(type, minimumVersionAcrossAllNodes) == false) {
                continue;
            }
            doc.field(fieldName(type));
            switch (type) {
                case BOOLEAN -> doc.value(true);
                case COUNTER_LONG, LONG, COUNTER_INTEGER, INTEGER, UNSIGNED_LONG, SHORT, BYTE -> doc.value(1);
                case COUNTER_DOUBLE, DOUBLE, FLOAT, HALF_FLOAT, SCALED_FLOAT -> doc.value(1.1);
                case KEYWORD, TEXT -> doc.value("foo");
                case DATETIME, DATE_NANOS -> doc.value("2025-01-01T01:00:00Z");
                case IP -> doc.value("192.168.0.1");
                case VERSION -> doc.value("1.0.0-SNAPSHOT");
                case GEO_POINT, GEO_SHAPE -> doc.value("POINT (-71.34 41.12)");
                case NULL -> doc.nullValue();
                case AGGREGATE_METRIC_DOUBLE -> {
                    doc.startObject();
                    doc.field("min", -302.50);
                    doc.field("max", 702.30);
                    doc.field("sum", 200.0);
                    doc.field("value_count", 25);
                    doc.endObject();
                }
                case DATE_RANGE -> {
                    doc.startObject();
                    doc.field("gte", "1989-01-01");
                    doc.field("lt", "2025-01-01");
                    doc.endObject();
                }
                case EXPONENTIAL_HISTOGRAM -> ExponentialHistogramXContent.serialize(doc, EXPONENTIAL_HISTOGRAM_VALUE);
                case DENSE_VECTOR -> doc.value(List.of(0.5, 10, 6));
                case HISTOGRAM -> createHistogramValue(doc);
                case TDIGEST -> createTDigestValue(doc);
                default -> throw new AssertionError("unsupported field type [" + type + "]");
            }
        }
        doc.endObject();
        Request request = new Request("POST", indexName + "/_doc");
        request.addParameter("refresh", "");
        request.setOptions(DEPRECATED_DEFAULT_METRIC_WARNING_HANDLER);
        request.setJsonEntity(Strings.toString(doc));
        client.performRequest(request);
    }

    private static final ExponentialHistogram EXPONENTIAL_HISTOGRAM_VALUE = ExponentialHistogram.create(
        10,
        ExponentialHistogramCircuitBreaker.noop(),
        IntStream.range(0, 100).mapToDouble(i -> i).toArray()
    );

    private static void createTDigestValue(XContentBuilder doc) throws IOException {
        doc.startObject();
        doc.field("min", 0.1);
        doc.field("max", 0.3);
        doc.field("sum", 15.5);
        doc.startArray("centroids");
        doc.value(0.1);
        doc.value(0.2);
        doc.value(0.3);
        doc.endArray();
        doc.startArray("counts");
        doc.value(3);
        doc.value(7);
        doc.value(23);
        doc.endArray();
        doc.endObject();
    }

    private static void createHistogramValue(XContentBuilder doc) throws IOException {
        doc.startObject();
        doc.startArray("values");
        doc.value(0.1);
        doc.value(0.2);
        doc.value(0.3);
        doc.endArray();
        doc.startArray("counts");
        doc.value(3);
        doc.value(7);
        doc.value(23);
        doc.endArray();
        doc.endObject();
    }

    protected static void createEnrichPolicy(
        RestClient client,
        TransportVersion minimumVersionAcrossAllNodes,
        String indexName,
        String policyName
    ) throws IOException {
        XContentBuilder policyConfig = JsonXContent.contentBuilder().startObject();
        {
            policyConfig.startObject("match");

            policyConfig.field("indices", indexName);
            policyConfig.field("match_field", LOOKUP_ID_FIELD);
            List<String> enrichFields = new ArrayList<>();
            for (DataType type : DataType.values()) {
                if (supportedInIndex(type, minimumVersionAcrossAllNodes) == false || supportedInEnrich(type) == false) {
                    continue;
                }
                enrichFields.add(fieldName(type));
            }
            policyConfig.field("enrich_fields", enrichFields);

            policyConfig.endObject();
        }
        policyConfig.endObject();

        Request request = new Request("PUT", "_enrich/policy/" + policyName);
        request.setJsonEntity(Strings.toString(policyConfig));
        client.performRequest(request);

        Request execute = new Request("PUT", "_enrich/policy/" + policyName + "/_execute");
        request.addParameter("wait_for_completion", "true");
        client.performRequest(execute);
    }

    private Matcher<?> expectedValue(
        DataType type,
        TransportVersion coordinatorVersion,
        TransportVersion minimumVersion,
        IndexMode indexMode,
        MappedFieldType.FieldExtractPreference extractPreference
    ) {
        return switch (type) {
            case BOOLEAN -> equalTo(true);
            case COUNTER_LONG, LONG, COUNTER_INTEGER, INTEGER, UNSIGNED_LONG, SHORT, BYTE -> equalTo(1);
            case COUNTER_DOUBLE, DOUBLE -> equalTo(1.1);
            case FLOAT -> equalTo(1.100000023841858);
            case HALF_FLOAT -> equalTo(1.099609375);
            case SCALED_FLOAT -> equalTo(1.0);
            // TODO what about the extra types and ES supports and ESQL flattens away like semantic_text and wildcard?
            case KEYWORD, TEXT -> equalTo("foo");
            case DATETIME, DATE_NANOS -> equalTo("2025-01-01T01:00:00.000Z");
            case IP -> equalTo("192.168.0.1");
            case VERSION -> equalTo("1.0.0-SNAPSHOT");
            case GEO_POINT -> extractPreference == MappedFieldType.FieldExtractPreference.DOC_VALUES || syntheticSourceByDefault()
                ? equalTo("POINT (-71.34000004269183 41.1199999647215)")
                : equalTo("POINT (-71.34 41.12)");
            case GEO_SHAPE -> equalTo("POINT (-71.34 41.12)");
            case NULL -> nullValue();
            case AGGREGATE_METRIC_DOUBLE -> {
                // See expectedType for an explanation
                if (DataType.AGGREGATE_METRIC_DOUBLE.supportedVersion().supportedOn(minimumVersion, false)
                    && coordinatorVersion.supports(RESOLVE_FIELDS_RESPONSE_USED_TV)) {
                    yield equalTo("{\"min\":-302.5,\"max\":702.3,\"sum\":200.0,\"value_count\":25}");
                }
                if (DataType.AGGREGATE_METRIC_DOUBLE.supportedVersion().supportedOn(minimumVersion, true) && Build.current().isSnapshot()) {
                    yield anyOf(nullValue(), equalTo("{\"min\":-302.5,\"max\":702.3,\"sum\":200.0,\"value_count\":25}"));
                }
                yield nullValue();
            }
            case EXPONENTIAL_HISTOGRAM -> equalTo(
                xContentToMap(builder -> ExponentialHistogramXContent.serialize(builder, EXPONENTIAL_HISTOGRAM_VALUE))
            );
            case TDIGEST -> equalTo(xContentToJson(AllSupportedFieldsTestCase::createTDigestValue));
            case HISTOGRAM -> {
                if (HISTOGRAM.supportedVersion().supportedOn(minimumVersion, Build.current().isSnapshot())) {
                    yield equalTo(xContentToJson(AllSupportedFieldsTestCase::createHistogramValue));
                }
                yield nullValue();
            }
            case DENSE_VECTOR -> {
                // See expectedType for an explanation
                if (DataType.DENSE_VECTOR.supportedVersion().supportedOn(minimumVersion, false)
                    && coordinatorVersion.supports(RESOLVE_FIELDS_RESPONSE_USED_TV)) {
                    yield equalTo(List.of(0.5, 10.0, 5.9999995));
                }
                if (DataType.DENSE_VECTOR.supportedVersion().supportedOn(minimumVersion, true) && Build.current().isSnapshot()) {
                    // On previous versions where DENSE_VECTOR was still under construction, we could end up with
                    // [0.04283529, 0.85670584, 0.5140235] instead of [0.5, 10.0, 5.9999995]. We'll ignore the exact value for versions
                    // before the actual release of the type.
                    if (DataType.DENSE_VECTOR.supportedVersion().supportedOn(minimumVersion, false) == false) {
                        yield anyOf(nullValue(), instanceOf(List.class));
                    }
                }
                yield nullValue();
            }
            case DATE_RANGE -> {
                if (DATE_RANGE.supportedVersion().supportedOn(minimumVersion, Build.current().isSnapshot())) {
                    yield equalTo("1989-01-01T00:00:00.000Z..2024-12-31T23:59:59.999Z");
                }
                yield nullValue();
            }

            default -> throw new AssertionError("unsupported field type [" + type + "]");
        };
    }

    private static Map<String, ?> xContentToMap(ThrowingConsumer<XContentBuilder> generator) {
        return XContentHelper.convertToMap(JsonXContent.jsonXContent, xContentToJson(generator), true);
    }

    private static String xContentToJson(ThrowingConsumer<XContentBuilder> generator) {
        try (XContentBuilder builder = JsonXContent.contentBuilder()) {
            generator.accept(builder);
            return Strings.toString(builder);
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }

    /**
     * Is the type supported in indices?
     */
    private static boolean supportedInIndex(DataType t, TransportVersion version) {
        return switch (t) {
            // These are supported but implied by the index process.
            // TODO: current versions already support _tsid; update this once we can tell whether all nodes support it.
            case OBJECT, SOURCE, DOC_DATA_TYPE, TSID_DATA_TYPE,
                // Internal only
                UNSUPPORTED,
                // You can't index these - they are just constants.
                DATE_PERIOD, TIME_DURATION, GEOTILE, GEOHASH, GEOHEX,
                // TODO fix geo
                CARTESIAN_POINT, CARTESIAN_SHAPE -> false;
            // EXPONENTIAL_HISTOGRAM was added to ES and ES|QL at the same time, which is why we can use supportedVersion()
            // to decide whether indices can have fields of this type.
            case EXPONENTIAL_HISTOGRAM -> DataType.EXPONENTIAL_HISTOGRAM.supportedVersion()
                .supportedOn(version, Build.current().isSnapshot());
            case TDIGEST -> DataType.TDIGEST.supportedVersion().supportedOn(version, Build.current().isSnapshot());
            default -> true;
        };
    }

    /**
     * Is the type supported in enrich policies?
     */
    private static boolean supportedInEnrich(DataType t) {
        return switch (t) {
            // Enrich policies don't work with types that have mandatory fields in the mapping.
            // https://github.com/elastic/elasticsearch/issues/127350
            case AGGREGATE_METRIC_DOUBLE, SCALED_FLOAT,
                // https://github.com/elastic/elasticsearch/issues/139255
                EXPONENTIAL_HISTOGRAM, TDIGEST -> false;
            default -> true;
        };
    }

    private static Map<String, Object> nameToType(List<?> columns) {
        Map<String, Object> result = new TreeMap<>();
        for (Object c : columns) {
            Map<?, ?> map = (Map<?, ?>) c;
            result.put(map.get("name").toString(), map.get("type"));
        }
        return result;
    }

    private static List<String> names(List<?> columns) {
        List<String> result = new ArrayList<>();
        for (Object c : columns) {
            Map<?, ?> map = (Map<?, ?>) c;
            result.add(map.get("name").toString());
        }
        return result;
    }

    private static Map<String, Map<String, Object>> indexToRow(List<?> columns, List<?> values) {
        List<String> names = names(columns);
        int indexNameIdx = names.indexOf("_index");
        if (indexNameIdx < 0) {
            throw new IllegalStateException("query didn't return _index");
        }
        Map<String, Map<String, Object>> result = new TreeMap<>();
        for (Object r : values) {
            List<?> row = (List<?>) r;
            result.put(row.get(indexNameIdx).toString(), nameToValue(names, row));
        }
        return result;
    }

    private static Map<String, Object> nameToValue(List<String> names, List<?> values) {
        Map<String, Object> result = new TreeMap<>();
        for (int i = 0; i < values.size(); i++) {
            result.put(names.get(i), values.get(i));
        }
        return result;
    }

    private static Matcher<String> expectedType(
        DataType type,
        TransportVersion coordinatorVersion,
        TransportVersion minimumVersion,
        IndexMode indexMode
    ) {
        return switch (type) {
            case COUNTER_DOUBLE, COUNTER_LONG, COUNTER_INTEGER -> {
                if (indexMode == IndexMode.TIME_SERIES) {
                    yield equalTo(type.esType());
                }
                yield equalTo(type.esType().replace("counter_", ""));
            }
            case BYTE, SHORT -> equalTo("integer");
            case HALF_FLOAT, SCALED_FLOAT, FLOAT -> equalTo("double");
            case NULL -> equalTo("keyword");
            case AGGREGATE_METRIC_DOUBLE -> {
                // 9.2.0 nodes have ESQL_AGGREGATE_METRIC_DOUBLE_CREATED_VERSION and support this type
                // when they are data nodes, but not as coordinators!
                // (Unless the query uses functions that depend on this type, which is a workaround
                // for missing version-awareness in 9.2.0, and not considered here.)
                // RESOLVE_FIELDS_RESPONSE_USED_TV is newer and marks the point when coordinators
                // started to be able to plan for this data type if the field caps response indicated
                // a sufficiently high minimum transport version across all nodes.
                // On SNAPSHOT builds, we considered the type supported since the moment it was added.
                if (DataType.AGGREGATE_METRIC_DOUBLE.supportedVersion().supportedOn(minimumVersion, false)
                    && (coordinatorVersion.supports(RESOLVE_FIELDS_RESPONSE_USED_TV))) {
                    yield equalTo("aggregate_metric_double");
                }
                if (DataType.AGGREGATE_METRIC_DOUBLE.supportedVersion().supportedOn(minimumVersion, true) && Build.current().isSnapshot()) {
                    // In CCS, a new coordinating cluster with an old remote cluster may end up treating the type as unsupported
                    // because the old remote may not tell us its minimum transport version in the field caps response.
                    // In this case, the coordinator has to assume the minimum compatible transport version, which may not support
                    // the type, yet.
                    yield anyOf(equalTo("aggregate_metric_double"), equalTo("unsupported"));
                }
                yield equalTo("unsupported");
            }
            case DENSE_VECTOR -> {
                // Same dance as for AGGREGATE_METRIC_DOUBLE
                if (DataType.DENSE_VECTOR.supportedVersion().supportedOn(minimumVersion, false)
                    && coordinatorVersion.supports(RESOLVE_FIELDS_RESPONSE_USED_TV)) {
                    yield equalTo("dense_vector");
                }
                if (DataType.DENSE_VECTOR.supportedVersion().supportedOn(minimumVersion, true) && Build.current().isSnapshot()) {
                    yield anyOf(equalTo("dense_vector"), equalTo("unsupported"));
                }
                yield equalTo("unsupported");
            }
            case DATE_RANGE -> {
                if (DATE_RANGE.supportedVersion().supportedOn(minimumVersion, Build.current().isSnapshot())) {
                    yield equalTo("date_range");
                }
                yield equalTo("unsupported");
            }
            case HISTOGRAM -> {
                // support for histogram was added later
                if (HISTOGRAM.supportedVersion().supportedOn(minimumVersion, false) == false) {
                    yield equalTo("unsupported");
                }
                yield equalTo("histogram");
            }
            default -> equalTo(type.esType());
        };
    }

    @Override
    protected boolean preserveClusterUponCompletion() {
        return true;
    }

    private boolean syntheticSourceByDefault() {
        return switch (indexMode) {
            case TIME_SERIES, LOGSDB -> true;
            case STANDARD, LOOKUP -> false;
        };
    }

    private Map<String, NodeInfo> expectedIndices(IndexMode indexMode) throws IOException {
        return expectedIndices(indexMode, allNodeToInfo());
    }

    protected Map<String, NodeInfo> expectedIndices(IndexMode indexMode, Map<String, NodeInfo> nodeToInfo) throws IOException {
        Map<String, NodeInfo> result = new TreeMap<>();
        if (supportsNodeAssignment()) {
            for (Map.Entry<String, NodeInfo> e : nodeToInfo.entrySet()) {
                String name = indexName(indexMode, e.getKey());
                if (e.getValue().cluster != null) {
                    name = e.getValue().cluster + ":" + name;
                }
                result.put(name, e.getValue());
            }
        } else {
            for (Map.Entry<String, NodeInfo> e : nodeToInfo.entrySet()) {
                String name = indexName(indexMode, null);
                if (e.getValue().cluster != null) {
                    name = e.getValue().cluster + ":" + name;
                }
                // We should only end up with one per cluster
                result.put(
                    name,
                    new NodeInfo(
                        e.getValue().cluster,
                        null,
                        e.getValue().snapshot(),
                        e.getValue().version(),
                        null,
                        e.getValue().boundAddress()
                    )
                );
            }
        }
        return result;
    }

    protected TransportVersion minVersion() throws IOException {
        return minVersion(allNodeToInfo());
    }

    protected static TransportVersion minVersion(Map<String, NodeInfo> nodeToInfo) {
        return nodeToInfo.values().stream().map(NodeInfo::version).min(Comparator.naturalOrder()).get();
    }

    /**
     * Assert that the profile is what we expect. Note that the profile is
     * generally not backwards compatible, so we should only run this in the
     * single-node case.
     */
    private void assertProfile(Map<?, ?> profile) {
        List<?> drivers = (List<?>) profile.get("drivers");
        boolean atleastOneDataDriver = false;
        for (Object d : drivers) {
            Map<?, ?> driver = (Map<?, ?>) d;
            if ("data".equals(driver.get("description"))) {
                atleastOneDataDriver = true;
                assertDataDriverProfile((List<?>) driver.get("operators"));
            }
        }
        assertTrue("at least one data driver found", atleastOneDataDriver);
    }

    private void assertDataDriverProfile(List<?> operators) {
        int readerCount = 0;
        for (Object o : operators) {
            Map<?, ?> operator = (Map<?, ?>) o;
            String name = operator.get("operator").toString();
            if (name.startsWith("ValuesSourceReaderOperator")) {
                readerCount++;
                Map<?, ?> status = (Map<?, ?>) operator.get("status");
                assertReadersBuilt((Map<?, ?>) status.get("readers_built"));
            }
        }
        assertThat(readerCount, equalTo(1));
    }

    private void assertReadersBuilt(Map<?, ?> readersBuilt) {
        Map<String, List<String>> found = new TreeMap<>();
        for (Object k : readersBuilt.keySet()) {
            String key = k.toString();
            if (key.startsWith("stored_fields")) {
                continue;
            }
            int firstColon = key.indexOf(':');
            String field = key.substring(0, firstColon);
            String reader = key.substring(firstColon + 1);
            found.computeIfAbsent(field, unused -> new ArrayList<>()).add(reader);
        }
        for (List<String> v : found.values()) {
            Collections.sort(v);
        }

        MapMatcher expected = matchesMap();
        for (DataType type : DataType.values()) {
            if (supportedInIndex(type, TransportVersion.current()) == false) {
                continue;
            }

            expected = expected.entry(fieldName(type), loadersFor(type));
        }

        expected = expected.entry(
            "_id",
            indexMode == IndexMode.TIME_SERIES
                ? matchesList().item("column_at_a_time:TsIdFieldReader")
                : matchesList().item("column_at_a_time:null").item("row_stride:BlockStoredFieldsReader.Id")
        )
            .entry("_ignored", matchesList().item("column_at_a_time:constant_nulls"))
            .entry("_index_mode", matchesList().item(startsWith("column_at_a_time:constant")))
            .entry("_index", matchesList().item(startsWith("column_at_a_time:constant")))
            .entry("_source", matchesList().item("column_at_a_time:null").item("row_stride:BlockStoredFieldsReader.Source"))
            .entry("_version", matchesList().item("column_at_a_time:LongsFromDocValues.Singleton"))
            .entry("lookup_id", loadersFor(DataType.INTEGER));
        assertMap(found, expected);
    }

    ListMatcher loadersFor(DataType type) {
        return switch (type) {
            case AGGREGATE_METRIC_DOUBLE -> matchesList().item("column_at_a_time:BlockDocValuesReader.AggregateMetricDouble");
            case BOOLEAN -> matchesList().item("column_at_a_time:BooleansFromDocValues.Singleton");
            case DATE_RANGE -> matchesList().item("column_at_a_time:BlockDocValuesReader.DateRangeDocValuesReader");
            case DOUBLE, COUNTER_DOUBLE, FLOAT, HALF_FLOAT, SCALED_FLOAT -> useStoredLoader()
                ? matchesList().item("column_at_a_time:null").item("row_stride:BlockSourceReader.Doubles")
                : matchesList().item("column_at_a_time:DoublesFromDocValues.Singleton");
            case EXPONENTIAL_HISTOGRAM -> matchesList().item("column_at_a_time:BlockDocValuesReader.ExponentialHistogram");
            case DENSE_VECTOR -> matchesList().item("column_at_a_time:FloatDenseVectorFromDocValues.Normalized.Load");
            case GEO_POINT -> extractPreference == MappedFieldType.FieldExtractPreference.STORED || syntheticSourceByDefault() == false
                ? matchesList().item("column_at_a_time:null").item("row_stride:BlockSourceReader.Geometries")
                : matchesList().item("column_at_a_time:BlockDocValuesReader.BytesRefsFromLong");
            case GEO_SHAPE -> {
                String last;
                if (syntheticSourceByDefault()) {
                    last = "row_stride:FallbackToSource[Geometry]";
                } else if (extractPreference == MappedFieldType.FieldExtractPreference.STORED) {
                    last = "row_stride:BlockSourceReader.Geometries";
                } else {
                    last = "row_stride:BlockSourceReader.Geometries";
                }
                yield matchesList().item("column_at_a_time:null").item(last);
            }
            case HISTOGRAM -> matchesList().item("column_at_a_time:BlockDocValuesReader.Bytes");
            case INTEGER, COUNTER_INTEGER, SHORT, BYTE -> useStoredLoader()
                ? matchesList().item("column_at_a_time:null").item("row_stride:BlockSourceReader.Ints")
                : matchesList().item("column_at_a_time:IntsFromDocValues.Singleton");
            case IP -> useStoredLoader()
                ? matchesList().item("column_at_a_time:null").item("row_stride:BlockSourceReader.Ips")
                : matchesList().item("column_at_a_time:BytesRefsFromOrds.Singleton");
            case KEYWORD -> useStoredLoader()
                ? matchesList().item("column_at_a_time:null").item("row_stride:BlockSourceReader.Bytes")
                : matchesList().item("column_at_a_time:BytesRefsFromOrds.Singleton");
            case LONG, COUNTER_LONG, UNSIGNED_LONG -> useStoredLoader()
                ? matchesList().item("column_at_a_time:null").item("row_stride:BlockSourceReader.Longs")
                : matchesList().item("column_at_a_time:LongsFromDocValues.Singleton");
            case DATETIME, DATE_NANOS -> matchesList().item("column_at_a_time:LongsFromDocValues.Singleton");
            case NULL -> // TODO figure out why stored field preference makes this go to _source
                useStoredLoader()
                    ? matchesList().item("column_at_a_time:null").item("row_stride:BlockSourceReader.Bytes")
                    : matchesList().item("column_at_a_time:constant_nulls");
            case TDIGEST -> matchesList().item("column_at_a_time:BlockDocValuesReader.TDigest");
            case TEXT -> syntheticSourceByDefault()
                ? matchesList().item("column_at_a_time:BlockDocValuesReader.BytesCustom")
                : matchesList().item("column_at_a_time:null").item("row_stride:BlockSourceReader.Bytes");
            case VERSION -> matchesList().item("column_at_a_time:BytesRefsFromOrds.Singleton");
            default -> matchesList();
        };
    }

    /**
     * Do the preference and {@code index_mode} indicate that we should load from stored fields?
     */
    private boolean useStoredLoader() {
        return extractPreference == MappedFieldType.FieldExtractPreference.STORED && false == syntheticSourceByDefault();
    }
}
