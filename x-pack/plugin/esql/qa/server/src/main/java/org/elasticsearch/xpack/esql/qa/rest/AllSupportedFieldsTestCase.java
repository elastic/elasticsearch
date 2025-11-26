/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.qa.rest;

import com.carrotsearch.randomizedtesting.annotations.ParametersFactory;

import org.apache.http.util.EntityUtils;
import org.elasticsearch.Build;
import org.elasticsearch.TransportVersion;
import org.elasticsearch.client.Request;
import org.elasticsearch.client.ResponseException;
import org.elasticsearch.client.RestClient;
import org.elasticsearch.common.Strings;
import org.elasticsearch.index.IndexMode;
import org.elasticsearch.index.mapper.MappedFieldType;
import org.elasticsearch.logging.LogManager;
import org.elasticsearch.logging.Logger;
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
import java.util.Comparator;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Set;
import java.util.TreeMap;
import java.util.stream.Collectors;

import static org.elasticsearch.common.xcontent.support.XContentMapValues.extractValue;
import static org.elasticsearch.test.ListMatcher.matchesList;
import static org.elasticsearch.test.MapMatcher.assertMap;
import static org.elasticsearch.test.MapMatcher.matchesMap;
import static org.elasticsearch.xpack.esql.action.EsqlResolveFieldsResponse.RESOLVE_FIELDS_RESPONSE_USED_TV;
import static org.elasticsearch.xpack.esql.core.type.DataType.DataTypesTransportVersions.ESQL_AGGREGATE_METRIC_DOUBLE_CREATED_VERSION;
import static org.elasticsearch.xpack.esql.core.type.DataType.DataTypesTransportVersions.ESQL_DENSE_VECTOR_CREATED_VERSION;
import static org.elasticsearch.xpack.esql.core.type.DataType.DataTypesTransportVersions.INDEX_SOURCE;
import static org.hamcrest.Matchers.any;
import static org.hamcrest.Matchers.anyOf;
import static org.hamcrest.Matchers.containsString;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.nullValue;

/**
 * Creates indices with all supported fields and fetches values from them to
 * confirm that release builds correctly handle data types, even if they were
 * introduced in later versions.
 * <p>
 *     Entirely skipped in snapshot builds; data types that are under
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
    private static final Logger logger = LogManager.getLogger(FieldExtractorTestCase.class);

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

    protected record NodeInfo(String cluster, String id, boolean snapshot, TransportVersion version, Set<String> roles) {}

    private static Map<String, NodeInfo> nodeToInfo;

    private Map<String, NodeInfo> nodeToInfo() throws IOException {
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
        return nodeToInfo();
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

            /*
             * Figuring out is a node is a snapshot is kind of tricky. The main version
             * doesn't include -SNAPSHOT. But ${VERSION}-SNAPSHOT is in the node info
             * *somewhere*. So we do this silly toString here.
             */
            String version = (String) extractValue(nodeInfo, "version");
            boolean snapshot = nodeInfo.toString().contains(version + "-SNAPSHOT");

            TransportVersion transportVersion = TransportVersion.fromId((Integer) extractValue(nodeInfo, "transport_version"));
            List<?> roles = (List<?>) nodeInfo.get("roles");

            nodeToInfo.put(
                nodeName,
                new NodeInfo(cluster, id, snapshot, transportVersion, roles.stream().map(Object::toString).collect(Collectors.toSet()))
            );
        }

        return nodeToInfo;
    }

    @Before
    public void createIndices() throws IOException {
        if (supportsNodeAssignment()) {
            for (Map.Entry<String, NodeInfo> e : nodeToInfo().entrySet()) {
                createIndexForNode(client(), e.getKey(), e.getValue().id());
            }
        } else {
            createIndexForNode(client(), null, null);
        }
    }

    /**
     * Make sure the test doesn't run on snapshot builds. Release builds only.
     * <p>
     *     {@link Build#isSnapshot()} checks if the version under test is a snapshot.
     *     But! This run test runs against many versions and if *any* are snapshots
     *     then this will fail. So we check the versions of each node in the cluster too.
     * </p>
     */
    @Before
    public void skipSnapshots() throws IOException {
        assumeFalse("Only supported on production builds", Build.current().isSnapshot());
        for (NodeInfo n : allNodeToInfo().values()) {
            assumeFalse("Only supported on production builds", n.snapshot());
        }
    }

    // TODO: Also add a test for _tsid once we can determine the minimum transport version of all nodes.
    public final void testFetchAll() throws IOException {
        Map<String, Object> response = esql("""
            , _id, _ignored, _index_mode, _score, _source, _version
            | LIMIT 1000
            """);
        if ((Boolean) response.get("is_partial")) {
            throw new AssertionError("partial results: " + response);
        }
        List<?> columns = (List<?>) response.get("columns");
        List<?> values = (List<?>) response.get("values");

        MapMatcher expectedColumns = matchesMap();
        for (DataType type : DataType.values()) {
            if (supportedInIndex(type) == false) {
                continue;
            }
            expectedColumns = expectedColumns.entry(fieldName(type), expectedType(type));
        }
        expectedColumns = expectedColumns.entry("_id", "keyword")
            .entry("_ignored", "keyword")
            .entry("_index", "keyword")
            .entry("_index_mode", "keyword")
            .entry("_score", "double")
            .entry("_source", "_source")
            .entry("_version", "long");
        assertMap(nameToType(columns), expectedColumns);

        MapMatcher expectedAllValues = matchesMap();
        for (Map.Entry<String, NodeInfo> e : expectedIndices().entrySet()) {
            String indexName = e.getKey();
            NodeInfo nodeInfo = e.getValue();
            MapMatcher expectedValues = matchesMap();
            for (DataType type : DataType.values()) {
                if (supportedInIndex(type) == false) {
                    continue;
                }
                expectedValues = expectedValues.entry(fieldName(type), expectedValue(type, nodeInfo));
            }
            expectedValues = expectedValues.entry("_id", any(String.class))
                .entry("_ignored", nullValue())
                .entry("_index", indexName)
                .entry("_index_mode", indexMode.toString())
                .entry("_score", 0.0)
                .entry("_source", matchesMap().extraOk())
                .entry("_version", 1);
            expectedAllValues = expectedAllValues.entry(indexName, expectedValues);
        }
        assertMap(indexToRow(columns, values), expectedAllValues);
        profileLogger.clearProfile();
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
            response = esql(request);
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
        for (Map.Entry<String, NodeInfo> e : expectedIndices().entrySet()) {
            String indexName = e.getKey();
            NodeInfo nodeInfo = e.getValue();
            MapMatcher expectedValues = matchesMap();
            expectedValues = expectedValues.entry("f_dense_vector", expectedDenseVector(nodeInfo.version));
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
            response = esql(request);
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
        for (Map.Entry<String, NodeInfo> e : expectedIndices().entrySet()) {
            String indexName = e.getKey();
            NodeInfo nodeInfo = e.getValue();
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

    private Map<String, Object> esql(String query) throws IOException {
        Request request = new Request("POST", "_query");
        XContentBuilder body = JsonXContent.contentBuilder().startObject();
        body.field("query", "FROM *:%mode%*,%mode%* METADATA _index".replace("%mode%", indexMode.toString()) + query);
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

        Map<String, Object> response = responseAsMap(client().performRequest(request));
        profileLogger.extractProfile(response, true);
        return response;
    }

    protected void createIndexForNode(RestClient client, String nodeName, String nodeId) throws IOException {
        String indexName = indexMode.toString();
        if (nodeName != null) {
            indexName += "_" + nodeName.toLowerCase(Locale.ROOT);
        }
        if (false == indexExists(client, indexName)) {
            createAllTypesIndex(client, indexName, nodeId);
            createAllTypesDoc(client, indexName);
        }
    }

    private void createAllTypesIndex(RestClient client, String indexName, String nodeId) throws IOException {
        XContentBuilder config = JsonXContent.contentBuilder().startObject();
        {
            config.startObject("settings");
            config.startObject("index");
            config.field("mode", indexMode);
            if (indexMode == IndexMode.TIME_SERIES) {
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
            for (DataType type : DataType.values()) {
                if (supportedInIndex(type) == false) {
                    continue;
                }
                config.startObject(fieldName(type));
                typeMapping(indexMode, config, type);
                config.endObject();
            }
            config.endObject().endObject().endObject();
        }
        Request request = new Request("PUT", indexName);
        request.setJsonEntity(Strings.toString(config));
        client.performRequest(request);
    }

    private String fieldName(DataType type) {
        return type == DataType.DATETIME ? "@timestamp" : "f_" + type.esType();
    }

    private void typeMapping(IndexMode indexMode, XContentBuilder config, DataType type) throws IOException {
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

    private void createAllTypesDoc(RestClient client, String indexName) throws IOException {
        XContentBuilder doc = JsonXContent.contentBuilder().startObject();
        for (DataType type : DataType.values()) {
            if (supportedInIndex(type) == false) {
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
                case DENSE_VECTOR -> doc.value(List.of(0.5, 10, 6));
                default -> throw new AssertionError("unsupported field type [" + type + "]");
            }
        }
        doc.endObject();
        Request request = new Request("POST", indexName + "/_doc");
        request.addParameter("refresh", "");
        request.setJsonEntity(Strings.toString(doc));
        client.performRequest(request);
    }

    // This will become dependent on the minimum transport version of all nodes once we can determine that.
    private Matcher<?> expectedValue(DataType type, NodeInfo nodeInfo) throws IOException {
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
                if (minVersion().supports(RESOLVE_FIELDS_RESPONSE_USED_TV) == false
                    || minVersion().supports(ESQL_AGGREGATE_METRIC_DOUBLE_CREATED_VERSION) == false) {
                    yield nullValue();
                }
                yield equalTo("{\"min\":-302.5,\"max\":702.3,\"sum\":200.0,\"value_count\":25}");
            }
            case DENSE_VECTOR -> {
                if (minVersion().supports(RESOLVE_FIELDS_RESPONSE_USED_TV) == false
                    || minVersion().supports(ESQL_DENSE_VECTOR_CREATED_VERSION) == false) {
                    yield nullValue();
                }
                yield equalTo(List.of(0.5, 10.0, 5.9999995));
            }
            default -> throw new AssertionError("unsupported field type [" + type + "]");
        };
    }

    private Matcher<List<?>> expectedDenseVector(TransportVersion version) {
        return version.supports(INDEX_SOURCE) // *after* 9.1
            ? matchesList().item(0.5).item(10.0).item(5.9999995)
            : matchesList().item(0.04283529).item(0.85670584).item(0.5140235);
    }

    /**
     * Is the type supported in indices?
     */
    private static boolean supportedInIndex(DataType t) {
        return switch (t) {
            // These are supported but implied by the index process.
            // TODO: current versions already support _tsid; update this once we can tell whether all nodes support it.
            case OBJECT, SOURCE, DOC_DATA_TYPE, TSID_DATA_TYPE,
                // Internal only
                UNSUPPORTED, PARTIAL_AGG,
                // You can't index these - they are just constants.
                DATE_PERIOD, TIME_DURATION, GEOTILE, GEOHASH, GEOHEX,
                // TODO(b/133393): Once we remove the feature-flag of the exp-histo field type (!= ES|QL type),
                // replace this with a capability check
                EXPONENTIAL_HISTOGRAM,
                // TODO fix geo
                CARTESIAN_POINT, CARTESIAN_SHAPE -> false;
            default -> true;
        };
    }

    private Map<String, Object> nameToType(List<?> columns) {
        Map<String, Object> result = new TreeMap<>();
        for (Object c : columns) {
            Map<?, ?> map = (Map<?, ?>) c;
            result.put(map.get("name").toString(), map.get("type"));
        }
        return result;
    }

    private List<String> names(List<?> columns) {
        List<String> result = new ArrayList<>();
        for (Object c : columns) {
            Map<?, ?> map = (Map<?, ?>) c;
            result.add(map.get("name").toString());
        }
        return result;
    }

    private Map<String, Map<String, Object>> indexToRow(List<?> columns, List<?> values) {
        List<String> names = names(columns);
        int timestampIdx = names.indexOf("_index");
        if (timestampIdx < 0) {
            throw new IllegalStateException("query didn't return _index");
        }
        Map<String, Map<String, Object>> result = new TreeMap<>();
        for (Object r : values) {
            List<?> row = (List<?>) r;
            result.put(row.get(timestampIdx).toString(), nameToValue(names, row));
        }
        return result;
    }

    private Map<String, Object> nameToValue(List<String> names, List<?> values) {
        Map<String, Object> result = new TreeMap<>();
        for (int i = 0; i < values.size(); i++) {
            result.put(names.get(i), values.get(i));
        }
        return result;
    }

    // This will become dependent on the minimum transport version of all nodes once we can determine that.
    private Matcher<String> expectedType(DataType type) throws IOException {
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
                // RESOLVE_FIELDS_RESPONSE_USED_TV is newer and technically sufficient to check.
                // We also check for ESQL_AGGREGATE_METRIC_DOUBLE_CREATED_VERSION for clarity.
                // Future data types added here should only require the TV when they were created,
                // because it will be after RESOLVE_FIELDS_RESPONSE_USED_TV.
                if (minVersion().supports(RESOLVE_FIELDS_RESPONSE_USED_TV) == false
                    || minVersion().supports(ESQL_AGGREGATE_METRIC_DOUBLE_CREATED_VERSION) == false) {
                    yield equalTo("unsupported");
                }
                yield equalTo("aggregate_metric_double");
            }
            case DENSE_VECTOR -> {
                logger.error("ADFDAFAF " + minVersion());
                if (minVersion().supports(RESOLVE_FIELDS_RESPONSE_USED_TV) == false
                    || minVersion().supports(ESQL_DENSE_VECTOR_CREATED_VERSION) == false) {
                    yield equalTo("unsupported");
                }
                yield equalTo("dense_vector");
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

    private Map<String, NodeInfo> expectedIndices() throws IOException {
        Map<String, NodeInfo> result = new TreeMap<>();
        if (supportsNodeAssignment()) {
            for (Map.Entry<String, NodeInfo> e : allNodeToInfo().entrySet()) {
                String name = indexMode + "_" + e.getKey();
                if (e.getValue().cluster != null) {
                    name = e.getValue().cluster + ":" + name;
                }
                result.put(name, e.getValue());
            }
        } else {
            for (Map.Entry<String, NodeInfo> e : allNodeToInfo().entrySet()) {
                String name = indexMode.toString();
                if (e.getValue().cluster != null) {
                    name = e.getValue().cluster + ":" + name;
                }
                // We should only end up with one per cluster
                result.put(name, new NodeInfo(e.getValue().cluster, null, e.getValue().snapshot(), e.getValue().version(), null));
            }
        }
        return result;
    }

    protected TransportVersion minVersion() throws IOException {
        return allNodeToInfo().values().stream().map(NodeInfo::version).min(Comparator.naturalOrder()).get();
    }
}
