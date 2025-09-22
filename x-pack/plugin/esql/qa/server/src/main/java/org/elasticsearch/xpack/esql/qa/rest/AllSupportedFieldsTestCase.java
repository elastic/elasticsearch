/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.qa.rest;

import com.carrotsearch.randomizedtesting.annotations.ParametersFactory;

import org.elasticsearch.Version;
import org.elasticsearch.client.Request;
import org.elasticsearch.client.Response;
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

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.TreeMap;

import static org.elasticsearch.test.ListMatcher.matchesList;
import static org.elasticsearch.test.MapMatcher.assertMap;
import static org.elasticsearch.test.MapMatcher.matchesMap;
import static org.hamcrest.Matchers.any;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.nullValue;

/**
 * Creates indices with all supported fields and fetches values from them.
 * <p>
 *     In a single cluster where all nodes are on a single version this is
 *     just an "is it plugged in" style smoke test. In a mixed version cluster
 *     this is testing the behavior of fetching potentially unsupported field
 *     types. The same is true multi-cluster cases.
 * </p>
 * <p>
 *     This isn't trying to test complex interactions with field loading so we
 *     load constant field values and have simple mappings.
 * </p>
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

    private record NodeInfo(String id, Version version) {}
    private Map<String, NodeInfo> nodeToInfo;

    protected AllSupportedFieldsTestCase(MappedFieldType.FieldExtractPreference extractPreference, IndexMode indexMode) {
        this.extractPreference = extractPreference;
        this.indexMode = indexMode;
    }

    private Map<String, NodeInfo> nodeToInfo() throws IOException {
        if (nodeToInfo != null) {
            return nodeToInfo;
        }

        nodeToInfo = new TreeMap<>();
        Request getIds = new Request("GET", "_cat/nodes");
        getIds.addParameter("h", "name,id,version");
        getIds.addParameter("full_id", "");
        Response idsResponse = client().performRequest(getIds);
        BufferedReader idsReader = new BufferedReader(new InputStreamReader(idsResponse.getEntity().getContent()));
        String line;
        while ((line = idsReader.readLine()) != null) {
            String[] l = line.split(" ");
            // TODO what's the right thing to use instead of Version?
            nodeToInfo.put(l[0], new NodeInfo(l[1], Version.fromString(l[2])));
        }
        return nodeToInfo;
    }

    @Before
    public void createIndices() throws IOException {
        for (Map.Entry<String, NodeInfo> e : nodeToInfo().entrySet()) {
            createIndexForNode(e.getKey(), e.getValue().id());
        }
    }

    public final void test() throws IOException {
        Request request = new Request("POST", "_query");
        XContentBuilder body = JsonXContent.contentBuilder().startObject();
        body.field("query", """
            FROM %mode%* METADATA _id, _ignored, _index, _index_mode, _score, _source, _version
            | LIMIT 1000
            """.replace("%mode%", indexMode.toString()));
        {
            body.startObject("pragma");
            if (extractPreference != null) {
                body.field("field_extract_preference", extractPreference);
            }
            body.endObject();
        }
        body.field("accept_pragma_risks", "true");
        body.field("profile", true);
        body.endObject();
        request.setJsonEntity(Strings.toString(body));

        Map<String, Object> response = responseAsMap(client().performRequest(request));
        List<?> columns = (List<?>) response.get("columns");
        List<?> values = (List<?>) response.get("values");
        profileLogger.extractProfile(response, true);

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
        for (Map.Entry<String, NodeInfo> e : nodeToInfo().entrySet()) {
            String nodeName = e.getKey();
            NodeInfo nodeInfo = e.getValue();
            MapMatcher expectedValues = matchesMap();
            for (DataType type : DataType.values()) {
                if (supportedInIndex(type) == false) {
                    continue;
                }
                expectedValues = expectedValues.entry(fieldName(type), expectedValue(nodeInfo.version, type));
            }
            String expectedIndex = indexMode + "_" + nodeName;
            expectedValues = expectedValues.entry("_id", any(String.class))
                .entry("_ignored", nullValue())
                .entry("_index", expectedIndex)
                .entry("_index_mode", indexMode.toString())
                .entry("_score", 0.0)
                .entry("_source", matchesMap().extraOk())
                .entry("_version", 1);
            expectedAllValues = expectedAllValues.entry(expectedIndex, expectedValues);
        }
        assertMap(indexToRow(columns, values), expectedAllValues);
        profileLogger.clearProfile();
    }

    private void createIndexForNode(String nodeName, String nodeId) throws IOException {
        String indexName = indexMode + "_" + nodeName;
        if (false == indexExists(indexName)) {
            createAllTypesIndex(indexName, nodeId);
            createAllTypesDoc(indexName);
        }
    }

    private void createAllTypesIndex(String indexName, String nodeId) throws IOException {
        XContentBuilder config = JsonXContent.contentBuilder().startObject();
        {
            config.startObject("settings");
            config.startObject("index");
            config.field("mode", indexMode);
            if (indexMode == IndexMode.TIME_SERIES) {
                config.field("routing_path", "f_keyword");
            }
            config.field("routing.allocation.include._id", nodeId);
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
                typeMapping(config, type);
                config.endObject();
            }
            config.endObject().endObject().endObject();
        }
        Request request = new Request("PUT", indexName);
        request.setJsonEntity(Strings.toString(config));
        client().performRequest(request);
    }

    private String fieldName(DataType type) {
        return type == DataType.DATETIME ? "@timestamp" : "f_" + type.esType();
    }

    private void typeMapping(XContentBuilder config, DataType type) throws IOException {
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

    private void createAllTypesDoc(String indexName) throws IOException {
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
        client().performRequest(request);
    }

    private Matcher<?> expectedValue(Version version, DataType type) {
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
            case AGGREGATE_METRIC_DOUBLE ->
                // TODO why not a map?
                equalTo("{\"min\":-302.5,\"max\":702.3,\"sum\":200.0,\"value_count\":25}");
            case DENSE_VECTOR ->
                version.onOrAfter(Version.V_9_2_0)
                ? matchesList().item(0.5).item(10.0).item(5.9999995)
                : matchesList().item(0.04283529).item(0.85670584).item(0.5140235);
            default -> throw new AssertionError("unsupported field type [" + type + "]");
        };
    }

    /**
     * Is the type supported in indices?
     */
    private boolean supportedInIndex(DataType t) {
        return switch (t) {
            // These are supported but implied by the index process.
            case OBJECT, SOURCE, DOC_DATA_TYPE, TSID_DATA_TYPE,
                // Internal only
                UNSUPPORTED, PARTIAL_AGG,
                // You can't index these - they are just constants.
                DATE_PERIOD, TIME_DURATION, GEOTILE, GEOHASH, GEOHEX,
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

    private String expectedType(DataType type) {
        return switch (type) {
            case COUNTER_DOUBLE, COUNTER_LONG, COUNTER_INTEGER -> {
                if (indexMode == IndexMode.TIME_SERIES) {
                    yield type.esType();
                }
                yield type.esType().replace("counter_", "");
            }
            case BYTE, SHORT -> "integer";
            case HALF_FLOAT, SCALED_FLOAT, FLOAT -> "double";
            case NULL -> "keyword";
            default -> type.esType();
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
}
