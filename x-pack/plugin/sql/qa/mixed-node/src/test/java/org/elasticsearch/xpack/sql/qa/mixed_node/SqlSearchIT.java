/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */

package org.elasticsearch.xpack.sql.qa.mixed_node;

import org.apache.http.HttpHost;
import org.elasticsearch.Version;
import org.elasticsearch.client.Request;
import org.elasticsearch.client.Response;
import org.elasticsearch.client.RestClient;
import org.elasticsearch.cluster.metadata.IndexMetadata;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.xcontent.XContentHelper;
import org.elasticsearch.common.xcontent.json.JsonXContent;
import org.elasticsearch.test.NotEqualMessageBuilder;
import org.elasticsearch.test.rest.ESRestTestCase;
import org.elasticsearch.test.rest.yaml.ObjectPath;
import org.elasticsearch.xpack.sql.type.SqlDataTypes;
import org.junit.After;
import org.junit.Before;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.function.BiConsumer;
import java.util.function.Consumer;
import java.util.stream.Collectors;

import static java.util.Collections.unmodifiableMap;

public class SqlSearchIT extends ESRestTestCase {

    private static final Version FIELDS_API_QL_INTRODUCTION = Version.V_7_12_0;
    private static final String index = "test_sql_mixed_versions";
    private static int numShards;
    private static int numReplicas = 1;
    private static int numDocs;
    private static Nodes nodes;
    private static List<Node> allNodes;
    private static List<Node> newNodes;
    private static List<Node> bwcNodes;
    private static Version bwcVersion;
    private static Version newVersion;
    private static boolean isBeforeFieldsApiInQL;

    @Before
    public void createIndex() throws IOException {
        nodes = buildNodeAndVersions(client());
        numShards = nodes.size();
        numDocs = randomIntBetween(numShards, 16);
        allNodes = new ArrayList<>();
        allNodes.addAll(nodes.getBWCNodes());
        allNodes.addAll(nodes.getNewNodes());
        newNodes = new ArrayList<>();
        newNodes.addAll(nodes.getNewNodes());
        bwcNodes = new ArrayList<>();
        bwcNodes.addAll(nodes.getBWCNodes());
        bwcVersion = nodes.getBWCNodes().get(0).getVersion();
        newVersion = nodes.getNewNodes().get(0).getVersion();
        isBeforeFieldsApiInQL = bwcVersion.before(FIELDS_API_QL_INTRODUCTION);
        
        String mappings = readResource("/all_field_types.json");
        createIndex(
            index,
            Settings.builder()
                .put(IndexMetadata.INDEX_NUMBER_OF_SHARDS_SETTING.getKey(), numShards)
                .put(IndexMetadata.SETTING_NUMBER_OF_REPLICAS, numReplicas)
                .build(),
            mappings
        );
    }

    @After
    public void cleanUpIndex() throws IOException {
        if (indexExists(index) == true) {
            deleteIndex(index);
        }
    }

    public void testAllTypesWithRequestToOldNodes() throws Exception {
        Map<String, Object> expectedResponse = prepareTestData(
            columns -> {
                columns.add(columnInfo("geo_point_field", "geo_point"));
                columns.add(columnInfo("float_field", "float"));
                columns.add(columnInfo("half_float_field", "half_float"));
            },
            (builder, fieldValues) -> {
                Float randomFloat = randomFloat();
                // before "fields" API being added to QL, numbers were re-parsed from _source with a similar approach to
                // indexing docvalues and for floating point numbers this may be different from the actual value passed in the _source
                // floats were indexed as Doubles and the values returned had a greater precision and more decimals
                builder.append(",");
                if (isBeforeFieldsApiInQL) {
                    builder.append("\"geo_point_field\":{\"lat\":\"37.386483\", \"lon\":\"-122.083843\"},");
                    fieldValues.put("geo_point_field", "POINT (-122.08384302444756 37.38648299127817)");
                    builder.append("\"float_field\":" + randomFloat + ",");
                    fieldValues.put("float_field", Double.valueOf(randomFloat));
                    builder.append("\"half_float_field\":123.456");
                    fieldValues.put("half_float_field", 123.45600128173828d);
                } else {
                    builder.append("\"geo_point_field\":{\"lat\":\"37.386483\", \"lon\":\"-122.083843\"},");
                    fieldValues.put("geo_point_field", "POINT (-122.083843 37.386483)");
                    builder.append("\"float_field\":" + randomFloat + ",");
                    fieldValues.put("float_field", Double.valueOf(Float.valueOf(randomFloat).toString()));
                    builder.append("\"half_float_field\":" + fieldValues.computeIfAbsent("half_float_field", v -> 123.456));
                }
            }
        );
        assertAllTypesWithNodes(expectedResponse, bwcNodes);
    }

    public void testAllTypesWithRequestToUpgradedNodes() throws Exception {
        Map<String, Object> expectedResponse = prepareTestData(
            columns -> {
                columns.add(columnInfo("geo_point_field", "geo_point"));
                columns.add(columnInfo("float_field", "float"));
                columns.add(columnInfo("half_float_field", "half_float"));
            },
            (builder, fieldValues) -> {
                builder.append(",");
                builder.append("\"geo_point_field\":{\"lat\":\"37.386483\", \"lon\":\"-122.083843\"},");
                fieldValues.put("geo_point_field", "POINT (-122.083843 37.386483)");
                Float randomFloat = randomFloat();
                builder.append("\"float_field\":" + randomFloat + ",");
                fieldValues.put("float_field", Double.valueOf(Float.valueOf(randomFloat).toString()));
                builder.append("\"half_float_field\":" + fieldValues.computeIfAbsent("half_float_field", v -> 123.456));
            }
        );
        assertAllTypesWithNodes(expectedResponse, newNodes);
    }

    @SuppressWarnings("unchecked")
    private Map<String, Object> prepareTestData(Consumer<List<Map<String, Object>>> additionalColumns,
        BiConsumer<StringBuilder, Map<String, Object>> additionalValues) throws IOException {
        Map<String, Object> expectedResponse = new HashMap<>();
        List<Map<String, Object>> columns = new ArrayList<>();
        columns.add(columnInfo("long_field", "long"));
        columns.add(columnInfo("integer_field", "integer"));
        columns.add(columnInfo("short_field", "short"));
        columns.add(columnInfo("byte_field", "byte"));
        columns.add(columnInfo("double_field", "double"));
        columns.add(columnInfo("scaled_float_field", "scaled_float"));
        columns.add(columnInfo("boolean_field", "boolean"));
        columns.add(columnInfo("ip_field", "ip"));
        columns.add(columnInfo("text_field", "text"));
        columns.add(columnInfo("keyword_field", "keyword"));
        columns.add(columnInfo("constant_keyword_field", "keyword"));
        columns.add(columnInfo("wildcard_field", "keyword"));
        columns.add(columnInfo("geo_point_no_dv_field", "geo_point"));
        columns.add(columnInfo("geo_shape_field", "geo_shape"));
        columns.add(columnInfo("shape_field", "shape"));
        
        expectedResponse.put("columns", columns);
        additionalColumns.accept(columns);
        List<List<Object>> rows = new ArrayList<>(numDocs);
        expectedResponse.put("rows", rows);

        Map<String, Object> fieldValues;
        String constantKeywordValue = randomAlphaOfLength(5);
        for (int i = 0; i < numDocs; i++) {
            fieldValues = new LinkedHashMap<>();
            StringBuilder builder = new StringBuilder();
            builder.append("{");
            builder.append("\"id\":" + i + ",");
            builder.append("\"long_field\":" + fieldValues.computeIfAbsent("long_field", v -> randomLong()) + ",");
            builder.append("\"integer_field\":" + fieldValues.computeIfAbsent("integer_field", v -> randomInt()) + ",");
            builder.append("\"short_field\":" + fieldValues.computeIfAbsent("short_field", v -> Integer.valueOf(randomShort())) + ",");
            builder.append("\"byte_field\":" + fieldValues.computeIfAbsent("byte_field", v -> Integer.valueOf(randomByte())) + ",");
            builder.append("\"double_field\":" + fieldValues.computeIfAbsent("double_field", v -> randomDouble()) + ",");
            builder.append("\"scaled_float_field\":" + fieldValues.computeIfAbsent("scaled_float_field", v -> 123.5d) + ",");
            builder.append("\"boolean_field\":" + fieldValues.computeIfAbsent("boolean_field", v -> randomBoolean()) + ",");
            builder.append("\"ip_field\":\"" + fieldValues.computeIfAbsent("ip_field", v -> "123.123.123.123") + "\",");            
            builder.append("\"text_field\": \"" + fieldValues.computeIfAbsent("text_field", v -> randomAlphaOfLength(5)) + "\",");
            builder.append("\"keyword_field\": \"" + fieldValues.computeIfAbsent("keyword_field", v -> randomAlphaOfLength(5)) + "\",");
            builder.append("\"constant_keyword_field\": \"" + fieldValues.computeIfAbsent("constant_keyword_field",
                v -> constantKeywordValue) + "\",");
            builder.append("\"wildcard_field\": \"" + fieldValues.computeIfAbsent("wildcard_field", v -> randomAlphaOfLength(5)) + "\",");
            builder.append("\"geo_point_no_dv_field\":{\"lat\":\"40.123456\", \"lon\":\"100.234567\"},");
            fieldValues.put("geo_point_no_dv_field", "POINT (100.234567 40.123456)");
            builder.append("\"geo_shape_field\":\"POINT (-122.083843 37.386483 30)\",");
            fieldValues.put("geo_shape_field", "POINT (-122.083843 37.386483 30.0)");
            builder.append("\"shape_field\":\"POINT (-122.083843 37.386483 30)\"");
            fieldValues.put("shape_field", "POINT (-122.083843 37.386483 30.0)");
            additionalValues.accept(builder, fieldValues);
            builder.append("}");
            
            Request request = new Request("PUT", index + "/_doc/" + i);
            request.setJsonEntity(builder.toString());
            assertOK(client().performRequest(request));

            List<Object> row = new ArrayList<>(fieldValues.values());
            rows.add(row);
        }
        return expectedResponse;
    }

    private Map<String, Object> columnInfo(String name, String type) {
        Map<String, Object> column = new HashMap<>();
        column.put("name", name);
        column.put("type", type);
        //if (bwcVersion.onOrAfter(Version.V_7_2_0)) {
        column.put("display_size", SqlDataTypes.displaySize(SqlDataTypes.fromTypeName(type)));
        //} else {
            //column.put("display_size", 0);
        //}
        return unmodifiableMap(column);
    }

    private void assertAllTypesWithNodes(Map<String, Object> expectedResponse, List<Node> nodesList) throws Exception {
        try (
            RestClient client = buildClient(restClientSettings(), nodesList.stream().map(Node::getPublishAddress).toArray(HttpHost[]::new))
        ) {
            Request request = new Request("POST", "_sql");
            String versionSupport = bwcVersion.onOrAfter(Version.V_7_7_0) ? ",\"version\":\"" + newVersion.toString() + "\"" : "";
            String binaryFormatSupport = bwcVersion.onOrAfter(Version.V_7_6_0) ? ",\"binary_format\":\"false\"" : "";

            @SuppressWarnings("unchecked")
            List<Map<String, Object>> columns = (List<Map<String, Object>>) expectedResponse.get("columns");
            String fieldsList = columns.stream().map(m -> (String) m.get("name")).collect(Collectors.toList()).stream()
                .collect(Collectors.joining(", "));
            request.setJsonEntity(
                "{\"mode\":\"jdbc\"" + versionSupport + binaryFormatSupport + ",\"query\":\"SELECT " + fieldsList + " FROM "
                    + index
                    + " ORDER BY id\"}"
            );
            assertBusy(() -> { assertResponse(expectedResponse, runSql(client, request)); });
        }
    }

    private void assertResponse(Map<String, Object> expected, Map<String, Object> actual) {
        if (false == expected.equals(actual)) {
            NotEqualMessageBuilder message = new NotEqualMessageBuilder();
            message.compareMaps(actual, expected);
            fail("Response does not match:\n" + message.toString());
        }
    }

    private Map<String, Object> runSql(RestClient client, Request request) throws IOException {
        Response response = client.performRequest(request);
        try (InputStream content = response.getEntity().getContent()) {
            return XContentHelper.convertToMap(JsonXContent.jsonXContent, content, false);
        }
    }

    private static String readResource(String location) throws IOException {
        StringBuilder builder = new StringBuilder();
        try (BufferedReader reader = new BufferedReader(new InputStreamReader(SqlSearchIT.class.getResourceAsStream(location),
            StandardCharsets.UTF_8)))
        {
            String line = reader.readLine();
            while (line != null) {
                if (line.trim().startsWith("//") == false) {
                    builder.append(line);
                    builder.append('\n');
                }
                line = reader.readLine();
            }
            return builder.toString();
        }
    }

    public static Nodes buildNodeAndVersions(RestClient client) throws IOException {
        Response response = client.performRequest(new Request("GET", "_nodes"));
        ObjectPath objectPath = ObjectPath.createFromResponse(response);
        Map<String, Object> nodesAsMap = objectPath.evaluate("nodes");
        Nodes nodes = new Nodes();
        for (String id : nodesAsMap.keySet()) {
            nodes.add(
                new Node(
                    id,
                    Version.fromString(objectPath.evaluate("nodes." + id + ".version")),
                    HttpHost.create(objectPath.evaluate("nodes." + id + ".http.publish_address"))
                )
            );
        }
        return nodes;
    }

    public static final class Nodes extends HashMap<String, Node> {

        public void add(Node node) {
            put(node.getId(), node);
        }

        public List<Node> getNewNodes() {
            Version bwcVersion = getBWCVersion();
            return values().stream().filter(n -> n.getVersion().after(bwcVersion)).collect(Collectors.toList());
        }

        public List<Node> getBWCNodes() {
            Version bwcVersion = getBWCVersion();
            return values().stream().filter(n -> n.getVersion().equals(bwcVersion)).collect(Collectors.toList());
        }

        public Version getBWCVersion() {
            if (isEmpty()) {
                throw new IllegalStateException("no nodes available");
            }
            return Version.fromId(values().stream().map(node -> node.getVersion().id).min(Integer::compareTo).get());
        }

        @Override
        public String toString() {
            return "Nodes{"
                + values().stream().map(Node::toString).collect(Collectors.joining("\n"))
                + '}';
        }
    }

    public static final class Node {
        private final String id;
        private final Version version;
        private final HttpHost publishAddress;

        Node(String id, Version version, HttpHost publishAddress) {
            this.id = id;
            this.version = version;
            this.publishAddress = publishAddress;
        }

        public String getId() {
            return id;
        }

        public HttpHost getPublishAddress() {
            return publishAddress;
        }

        public Version getVersion() {
            return version;
        }

        @Override
        public String toString() {
            return "Node{" + "id='" + id + '\'' + ", version=" + version + '}';
        }
    }
}
