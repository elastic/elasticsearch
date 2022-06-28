/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.sql.qa.mixed_node;

import org.apache.http.HttpHost;
import org.apache.lucene.document.HalfFloatPoint;
import org.elasticsearch.Version;
import org.elasticsearch.client.Request;
import org.elasticsearch.client.Response;
import org.elasticsearch.client.RestClient;
import org.elasticsearch.cluster.metadata.IndexMetadata;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.xcontent.XContentHelper;
import org.elasticsearch.test.NotEqualMessageBuilder;
import org.elasticsearch.test.rest.ESRestTestCase;
import org.elasticsearch.xcontent.json.JsonXContent;
import org.elasticsearch.xpack.ql.TestNode;
import org.elasticsearch.xpack.ql.TestNodes;
import org.junit.After;
import org.junit.Before;

import java.io.IOException;
import java.io.InputStream;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.function.BiConsumer;
import java.util.function.Consumer;
import java.util.stream.Collectors;

import static java.util.Collections.unmodifiableMap;
import static org.elasticsearch.xpack.ql.TestUtils.buildNodeAndVersions;
import static org.elasticsearch.xpack.ql.TestUtils.readResource;

public class SqlSearchIT extends ESRestTestCase {

    /*
     * The version where we made a significant change to how we query ES and how we interpret the results we get from ES, is 7.12
     * (the switch from extracting from _source and docvalues to using the "fields" API). The behavior of the tests is slightly
     * changed on some versions and it all depends on when this above mentioned change was made.
     */
    private static final Version FIELDS_API_QL_INTRODUCTION = Version.V_7_12_0;
    private static final String index = "test_sql_mixed_versions";
    private static int numShards;
    private static int numReplicas = 1;
    private static int numDocs;
    private static TestNodes nodes;
    private static List<TestNode> newNodes;
    private static List<TestNode> bwcNodes;
    private static Version bwcVersion;
    private static boolean isBwcNodeBeforeFieldsApiInQL;
    private static boolean halfFloatMightReturnFullFloatPrecision;

    @Before
    public void createIndex() throws IOException {
        nodes = buildNodeAndVersions(client());
        numShards = nodes.size();
        numDocs = randomIntBetween(numShards, 15);
        newNodes = new ArrayList<>(nodes.getNewNodes());
        bwcNodes = new ArrayList<>(nodes.getBWCNodes());
        bwcVersion = nodes.getBWCNodes().get(0).getVersion();
        isBwcNodeBeforeFieldsApiInQL = bwcVersion.before(FIELDS_API_QL_INTRODUCTION);
        halfFloatMightReturnFullFloatPrecision = bwcVersion.before(Version.V_7_13_0);

        String mappings = readResource(SqlSearchIT.class.getResourceAsStream("/all_field_types.json"));
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
        if (indexExists(index)) {
            deleteIndex(index);
        }
    }

    public void testAllTypesWithRequestToOldNodes() throws Exception {
        Map<String, Object> expectedResponse = prepareTestData(columns -> {
            columns.add(columnInfo("geo_point_field", "geo_point"));
            columns.add(columnInfo("float_field", "float"));
            /*
             * In 7.12.x we got full float precision from half floats
             * back from the fields API. When the cluster is mixed with
             * that version we don't fetch the half float because we
             * can't make any assertions about it.
             */
            if (isBwcNodeBeforeFieldsApiInQL || false == halfFloatMightReturnFullFloatPrecision) {
                columns.add(columnInfo("half_float_field", "half_float"));
            }
        }, (builder, fieldValues) -> {
            Float randomFloat = randomFloat();
            // before "fields" API being added to QL, numbers were re-parsed from _source with a similar approach to
            // indexing docvalues and for floating point numbers this may be different from the actual value passed in the _source
            // floats were indexed as Doubles and the values returned had a greater precision and more decimals
            builder.append(",");
            if (isBwcNodeBeforeFieldsApiInQL) {
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
                /*
                 * Double.valueOf(float.toString) gets a `double` representing
                 * the `float` that we'd get by going through json which is
                 * base 10. just casting the `float` to a `double` will get
                 * a lower number with a lot more trailing digits because
                 * the cast adds *binary* 0s to the end. And those binary
                 * 0s don't translate the same as json's decimal 0s.
                 */
                fieldValues.put("float_field", Double.valueOf(Float.valueOf(randomFloat).toString()));
                /*
                 * In 7.12.x we got full float precision from half floats
                 * back from the fields API. When the cluster is mixed with
                 * that version we don't fetch the half float because we
                 * can't make any assertions about it.
                 */
                float roundedHalfFloat = HalfFloatPoint.sortableShortToHalfFloat(HalfFloatPoint.halfFloatToSortableShort(randomFloat));
                builder.append("\"half_float_field\":\"" + randomFloat + "\"");
                if (false == halfFloatMightReturnFullFloatPrecision) {
                    fieldValues.put("half_float_field", Double.valueOf(Float.toString(roundedHalfFloat)));
                }
            }
        });
        assertAllTypesWithNodes(expectedResponse, bwcNodes);
    }

    public void testAllTypesWithRequestToUpgradedNodes() throws Exception {
        Map<String, Object> expectedResponse = prepareTestData(columns -> {
            columns.add(columnInfo("geo_point_field", "geo_point"));
            columns.add(columnInfo("float_field", "float"));
            /*
             * In 7.12.x we got full float precision from half floats
             * back from the fields API. When the cluster is mixed with
             * that version we don't fetch the half float because we
             * can't make any assertions about it.
             */
            if (isBwcNodeBeforeFieldsApiInQL || false == halfFloatMightReturnFullFloatPrecision) {
                columns.add(columnInfo("half_float_field", "half_float"));
            }
        }, (builder, fieldValues) -> {
            Float randomFloat = randomFloat();
            builder.append(",");
            if (isBwcNodeBeforeFieldsApiInQL) {
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
                /*
                 * Double.valueOf(float.toString) gets a `double` representing
                 * the `float` that we'd get by going through json which is
                 * base 10. just casting the `float` to a `double` will get
                 * a lower number with a lot more trailing digits because
                 * the cast adds *binary* 0s to the end. And those binary
                 * 0s don't translate the same as json's decimal 0s.
                 */
                fieldValues.put("float_field", Double.valueOf(Float.valueOf(randomFloat).toString()));
                /*
                 * In 7.12.x we got full float precision from half floats
                 * back from the fields API. When the cluster is mixed with
                 * that version we don't fetch the half float because we
                 * can't make any assertions about it.
                 */
                float roundedHalfFloat = HalfFloatPoint.sortableShortToHalfFloat(HalfFloatPoint.halfFloatToSortableShort(randomFloat));
                builder.append("\"half_float_field\":\"" + randomFloat + "\"");
                if (false == halfFloatMightReturnFullFloatPrecision) {
                    fieldValues.put("half_float_field", Double.valueOf(Float.toString(roundedHalfFloat)));
                }
            }
        });
        assertAllTypesWithNodes(expectedResponse, newNodes);
    }

    @SuppressWarnings("unchecked")
    private Map<String, Object> prepareTestData(
        Consumer<List<Map<String, Object>>> additionalColumns,
        BiConsumer<StringBuilder, Map<String, Object>> additionalValues
    ) throws IOException {
        Map<String, Object> expectedResponse = new HashMap<>();
        List<Map<String, Object>> columns = new ArrayList<>();
        columns.add(columnInfo("interval_year", "interval_year"));
        columns.add(columnInfo("interval_minute", "interval_minute"));
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
            fieldValues.put("interval_year", "P150Y");
            fieldValues.put("interval_minute", "PT2H43M");

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
            builder.append(
                "\"constant_keyword_field\": \"" + fieldValues.computeIfAbsent("constant_keyword_field", v -> constantKeywordValue) + "\","
            );
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
        return unmodifiableMap(column);
    }

    private void assertAllTypesWithNodes(Map<String, Object> expectedResponse, List<TestNode> nodesList) throws Exception {
        try (
            RestClient client = buildClient(
                restClientSettings(),
                nodesList.stream().map(TestNode::getPublishAddress).toArray(HttpHost[]::new)
            )
        ) {
            @SuppressWarnings("unchecked")
            List<Map<String, Object>> columns = (List<Map<String, Object>>) expectedResponse.get("columns");

            String intervalYearMonth = "INTERVAL '150' YEAR AS interval_year, ";
            String intervalDayTime = "INTERVAL '163' MINUTE AS interval_minute, ";
            // get all fields names from the expected response built earlier, skipping the intervals as they execute locally
            // and not taken from the index itself
            String fieldsList = columns.stream()
                .map(m -> (String) m.get("name"))
                .filter(str -> str.startsWith("interval") == false)
                .collect(Collectors.toList())
                .stream()
                .collect(Collectors.joining(", "));
            String query = "SELECT " + intervalYearMonth + intervalDayTime + fieldsList + " FROM " + index + " ORDER BY id";

            Request request = new Request("POST", "_sql");
            request.setJsonEntity(SqlCompatIT.sqlQueryEntityWithOptionalMode(query, bwcVersion));
            assertBusy(() -> { assertResponse(expectedResponse, dropDisplaySizes(runSql(client, request))); });
        }
    }

    private Map<String, Object> dropDisplaySizes(Map<String, Object> response) {
        // in JDBC mode, display_size will be part of the response, so remove it because it's not part of the expected response
        @SuppressWarnings("unchecked")
        List<Map<String, Object>> columns = (List<Map<String, Object>>) response.get("columns");
        List<Map<String, Object>> columnsWithoutDisplaySizes = columns.stream()
            .peek(column -> column.remove("display_size"))
            .collect(Collectors.toList());
        response.put("columns", columnsWithoutDisplaySizes);
        return response;
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
}
