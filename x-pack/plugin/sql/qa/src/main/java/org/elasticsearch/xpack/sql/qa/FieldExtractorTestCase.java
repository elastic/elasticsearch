/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */

package org.elasticsearch.xpack.sql.qa;

import org.apache.http.entity.ContentType;
import org.apache.http.entity.StringEntity;
import org.elasticsearch.client.Request;
import org.elasticsearch.client.Response;
import org.elasticsearch.common.Strings;
import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.common.xcontent.XContentHelper;
import org.elasticsearch.common.xcontent.json.JsonXContent;
import org.elasticsearch.test.rest.ESRestTestCase;
import org.elasticsearch.xpack.sql.qa.rest.RestSqlTestCase;

import java.io.IOException;
import java.io.InputStream;
import java.sql.JDBCType;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import java.util.Map.Entry;

import static java.util.Collections.singletonList;
import static org.hamcrest.Matchers.containsString;
import static org.elasticsearch.xpack.sql.qa.rest.RestSqlTestCase.assertResponse;
import static org.elasticsearch.xpack.sql.qa.rest.RestSqlTestCase.columnInfo;
import static org.elasticsearch.xpack.sql.qa.rest.RestSqlTestCase.expectBadRequest;

/**
 * Test class covering parameters/settings that can be used in the mapping of an index
 * and which can affect the outcome of _source extraction and parsing when retrieving
 * values from Elasticsearch.
 */
public abstract class FieldExtractorTestCase extends ESRestTestCase {
    
    public void testTextField() throws IOException {
        String query = "SELECT text_field FROM test";
        String text = randomAlphaOfLength(20);
        boolean explicitSourceSetting = randomBoolean(); // default (no _source setting) or explicit setting
        boolean enableSource = randomBoolean();          // enable _source at index level
        
        Map<String, Object> indexProps = new HashMap<>(1);
        indexProps.put("_source", enableSource);
        
        createIndexWithFieldTypeAndProperties(null, explicitSourceSetting ? indexProps : null, "text");
        index("{\"text_field\":\"" + text + "\"}");
        
        if (explicitSourceSetting == false || enableSource == true) {
            Map<String, Object> expected = new HashMap<>();
            expected.put("columns", Arrays.asList(
                    columnInfo("plain", "text_field", "text", JDBCType.VARCHAR, Integer.MAX_VALUE)
            ));
            expected.put("rows", singletonList(singletonList(text)));
            assertResponse(expected, runSql(query));
        } else {
            expectBadRequest(() -> {
                client().performRequest(buildRequest(query));
                return Collections.emptyMap();
            }, containsString("unable to fetch fields from _source field: _source is disabled in the mappings for index [test]"));
        }
    }
    
    public void testKeywordField() throws IOException {
        String keyword = randomAlphaOfLength(20);
        // _source for `keyword` fields doesn't matter, as they should be taken from docvalue_fields
        boolean explicitSourceSetting = randomBoolean(); // default (no _source setting) or explicit setting
        boolean enableSource = randomBoolean();          // enable _source at index level
        boolean ignoreAbove = randomBoolean();
        
        Map<String, Object> indexProps = new HashMap<>(1);
        indexProps.put("_source", enableSource);
        
        Map<String, Map<String, Object>> fieldProps = null;
        if (ignoreAbove) {
            fieldProps = new HashMap<>(1);
            Map<String, Object> fieldProp = new HashMap<>(1);
            fieldProp.put("ignore_above", 10);
            fieldProps.put("keyword_field", fieldProp);
        }
        
        createIndexWithFieldTypeAndProperties(fieldProps, explicitSourceSetting ? indexProps : null, "keyword");
        index("{\"keyword_field\":\"" + keyword + "\"}");
        
        Map<String, Object> expected = new HashMap<>();
        expected.put("columns", Arrays.asList(
                columnInfo("plain", "keyword_field", "keyword", JDBCType.VARCHAR, Integer.MAX_VALUE)
        ));
        expected.put("rows", singletonList(singletonList(ignoreAbove ? null : keyword)));
        assertResponse(expected, runSql("SELECT keyword_field FROM test"));
    }
    
    public void testFractionsForNonFloatingPointTypes() throws IOException {
        String floatingPointNumber = "123.456";
        String fieldType = randomFrom("long", "integer", "short", "byte");
        
        createIndexWithFieldTypeAndProperties(null, null, fieldType);
        index("{\"" + fieldType + "_field\":\"" + floatingPointNumber + "\"}");
        
        Map<String, Object> expected = new HashMap<>();
        expected.put("columns", Arrays.asList(
                columnInfo("plain", fieldType + "_field", fieldType, jdbcTypeFor(fieldType), Integer.MAX_VALUE)
        ));
        
        // because "coerce" is true, a "123.456" floating point number STRING should be converted to 123, no matter the numeric field type
        expected.put("rows", singletonList(singletonList(123)));
        assertResponse(expected, runSql("SELECT " + fieldType + "_field FROM test"));
    }
    
    public void testCoerceForFloatingPointTypes() throws IOException {
        String floatingPointNumber = "123.456";
        String fieldType = randomFrom("double", "float", "half_float", "scaled_float");
        boolean isScaledFloat = fieldType == "scaled_float";
        
        Map<String, Map<String, Object>> fieldProps = null;
        if (isScaledFloat) {
            fieldProps = new HashMap<>(1);
            Map<String, Object> fieldProp = new HashMap<>(1);
            fieldProp.put("scaling_factor", 10);            // scaling_factor is required for "scaled_float"
            fieldProps.put(fieldType + "_field", fieldProp);
        }
        
        createIndexWithFieldTypeAndProperties(fieldProps, null, fieldType);
        // important here is to pass floatingPointNumber as a string: "float_field": "123.456"
        index("{\"" + fieldType + "_field\":\"" + floatingPointNumber + "\"}");
        
        Map<String, Object> expected = new HashMap<>();
        expected.put("columns", Arrays.asList(
                columnInfo("plain", fieldType + "_field", fieldType, jdbcTypeFor(fieldType), Integer.MAX_VALUE)
        ));
        
        // because "coerce" is true, a "123.456" floating point number STRING should be converted to 123.456 as number
        // and converted to 123.5 for "scaled_float" type
        expected.put("rows", singletonList(singletonList(isScaledFloat ? 123.5 : 123.456)));
        assertResponse(expected, runSql("SELECT " + fieldType + "_field FROM test"));
    }
    
    public void testLongFieldType() throws IOException {
        testField("long", randomLong());
    }
    
    public void testIntegerFieldType() throws IOException {
        testField("integer", randomInt());
    }
    
    public void testShortFieldType() throws IOException {
        // making these Integers because the json parser that is used to read the values from the response will create
        // Integers for short and byte values
        testField("short", ((Number) randomShort()).intValue());
    }
    
    public void testByteFieldType() throws IOException {
        // making these Integers because the json parser that is used to read the values from the response will create
        // Integers for short and byte values
        testField("byte", ((Number) randomByte()).intValue());
    }
    
    private void testField(String fieldType, Object value) throws IOException {
        String fieldName = fieldType + "_field";
        String query = "SELECT " + fieldName + " FROM test";
        Object actualValue = value;
        boolean explicitSourceSetting = randomBoolean(); // default (no _source setting) or explicit setting
        boolean enableSource = randomBoolean();          // enable _source at index level
        boolean ignoreMalformed = randomBoolean();       // ignore_malformed is true, thus test a non-number value
        
        Map<String, Object> indexProps = new HashMap<>(1);
        indexProps.put("_source", enableSource);
        
        Map<String, Map<String, Object>> fieldProps = null;
        if (ignoreMalformed) {
            fieldProps = new HashMap<>(1);
            Map<String, Object> fieldProp = new HashMap<>(1);
            // on purpose use a string instead of a number and check for null when querying the field's value
            fieldProp.put("ignore_malformed", true);
            fieldProps.put(fieldName, fieldProp);
            actualValue = "\"foo\"";
        }
        
        createIndexWithFieldTypeAndProperties(fieldProps, explicitSourceSetting ? indexProps : null, fieldType);
        index("{\"" + fieldName + "\":" + actualValue + "}");

        if (explicitSourceSetting == false || enableSource == true) {
            Map<String, Object> expected = new HashMap<>();
            expected.put("columns", Arrays.asList(
                    columnInfo("plain", fieldName, fieldType, jdbcTypeFor(fieldType), Integer.MAX_VALUE)
            ));
            expected.put("rows", singletonList(singletonList(ignoreMalformed ? null : actualValue)));
            assertResponse(expected, runSql(query));
        } else {
            expectBadRequest(() -> {
                client().performRequest(buildRequest(query));
                return Collections.emptyMap();
            }, containsString("unable to fetch fields from _source field: _source is disabled in the mappings for index [test]"));
        }
    }
    
    public void testBooleanField() throws IOException {
        String query = "SELECT boolean_field FROM test";
        boolean booleanField = randomBoolean();
        boolean explicitSourceSetting = randomBoolean(); // default (no _source setting) or explicit setting
        boolean enableSource = randomBoolean();          // enable _source at index level
        boolean asString = randomBoolean();              // pass true or false as string "true" or "false
        
        Map<String, Object> indexProps = new HashMap<>(1);
        indexProps.put("_source", enableSource);
        
        createIndexWithFieldTypeAndProperties(null, explicitSourceSetting ? indexProps : null, "boolean");
        if (asString) {
            index("{\"boolean_field\":\"" + booleanField + "\"}");
        } else {
            index("{\"boolean_field\":" + booleanField + "}");
        }
        
        if (explicitSourceSetting == false || enableSource == true) {
            Map<String, Object> expected = new HashMap<>();
            expected.put("columns", Arrays.asList(
                    columnInfo("plain", "boolean_field", "boolean", JDBCType.BOOLEAN, Integer.MAX_VALUE)
            ));
            // adding the boolean as a String here because parsing the response will yield a "true"/"false" String
            expected.put("rows", singletonList(singletonList(asString ? String.valueOf(booleanField) : booleanField)));
            assertResponse(expected, runSql(query));
        } else {
            expectBadRequest(() -> {
                client().performRequest(buildRequest(query));
                return Collections.emptyMap();
            }, containsString("unable to fetch fields from _source field: _source is disabled in the mappings for index [test]"));
        }
    }
    
    public void testIpField() throws IOException {
        String query = "SELECT ip_field FROM test";
        String ipField = "192.168.1.1";
        boolean explicitSourceSetting = randomBoolean(); // default (no _source setting) or explicit setting
        boolean enableSource = randomBoolean();          // enable _source at index level
        
        Map<String, Object> indexProps = new HashMap<>(1);
        indexProps.put("_source", enableSource);
        
        createIndexWithFieldTypeAndProperties(null, explicitSourceSetting ? indexProps : null, "ip");
        index("{\"ip_field\":\"" + ipField + "\"}");
        
        if (explicitSourceSetting == false || enableSource == true) {
            Map<String, Object> expected = new HashMap<>();
            expected.put("columns", Arrays.asList(
                    columnInfo("plain", "ip_field", "ip", JDBCType.VARCHAR, Integer.MAX_VALUE)
            ));
            expected.put("rows", singletonList(singletonList(ipField)));
            assertResponse(expected, runSql(query));
        } else {
            expectBadRequest(() -> {
                client().performRequest(buildRequest(query));
                return Collections.emptyMap();
            }, containsString("unable to fetch fields from _source field: _source is disabled in the mappings for index [test]"));
        }
    }
    
    public void testAliasFromDocValueField() throws IOException {
        String keyword = randomAlphaOfLength(20);
        
        createIndexWithFieldTypeAndAlias(null, null, "keyword");
        index("{\"keyword_field\":\"" + keyword + "\"}");

        Map<String, Object> expected = new HashMap<>();
        expected.put("columns", Arrays.asList(
                columnInfo("plain", "keyword_field", "keyword", JDBCType.VARCHAR, Integer.MAX_VALUE),
                columnInfo("plain", "keyword_field_alias", "keyword", JDBCType.VARCHAR, Integer.MAX_VALUE),
                columnInfo("plain", "a.b.c.keyword_field_alias", "keyword", JDBCType.VARCHAR, Integer.MAX_VALUE)
        ));
        expected.put("rows", singletonList(Arrays.asList(keyword, keyword, keyword)));
        assertResponse(expected, runSql("SELECT keyword_field, keyword_field_alias, a.b.c.keyword_field_alias FROM test"));
    }
    
    public void testAliasFromSourceField() throws IOException {
        String text = randomAlphaOfLength(20);
        
        createIndexWithFieldTypeAndAlias(null, null, "text");
        index("{\"text_field\":\"" + text + "\"}");

        Map<String, Object> expected = new HashMap<>();
        expected.put("columns", Arrays.asList(
                columnInfo("plain", "text_field", "text", JDBCType.VARCHAR, Integer.MAX_VALUE),
                columnInfo("plain", "text_field_alias", "text", JDBCType.VARCHAR, Integer.MAX_VALUE),
                columnInfo("plain", "a.b.c.text_field_alias", "text", JDBCType.VARCHAR, Integer.MAX_VALUE)
        ));
        expected.put("rows", singletonList(Arrays.asList(text, null, null)));
        assertResponse(expected, runSql("SELECT text_field, text_field_alias, a.b.c.text_field_alias FROM test"));
    }
    
    public void testAliasAggregatableFromSourceField() throws IOException {
        int number = randomInt();
        
        createIndexWithFieldTypeAndAlias(null, null, "integer");
        index("{\"integer_field\":" + number + "}");

        Map<String, Object> expected = new HashMap<>();
        expected.put("columns", Arrays.asList(
                columnInfo("plain", "integer_field", "integer", JDBCType.INTEGER, Integer.MAX_VALUE),
                columnInfo("plain", "integer_field_alias", "integer", JDBCType.INTEGER, Integer.MAX_VALUE),
                columnInfo("plain", "a.b.c.integer_field_alias", "integer", JDBCType.INTEGER, Integer.MAX_VALUE)
        ));
        expected.put("rows", singletonList(Arrays.asList(number, null, number)));
        assertResponse(expected, runSql("SELECT integer_field, integer_field_alias, a.b.c.integer_field_alias FROM test"));
    }
    
    private void createIndexWithFieldTypeAndAlias(Map<String, Map<String, Object>> fieldProps, Map<String, Object> indexProps,
            String... types) throws IOException {
        createIndexWithFieldTypeAndProperties(fieldProps, indexProps, true, types);
    }
    
    private void createIndexWithFieldTypeAndProperties(Map<String, Map<String, Object>> fieldProps, Map<String, Object> indexProps,
            String... types) throws IOException {
        createIndexWithFieldTypeAndProperties(fieldProps, indexProps, false, types);
    }
    
    private void createIndexWithFieldTypeAndProperties(Map<String, Map<String, Object>> fieldProps, Map<String, Object> indexProps,
            boolean withAlias, String... types) throws IOException {
        Request request = new Request("PUT", "/test");      
        XContentBuilder index = JsonXContent.contentBuilder().prettyPrint().startObject();

        index.startObject("mappings"); {
            if (indexProps != null) {
                for (Entry<String, Object> prop : indexProps.entrySet()) {
                    if (prop.getValue() instanceof Boolean) {
                        index.startObject(prop.getKey()); {
                            index.field("enabled", prop.getValue());
                        }
                        index.endObject();
                    }
                }
            }
            index.startObject("properties"); {
                for (String type : types) {
                    String fieldName = type + "_field";
                    index.startObject(fieldName); {
                        index.field("type", type);
                        if (fieldProps != null) {
                            Map<String, Object> fieldProp = fieldProps.get(fieldName);
                            for (Entry<String, Object> prop : fieldProp.entrySet()) {
                                index.field(prop.getKey(), prop.getValue());
                            }
                        }
                    }
                    index.endObject();
                    
                    if (withAlias) {
                        // create two aliases - one within a hierarchy, the other just a simple field w/o hierarchy
                        index.startObject(fieldName + "_alias"); {
                            index.field("type", "alias");
                            index.field("path", fieldName);
                        }
                        index.endObject();
                        index.startObject("a.b.c." + fieldName + "_alias"); {
                            index.field("type", "alias");
                            index.field("path", fieldName);
                        }
                        index.endObject();
                    }
                }
            }
            index.endObject();
        }
        index.endObject();
        index.endObject();

        request.setJsonEntity(Strings.toString(index));
        client().performRequest(request);
    }
    
    private void index(String... docs) throws IOException {
        Request request = new Request("POST", "/test/_bulk");
        request.addParameter("refresh", "true");
        StringBuilder bulk = new StringBuilder();
        for (String doc : docs) {
            bulk.append("{\"index\":{}\n");
            bulk.append(doc + "\n");
        }
        request.setJsonEntity(bulk.toString());
        client().performRequest(request);
    }
    
    private Request buildRequest(String query) {
        Request request = new Request("POST", RestSqlTestCase.SQL_QUERY_REST_ENDPOINT);
        request.addParameter("error_trace", "true");
        request.addParameter("pretty", "true");
        request.setEntity(new StringEntity("{\"query\":\"" + query + "\",\"mode\":\"plain\"}", ContentType.APPLICATION_JSON));
        
        return request;
    }
    
    private Map<String, Object> runSql(String query) throws IOException {
        Response response = client().performRequest(buildRequest(query));
        try (InputStream content = response.getEntity().getContent()) {
            return XContentHelper.convertToMap(JsonXContent.jsonXContent, content, false);
        }
    }
    
    private JDBCType jdbcTypeFor(String esType) {
        switch(esType) {
            case "long":
                return JDBCType.BIGINT;
            case "integer":
                return JDBCType.INTEGER;
            case "short":
                return JDBCType.SMALLINT;
            case "byte":
                return JDBCType.TINYINT;
            case "float":
                return JDBCType.REAL;
            case "double":
                return JDBCType.DOUBLE;
            case "half_float":
                return JDBCType.FLOAT;
            case "scaled_float":
                return JDBCType.DOUBLE;
            default:
                throw new AssertionError("Illegal value [" + esType + "] for data type");
        }
    }
}
