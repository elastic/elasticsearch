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
import org.elasticsearch.xpack.sql.qa.rest.BaseRestSqlTestCase;
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
import static org.elasticsearch.xpack.sql.qa.rest.RestSqlTestCase.assertResponse;
import static org.elasticsearch.xpack.sql.qa.rest.RestSqlTestCase.columnInfo;
import static org.elasticsearch.xpack.sql.qa.rest.RestSqlTestCase.expectBadRequest;
import static org.hamcrest.Matchers.containsString;

/**
 * Test class covering parameters/settings that can be used in the mapping of an index
 * and which can affect the outcome of _source extraction and parsing when retrieving
 * values from Elasticsearch.
 */
public abstract class FieldExtractorTestCase extends BaseRestSqlTestCase {

    /*
     *    "text_field": {
     *       "text": "keyword"
     *    }
     */
    public void testTextField() throws IOException {
        String query = "SELECT text_field FROM test";
        String text = randomAlphaOfLength(20);
        boolean explicitSourceSetting = randomBoolean(); // default (no _source setting) or explicit setting
        boolean enableSource = randomBoolean();          // enable _source at index level

        Map<String, Object> indexProps = new HashMap<>(1);
        indexProps.put("_source", enableSource);

        createIndexWithFieldTypeAndProperties("text", null, explicitSourceSetting ? indexProps : null);
        index("{\"text_field\":\"" + text + "\"}");

        if (explicitSourceSetting == false || enableSource) {
            Map<String, Object> expected = new HashMap<>();
            expected.put("columns", Arrays.asList(columnInfo("plain", "text_field", "text", JDBCType.VARCHAR, Integer.MAX_VALUE)));
            expected.put("rows", singletonList(singletonList(text)));
            assertResponse(expected, runSql(query));
        } else {
            expectSourceDisabledError(query);
        }
    }

    /*
     *    "keyword_field": {
     *       "type": "keyword",
     *       "ignore_above": 10
     *    }
     */
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

        createIndexWithFieldTypeAndProperties("keyword", fieldProps, explicitSourceSetting ? indexProps : null);
        index("{\"keyword_field\":\"" + keyword + "\"}");

        Map<String, Object> expected = new HashMap<>();
        expected.put("columns", Arrays.asList(columnInfo("plain", "keyword_field", "keyword", JDBCType.VARCHAR, Integer.MAX_VALUE)));
        expected.put("rows", singletonList(singletonList(ignoreAbove ? null : keyword)));
        assertResponse(expected, runSql("SELECT keyword_field FROM test"));
    }

    /*
     *    "constant_keyword_field": {
     *       "type": "constant_keyword",
     *       "value": "foo"
     *    }
     */
    public void testConstantKeywordField() throws IOException {
        String value = randomAlphaOfLength(20);
        // _source for `constant_keyword` fields doesn't matter, as they should be taken from docvalue_fields
        boolean explicitSourceSetting = randomBoolean(); // default (no _source setting) or explicit setting
        boolean enableSource = randomBoolean();          // enable _source at index level

        Map<String, Object> indexProps = new HashMap<>(1);
        indexProps.put("_source", enableSource);

        Map<String, Map<String, Object>> fieldProps = null;
        if (randomBoolean()) {
            fieldProps = new HashMap<>(1);
            Map<String, Object> fieldProp = new HashMap<>(1);
            fieldProp.put("value", value);
            fieldProps.put("constant_keyword_field", fieldProp);
        }

        createIndexWithFieldTypeAndProperties("constant_keyword", fieldProps, explicitSourceSetting ? indexProps : null);
        index("{\"constant_keyword_field\":\"" + value + "\"}");

        Map<String, Object> expected = new HashMap<>();
        expected.put(
            "columns",
            Arrays.asList(columnInfo("plain", "constant_keyword_field", "keyword", JDBCType.VARCHAR, Integer.MAX_VALUE))
        );
        expected.put("rows", singletonList(singletonList(value)));
        assertResponse(expected, runSql("SELECT constant_keyword_field FROM test"));
    }

    /*
     *    "long/integer/short/byte_field": {
     *       "type": "long/integer/short/byte"
     *    }
     */
    public void testFractionsForNonFloatingPointTypes() throws IOException {
        String floatingPointNumber = "123.456";
        String fieldType = randomFrom("long", "integer", "short", "byte");

        createIndexWithFieldTypeAndProperties(fieldType, null, null);
        index("{\"" + fieldType + "_field\":\"" + floatingPointNumber + "\"}");

        Map<String, Object> expected = new HashMap<>();
        expected.put(
            "columns",
            Arrays.asList(columnInfo("plain", fieldType + "_field", fieldType, jdbcTypeFor(fieldType), Integer.MAX_VALUE))
        );

        // because "coerce" is true, a "123.456" floating point number STRING should be converted to 123, no matter the numeric field type
        expected.put("rows", singletonList(singletonList(123)));
        assertResponse(expected, runSql("SELECT " + fieldType + "_field FROM test"));
    }

    /*
     *    "double/float/half_float/scaled_float_field": {
     *       "type": "double/float/half_float/scaled_float",
     *       "scaling_factor": 10 (for scaled_float type only)
     *    }
     */
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

        createIndexWithFieldTypeAndProperties(fieldType, fieldProps, null);
        // important here is to pass floatingPointNumber as a string: "float_field": "123.456"
        index("{\"" + fieldType + "_field\":\"" + floatingPointNumber + "\"}");

        Map<String, Object> expected = new HashMap<>();
        expected.put(
            "columns",
            Arrays.asList(columnInfo("plain", fieldType + "_field", fieldType, jdbcTypeFor(fieldType), Integer.MAX_VALUE))
        );

        // because "coerce" is true, a "123.456" floating point number STRING should be converted to 123.456 as number
        // and converted to 123.5 for "scaled_float" type
        expected.put(
            "rows",
            singletonList(
                singletonList(
                    isScaledFloat ? 123.5 : (fieldType != "double" ? Double.valueOf(123.456f) : Double.valueOf(floatingPointNumber))
                )
            )
        );
        assertResponse(expected, runSql("SELECT " + fieldType + "_field FROM test"));
    }

    /*
     *    "long_field": {
     *       "type": "long",
     *       "ignore_malformed": true/false
     *    }
     */
    public void testLongFieldType() throws IOException {
        testField("long", randomLong());
    }

    /*
     *    "integer_field": {
     *       "type": "integer",
     *       "ignore_malformed": true/false
     *    }
     */
    public void testIntegerFieldType() throws IOException {
        testField("integer", randomInt());
    }

    /*
     *    "short_field": {
     *       "type": "short",
     *       "ignore_malformed": true/false
     *    }
     */
    public void testShortFieldType() throws IOException {
        // Use Integer as the json parser that is used to read the values from the response will create
        // Integers for short and byte values
        testField("short", ((Number) randomShort()).intValue());
    }

    /*
     *    "byte_field": {
     *       "type": "byte",
     *       "ignore_malformed": true/false
     *    }
     */
    public void testByteFieldType() throws IOException {
        // Use Integer as the json parser that is used to read the values from the response will create
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

        createIndexWithFieldTypeAndProperties(fieldType, fieldProps, explicitSourceSetting ? indexProps : null);
        index("{\"" + fieldName + "\":" + actualValue + "}");

        if (explicitSourceSetting == false || enableSource) {
            Map<String, Object> expected = new HashMap<>();
            expected.put("columns", Arrays.asList(columnInfo("plain", fieldName, fieldType, jdbcTypeFor(fieldType), Integer.MAX_VALUE)));
            expected.put("rows", singletonList(singletonList(ignoreMalformed ? null : actualValue)));
            assertResponse(expected, runSql(query));
        } else {
            expectSourceDisabledError(query);
        }
    }

    /*
     *    "boolean_field": {
     *       "type": "boolean"
     *    }
     */
    public void testBooleanField() throws IOException {
        String query = "SELECT boolean_field FROM test";
        boolean booleanField = randomBoolean();
        boolean explicitSourceSetting = randomBoolean(); // default (no _source setting) or explicit setting
        boolean enableSource = randomBoolean();          // enable _source at index level
        boolean asString = randomBoolean();              // pass true or false as string "true" or "false

        Map<String, Object> indexProps = new HashMap<>(1);
        indexProps.put("_source", enableSource);

        createIndexWithFieldTypeAndProperties("boolean", null, explicitSourceSetting ? indexProps : null);
        if (asString) {
            index("{\"boolean_field\":\"" + booleanField + "\"}");
        } else {
            index("{\"boolean_field\":" + booleanField + "}");
        }

        if (explicitSourceSetting == false || enableSource) {
            Map<String, Object> expected = new HashMap<>();
            expected.put("columns", Arrays.asList(columnInfo("plain", "boolean_field", "boolean", JDBCType.BOOLEAN, Integer.MAX_VALUE)));
            // adding the boolean as a String here because parsing the response will yield a "true"/"false" String
            expected.put("rows", singletonList(singletonList(asString ? String.valueOf(booleanField) : booleanField)));
            assertResponse(expected, runSql(query));
        } else {
            expectSourceDisabledError(query);
        }
    }

    /*
     *    "ip_field": {
     *       "type": "ip"
     *    }
     */
    public void testIpField() throws IOException {
        String query = "SELECT ip_field FROM test";
        String ipField = "192.168.1.1";
        boolean explicitSourceSetting = randomBoolean(); // default (no _source setting) or explicit setting
        boolean enableSource = randomBoolean();          // enable _source at index level

        Map<String, Object> indexProps = new HashMap<>(1);
        indexProps.put("_source", enableSource);

        createIndexWithFieldTypeAndProperties("ip", null, explicitSourceSetting ? indexProps : null);
        index("{\"ip_field\":\"" + ipField + "\"}");

        if (explicitSourceSetting == false || enableSource) {
            Map<String, Object> expected = new HashMap<>();
            expected.put("columns", Arrays.asList(columnInfo("plain", "ip_field", "ip", JDBCType.VARCHAR, Integer.MAX_VALUE)));
            expected.put("rows", singletonList(singletonList(ipField)));
            assertResponse(expected, runSql(query));
        } else {
            expectSourceDisabledError(query);
        }
    }

    /*
     *    "keyword_field": {
     *       "type": "keyword"
     *    },
     *    "keyword_field_alias": {
     *       "type": "alias",
     *       "path": "keyword_field"
     *    },
     *    "a.b.c.keyword_field_alias": {
     *       "type": "alias",
     *       "path": "keyword_field"
     *    }
     */
    public void testAliasFromDocValueField() throws IOException {
        String keyword = randomAlphaOfLength(20);

        createIndexWithFieldTypeAndAlias("keyword", null, null);
        index("{\"keyword_field\":\"" + keyword + "\"}");

        Map<String, Object> expected = new HashMap<>();
        expected.put(
            "columns",
            Arrays.asList(
                columnInfo("plain", "keyword_field", "keyword", JDBCType.VARCHAR, Integer.MAX_VALUE),
                columnInfo("plain", "keyword_field_alias", "keyword", JDBCType.VARCHAR, Integer.MAX_VALUE),
                columnInfo("plain", "a.b.c.keyword_field_alias", "keyword", JDBCType.VARCHAR, Integer.MAX_VALUE)
            )
        );
        expected.put("rows", singletonList(Arrays.asList(keyword, keyword, keyword)));
        assertResponse(expected, runSql("SELECT keyword_field, keyword_field_alias, a.b.c.keyword_field_alias FROM test"));
    }

    /*
     *    "text_field": {
     *       "type": "text"
     *    },
     *    "text_field_alias": {
     *       "type": "alias",
     *       "path": "text_field"
     *    },
     *    "a.b.c.text_field_alias": {
     *       "type": "alias",
     *       "path": "text_field"
     *    }
     */
    public void testAliasFromSourceField() throws IOException {
        String text = randomAlphaOfLength(20);

        createIndexWithFieldTypeAndAlias("text", null, null);
        index("{\"text_field\":\"" + text + "\"}");

        Map<String, Object> expected = new HashMap<>();
        expected.put(
            "columns",
            Arrays.asList(
                columnInfo("plain", "text_field", "text", JDBCType.VARCHAR, Integer.MAX_VALUE),
                columnInfo("plain", "text_field_alias", "text", JDBCType.VARCHAR, Integer.MAX_VALUE),
                columnInfo("plain", "a.b.c.text_field_alias", "text", JDBCType.VARCHAR, Integer.MAX_VALUE)
            )
        );
        expected.put("rows", singletonList(Arrays.asList(text, null, null)));
        assertResponse(expected, runSql("SELECT text_field, text_field_alias, a.b.c.text_field_alias FROM test"));
    }

    /*
     *    "integer_field": {
     *       "type": "integer"
     *    },
     *    "integer_field_alias": {
     *       "type": "alias",
     *       "path": "integer_field"
     *    },
     *    "a.b.c.integer_field_alias": {
     *       "type": "alias",
     *       "path": "integer_field"
     *    }
     */
    public void testAliasAggregatableFromSourceField() throws IOException {
        int number = randomInt();

        createIndexWithFieldTypeAndAlias("integer", null, null);
        index("{\"integer_field\":" + number + "}");

        Map<String, Object> expected = new HashMap<>();
        expected.put(
            "columns",
            Arrays.asList(
                columnInfo("plain", "integer_field", "integer", JDBCType.INTEGER, Integer.MAX_VALUE),
                columnInfo("plain", "integer_field_alias", "integer", JDBCType.INTEGER, Integer.MAX_VALUE),
                columnInfo("plain", "a.b.c.integer_field_alias", "integer", JDBCType.INTEGER, Integer.MAX_VALUE)
            )
        );
        expected.put("rows", singletonList(Arrays.asList(number, null, number)));
        assertResponse(expected, runSql("SELECT integer_field, integer_field_alias, a.b.c.integer_field_alias FROM test"));
    }

    /*
     *    "text_field": {
     *       "type": "text",
     *       "fields": {
     *         "keyword_subfield": {
     *           "type": "keyword",
     *           "ignore_above": 10
     *         }
     *       }
     *     }
     */
    public void testTextFieldWithKeywordSubfield() throws IOException {
        String text = randomAlphaOfLength(10) + " " + randomAlphaOfLength(10);
        // _source for `keyword` fields doesn't matter, as they should be taken from docvalue_fields
        boolean explicitSourceSetting = randomBoolean(); // default (no _source setting) or explicit setting
        boolean enableSource = randomBoolean();          // enable _source at index level
        boolean ignoreAbove = randomBoolean();
        String fieldName = "text_field";
        String subFieldName = "text_field.keyword_subfield";
        String query = "SELECT " + fieldName + "," + subFieldName + " FROM test";

        Map<String, Object> indexProps = new HashMap<>(1);
        indexProps.put("_source", enableSource);

        Map<String, Map<String, Object>> subFieldsProps = null;
        if (ignoreAbove) {
            subFieldsProps = new HashMap<>(1);
            Map<String, Object> fieldProp = new HashMap<>(1);
            fieldProp.put("ignore_above", 10);
            subFieldsProps.put(subFieldName, fieldProp);
        }

        createIndexWithFieldTypeAndSubFields("text", null, explicitSourceSetting ? indexProps : null, subFieldsProps, "keyword");
        index("{\"" + fieldName + "\":\"" + text + "\"}");

        if (explicitSourceSetting == false || enableSource) {
            Map<String, Object> expected = new HashMap<>();
            expected.put(
                "columns",
                Arrays.asList(
                    columnInfo("plain", fieldName, "text", JDBCType.VARCHAR, Integer.MAX_VALUE),
                    columnInfo("plain", subFieldName, "keyword", JDBCType.VARCHAR, Integer.MAX_VALUE)
                )
            );

            expected.put("rows", singletonList(Arrays.asList(text, ignoreAbove ? null : text)));
            assertResponse(expected, runSql(query));
        } else {
            expectSourceDisabledError(query);

            // even if the _source is disabled, selecting only the keyword sub-field should work as expected
            Map<String, Object> expected = new HashMap<>();
            expected.put("columns", Arrays.asList(columnInfo("plain", subFieldName, "keyword", JDBCType.VARCHAR, Integer.MAX_VALUE)));

            expected.put("rows", singletonList(singletonList(ignoreAbove ? null : text)));
            assertResponse(expected, runSql("SELECT text_field.keyword_subfield FROM test"));
        }
    }

    /*
     *    "text_field": {
     *       "type": "text",
     *       "fields": {
     *         "integer_subfield": {
     *           "type": "integer",
     *           "ignore_malformed": true/false
     *         }
     *       }
     *     }
     */
    public void testTextFieldWithIntegerNumberSubfield() throws IOException {
        Integer number = randomInt();
        boolean explicitSourceSetting = randomBoolean(); // default (no _source setting) or explicit setting
        boolean enableSource = randomBoolean();          // enable _source at index level
        boolean ignoreMalformed = randomBoolean();       // ignore_malformed is true, thus test a non-number value
        Object actualValue = number;
        String fieldName = "text_field";
        String subFieldName = "text_field.integer_subfield";
        String query = "SELECT " + fieldName + "," + subFieldName + " FROM test";

        Map<String, Object> indexProps = new HashMap<>(1);
        indexProps.put("_source", enableSource);

        Map<String, Map<String, Object>> subFieldsProps = null;
        if (ignoreMalformed) {
            subFieldsProps = new HashMap<>(1);
            Map<String, Object> fieldProp = new HashMap<>(1);
            // on purpose use a string instead of a number and check for null when querying the field's value
            fieldProp.put("ignore_malformed", true);
            subFieldsProps.put(subFieldName, fieldProp);
            actualValue = "foo";
        }

        createIndexWithFieldTypeAndSubFields("text", null, explicitSourceSetting ? indexProps : null, subFieldsProps, "integer");
        index("{\"" + fieldName + "\":\"" + actualValue + "\"}");

        if (explicitSourceSetting == false || enableSource) {
            Map<String, Object> expected = new HashMap<>();
            expected.put(
                "columns",
                Arrays.asList(
                    columnInfo("plain", fieldName, "text", JDBCType.VARCHAR, Integer.MAX_VALUE),
                    columnInfo("plain", subFieldName, "integer", JDBCType.INTEGER, Integer.MAX_VALUE)
                )
            );
            if (ignoreMalformed) {
                expected.put("rows", singletonList(Arrays.asList("foo", null)));
            } else {
                expected.put("rows", singletonList(Arrays.asList(String.valueOf(number), number)));
            }
            assertResponse(expected, runSql(query));
        } else {
            expectSourceDisabledError(query);
            // if the _source is disabled, selecting only the integer sub-field shouldn't work as well
            expectSourceDisabledError("SELECT " + subFieldName + " FROM test");
        }
    }

    /*
     *    "integer_field": {
     *       "type": "integer",
     *       "ignore_malformed": true/false,
     *       "fields": {
     *         "keyword_subfield/text_subfield": {
     *           "type": "keyword/text"
     *         }
     *       }
     *     }
     */
    public void testNumberFieldWithTextOrKeywordSubfield() throws IOException {
        Integer number = randomInt();
        boolean explicitSourceSetting = randomBoolean(); // default (no _source setting) or explicit setting
        boolean enableSource = randomBoolean();          // enable _source at index level
        boolean ignoreMalformed = randomBoolean();       // ignore_malformed is true, thus test a non-number value
        boolean isKeyword = randomBoolean();             // text or keyword subfield
        Object actualValue = number;
        String fieldName = "integer_field";
        String subFieldName = "integer_field." + (isKeyword ? "keyword_subfield" : "text_subfield");
        String query = "SELECT " + fieldName + "," + subFieldName + " FROM test";

        Map<String, Object> indexProps = new HashMap<>(1);
        indexProps.put("_source", enableSource);

        Map<String, Map<String, Object>> fieldProps = null;
        if (ignoreMalformed) {
            fieldProps = new HashMap<>(1);
            Map<String, Object> fieldProp = new HashMap<>(1);
            // on purpose use a string instead of a number and check for null when querying the field's value
            fieldProp.put("ignore_malformed", true);
            fieldProps.put(fieldName, fieldProp);
            actualValue = "foo";
        }

        createIndexWithFieldTypeAndSubFields(
            "integer",
            fieldProps,
            explicitSourceSetting ? indexProps : null,
            null,
            isKeyword ? "keyword" : "text"
        );
        index("{\"" + fieldName + "\":\"" + actualValue + "\"}");

        if (explicitSourceSetting == false || enableSource) {
            Map<String, Object> expected = new HashMap<>();
            expected.put(
                "columns",
                Arrays.asList(
                    columnInfo("plain", fieldName, "integer", JDBCType.INTEGER, Integer.MAX_VALUE),
                    columnInfo("plain", subFieldName, isKeyword ? "keyword" : "text", JDBCType.VARCHAR, Integer.MAX_VALUE)
                )
            );
            if (ignoreMalformed) {
                expected.put("rows", singletonList(Arrays.asList(null, "foo")));
            } else {
                expected.put("rows", singletonList(Arrays.asList(number, String.valueOf(number))));
            }
            assertResponse(expected, runSql(query));
        } else {
            if (isKeyword) {
                // selecting only the keyword subfield when the _source is disabled should work
                Map<String, Object> expected = new HashMap<>();
                expected.put("columns", singletonList(columnInfo("plain", subFieldName, "keyword", JDBCType.VARCHAR, Integer.MAX_VALUE)));
                if (ignoreMalformed) {
                    expected.put("rows", singletonList(singletonList("foo")));
                } else {
                    expected.put("rows", singletonList(singletonList(String.valueOf(number))));
                }
                assertResponse(expected, runSql("SELECT integer_field.keyword_subfield FROM test"));
            } else {
                expectSourceDisabledError(query);
            }

            // if the _source is disabled, selecting only the integer field shouldn't work
            expectSourceDisabledError("SELECT " + fieldName + " FROM test");
        }
    }

    /*
     *    "integer_field": {
     *       "type": "integer",
     *       "ignore_malformed": true/false,
     *       "fields": {
     *         "byte_subfield": {
     *           "type": "byte",
     *           "ignore_malformed": true/false
     *         }
     *       }
     *     }
     */
    public void testIntegerFieldWithByteSubfield() throws IOException {
        boolean isByte = randomBoolean();
        Integer number = isByte ? randomByte() : randomIntBetween(Byte.MAX_VALUE + 1, Integer.MAX_VALUE);
        boolean explicitSourceSetting = randomBoolean();   // default (no _source setting) or explicit setting
        boolean enableSource = randomBoolean();            // enable _source at index level
        boolean rootIgnoreMalformed = randomBoolean();     // root field ignore_malformed
        boolean subFieldIgnoreMalformed = randomBoolean(); // sub-field ignore_malformed
        String fieldName = "integer_field";
        String subFieldName = "integer_field.byte_subfield";
        String query = "SELECT " + fieldName + "," + subFieldName + " FROM test";

        Map<String, Object> indexProps = new HashMap<>(1);
        indexProps.put("_source", enableSource);

        Map<String, Map<String, Object>> fieldProps = null;
        if (rootIgnoreMalformed) {
            fieldProps = new HashMap<>(1);
            Map<String, Object> fieldProp = new HashMap<>(1);
            fieldProp.put("ignore_malformed", true);
            fieldProps.put(fieldName, fieldProp);
        }
        Map<String, Map<String, Object>> subFieldProps = null;
        if (subFieldIgnoreMalformed) {
            subFieldProps = new HashMap<>(1);
            Map<String, Object> fieldProp = new HashMap<>(1);
            fieldProp.put("ignore_malformed", true);
            subFieldProps.put(subFieldName, fieldProp);
        }

        createIndexWithFieldTypeAndSubFields("integer", fieldProps, explicitSourceSetting ? indexProps : null, subFieldProps, "byte");
        index("{\"" + fieldName + "\":" + number + "}");

        Map<String, Object> expected = new HashMap<>();
        expected.put(
            "columns",
            Arrays.asList(
                columnInfo("plain", fieldName, "integer", JDBCType.INTEGER, Integer.MAX_VALUE),
                columnInfo("plain", subFieldName, "byte", JDBCType.TINYINT, Integer.MAX_VALUE)
            )
        );
        if (explicitSourceSetting == false || enableSource) {
            if (isByte || subFieldIgnoreMalformed) {
                expected.put("rows", singletonList(Arrays.asList(number, isByte ? number : null)));
            } else {
                expected.put("rows", Collections.emptyList());
            }
            assertResponse(expected, runSql(query));
        } else {
            if (isByte || subFieldIgnoreMalformed) {
                expectSourceDisabledError(query);
            } else {
                expected.put("rows", Collections.emptyList());
                assertResponse(expected, runSql(query));
            }
        }
    }

    /*
     *    "byte_field": {
     *       "type": "byte",
     *       "ignore_malformed": true/false,
     *       "fields": {
     *         "integer_subfield": {
     *           "type": "integer",
     *           "ignore_malformed": true/false
     *         }
     *       }
     *     }
     */
    public void testByteFieldWithIntegerSubfield() throws IOException {
        boolean isByte = randomBoolean();
        Integer number = isByte ? randomByte() : randomIntBetween(Byte.MAX_VALUE + 1, Integer.MAX_VALUE);
        boolean explicitSourceSetting = randomBoolean();   // default (no _source setting) or explicit setting
        boolean enableSource = randomBoolean();            // enable _source at index level
        boolean rootIgnoreMalformed = randomBoolean();     // root field ignore_malformed
        boolean subFieldIgnoreMalformed = randomBoolean(); // sub-field ignore_malformed
        String fieldName = "byte_field";
        String subFieldName = "byte_field.integer_subfield";
        String query = "SELECT " + fieldName + "," + subFieldName + " FROM test";

        Map<String, Object> indexProps = new HashMap<>(1);
        indexProps.put("_source", enableSource);

        Map<String, Map<String, Object>> fieldProps = null;
        if (rootIgnoreMalformed) {
            fieldProps = new HashMap<>(1);
            Map<String, Object> fieldProp = new HashMap<>(1);
            fieldProp.put("ignore_malformed", true);
            fieldProps.put(fieldName, fieldProp);
        }
        Map<String, Map<String, Object>> subFieldProps = null;
        if (subFieldIgnoreMalformed) {
            subFieldProps = new HashMap<>(1);
            Map<String, Object> fieldProp = new HashMap<>(1);
            fieldProp.put("ignore_malformed", true);
            subFieldProps.put(subFieldName, fieldProp);
        }

        createIndexWithFieldTypeAndSubFields("byte", fieldProps, explicitSourceSetting ? indexProps : null, subFieldProps, "integer");
        index("{\"" + fieldName + "\":" + number + "}");

        Map<String, Object> expected = new HashMap<>();
        expected.put(
            "columns",
            Arrays.asList(
                columnInfo("plain", fieldName, "byte", JDBCType.TINYINT, Integer.MAX_VALUE),
                columnInfo("plain", subFieldName, "integer", JDBCType.INTEGER, Integer.MAX_VALUE)
            )
        );
        if (explicitSourceSetting == false || enableSource) {
            if (isByte || rootIgnoreMalformed) {
                expected.put("rows", singletonList(Arrays.asList(isByte ? number : null, number)));
            } else {
                expected.put("rows", Collections.emptyList());
            }
            assertResponse(expected, runSql(query));
        } else {
            if (isByte || rootIgnoreMalformed) {
                expectSourceDisabledError(query);
            } else {
                expected.put("rows", Collections.emptyList());
                assertResponse(expected, runSql(query));
            }
        }
    }

    private void expectSourceDisabledError(String query) {
        expectBadRequest(() -> {
            client().performRequest(buildRequest(query));
            return Collections.emptyMap();
        }, containsString("unable to fetch fields from _source field: _source is disabled in the mappings for index [test]"));
    }

    private void createIndexWithFieldTypeAndAlias(String type, Map<String, Map<String, Object>> fieldProps, Map<String, Object> indexProps)
        throws IOException {
        createIndexWithFieldTypeAndProperties(type, fieldProps, indexProps, true, false, null);
    }

    private void createIndexWithFieldTypeAndProperties(
        String type,
        Map<String, Map<String, Object>> fieldProps,
        Map<String, Object> indexProps
    ) throws IOException {
        createIndexWithFieldTypeAndProperties(type, fieldProps, indexProps, false, false, null);
    }

    private void createIndexWithFieldTypeAndSubFields(
        String type,
        Map<String, Map<String, Object>> fieldProps,
        Map<String, Object> indexProps,
        Map<String, Map<String, Object>> subFieldsProps,
        String... subFieldsTypes
    ) throws IOException {
        createIndexWithFieldTypeAndProperties(type, fieldProps, indexProps, false, true, subFieldsProps, subFieldsTypes);
    }

    private void createIndexWithFieldTypeAndProperties(
        String type,
        Map<String, Map<String, Object>> fieldProps,
        Map<String, Object> indexProps,
        boolean withAlias,
        boolean withSubFields,
        Map<String, Map<String, Object>> subFieldsProps,
        String... subFieldsTypes
    ) throws IOException {
        Request request = new Request("PUT", "/test");
        XContentBuilder index = JsonXContent.contentBuilder().prettyPrint().startObject();

        index.startObject("mappings");
        {
            if (indexProps != null) {
                for (Entry<String, Object> prop : indexProps.entrySet()) {
                    if (prop.getValue() instanceof Boolean) {
                        index.startObject(prop.getKey());
                        {
                            index.field("enabled", prop.getValue());
                        }
                        index.endObject();
                    }
                }
            }
            index.startObject("properties");
            {
                String fieldName = type + "_field";
                index.startObject(fieldName);
                {
                    index.field("type", type);
                    if (fieldProps != null && fieldProps.containsKey(fieldName)) {
                        for (Entry<String, Object> prop : fieldProps.get(fieldName).entrySet()) {
                            index.field(prop.getKey(), prop.getValue());
                        }
                    }

                    if (withSubFields) {
                        index.startObject("fields");
                        for (String subFieldType : subFieldsTypes) {
                            String subFieldName = subFieldType + "_subfield";
                            String fullSubFieldName = fieldName + "." + subFieldName;
                            index.startObject(subFieldName);
                            index.field("type", subFieldType);
                            if (subFieldsProps != null && subFieldsProps.containsKey(fullSubFieldName)) {
                                for (Entry<String, Object> prop : subFieldsProps.get(fullSubFieldName).entrySet()) {
                                    index.field(prop.getKey(), prop.getValue());
                                }
                            }
                            index.endObject();
                        }
                        index.endObject();
                    }
                }
                index.endObject();

                if (withAlias) {
                    // create two aliases - one within a hierarchy, the other just a simple field w/o hierarchy
                    index.startObject(fieldName + "_alias");
                    {
                        index.field("type", "alias");
                        index.field("path", fieldName);
                    }
                    index.endObject();
                    index.startObject("a.b.c." + fieldName + "_alias");
                    {
                        index.field("type", "alias");
                        index.field("path", fieldName);
                    }
                    index.endObject();
                }
            }
            index.endObject();
        }
        index.endObject();
        index.endObject();

        request.setJsonEntity(Strings.toString(index));
        client().performRequest(request);
    }

    private Request buildRequest(String query) {
        Request request = new Request("POST", RestSqlTestCase.SQL_QUERY_REST_ENDPOINT);
        request.addParameter("error_trace", "true");
        request.addParameter("pretty", "true");
        request.setEntity(new StringEntity(query(query).mode("plain").toString(), ContentType.APPLICATION_JSON));

        return request;
    }

    private Map<String, Object> runSql(String query) throws IOException {
        Response response = client().performRequest(buildRequest(query));
        try (InputStream content = response.getEntity().getContent()) {
            return XContentHelper.convertToMap(JsonXContent.jsonXContent, content, false);
        }
    }

    private JDBCType jdbcTypeFor(String esType) {
        switch (esType) {
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
