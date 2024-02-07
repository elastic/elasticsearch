/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.qa.rest;

import com.carrotsearch.randomizedtesting.annotations.Repeat;

import org.elasticsearch.client.Request;
import org.elasticsearch.common.Strings;
import org.elasticsearch.core.CheckedConsumer;
import org.elasticsearch.index.mapper.SourceFieldMapper;
import org.elasticsearch.test.rest.ESRestTestCase;
import org.elasticsearch.xcontent.XContentBuilder;
import org.elasticsearch.xcontent.json.JsonXContent;

import java.io.IOException;
import java.math.BigInteger;
import java.util.List;
import java.util.Locale;
import java.util.Map;

import static org.elasticsearch.test.MapMatcher.assertMap;
import static org.elasticsearch.test.MapMatcher.matchesMap;
import static org.elasticsearch.test.ListMatcher.matchesList;
import static org.elasticsearch.xpack.esql.qa.rest.RestEsqlTestCase.runEsqlSync;

/**
 * Creates indices with many different mappings and fetches values from them to make sure
 * we can do it. This is a port of a test with the same name on the SQL side.
 */
@Repeat(iterations = 10)
public abstract class FieldExtractorTestCase extends ESRestTestCase {
    private static final String TEST_INDEX = "test";

    private SourceMode sourceMode;

    @Override
    public void setUp() throws Exception {
        super.setUp();
        sourceMode = randomFrom(SourceMode.values());
        logger.info("_source mode: {}", sourceMode);
    }

    public void testTextField() throws IOException {
        boolean store = sourceMode == SourceMode.SYNTHETIC || randomBoolean();
        logger.info("store: {}");
        createIndex("text", builder -> {
            if (store) {
                builder.field("store", true);
            }
        });

        String value = randomAlphaOfLength(20);
        index("""
            {"text_field":"$VALUE"}
            """.replace("$VALUE", value));

        assertMap(
            fetchAll(),
            matchesMap().entry("columns", List.of(columnInfo("text_field", "text")))
                // Text fields without
                .entry("values", List.of(matchesList().item(sourceMode == SourceMode.DISABLED && store == false ? null : value)))
        );
    }

    public void testKeywordField() throws IOException {
        Integer ignoreAbove = randomBoolean() ? null : between(10, 50);
        int length = between(10, 50);
        boolean docValues = sourceMode == SourceMode.SYNTHETIC || randomBoolean();
        boolean store = randomBoolean();
        logger.info("ignore_above: {}, length: {}, doc_values: {}, store: {}", ignoreAbove, length, docValues, store);

        createIndex("keyword", builder -> {
            if (ignoreAbove != null) {
                builder.field("ignore_above", ignoreAbove);
            }
            if (docValues == false) {
                builder.field("doc_values", false);
            }
            if (store) {
                builder.field("store", true);
            }
        });

        String value = randomAlphaOfLength(length);
        index("""
            {"keyword_field":"$VALUE"}
            """.replace("$VALUE", value));

        boolean stored = sourceMode.stored() || docValues;
        assertMap(
            fetchAll(),
            matchesMap().entry("columns", List.of(columnInfo("keyword_field", "keyword")))
                .entry("values", List.of(matchesList().item(ignoredByIgnoreAbove(ignoreAbove, length) || stored == false ? null : value)))
        );
    }

    public void testConstantKeywordField() throws IOException {
        boolean specifyInMapping = randomBoolean();
        boolean specifyInDocument = randomBoolean();
        logger.info("specify_in_mapping: {}, specify_in_document: {}", specifyInDocument, specifyInDocument);
        String value = randomAlphaOfLength(20);

        createIndex("constant_keyword", builder -> {
            if (specifyInMapping) {
                builder.field("value", value);
            }
        });

        if (specifyInDocument) {
            index("""
                {"constant_keyword_field":"$VALUE"}
                """.replace("$VALUE", value));
        } else {
            index("{}");
        }

        assertMap(
            fetchAll(),
            matchesMap().entry("columns", List.of(columnInfo("constant_keyword_field", "keyword")))
                .entry("values", List.of(matchesList().item(specifyInMapping || specifyInDocument ? value : null)))
        );
    }

    public void testWildcardField() throws IOException {
        Integer ignoreAbove = randomBoolean() ? null : between(10, 50);
        int length = between(10, 50);

        logger.info("ignore_above: {}, length: {}", ignoreAbove, length);

        createIndex("wildcard", builder -> {
            if (ignoreAbove != null) {
                builder.field("ignore_above", ignoreAbove);
            }
        });

        String value = randomAlphaOfLength(length);
        index("""
            {"wildcard_field":"$VALUE"}
            """.replace("$VALUE", value));

        assertMap(
            fetchAll(),
            matchesMap().entry("columns", List.of(columnInfo("wildcard_field", "keyword")))
                .entry("values", List.of(matchesList().item(ignoredByIgnoreAbove(ignoreAbove, length) ? null : value)))
        );
    }

    public void testLong() throws IOException {
        long value = randomLong();
        testStructured("long", randomBoolean(), randomBoolean() ? Long.toString(value) : value, "long", value);
    }

    public void testLongMalformed() throws IOException {
        testStructured("long", true, randomAlphaOfLength(5), "long", null);
    }

    public void testInt() throws IOException {
        int value = randomInt();
        testStructured("integer", randomBoolean(), randomBoolean() ? Integer.toString(value) : value, "integer", value);
    }

    public void testIntMalformed() throws IOException {
        testStructured("integer", true, randomAlphaOfLength(5), "integer", null);
    }

    public void testShort() throws IOException {
        short value = randomShort();
        testStructured("short", randomBoolean(), randomBoolean() ? Short.toString(value) : value, "integer", (int) value);
    }

    public void testShortMalformed() throws IOException {
        testStructured("short", true, randomAlphaOfLength(5), "integer", null);
    }

    public void testByte() throws IOException {
        byte value = randomByte();
        testStructured("byte", randomBoolean(), randomBoolean() ? Byte.toString(value) : value, "integer", (int) value);
    }

    public void testByteMalformed() throws IOException {
        testStructured("byte", true, randomAlphaOfLength(5), "integer", null);
    }

    public void testUnsignedLong() throws IOException {
        boolean ignoreMalformed = sourceMode == SourceMode.SYNTHETIC ? false : randomBoolean();
        BigInteger value = randomUnsignedLong();
        testStructured(
            "unsigned_long",
            ignoreMalformed,
            randomBoolean() ? value.toString() : value,
            "unsigned_long",
            value.compareTo(BigInteger.valueOf(Long.MAX_VALUE)) <= 0 ? value.longValue() : value
        );
    }

    public void testUnsignedLongMalformed() throws IOException {
        assumeFalse("unsigned long doesn't support synthetic source with ignore_malformed", sourceMode == SourceMode.SYNTHETIC);
        testStructured("unsigned_long", true, randomAlphaOfLength(5), "unsigned_long", null);
    }

    //
    // /*
    // * "long/integer/short/byte_field": {
    // * "type": "long/integer/short/byte"
    // * }
    // * Note: no unsigned_long tested -- the mapper for it won't accept float formats.
    // */
    // public void testFractionsForNonFloatingPointTypes() throws IOException {
    // String floatingPointNumber = "123.456";
    // String fieldType = randomFrom("long", "integer", "short", "byte");
    //
    // createIndexWithFieldTypeAndProperties(fieldType, null, null);
    // index("{\"" + fieldType + "_field\":\"" + floatingPointNumber + "\"}");
    //
    // Map<String, Object> expected = new HashMap<>();
    // expected.put("columns", asList(columnInfo("plain", fieldType + "_field", fieldType, jdbcTypeFor(fieldType), Integer.MAX_VALUE)));
    //
    // // because "coerce" is true, a "123.456" floating point number STRING should be converted to 123, no matter the numeric field type
    // expected.put("rows", singletonList(singletonList(123)));
    // assertResponse(expected, runSql("SELECT " + fieldType + "_field FROM test"));
    // }
    //
    // /*
    // * "double/float/half_float/scaled_float_field": {
    // * "type": "double/float/half_float/scaled_float",
    // * "scaling_factor": 10 (for scaled_float type only)
    // * }
    // */
    // public void testCoerceForFloatingPointTypes() throws IOException {
    // String floatingPointNumber = "123.456";
    // String fieldType = randomFrom("double", "float", "half_float", "scaled_float");
    // boolean isScaledFloat = fieldType == "scaled_float";
    //
    // Map<String, Map<String, Object>> fieldProps = null;
    // if (isScaledFloat) {
    // fieldProps = Maps.newMapWithExpectedSize(1);
    // Map<String, Object> fieldProp = Maps.newMapWithExpectedSize(1);
    // fieldProp.put("scaling_factor", 10); // scaling_factor is required for "scaled_float"
    // fieldProps.put(fieldType + "_field", fieldProp);
    // }
    //
    // createIndexWithFieldTypeAndProperties(fieldType, fieldProps, null);
    // // important here is to pass floatingPointNumber as a string: "float_field": "123.456"
    // index("{\"" + fieldType + "_field\":\"" + floatingPointNumber + "\"}");
    //
    // Map<String, Object> expected = new HashMap<>();
    // expected.put("columns", asList(columnInfo("plain", fieldType + "_field", fieldType, jdbcTypeFor(fieldType), Integer.MAX_VALUE)));
    //
    // // because "coerce" is true, a "123.456" floating point number STRING should be converted to 123.456 as number
    // // and converted to 123.5 for "scaled_float" type
    // // and 123.4375 for "half_float" because that is all it stores.
    // double expectedNumber = isScaledFloat ? 123.5 : fieldType.equals("half_float") ? 123.4375 : 123.456;
    // expected.put("rows", singletonList(singletonList(expectedNumber)));
    // assertResponse(expected, runSql("SELECT " + fieldType + "_field FROM test"));
    // }
    //

    /**
     * Tests field types with some kind of parser that support {@code doc_values}
     * and {@code ignore_malformed}. Types like {@code long}, {@code double}, {@code ip}.
     */
    private void testStructured(String type, boolean ignoreMalformed, Object value, String expectedType, Object expectedValue)
        throws IOException {

        boolean docValues = sourceMode == SourceMode.SYNTHETIC || randomBoolean();
        logger.info("ignore_malformed: {}, doc_values: {}", ignoreMalformed, docValues);

        createIndex(type, builder -> {
            if (ignoreMalformed) {
                builder.field("ignore_malformed", true);
            }
            if (docValues == false) {
                builder.field("doc_values", false);
            }
        });

        index(Strings.toString(JsonXContent.contentBuilder().startObject().field(type + "_field", value).endObject()));

        boolean stored = sourceMode.stored() || docValues;
        assertMap(
            fetchAll(),
            matchesMap().entry("columns", List.of(columnInfo(type + "_field", expectedType)))
                .entry("values", List.of(matchesList().item(stored ? expectedValue : null)))
        );
    }

    // /*
    // * "boolean_field": {
    // * "type": "boolean"
    // * }
    // */
    // public void testBooleanField() throws IOException {
    // String query = "SELECT boolean_field FROM test";
    // boolean booleanField = randomBoolean();
    // boolean asString = randomBoolean(); // pass true or false as string "true" or "false
    //
    // createIndexWithFieldTypeAndProperties("boolean", null, getIndexProps());
    // if (asString) {
    // index("{\"boolean_field\":\"" + booleanField + "\"}");
    // } else {
    // index("{\"boolean_field\":" + booleanField + "}");
    // }
    //
    // Map<String, Object> expected = new HashMap<>();
    // expected.put("columns", asList(columnInfo("plain", "boolean_field", "boolean", JDBCType.BOOLEAN, Integer.MAX_VALUE)));
    // // adding the boolean as a String here because parsing the response will yield a "true"/"false" String
    // expected.put("rows", singletonList(singletonList(getExpectedValueFromSource(booleanField))));
    // assertResponse(expected, runSql(query));
    // }
    //
    // /*
    // * "ip_field": {
    // * "type": "ip",
    // * "ignore_malformed": true/false
    // * }
    // */
    // public void testIpField() throws IOException {
    // String query = "SELECT ip_field FROM test";
    // String actualValue = "192.168.1.1";
    // boolean ignoreMalformed = randomBoolean();
    //
    // Map<String, Map<String, Object>> fieldProps = null;
    // if (ignoreMalformed) {
    // fieldProps = Maps.newMapWithExpectedSize(1);
    // Map<String, Object> fieldProp = Maps.newMapWithExpectedSize(1);
    // // on purpose use a non-IP and check for null when querying the field's value
    // fieldProp.put("ignore_malformed", true);
    // fieldProps.put("ip_field", fieldProp);
    // actualValue = "foo";
    // }
    // createIndexWithFieldTypeAndProperties("ip", fieldProps, getIndexProps());
    // index("{\"ip_field\":\"" + actualValue + "\"}");
    //
    // Map<String, Object> expected = new HashMap<>();
    // expected.put("columns", asList(columnInfo("plain", "ip_field", "ip", JDBCType.VARCHAR, Integer.MAX_VALUE)));
    // expected.put("rows", singletonList(singletonList(getExpectedValueFromSource(ignoreMalformed ? null : actualValue))));
    // assertResponse(expected, runSql(query));
    // }
    //
    // /*
    // * "version_field": {
    // * "type": "version",
    // * }
    // */
    // public void testVersionField() throws IOException {
    // String query = "SELECT version_field FROM test";
    // String actualValue = "2.11.4";
    //
    // Map<String, Map<String, Object>> fieldProps = null;
    // createIndexWithFieldTypeAndProperties("version", fieldProps, getIndexProps());
    // index("{\"version_field\":\"" + actualValue + "\"}");
    //
    // Map<String, Object> expected = new HashMap<>();
    // expected.put("columns", asList(columnInfo("plain", "version_field", "version", JDBCType.VARCHAR, Integer.MAX_VALUE)));
    // expected.put("rows", singletonList(singletonList(getExpectedValueFromSource(actualValue))));
    // assertResponse(expected, runSql(query));
    // }
    //
    // /*
    // * "geo_point_field": {
    // * "type": "geo_point",
    // * "ignore_malformed": true/false
    // * }
    // */
    // public void testGeoPointField() throws IOException {
    // String query = "SELECT geo_point_field FROM test";
    // String geoPointField = "41.12,-71.34";
    // String actualValue = geoPointField;
    // boolean ignoreMalformed = randomBoolean();
    //
    // Map<String, Map<String, Object>> fieldProps = null;
    // if (ignoreMalformed) {
    // fieldProps = Maps.newMapWithExpectedSize(1);
    // Map<String, Object> fieldProp = Maps.newMapWithExpectedSize(1);
    // // on purpose use a non-geo-point and check for null when querying the field's value
    // fieldProp.put("ignore_malformed", true);
    // fieldProps.put("geo_point_field", fieldProp);
    // actualValue = "foo";
    // }
    // createIndexWithFieldTypeAndProperties("geo_point", fieldProps, getIndexProps());
    // index("{\"geo_point_field\":\"" + actualValue + "\"}");
    //
    // Map<String, Object> expected = new HashMap<>();
    // expected.put("columns", asList(columnInfo("plain", "geo_point_field", "geo_point", JDBCType.VARCHAR, Integer.MAX_VALUE)));
    // expected.put("rows", singletonList(singletonList(getExpectedValueFromSource(ignoreMalformed ? null : "POINT (-71.34 41.12)"))));
    // assertResponse(expected, runSql(query));
    // }
    //
    // /*
    // * "geo_shape_field": {
    // * "type": "point",
    // * "ignore_malformed": true/false
    // * }
    // */
    // public void testGeoShapeField() throws IOException {
    // String query = "SELECT geo_shape_field FROM test";
    // String actualValue = "[-77.03653, 38.897676]";
    // boolean ignoreMalformed = randomBoolean();
    //
    // Map<String, Map<String, Object>> fieldProps = null;
    // if (ignoreMalformed) {
    // fieldProps = Maps.newMapWithExpectedSize(1);
    // Map<String, Object> fieldProp = Maps.newMapWithExpectedSize(1);
    // // on purpose use a non-geo-shape and check for null when querying the field's value
    // fieldProp.put("ignore_malformed", true);
    // fieldProps.put("geo_shape_field", fieldProp);
    // actualValue = "\"foo\"";
    // }
    // createIndexWithFieldTypeAndProperties("geo_shape", fieldProps, getIndexProps());
    // index(String.format(java.util.Locale.ROOT, """
    // {"geo_shape_field":{"type":"point","coordinates":%s}}
    // """, actualValue));
    //
    // Map<String, Object> expected = new HashMap<>();
    // expected.put("columns", asList(columnInfo("plain", "geo_shape_field", "geo_shape", JDBCType.VARCHAR, Integer.MAX_VALUE)));
    // expected.put(
    // "rows",
    // singletonList(singletonList(getExpectedValueFromSource(ignoreMalformed ? null : "POINT (-77.03653 38.897676)")))
    // );
    // assertResponse(expected, runSql(query));
    // }
    //
    // /*
    // * "shape_field": {
    // * "type": "shape",
    // * "ignore_malformed": true/false
    // * }
    // */
    // public void testShapeField() throws IOException {
    // String query = "SELECT shape_field FROM test";
    // String shapeField = "POINT (-377.03653 389.897676)";
    // String actualValue = shapeField;
    // boolean ignoreMalformed = randomBoolean();
    //
    // Map<String, Map<String, Object>> fieldProps = null;
    // if (ignoreMalformed) {
    // fieldProps = Maps.newMapWithExpectedSize(1);
    // Map<String, Object> fieldProp = Maps.newMapWithExpectedSize(1);
    // // on purpose use a non-geo-point and check for null when querying the field's value
    // fieldProp.put("ignore_malformed", true);
    // fieldProps.put("shape_field", fieldProp);
    // actualValue = "foo";
    // }
    // createIndexWithFieldTypeAndProperties("shape", fieldProps, getIndexProps());
    // index("{\"shape_field\":\"" + actualValue + "\"}");
    //
    // Map<String, Object> expected = new HashMap<>();
    // expected.put("columns", asList(columnInfo("plain", "shape_field", "shape", JDBCType.VARCHAR, Integer.MAX_VALUE)));
    // expected.put("rows", singletonList(singletonList(getExpectedValueFromSource(ignoreMalformed ? null : shapeField))));
    // assertResponse(expected, runSql(query));
    // }
    //
    // /*
    // * "keyword_field": {
    // * "type": "keyword"
    // * },
    // * "keyword_field_alias": {
    // * "type": "alias",
    // * "path": "keyword_field"
    // * },
    // * "a.b.c.keyword_field_alias": {
    // * "type": "alias",
    // * "path": "keyword_field"
    // * }
    // */
    // public void testAliasFromDocValueField() throws IOException {
    // String keyword = randomAlphaOfLength(20);
    //
    // createIndexWithFieldTypeAndAlias("keyword", null, null);
    // index("{\"keyword_field\":\"" + keyword + "\"}");
    //
    // Map<String, Object> expected = new HashMap<>();
    // expected.put(
    // "columns",
    // asList(
    // columnInfo("plain", "keyword_field", "keyword", JDBCType.VARCHAR, Integer.MAX_VALUE),
    // columnInfo("plain", "keyword_field_alias", "keyword", JDBCType.VARCHAR, Integer.MAX_VALUE),
    // columnInfo("plain", "a.b.c.keyword_field_alias", "keyword", JDBCType.VARCHAR, Integer.MAX_VALUE)
    // )
    // );
    // expected.put("rows", singletonList(asList(keyword, keyword, keyword)));
    // assertResponse(expected, runSql("SELECT keyword_field, keyword_field_alias, a.b.c.keyword_field_alias FROM test"));
    // }
    //
    // /*
    // * "text_field": {
    // * "type": "text"
    // * },
    // * "text_field_alias": {
    // * "type": "alias",
    // * "path": "text_field"
    // * },
    // * "a.b.c.text_field_alias": {
    // * "type": "alias",
    // * "path": "text_field"
    // * }
    // */
    // public void testAliasFromSourceField() throws IOException {
    // String text = randomAlphaOfLength(20);
    //
    // createIndexWithFieldTypeAndAlias("text", null, null);
    // index("{\"text_field\":\"" + text + "\"}");
    //
    // Map<String, Object> expected = new HashMap<>();
    // expected.put(
    // "columns",
    // asList(
    // columnInfo("plain", "text_field", "text", JDBCType.VARCHAR, Integer.MAX_VALUE),
    // columnInfo("plain", "text_field_alias", "text", JDBCType.VARCHAR, Integer.MAX_VALUE),
    // columnInfo("plain", "a.b.c.text_field_alias", "text", JDBCType.VARCHAR, Integer.MAX_VALUE)
    // )
    // );
    // expected.put("rows", singletonList(asList(text, text, text)));
    // assertResponse(expected, runSql("SELECT text_field, text_field_alias, a.b.c.text_field_alias FROM test"));
    // }
    //
    // /*
    // * "integer_field": {
    // * "type": "integer"
    // * },
    // * "integer_field_alias": {
    // * "type": "alias",
    // * "path": "integer_field"
    // * },
    // * "a.b.c.integer_field_alias": {
    // * "type": "alias",
    // * "path": "integer_field"
    // * }
    // */
    // public void testAliasAggregatableFromSourceField() throws IOException {
    // int number = randomInt();
    //
    // createIndexWithFieldTypeAndAlias("integer", null, null);
    // index("{\"integer_field\":" + number + "}");
    //
    // Map<String, Object> expected = new HashMap<>();
    // expected.put(
    // "columns",
    // asList(
    // columnInfo("plain", "integer_field", "integer", JDBCType.INTEGER, Integer.MAX_VALUE),
    // columnInfo("plain", "integer_field_alias", "integer", JDBCType.INTEGER, Integer.MAX_VALUE),
    // columnInfo("plain", "a.b.c.integer_field_alias", "integer", JDBCType.INTEGER, Integer.MAX_VALUE)
    // )
    // );
    // expected.put("rows", singletonList(asList(number, number, number)));
    // assertResponse(expected, runSql("SELECT integer_field, integer_field_alias, a.b.c.integer_field_alias FROM test"));
    // }
    //
    // /*
    // * "text_field": {
    // * "type": "text",
    // * "fields": {
    // * "keyword_subfield": {
    // * "type": "keyword",
    // * "ignore_above": 10
    // * }
    // * }
    // * }
    // */
    // public void testTextFieldWithKeywordSubfield() throws IOException {
    // String text = randomAlphaOfLength(10) + " " + randomAlphaOfLength(10);
    // boolean ignoreAbove = randomBoolean();
    // String fieldName = "text_field";
    // String subFieldName = "text_field.keyword_subfield";
    // String query = "SELECT " + fieldName + "," + subFieldName + " FROM test";
    //
    // Map<String, Map<String, Object>> subFieldsProps = null;
    // if (ignoreAbove) {
    // subFieldsProps = Maps.newMapWithExpectedSize(1);
    // Map<String, Object> fieldProp = Maps.newMapWithExpectedSize(1);
    // fieldProp.put("ignore_above", 10);
    // subFieldsProps.put(subFieldName, fieldProp);
    // }
    //
    // createIndexWithFieldTypeAndSubFields("text", null, getIndexProps(), subFieldsProps, "keyword");
    // index("{\"" + fieldName + "\":\"" + text + "\"}");
    //
    // Map<String, Object> expected = new HashMap<>();
    // expected.put(
    // "columns",
    // asList(
    // columnInfo("plain", fieldName, "text", JDBCType.VARCHAR, Integer.MAX_VALUE),
    // columnInfo("plain", subFieldName, "keyword", JDBCType.VARCHAR, Integer.MAX_VALUE)
    // )
    // );
    //
    // expected.put(
    // "rows",
    // singletonList(asList(getExpectedValueFromSource(text), getExpectedValueFromSource(ignoreAbove ? null : text)))
    // );
    // assertResponse(expected, runSql(query));
    // }
    //
    // /*
    // * "keyword_field": {
    // * "type": "keyword",
    // * "ignore_above": 10
    // * },
    // * "date": {
    // * "type": "date"
    // * }
    // * Test for bug https://github.com/elastic/elasticsearch/issues/80653
    // */
    // public void testTopHitsAggBug_With_IgnoreAbove_Subfield() throws IOException {
    // String text = randomAlphaOfLength(10) + " " + randomAlphaOfLength(10);
    // String function = randomFrom("FIRST", "LAST");
    // String query = "select keyword_field from test group by keyword_field order by " + function + "(date)";
    //
    // Map<String, Map<String, Object>> fieldProps = Maps.newMapWithExpectedSize(1);
    // Map<String, Object> fieldProp = Maps.newMapWithExpectedSize(1);
    // fieldProp.put("ignore_above", 10);
    // fieldProps.put("keyword_field", fieldProp);
    //
    // createIndexWithFieldTypeAndProperties("keyword", fieldProps, null);
    // index("{\"keyword_field\":\"" + text + "\",\"date\":\"2021-11-11T11:11:11.000Z\"}");
    //
    // Map<String, Object> expected = new HashMap<>();
    // expected.put("columns", singletonList(columnInfo("plain", "keyword_field", "keyword", JDBCType.VARCHAR, Integer.MAX_VALUE)));
    //
    // expected.put("rows", singletonList(singletonList(null)));
    // assertResponse(expected, runSql(query));
    // }
    //
    // /*
    // * "text_field": {
    // * "type": "text",
    // * "fields": {
    // * "integer_subfield": {
    // * "type": "integer",
    // * "ignore_malformed": true/false
    // * }
    // * }
    // * }
    // */
    // public void testTextFieldWithIntegerNumberSubfield() throws IOException {
    // Integer number = randomInt();
    // boolean ignoreMalformed = randomBoolean(); // ignore_malformed is true, thus test a non-number value
    // Object actualValue = number;
    // String fieldName = "text_field";
    // String subFieldName = "text_field.integer_subfield";
    // String query = "SELECT " + fieldName + "," + subFieldName + " FROM test";
    //
    // Map<String, Map<String, Object>> subFieldsProps = null;
    // if (ignoreMalformed) {
    // subFieldsProps = Maps.newMapWithExpectedSize(1);
    // Map<String, Object> fieldProp = Maps.newMapWithExpectedSize(1);
    // // on purpose use a string instead of a number and check for null when querying the field's value
    // fieldProp.put("ignore_malformed", true);
    // subFieldsProps.put(subFieldName, fieldProp);
    // actualValue = "foo";
    // }
    //
    // createIndexWithFieldTypeAndSubFields("text", null, getIndexProps(), subFieldsProps, "integer");
    // index("{\"" + fieldName + "\":\"" + actualValue + "\"}");
    //
    // // (explicitSourceSetting && enableSource == false)
    // Map<String, Object> expected = new HashMap<>();
    // expected.put(
    // "columns",
    // asList(
    // columnInfo("plain", fieldName, "text", JDBCType.VARCHAR, Integer.MAX_VALUE),
    // columnInfo("plain", subFieldName, "integer", JDBCType.INTEGER, Integer.MAX_VALUE)
    // )
    // );
    // if (ignoreMalformed) {
    // expected.put("rows", singletonList(asList(getExpectedValueFromSource("foo"), null)));
    // } else {
    // expected.put(
    // "rows",
    // singletonList(asList(getExpectedValueFromSource(String.valueOf(number)), getExpectedValueFromSource(number)))
    // );
    // }
    // assertResponse(expected, runSql(query));
    // }
    //
    // /*
    // * "text_field": {
    // * "type": "text",
    // * "fields": {
    // * "ip_subfield": {
    // * "type": "ip",
    // * "ignore_malformed": true/false
    // * }
    // * }
    // * }
    // */
    // public void testTextFieldWithIpSubfield() throws IOException {
    // String ip = "123.123.123.123";
    // boolean ignoreMalformed = randomBoolean(); // ignore_malformed is true, thus test a non-IP value
    // String actualValue = ip;
    // String fieldName = "text_field";
    // String subFieldName = "text_field.ip_subfield";
    // String query = "SELECT " + fieldName + "," + subFieldName + " FROM test";
    //
    // Map<String, Map<String, Object>> subFieldsProps = null;
    // if (ignoreMalformed) {
    // subFieldsProps = Maps.newMapWithExpectedSize(1);
    // Map<String, Object> fieldProp = Maps.newMapWithExpectedSize(1);
    // // on purpose use a non-IP value instead of an IP and check for null when querying the field's value
    // fieldProp.put("ignore_malformed", true);
    // subFieldsProps.put(subFieldName, fieldProp);
    // actualValue = "foo";
    // }
    //
    // createIndexWithFieldTypeAndSubFields("text", null, getIndexProps(), subFieldsProps, "ip");
    // index("{\"" + fieldName + "\":\"" + actualValue + "\"}");
    //
    // Map<String, Object> expected = new HashMap<>();
    // expected.put(
    // "columns",
    // asList(
    // columnInfo("plain", fieldName, "text", JDBCType.VARCHAR, Integer.MAX_VALUE),
    // columnInfo("plain", subFieldName, "ip", JDBCType.VARCHAR, Integer.MAX_VALUE)
    // )
    // );
    // if (ignoreMalformed) {
    // expected.put("rows", singletonList(asList(getExpectedValueFromSource("foo"), null)));
    // } else {
    // Object expectedValueFromSource = getExpectedValueFromSource(ip);
    // expected.put("rows", singletonList(asList(expectedValueFromSource, expectedValueFromSource)));
    // }
    // assertResponse(expected, runSql(query));
    // }
    //
    // /*
    // * "integer_field": {
    // * "type": "integer",
    // * "ignore_malformed": true/false,
    // * "fields": {
    // * "keyword_subfield/text_subfield": {
    // * "type": "keyword/text"
    // * }
    // * }
    // * }
    // */
    // public void testNumberFieldWithTextOrKeywordSubfield() throws IOException {
    // Integer number = randomInt();
    // boolean ignoreMalformed = randomBoolean(); // ignore_malformed is true, thus test a non-number value
    // boolean isKeyword = randomBoolean(); // text or keyword subfield
    // Object actualValue = number;
    // String fieldName = "integer_field";
    // String subFieldName = "integer_field." + (isKeyword ? "keyword_subfield" : "text_subfield");
    // String query = "SELECT " + fieldName + "," + subFieldName + " FROM test";
    //
    // Map<String, Map<String, Object>> fieldProps = null;
    // if (ignoreMalformed) {
    // fieldProps = Maps.newMapWithExpectedSize(1);
    // Map<String, Object> fieldProp = Maps.newMapWithExpectedSize(1);
    // // on purpose use a string instead of a number and check for null when querying the field's value
    // fieldProp.put("ignore_malformed", true);
    // fieldProps.put(fieldName, fieldProp);
    // actualValue = "foo";
    // }
    //
    // createIndexWithFieldTypeAndSubFields("integer", fieldProps, getIndexProps(), null, isKeyword ? "keyword" : "text");
    // index("{\"" + fieldName + "\":\"" + actualValue + "\"}");
    //
    // Map<String, Object> expected = new HashMap<>();
    // expected.put(
    // "columns",
    // asList(
    // columnInfo("plain", fieldName, "integer", JDBCType.INTEGER, Integer.MAX_VALUE),
    // columnInfo("plain", subFieldName, isKeyword ? "keyword" : "text", JDBCType.VARCHAR, Integer.MAX_VALUE)
    // )
    // );
    // if (ignoreMalformed) {
    // expected.put("rows", singletonList(asList(null, getExpectedValueFromSource("foo"))));
    // } else {
    // expected.put(
    // "rows",
    // singletonList(asList(getExpectedValueFromSource(number), getExpectedValueFromSource(String.valueOf(number))))
    // );
    // }
    // assertResponse(expected, runSql(query));
    // }
    //
    // /*
    // * "ip_field": {
    // * "type": "ip",
    // * "ignore_malformed": true/false,
    // * "fields": {
    // * "keyword_subfield/text_subfield": {
    // * "type": "keyword/text"
    // * }
    // * }
    // * }
    // */
    // public void testIpFieldWithTextOrKeywordSubfield() throws IOException {
    // String ip = "123.123.123.123";
    // boolean ignoreMalformed = randomBoolean(); // ignore_malformed is true, thus test a non-number value
    // boolean isKeyword = randomBoolean(); // text or keyword subfield
    // String actualValue = ip;
    // String fieldName = "ip_field";
    // String subFieldName = "ip_field." + (isKeyword ? "keyword_subfield" : "text_subfield");
    // String query = "SELECT " + fieldName + "," + subFieldName + " FROM test";
    //
    // Map<String, Map<String, Object>> fieldProps = null;
    // if (ignoreMalformed) {
    // fieldProps = Maps.newMapWithExpectedSize(1);
    // Map<String, Object> fieldProp = Maps.newMapWithExpectedSize(1);
    // // on purpose use a non-IP instead of an ip and check for null when querying the field's value
    // fieldProp.put("ignore_malformed", true);
    // fieldProps.put(fieldName, fieldProp);
    // actualValue = "foo";
    // }
    //
    // createIndexWithFieldTypeAndSubFields("ip", fieldProps, getIndexProps(), null, isKeyword ? "keyword" : "text");
    // index("{\"" + fieldName + "\":\"" + actualValue + "\"}");
    //
    // Map<String, Object> expected = new HashMap<>();
    // expected.put(
    // "columns",
    // asList(
    // columnInfo("plain", fieldName, "ip", JDBCType.VARCHAR, Integer.MAX_VALUE),
    // columnInfo("plain", subFieldName, isKeyword ? "keyword" : "text", JDBCType.VARCHAR, Integer.MAX_VALUE)
    // )
    // );
    // if (ignoreMalformed) {
    // expected.put("rows", singletonList(asList(null, getExpectedValueFromSource("foo"))));
    // } else {
    // Object expectedValueFromSource = getExpectedValueFromSource(ip);
    // expected.put("rows", singletonList(asList(expectedValueFromSource, expectedValueFromSource)));
    // }
    // assertResponse(expected, runSql(query));
    // }
    //
    // /*
    // * "integer_field": {
    // * "type": "integer",
    // * "ignore_malformed": true/false,
    // * "fields": {
    // * "byte_subfield": {
    // * "type": "byte",
    // * "ignore_malformed": true/false
    // * }
    // * }
    // * }
    // */
    // public void testIntegerFieldWithByteSubfield() throws IOException {
    // boolean isByte = randomBoolean();
    // Integer number = isByte ? randomByte() : randomIntBetween(Byte.MAX_VALUE + 1, Integer.MAX_VALUE);
    // boolean rootIgnoreMalformed = randomBoolean(); // root field ignore_malformed
    // boolean subFieldIgnoreMalformed = randomBoolean(); // sub-field ignore_malformed
    // String fieldName = "integer_field";
    // String subFieldName = "integer_field.byte_subfield";
    // String query = "SELECT " + fieldName + "," + subFieldName + " FROM test";
    //
    // Map<String, Map<String, Object>> fieldProps = null;
    // if (rootIgnoreMalformed) {
    // fieldProps = Maps.newMapWithExpectedSize(1);
    // Map<String, Object> fieldProp = Maps.newMapWithExpectedSize(1);
    // fieldProp.put("ignore_malformed", true);
    // fieldProps.put(fieldName, fieldProp);
    // }
    // Map<String, Map<String, Object>> subFieldProps = null;
    // if (subFieldIgnoreMalformed) {
    // subFieldProps = Maps.newMapWithExpectedSize(1);
    // Map<String, Object> fieldProp = Maps.newMapWithExpectedSize(1);
    // fieldProp.put("ignore_malformed", true);
    // subFieldProps.put(subFieldName, fieldProp);
    // }
    //
    // createIndexWithFieldTypeAndSubFields("integer", fieldProps, getIndexProps(), subFieldProps, "byte");
    // index("{\"" + fieldName + "\":" + number + "}");
    //
    // Map<String, Object> expected = new HashMap<>();
    // expected.put(
    // "columns",
    // asList(
    // columnInfo("plain", fieldName, "integer", JDBCType.INTEGER, Integer.MAX_VALUE),
    // columnInfo("plain", subFieldName, "byte", JDBCType.TINYINT, Integer.MAX_VALUE)
    // )
    // );
    // if (isByte || subFieldIgnoreMalformed) {
    // Object expectedValueFromSource = getExpectedValueFromSource(number);
    // expected.put("rows", singletonList(asList(expectedValueFromSource, isByte ? expectedValueFromSource : null)));
    // } else {
    // expected.put("rows", Collections.emptyList());
    // }
    // assertResponse(expected, runSql(query));
    // }
    //
    // /*
    // * "byte_field": {
    // * "type": "byte",
    // * "ignore_malformed": true/false,
    // * "fields": {
    // * "integer_subfield": {
    // * "type": "integer",
    // * "ignore_malformed": true/false
    // * }
    // * }
    // * }
    // */
    // public void testByteFieldWithIntegerSubfield() throws IOException {
    // boolean isByte = randomBoolean();
    // Integer number = isByte ? randomByte() : randomIntBetween(Byte.MAX_VALUE + 1, Integer.MAX_VALUE);
    // boolean rootIgnoreMalformed = randomBoolean(); // root field ignore_malformed
    // boolean subFieldIgnoreMalformed = randomBoolean(); // sub-field ignore_malformed
    // String fieldName = "byte_field";
    // String subFieldName = "byte_field.integer_subfield";
    // String query = "SELECT " + fieldName + "," + subFieldName + " FROM test";
    //
    // Map<String, Map<String, Object>> fieldProps = null;
    // if (rootIgnoreMalformed) {
    // fieldProps = Maps.newMapWithExpectedSize(1);
    // Map<String, Object> fieldProp = Maps.newMapWithExpectedSize(1);
    // fieldProp.put("ignore_malformed", true);
    // fieldProps.put(fieldName, fieldProp);
    // }
    // Map<String, Map<String, Object>> subFieldProps = null;
    // if (subFieldIgnoreMalformed) {
    // subFieldProps = Maps.newMapWithExpectedSize(1);
    // Map<String, Object> fieldProp = Maps.newMapWithExpectedSize(1);
    // fieldProp.put("ignore_malformed", true);
    // subFieldProps.put(subFieldName, fieldProp);
    // }
    //
    // createIndexWithFieldTypeAndSubFields("byte", fieldProps, getIndexProps(), subFieldProps, "integer");
    // index("{\"" + fieldName + "\":" + number + "}");
    //
    // Map<String, Object> expected = new HashMap<>();
    // expected.put(
    // "columns",
    // asList(
    // columnInfo("plain", fieldName, "byte", JDBCType.TINYINT, Integer.MAX_VALUE),
    // columnInfo("plain", subFieldName, "integer", JDBCType.INTEGER, Integer.MAX_VALUE)
    // )
    // );
    // if (isByte || rootIgnoreMalformed) {
    // Object expectedValueFromSource = getExpectedValueFromSource(number);
    // expected.put("rows", singletonList(asList(isByte ? expectedValueFromSource : null, expectedValueFromSource)));
    // } else {
    // expected.put("rows", Collections.emptyList());
    // }
    // assertResponse(expected, runSql(query));
    // }
    //
    // public void testNestedFieldsHierarchyWithMultiNestedValues() throws IOException {
    // Request request = new Request("PUT", "/test");
    // request.setJsonEntity("""
    // {
    // "mappings": {
    // "properties": {
    // "h": {
    // "type": "nested",
    // "properties": {
    // "i": {
    // "type": "keyword"
    // },
    // "j": {
    // "type": "keyword"
    // },
    // "f": {
    // "type": "nested",
    // "properties": {
    // "o": {
    // "type": "keyword"
    // },
    // "b": {
    // "properties": {
    // "a": {
    // "type": "keyword"
    // }
    // }
    // }
    // }
    // }
    // }
    // }
    // }
    // }
    // }""");
    // client().performRequest(request);
    // index(stripWhitespace("""
    // {
    // "h": [ { "i": "123", "j": "abc" }, { "i": "890", "j": "xyz" }, { "i": "567", "j": "klm" } ],
    // "test": "foo"
    // }"""));
    //
    // Map<String, Object> expected = new HashMap<>();
    // expected.put(
    // "columns",
    // asList(
    // columnInfo("plain", "h.i", "keyword", JDBCType.VARCHAR, Integer.MAX_VALUE),
    // columnInfo("plain", "h.j", "keyword", JDBCType.VARCHAR, Integer.MAX_VALUE),
    // columnInfo("plain", "test", "text", JDBCType.VARCHAR, Integer.MAX_VALUE)
    // )
    // );
    // expected.put("rows", asList(asList("123", "abc", "foo"), asList("890", "xyz", "foo"), asList("567", "klm", "foo")));
    // assertResponse(expected, runSql("SELECT h.i, h.j, test FROM test"));
    // }
    //
    // public void testNestedFieldsHierarchyWithMissingValue() throws IOException {
    // Request request = new Request("PUT", "/test");
    // request.setJsonEntity("""
    // {
    // "mappings": {
    // "properties": {
    // "h": {
    // "type": "nested",
    // "properties": {
    // "i": {
    // "type": "keyword"
    // },
    // "f": {
    // "type": "nested",
    // "properties": {
    // "o": {
    // "type": "keyword"
    // },
    // "b": {
    // "properties": {
    // "a": {
    // "type": "keyword"
    // }
    // }
    // }
    // }
    // }
    // }
    // }
    // }
    // }
    // }""");
    // client().performRequest(request);
    // index(stripWhitespace("""
    // {
    // "h": [ { "f": { "b": { "a": "ABC" } } } ]
    // }"""));
    //
    // Map<String, Object> expected = new HashMap<>();
    // expected.put("columns", singletonList(columnInfo("plain", "h.f.o", "keyword", JDBCType.VARCHAR, Integer.MAX_VALUE)));
    // expected.put("rows", singletonList(singletonList(null)));
    // assertResponse(expected, runSql("SELECT h.f.o FROM test"));
    //
    // expected.put("columns", singletonList(columnInfo("plain", "h.i", "keyword", JDBCType.VARCHAR, Integer.MAX_VALUE)));
    // assertResponse(expected, runSql("SELECT h.i FROM test"));
    // }
    //
    // public void testNestedFieldsHierarchyExtractDeeplyNestedValue() throws IOException {
    // Request request = new Request("PUT", "/test");
    // request.setJsonEntity("""
    // {
    // "mappings": {
    // "properties": {
    // "h": {
    // "type": "nested",
    // "properties": {
    // "i": {
    // "type": "keyword"
    // },
    // "f": {
    // "type": "nested",
    // "properties": {
    // "o": {
    // "type": "keyword"
    // },
    // "b": {
    // "properties": {
    // "a": {
    // "type": "keyword"
    // }
    // }
    // }
    // }
    // }
    // }
    // }
    // }
    // }
    // }""");
    // client().performRequest(request);
    // index(stripWhitespace("""
    // {
    // "h": [ { "f": { "b": { "a": "ABC" } } } ]
    // }"""));
    //
    // Map<String, Object> expected = new HashMap<>();
    // expected.put("columns", singletonList(columnInfo("plain", "h.f.b.a", "keyword", JDBCType.VARCHAR, Integer.MAX_VALUE)));
    // expected.put("rows", singletonList(singletonList("ABC")));
    // assertResponse(expected, runSql("SELECT h.f.b.a FROM test"));
    // }
    //
    // public void testNestedFieldsHierarchyWithArrayOfValues() throws IOException {
    // Request request = new Request("PUT", "/test");
    // request.setJsonEntity(stripWhitespace("""
    // {
    // "mappings": {
    // "properties": {
    // "h": {
    // "type": "nested",
    // "properties": {
    // "i": {
    // "type": "keyword"
    // },
    // "j": {
    // "type": "keyword"
    // },
    // "f": {
    // "type": "nested",
    // "properties": {
    // "o": {
    // "type": "keyword"
    // },
    // "b": {
    // "properties": {
    // "a": {
    // "type": "keyword"
    // }
    // }
    // }
    // }
    // }
    // }
    // }
    // }
    // }
    // }"""));
    // client().performRequest(request);
    // index(stripWhitespace("""
    // {
    // "h": [
    // {
    // "i": [ "123", "124", "125" ],
    // "j": "abc"
    // },
    // {
    // "i": "890",
    // "j": "xyz"
    // },
    // {
    // "i": "567",
    // "j": "klm"
    // }
    // ],
    // "test": "foo"
    // }"""));
    //
    // Map<String, Object> expected = new HashMap<>();
    // Map<String, Object> actual = new HashMap<>();
    // expected.put(
    // "columns",
    // asList(
    // columnInfo("plain", "h.i", "keyword", JDBCType.VARCHAR, Integer.MAX_VALUE),
    // columnInfo("plain", "h.j", "keyword", JDBCType.VARCHAR, Integer.MAX_VALUE),
    // columnInfo("plain", "test", "text", JDBCType.VARCHAR, Integer.MAX_VALUE)
    // )
    // );
    // expected.put("rows", asList(asList("123", "abc", "foo"), asList("890", "xyz", "foo"), asList("567", "klm", "foo")));
    // Request sqlRequest = new Request("POST", RestSqlTestCase.SQL_QUERY_REST_ENDPOINT);
    // sqlRequest.addParameter("error_trace", "true");
    // sqlRequest.addParameter("pretty", "true");
    // sqlRequest.setEntity(
    // new StringEntity(
    // query("SELECT h.i, h.j, test FROM test").mode("plain").fieldMultiValueLeniency(true).toString(),
    // ContentType.APPLICATION_JSON
    // )
    // );
    // Response response = client().performRequest(sqlRequest);
    // try (InputStream content = response.getEntity().getContent()) {
    // actual = XContentHelper.convertToMap(JsonXContent.jsonXContent, content, false);
    // }
    // assertResponse(expected, actual);
    // }
    //
    // /*
    // * From a randomly created mapping using "object" field types and "nested" field types like the one below, we look at
    // * extracting the values from the deepest "nested" field type.
    // * The query to use for the mapping below would be "SELECT HETeC.fdeuk.oDwgT FROM test"
    // * {
    // * "mappings" : {
    // * "properties" : {
    // * "HETeC" : {
    // * "type" : "nested",
    // * "properties" : {
    // * "iBtgB" : {
    // * "type" : "keyword"
    // * },
    // * "fdeuk" : {
    // * "type" : "nested",
    // * "properties" : {
    // * "oDwgT" : {
    // * "type" : "keyword"
    // * },
    // * "biXlb" : {
    // * "properties" : {
    // * "AlkJR" : {
    // * "type" : "keyword"
    // * }
    // * }
    // * }
    // * }
    // * }
    // * }
    // * }
    // * }
    // * }
    // * }
    // */
    // public void testNestedFieldsHierarchy() throws IOException {
    // final int minDepth = 2;
    // final int maxDepth = 6;
    // final int depth = between(minDepth, maxDepth);
    //
    // Request request = new Request("PUT", "/test");
    // XContentBuilder index = JsonXContent.contentBuilder().prettyPrint().startObject();
    // List<Tuple<String, NestedFieldType>> path = new ArrayList<>(depth);
    // StringBuilder bulkContent = new StringBuilder();
    // Holder<String> randomValue = new Holder<>("");
    // index.startObject("mappings");
    // {
    // index.startObject("properties");
    // {
    // addField(index, false, depth, path, bulkContent, randomValue);
    // }
    // index.endObject();
    // }
    // index.endObject();
    // index.endObject();
    //
    // request.setJsonEntity(Strings.toString(index));
    // client().performRequest(request);
    // index("{" + bulkContent.toString() + "}");
    //
    // // the path ends with either a NESTED field or an OBJECT field (both having a leaf field as a sub-field)
    // // if it's nested, we use this field
    // // if it's object, we need to strip every field starting from the end until we reach a nested field
    // int endOfPathIndex = path.size() - 2; // -1 because we skip the leaf field at the end and another -1 because it's 0-based
    // while (path.get(endOfPathIndex--).v2() != NestedFieldType.NESTED) {
    // } // find the first nested field starting from the end
    //
    // StringBuilder stringPath = new StringBuilder(path.get(0).v1()); // the path we will ask for in the sql query
    // for (int i = 1; i <= endOfPathIndex + 2; i++) { // +2 because the index is now at the [index_of_a_nested_field]-1
    // if (path.get(i).v2() != NestedFieldType.LEAF || i == endOfPathIndex + 2) {
    // stringPath.append(".");
    // stringPath.append(path.get(i).v1());
    // }
    // }
    //
    // Map<String, Object> expected = new HashMap<>();
    // expected.put("columns", singletonList(columnInfo("plain", stringPath.toString(), "keyword", JDBCType.VARCHAR, Integer.MAX_VALUE)));
    // expected.put("rows", singletonList(singletonList(randomValue.get())));
    // assertResponse(expected, runSql("SELECT " + stringPath.toString() + " FROM test"));
    // }
    //
    // private enum NestedFieldType {
    // NESTED,
    // OBJECT,
    // LEAF;
    // }
    //
    // private static void addField(
    // XContentBuilder index,
    // boolean nestedFieldAdded,
    // int remainingFields,
    // List<Tuple<String, NestedFieldType>> path,
    // StringBuilder bulkContent,
    // Holder<String> randomValue
    // ) throws IOException {
    // String fieldName = randomAlphaOfLength(5);
    // String leafFieldName = randomAlphaOfLength(5);
    //
    // // we need to make sure we add at least one nested field to the mapping, otherwise the test is not about nested fields
    // if (shouldAddNestedField() || (nestedFieldAdded == false && remainingFields == 1)) {
    // path.add(new Tuple<String, NestedFieldType>(fieldName, NestedFieldType.NESTED));
    // path.add(new Tuple<String, NestedFieldType>(leafFieldName, NestedFieldType.LEAF));
    // index.startObject(fieldName);
    // {
    // index.field("type", "nested");
    // index.startObject("properties");
    // {
    // // A nested field always has a leaf field, even if not all nested fields in a path will have this value
    // // indexed. We will index only the "leaf" field of the last nested field in the path, because this is the
    // // one we will ask back from ES
    // index.startObject(leafFieldName);
    // {
    // index.field("type", "keyword");
    // }
    // index.endObject();
    // // from time to time set a null value instead of an actual value
    // if (rarely()) {
    // randomValue.set(null);
    // bulkContent.append("\"" + fieldName + "\":{\"" + leafFieldName + "\":null");
    // } else {
    // randomValue.set(randomAlphaOfLength(10));
    // bulkContent.append("\"" + fieldName + "\":{\"" + leafFieldName + "\":\"" + randomValue.get() + "\"");
    // }
    // if (remainingFields > 1) {
    // bulkContent.append(",");
    // addField(index, true, remainingFields - 1, path, bulkContent, randomValue);
    // }
    // bulkContent.append("}");
    // }
    // index.endObject();
    // }
    // index.endObject();
    // } else {
    // path.add(new Tuple<String, NestedFieldType>(fieldName, NestedFieldType.OBJECT));
    // index.startObject(fieldName);
    // index.startObject("properties");
    // {
    // bulkContent.append("\"" + fieldName + "\":{");
    // // if this is the last field in the mapping and it's non-nested, add a keyword to it, otherwise the mapping
    // // is incomplete and an error will be thrown at mapping creation time
    // if (remainingFields == 1) {
    // path.add(new Tuple<String, NestedFieldType>(leafFieldName, NestedFieldType.LEAF));
    // index.startObject(leafFieldName);
    // {
    // index.field("type", "keyword");
    // }
    // index.endObject();
    // bulkContent.append("\"" + leafFieldName + "\":\"" + randomAlphaOfLength(10) + "\"");
    // } else {
    // addField(index, nestedFieldAdded, remainingFields - 1, path, bulkContent, randomValue);
    // }
    // bulkContent.append("}");
    // }
    // index.endObject();
    // index.endObject();
    // }
    // }

    private static boolean shouldAddNestedField() {  // NOCOMMIt used?
        return randomBoolean();
    }

    private void index(String... docs) throws IOException {
        indexWithIndexName(TEST_INDEX, docs);
    }

    private void indexWithIndexName(String indexName, String... docs) throws IOException {
        Request request = new Request("POST", "/" + indexName + "/_bulk");
        request.addParameter("refresh", "true");
        StringBuilder bulk = new StringBuilder();
        for (String doc : docs) {
            bulk.append(String.format(Locale.ROOT, """
                {"index":{}}
                %s
                """, doc));
        }
        request.setJsonEntity(bulk.toString());
        client().performRequest(request);
    }

    private void createIndexWithFieldTypeAndAlias(
        // NOCOMMIt used?
        String type,
        CheckedConsumer<XContentBuilder, IOException> fieldMapping
    ) throws IOException {
        createIndex(type, fieldMapping, true, false, null);
    }

    private void createIndex(String type, CheckedConsumer<XContentBuilder, IOException> fieldMapping) throws IOException {
        createIndex(type, fieldMapping, false, false, null);
    }

    private void createIndex(
        // NOCOMMIt make sure all args are used
        String type,
        CheckedConsumer<XContentBuilder, IOException> fieldMapping,
        Map<String, Map<String, Object>> subFieldsProps,
        String... subFieldsTypes
    ) throws IOException {
        createIndex(type, fieldMapping, false, true, subFieldsProps, subFieldsTypes);
    }

    private void createIndex(
        // NOCOMMIt make sure all args are used
        String type,
        CheckedConsumer<XContentBuilder, IOException> fieldMapping,
        boolean withAlias,
        boolean withSubFields,
        Map<String, Map<String, Object>> subFieldsProps,
        String... subFieldsTypes
    ) throws IOException {
        Request request = new Request("PUT", "/test");
        XContentBuilder index = JsonXContent.contentBuilder().prettyPrint().startObject();

        index.startObject("mappings");
        {
            sourceMode.sourceMapping(index);
            index.startObject("properties");
            {
                String fieldName = type + "_field";
                index.startObject(fieldName);
                {
                    index.field("type", type);
                    fieldMapping.accept(index);

                    if (withSubFields) {
                        index.startObject("fields");
                        for (String subFieldType : subFieldsTypes) {
                            String subFieldName = subFieldType + "_subfield";
                            String fullSubFieldName = fieldName + "." + subFieldName;
                            index.startObject(subFieldName);
                            index.field("type", subFieldType);
                            if (subFieldsProps != null && subFieldsProps.containsKey(fullSubFieldName)) {
                                for (Map.Entry<String, Object> prop : subFieldsProps.get(fullSubFieldName).entrySet()) {
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

    private Map<String, Object> fetchAll() throws IOException {
        return runEsqlSync(new RestEsqlTestCase.RequestObjectBuilder().query("from " + TEST_INDEX));
    }

    private Map<String, Object> columnInfo(String name, String type) {
        return Map.of("name", name, "type", type);
    }

    private enum SourceMode {
        DEFAULT {
            @Override
            void sourceMapping(XContentBuilder builder) {}

            @Override
            boolean stored() {
                return true;
            }
        },
        STORED {
            @Override
            void sourceMapping(XContentBuilder builder) throws IOException {
                builder.startObject(SourceFieldMapper.NAME).field("mode", "stored").endObject();
            }

            @Override
            boolean stored() {
                return true;
            }
        },
        DISABLED {
            @Override
            void sourceMapping(XContentBuilder builder) throws IOException {
                builder.startObject(SourceFieldMapper.NAME).field("mode", "disabled").endObject();
            }

            @Override
            boolean stored() {
                return false;
            }
        },
        SYNTHETIC {
            @Override
            void sourceMapping(XContentBuilder builder) throws IOException {
                builder.startObject(SourceFieldMapper.NAME).field("mode", "synthetic").endObject();
            }

            @Override
            boolean stored() {
                return false;
            }
        };

        abstract void sourceMapping(XContentBuilder builder) throws IOException;

        abstract boolean stored();
    }

    private boolean ignoredByIgnoreAbove(Integer ignoreAbove, int length) {
        return ignoreAbove != null && length > ignoreAbove;
    }

    private BigInteger randomUnsignedLong() {
        BigInteger big = BigInteger.valueOf(randomNonNegativeLong()).shiftLeft(1);
        return big.add(randomBoolean() ? BigInteger.ONE : BigInteger.ZERO);
    }
}
