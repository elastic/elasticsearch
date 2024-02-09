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
import org.elasticsearch.common.network.NetworkAddress;
import org.elasticsearch.geo.GeometryTestUtils;
import org.elasticsearch.index.mapper.BlockLoader;
import org.elasticsearch.index.mapper.SourceFieldMapper;
import org.elasticsearch.logging.LogManager;
import org.elasticsearch.logging.Logger;
import org.elasticsearch.test.ListMatcher;
import org.elasticsearch.test.rest.ESRestTestCase;
import org.elasticsearch.xcontent.XContentBuilder;
import org.elasticsearch.xcontent.json.JsonXContent;
import org.hamcrest.Matcher;

import java.io.IOException;
import java.math.BigDecimal;
import java.math.BigInteger;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Comparator;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.TreeMap;
import java.util.function.Function;

import static org.elasticsearch.test.MapMatcher.assertMap;
import static org.elasticsearch.test.MapMatcher.matchesMap;
import static org.elasticsearch.test.ListMatcher.matchesList;
import static org.elasticsearch.xpack.esql.qa.rest.RestEsqlTestCase.runEsqlSync;
import static org.hamcrest.Matchers.closeTo;

/**
 * Creates indices with many different mappings and fetches values from them to make sure
 * we can do it. Think of this as an integration test for {@link BlockLoader}
 * implementations <strong>and</strong> an integration test for field resolution.
 * This is a port of a test with the same name on the SQL side.
 */
@Repeat(iterations = 10)
public abstract class FieldExtractorTestCase extends ESRestTestCase {
    private static final String TEST_INDEX = "test";

    public void testTextField() throws IOException {
        textTest().test(randomAlphaOfLength(20));
    }

    private Test textTest() {
        return new Test("text").randomStoreUnlessSynthetic();
    }

    public void testKeywordField() throws IOException {
        // TODO randomize store
        Integer ignoreAbove = randomBoolean() ? null : between(10, 50);
        int length = between(10, 50);

        String value = randomAlphaOfLength(length);
        keywordTest().ignoreAbove(ignoreAbove).test(value, ignoredByIgnoreAbove(ignoreAbove, length) ? null : value);
    }

    private Test keywordTest() {
        return new Test("keyword").randomDocValuesAndStoreUnlessSynthetic();
    }

    public void testConstantKeywordField() throws IOException {
        boolean specifyInMapping = randomBoolean();
        boolean specifyInDocument = randomBoolean();

        String value = randomAlphaOfLength(20);
        new Test("constant_keyword").expectedType("keyword")
            .value(specifyInMapping ? value : null)
            .test(specifyInDocument ? value : null, specifyInMapping || specifyInDocument ? value : null);
    }

    public void testWildcardField() throws IOException {
        Integer ignoreAbove = randomBoolean() ? null : between(10, 50);
        int length = between(10, 50);

        String value = randomAlphaOfLength(length);
        new Test("wildcard").expectedType("keyword")
            .ignoreAbove(ignoreAbove)
            .test(value, ignoredByIgnoreAbove(ignoreAbove, length) ? null : value);
    }

    public void testLong() throws IOException {
        long value = randomLong();
        longTest().test(randomBoolean() ? Long.toString(value) : value, value);
    }

    public void testLongWithDecimalParts() throws IOException {
        long value = randomLong();
        int decimalPart = between(1, 99);
        BigDecimal withDecimals = new BigDecimal(value + "." + decimalPart);
        /*
         * It's possible to pass the BigDecimal here without converting to a string
         * but that rounds in a different way, and I'm not quite able to reproduce it
         * at the time.
         */
        longTest().test(withDecimals.toString(), value);
    }

    public void testLongMalformed() throws IOException {
        longTest().forceIgnoreMalformed().test(randomAlphaOfLength(5), null);
    }

    private Test longTest() {
        return new Test("long").randomIgnoreMalformedUnlessSynthetic().randomDocValuesUnlessSynthetic();
    }

    public void testInt() throws IOException {
        int value = randomInt();
        intTest().test(randomBoolean() ? Integer.toString(value) : value, value);
    }

    public void testIntWithDecimalParts() throws IOException {
        double value = randomDoubleBetween(Integer.MIN_VALUE, Integer.MAX_VALUE, true);
        intTest().test(randomBoolean() ? Double.toString(value) : value, (int) value);
    }

    public void testIntMalformed() throws IOException {
        intTest().forceIgnoreMalformed().test(randomAlphaOfLength(5), null);
    }

    private Test intTest() {
        return new Test("integer").randomIgnoreMalformedUnlessSynthetic().randomDocValuesUnlessSynthetic();
    }

    public void testShort() throws IOException {
        short value = randomShort();
        shortTest().test(randomBoolean() ? Short.toString(value) : value, (int) value);
    }

    public void testShortWithDecimalParts() throws IOException {
        double value = randomDoubleBetween(Short.MIN_VALUE, Short.MAX_VALUE, true);
        shortTest().test(randomBoolean() ? Double.toString(value) : value, (int) value);
    }

    public void testShortMalformed() throws IOException {
        shortTest().forceIgnoreMalformed().test(randomAlphaOfLength(5), null);
    }

    private Test shortTest() {
        return new Test("short").expectedType("integer").randomIgnoreMalformedUnlessSynthetic().randomDocValuesUnlessSynthetic();
    }

    public void testByte() throws IOException {
        byte value = randomByte();
        byteTest().test(Byte.toString(value), (int) value);
    }

    public void testByteWithDecimalParts() throws IOException {
        double value = randomDoubleBetween(Byte.MIN_VALUE, Byte.MAX_VALUE, true);
        byteTest().test(randomBoolean() ? Double.toString(value) : value, (int) value);
    }

    public void testByteMalformed() throws IOException {
        byteTest().forceIgnoreMalformed().test(randomAlphaOfLength(5), null);
    }

    private Test byteTest() {
        return new Test("byte").expectedType("integer").randomIgnoreMalformedUnlessSynthetic().randomDocValuesUnlessSynthetic();
    }

    public void testUnsignedLong() throws IOException {
        BigInteger value = randomUnsignedLong();
        new Test("unsigned_long").randomIgnoreMalformedUnlessSynthetic()
            .randomDocValuesUnlessSynthetic()
            .test(
                randomBoolean() ? value.toString() : value,
                value.compareTo(BigInteger.valueOf(Long.MAX_VALUE)) <= 0 ? value.longValue() : value
            );
    }

    public void testUnsignedLongMalformed() throws IOException {
        new Test("unsigned_long").forceIgnoreMalformed().randomDocValuesUnlessSynthetic().test(randomAlphaOfLength(5), null);
    }

    public void testDouble() throws IOException {
        double value = randomDouble();
        new Test("double").randomIgnoreMalformedUnlessSynthetic()
            .randomDocValuesUnlessSynthetic()
            .test(randomBoolean() ? Double.toString(value) : value, value);
    }

    public void testFloat() throws IOException {
        float value = randomFloat();
        new Test("float").expectedType("double")
            .randomIgnoreMalformedUnlessSynthetic()
            .randomDocValuesUnlessSynthetic()
            .test(randomBoolean() ? Float.toString(value) : value, (double) value);
    }

    public void testScaledFloat() throws IOException {
        double value = randomBoolean() ? randomDoubleBetween(-Double.MAX_VALUE, Double.MAX_VALUE, true) : randomFloat();
        double scalingFactor = randomDoubleBetween(0, Double.MAX_VALUE, false);
        new Test("scaled_float").expectedType("double")
            .randomIgnoreMalformedUnlessSynthetic()
            .randomDocValuesUnlessSynthetic()
            .scalingFactor(scalingFactor)
            .test(randomBoolean() ? Double.toString(value) : value, scaledFloatMatcher(scalingFactor, value));
    }

    private Matcher<Double> scaledFloatMatcher(double scalingFactor, double d) {
        long encoded = Math.round(d * scalingFactor);
        double decoded = encoded / scalingFactor;
        return closeTo(decoded, Math.ulp(decoded));
    }

    public void testBoolean() throws IOException {
        boolean value = randomBoolean();
        new Test("boolean").ignoreMalformed(randomBoolean())
            .randomDocValuesUnlessSynthetic()
            .test(randomBoolean() ? Boolean.toString(value) : value, value);
    }

    public void testIp() throws IOException {
        new Test("ip").ignoreMalformed(randomBoolean()).test(NetworkAddress.format(randomIp(randomBoolean())));
    }

    public void testVersionField() throws IOException {
        new Test("version").test(randomVersionString());
    }

    public void testGeoPoint() throws IOException {
        new Test("geo_point")
            // TODO we should support loading geo_point from doc values if source isn't enabled
            .sourceMode(randomValueOtherThanMany(s -> s.stored() == false, () -> randomFrom(SourceMode.values())))
            .ignoreMalformed(randomBoolean())
            .storeAndDocValues(randomBoolean(), randomBoolean())
            .test(GeometryTestUtils.randomPoint(false).toString());
    }

    public void testGeoShape() throws IOException {
        new Test("geo_shape")
            // TODO if source isn't enabled how can we load *something*? It's just triangles, right?
            .sourceMode(randomValueOtherThanMany(s -> s.stored() == false, () -> randomFrom(SourceMode.values())))
            .ignoreMalformed(randomBoolean())
            .storeAndDocValues(randomBoolean(), randomBoolean())
            // TODO pick supported random shapes
            .test(GeometryTestUtils.randomPoint(false).toString());
    }

    public void testAliasToKeyword() throws IOException {
        keywordTest().createAlias().test(randomAlphaOfLength(20));
    }

    public void testAliasToText() throws IOException {
        textTest().createAlias().test(randomAlphaOfLength(20));
    }

    public void testAliasToInt() throws IOException {
        intTest().createAlias().test(randomInt());
    }

    /**
     * <pre>
     * "text_field": {
     *   "type": "text",
     *   "fields": {
     *     "raw": {
     *       "type": "keyword",
     *       "ignore_above": 10
     *     }
     *   }
     * }
     * </pre>
     */
    public void testTextFieldWithKeywordSubfield() throws IOException {
        String value = randomAlphaOfLength(20);
        Map<String, Object> result = new Test("text").storeAndDocValues(randomBoolean(), null).sub("raw", keywordTest()).roundTrip(value);

        assertMap(
            result,
            matchesMap().entry("columns", List.of(columnInfo("text_field", "text"), columnInfo("text_field.raw", "keyword")))
                .entry("values", List.of(List.of(value, value)))
        );
    }

    /**
     * <pre>
     * "text_field": {
     *   "type": "text",
     *   "fields": {
     *     "int": {
     *       "type": "integer",
     *       "ignore_malformed": true/false
     *     }
     *   }
     * }
     * </pre>
     */
    public void testTextFieldWithIntegerSubfield() throws IOException {
        int value = randomInt();
        Map<String, Object> result = textTest().sub("int", intTest()).roundTrip(value);

        assertMap(
            result,
            matchesMap().entry("columns", List.of(columnInfo("text_field", "text"), columnInfo("text_field.int", "integer")))
                .entry("values", List.of(List.of(Integer.toString(value), value)))
        );
    }

    /**
     * <pre>
     * "text_field": {
     *   "type": "text",
     *   "fields": {
     *     "int": {
     *       "type": "integer",
     *       "ignore_malformed": true
     *     }
     *   }
     * }
     * </pre>
     */
    public void testTextFieldWithIntegerSubfieldMalformed() throws IOException {
        String value = randomAlphaOfLength(5);
        Map<String, Object> result = textTest().sourceMode(SourceMode.DEFAULT).sub("int", intTest().ignoreMalformed(true)).roundTrip(value);

        assertMap(
            result,
            matchesMap().entry("columns", List.of(columnInfo("text_field", "text"), columnInfo("text_field.int", "integer")))
                .entry("values", List.of(matchesList().item(value).item(null)))
        );
    }

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
        /* TODO add support to this test for disabling _source
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
         */
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

    private static String randomVersionString() {
        return randomVersionNumber() + (randomBoolean() ? "" : randomPrerelease());
    }

    private static String randomVersionNumber() {
        int numbers = between(1, 3);
        String v = Integer.toString(between(0, 100));
        for (int i = 1; i < numbers; i++) {
            v += "." + between(0, 100);
        }
        return v;
    }

    private static String randomPrerelease() {
        if (rarely()) {
            return randomFrom("alpha", "beta", "prerelease", "whatever");
        }
        return randomFrom("alpha", "beta", "") + randomVersionNumber();
    }

    private record StoreAndDocValues(Boolean store, Boolean docValues) {}

    private static class Test {
        private static final Logger logger = LogManager.getLogger(Test.class);

        private final String type;
        private final Map<String, Test> subFields = new TreeMap<>();

        private SourceMode sourceMode;
        private String expectedType;
        private Function<SourceMode, Boolean> ignoreMalformed;
        private Function<SourceMode, StoreAndDocValues> storeAndDocValues = s -> new StoreAndDocValues(null, null);
        private Double scalingFactor;
        private Integer ignoreAbove;
        private Object value;
        private boolean createAlias;

        Test(String type) {
            this.type = type;
            // Default the expected return type to the field type.
            this.expectedType = type;
        }

        Test sourceMode(SourceMode sourceMode) {
            this.sourceMode = sourceMode;
            return this;
        }

        Test expectedType(String expectedType) {
            this.expectedType = expectedType;
            return this;
        }

        Test ignoreMalformed(boolean ignoreMalformed) {
            this.ignoreMalformed = s -> ignoreMalformed;
            return this;
        }

        /**
         * Enable {@code ignore_malformed} and disable synthetic _source because
         * most fields don't support ignore_malformed and synthetic _source.
         */
        Test forceIgnoreMalformed() {
            return this.sourceMode(randomValueOtherThan(SourceMode.SYNTHETIC, () -> randomFrom(SourceMode.values()))).ignoreMalformed(true);
        }

        Test randomIgnoreMalformedUnlessSynthetic() {
            this.ignoreMalformed = s -> s == SourceMode.SYNTHETIC ? false : randomBoolean();
            return this;
        }

        Test storeAndDocValues(Boolean store, Boolean docValues) {
            this.storeAndDocValues = s -> new StoreAndDocValues(store, docValues);
            return this;
        }

        Test randomStoreUnlessSynthetic() {
            this.storeAndDocValues = s -> new StoreAndDocValues(s == SourceMode.SYNTHETIC ? true : randomBoolean(), null);
            return this;
        }

        Test randomDocValuesAndStoreUnlessSynthetic() {
            this.storeAndDocValues = s -> {
                if (s == SourceMode.SYNTHETIC) {
                    boolean store = randomBoolean();
                    return new StoreAndDocValues(store, store == false || randomBoolean());
                }
                return new StoreAndDocValues(randomBoolean(), randomBoolean());
            };
            return this;
        }

        Test randomDocValuesUnlessSynthetic() {
            this.storeAndDocValues = s -> new StoreAndDocValues(null, s == SourceMode.SYNTHETIC || randomBoolean());
            return this;
        }

        Test scalingFactor(double scalingFactor) {
            this.scalingFactor = scalingFactor;
            return this;
        }

        Test ignoreAbove(Integer ignoreAbove) {
            this.ignoreAbove = ignoreAbove;
            return this;
        }

        Test value(Object value) {
            this.value = value;
            return this;
        }

        Test createAlias() {
            this.createAlias = true;
            return this;
        }

        Test sub(String name, Test sub) {
            this.subFields.put(name, sub);
            return this;
        }

        Map<String, Object> roundTrip(Object value) throws IOException {
            if (sourceMode == null) {
                sourceMode(randomFrom(SourceMode.values()));
            }
            logger.info("source_mode: {}", sourceMode);
            createIndex();

            if (value == null) {
                index("{}");
                logger.info("indexing empty doc");
            } else {
                logger.info("indexing {}::{}", value, value.getClass().getName());
                index(Strings.toString(JsonXContent.contentBuilder().startObject().field(type + "_field", value).endObject()));
            }

            return fetchAll();
        }

        void test(Object value) throws IOException {
            test(value, value);
        }

        /**
         * Round trip the value through and index configured by the parameters
         * of this test and assert that it matches the {@code expectedValues}
         * which can be either the expected value or a subclass of {@link Matcher}.
         */
        void test(Object value, Object expectedValue) throws IOException {
            Map<String, Object> result = roundTrip(value);

            logger.info("expecting {}", expectedValue == null ? null : expectedValue + "::" + expectedValue.getClass().getName());

            List<Map<String, Object>> columns = new ArrayList<>();
            columns.add(columnInfo(type + "_field", expectedType));
            if (createAlias) {
                columns.add(columnInfo("a.b.c." + type + "_field_alias", expectedType));
                columns.add(columnInfo(type + "_field_alias", expectedType));
            }
            Collections.sort(columns, Comparator.comparing(m -> (String) m.get("name")));

            ListMatcher values = matchesList();
            values = values.item(expectedValue);
            if (createAlias) {
                values = values.item(expectedValue);
                values = values.item(expectedValue);
            }

            assertMap(result, matchesMap().entry("columns", columns).entry("values", List.of(values)));
        }

        private void createIndex() throws IOException {
            Request request = new Request("PUT", "/test");
            XContentBuilder index = JsonXContent.contentBuilder().prettyPrint().startObject();

            index.startObject("settings");
            {
                index.field("index.number_of_replicas", 0);
                index.field("index.number_of_shards", 1);
            }
            index.endObject();
            index.startObject("mappings");
            {
                sourceMode.sourceMapping(index);
                index.startObject("properties");
                {
                    String fieldName = type + "_field";

                    index.startObject(fieldName);
                    fieldMapping(index);
                    index.endObject();

                    if (createAlias) {
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

            String mapping = Strings.toString(index);
            logger.info("index: {}", Strings.toString(index));
            request.setJsonEntity(mapping);
            client().performRequest(request);
        }

        private void fieldMapping(XContentBuilder builder) throws IOException {
            builder.field("type", type);
            if (ignoreMalformed != null) {
                builder.field("ignore_malformed", ignoreMalformed.apply(sourceMode));
            }
            StoreAndDocValues sd = storeAndDocValues.apply(sourceMode);
            if (sd.docValues != null) {
                builder.field("doc_values", sd.docValues);
            }
            if (sd.store != null) {
                builder.field("store", sd.store);
            }
            if (scalingFactor != null) {
                builder.field("scaling_factor", scalingFactor);
            }
            if (ignoreAbove != null) {
                builder.field("ignore_above", ignoreAbove);
            }
            if (value != null) {
                builder.field("value", value);
            }

            if (subFields.isEmpty() == false) {
                builder.startObject("fields");
                for (Map.Entry<String, Test> sub : subFields.entrySet()) {
                    builder.startObject(sub.getKey());
                    if (sub.getValue().sourceMode != null) {
                        throw new IllegalStateException("source_mode can't be configured on sub-fields");
                    }
                    sub.getValue().sourceMode = sourceMode;
                    sub.getValue().fieldMapping(builder);
                    builder.endObject();
                }
                builder.endObject();
            }
        }

        private void index(String... docs) throws IOException {
            Request request = new Request("POST", "/" + TEST_INDEX + "/_bulk");
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

        private Map<String, Object> fetchAll() throws IOException {
            return runEsqlSync(new RestEsqlTestCase.RequestObjectBuilder().query("from " + TEST_INDEX));
        }

    }

    private static Map<String, Object> columnInfo(String name, String type) {
        return Map.of("name", name, "type", type);
    }
}
