/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.index.mapper;

import org.apache.lucene.util.BytesRef;
import org.elasticsearch.common.Strings;
import org.elasticsearch.common.bytes.BytesReference;
import org.elasticsearch.common.compress.CompressedXContent;
import org.elasticsearch.common.xcontent.XContentHelper;
import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.xcontent.XContentBuilder;
import org.elasticsearch.xcontent.XContentFactory;
import org.elasticsearch.xcontent.XContentParser;
import org.elasticsearch.xcontent.XContentParserConfiguration;
import org.elasticsearch.xcontent.XContentType;
import org.elasticsearch.xcontent.json.JsonXContent;

import java.io.IOException;
import java.math.BigDecimal;
import java.math.BigInteger;
import java.util.Arrays;
import java.util.Base64;
import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.stream.Stream;

import static org.hamcrest.Matchers.equalTo;

public class XContentDataHelperTests extends ESTestCase {

    private String dataInParser(XContentParser parser) throws IOException {
        XContentBuilder builder = XContentFactory.jsonBuilder();
        builder.humanReadable(true);
        builder.copyCurrentStructure(parser);
        return Strings.toString(builder);
    }

    private String encodeAndDecode(Object value) throws IOException {
        return encodeAndDecodeCustom(randomFrom(XContentType.values()), value);
    }

    private String encodeAndDecodeCustom(XContentType type, Object value) throws IOException {
        var builder = XContentFactory.contentBuilder(type);
        builder.startObject().field("foo", value).endObject();

        XContentParser parser = createParser(builder);
        assertThat(parser.nextToken(), equalTo(XContentParser.Token.START_OBJECT));
        assertThat(parser.nextToken(), equalTo(XContentParser.Token.FIELD_NAME));
        assertThat(parser.currentName(), equalTo("foo"));
        parser.nextToken();

        var encoded = XContentDataHelper.encodeToken(parser);
        assertThat(XContentDataHelper.isEncodedObject(encoded), equalTo(value instanceof Map));
        var decoded = XContentFactory.jsonBuilder();
        XContentDataHelper.decodeAndWrite(decoded, encoded);

        return Strings.toString(decoded);
    }

    public void testBoolean() throws IOException {
        boolean b = randomBoolean();
        assertEquals(b, Boolean.parseBoolean(encodeAndDecode(b)));
    }

    public void testString() throws IOException {
        String s = randomAlphaOfLength(5);
        assertEquals("\"" + s + "\"", encodeAndDecode(s));
    }

    public void testInt() throws IOException {
        int i = randomInt();
        assertEquals(i, Integer.parseInt(encodeAndDecode(i)));
    }

    public void testLong() throws IOException {
        long l = randomLong();
        assertEquals(l, Long.parseLong(encodeAndDecode(l)));
    }

    public void testFloat() throws IOException {
        float f = randomFloat();
        // JSON does not have special encoding for float
        assertEquals(0, Float.compare(f, Float.parseFloat(encodeAndDecodeCustom(XContentType.SMILE, f))));
    }

    public void testDouble() throws IOException {
        double d = randomDouble();
        assertEquals(0, Double.compare(d, Double.parseDouble(encodeAndDecode(d))));
    }

    public void testBigInteger() throws IOException {
        BigInteger i = randomBigInteger();
        // JSON does not have special encoding for BigInteger
        assertEquals(i, new BigInteger(encodeAndDecodeCustom(XContentType.SMILE, i), 10));
    }

    public void testBigDecimal() throws IOException {
        BigDecimal i = new BigDecimal(randomLong());
        // JSON does not have special encoding for BigDecimal
        assertEquals(i, new BigDecimal(encodeAndDecodeCustom(XContentType.SMILE, i)));
    }

    public void testNull() throws IOException {
        assertEquals("null", encodeAndDecode(null));
    }

    public void testEmbeddedObject() throws IOException {
        // XContentType.JSON never produces VALUE_EMBEDDED_OBJECT
        XContentBuilder builder = XContentBuilder.builder(XContentType.CBOR.xContent());
        builder.startObject();
        CompressedXContent embedded = new CompressedXContent("{\"field\":\"value\"}");
        builder.field("bytes", embedded.compressed());
        builder.endObject();
        var originalBytes = BytesReference.bytes(builder);

        try (XContentParser parser = createParser(builder)) {
            assertEquals(XContentParser.Token.START_OBJECT, parser.nextToken());
            assertEquals(XContentParser.Token.FIELD_NAME, parser.nextToken());
            parser.nextToken();
            var encoded = XContentDataHelper.encodeToken(parser);
            assertFalse(XContentDataHelper.isEncodedObject(encoded));

            var decoded = XContentFactory.jsonBuilder();
            XContentDataHelper.decodeAndWrite(decoded, encoded);

            assertEquals("\"" + Base64.getEncoder().encodeToString(embedded.compressed()) + "\"", Strings.toString(decoded));
        }

        var encoded = XContentDataHelper.encodeXContentBuilder(builder);
        assertTrue(XContentDataHelper.isEncodedObject(encoded));

        var decoded = XContentFactory.jsonBuilder();
        XContentDataHelper.decodeAndWrite(decoded, encoded);
        var decodedBytes = BytesReference.bytes(builder);

        assertEquals(originalBytes, decodedBytes);
    }

    public void testObject() throws IOException {
        String object = "{\"name\":\"foo\"}";
        XContentParser p = createParser(JsonXContent.jsonXContent, object);
        assertThat(p.nextToken(), equalTo(XContentParser.Token.START_OBJECT));

        XContentBuilder builder = XContentFactory.jsonBuilder();
        builder.humanReadable(true);
        var encoded = XContentDataHelper.encodeToken(p);
        assertTrue(XContentDataHelper.isEncodedObject(encoded));
        XContentDataHelper.decodeAndWrite(builder, encoded);
        assertEquals(object, Strings.toString(builder));

        XContentBuilder builder2 = XContentFactory.jsonBuilder();
        builder2.humanReadable(true);
        XContentDataHelper.decodeAndWrite(builder2, XContentDataHelper.encodeXContentBuilder(builder));
        assertEquals(object, Strings.toString(builder2));
    }

    public void testObjectWithFilter() throws IOException {
        String object = "{\"name\":\"foo\",\"path\":{\"filter\":{\"keep\":[0],\"field\":\"value\"}}}";
        String filterObject = "{\"name\":\"foo\",\"path\":{\"filter\":{\"keep\":[0]}}}";

        XContentParser p = createParser(JsonXContent.jsonXContent, object);
        assertThat(p.nextToken(), equalTo(XContentParser.Token.START_OBJECT));
        XContentParserConfiguration parserConfig = XContentParserConfiguration.EMPTY.withFiltering(
            null,
            null,
            Set.of("path.filter.field"),
            true
        );
        XContentBuilder builder = XContentFactory.jsonBuilder();
        builder.humanReadable(true);
        XContentDataHelper.decodeAndWriteXContent(parserConfig, builder, XContentType.JSON, XContentDataHelper.encodeToken(p));
        assertEquals(filterObject, Strings.toString(builder));

        XContentBuilder builder2 = XContentFactory.jsonBuilder();
        builder2.humanReadable(true);
        XContentDataHelper.decodeAndWriteXContent(
            parserConfig,
            builder2,
            XContentType.JSON,
            XContentDataHelper.encodeXContentBuilder(builder)
        );
        assertEquals(filterObject, Strings.toString(builder2));
    }

    public void testObjectWithFilterRootPath() throws IOException {
        String object = "{\"name\":\"foo\",\"path\":{\"filter\":{\"keep\":[0],\"field\":\"value\"}}}";
        String filterObject = "{\"path\":{\"filter\":{\"keep\":[0]}}}";

        XContentParser p = createParser(JsonXContent.jsonXContent, object);
        assertThat(p.nextToken(), equalTo(XContentParser.Token.START_OBJECT));
        XContentParserConfiguration parserConfig = XContentParserConfiguration.EMPTY.withFiltering(
            "root.obj.sub_obj",
            Set.of("root.obj.sub_obj.path"),
            Set.of("root.obj.sub_obj.path.filter.field"),
            true
        );
        XContentBuilder builder = XContentFactory.jsonBuilder();
        builder.humanReadable(true);
        XContentDataHelper.decodeAndWriteXContent(parserConfig, builder, XContentType.JSON, XContentDataHelper.encodeToken(p));
        assertEquals(filterObject, Strings.toString(builder));

        XContentBuilder builder2 = XContentFactory.jsonBuilder();
        builder2.humanReadable(true);
        XContentDataHelper.decodeAndWriteXContent(
            parserConfig,
            builder2,
            XContentType.JSON,
            XContentDataHelper.encodeXContentBuilder(builder)
        );
        assertEquals(filterObject, Strings.toString(builder2));
    }

    public void testArrayInt() throws IOException {
        String values = "["
            + String.join(",", List.of(Integer.toString(randomInt()), Integer.toString(randomInt()), Integer.toString(randomInt())))
            + "]";
        assertEquals("\"" + values + "\"", encodeAndDecode(values));
    }

    public void testCloneSubContextWithParser() throws IOException {
        String data = """
            { "key1": "value1", "key2": "value2", "path": { "to": { "key3": "value3" }} }""".replace(" ", "");
        XContentParser xContentParser = createParser(JsonXContent.jsonXContent, data);
        xContentParser.nextToken();
        TestDocumentParserContext context = new TestDocumentParserContext(xContentParser);
        assertFalse(context.getRecordedSource());
        var tuple = XContentDataHelper.cloneSubContextWithParser(context);
        assertEquals(data, dataInParser(tuple.v1().parser()));
        assertEquals(data, dataInParser(tuple.v2()));
        assertTrue(tuple.v1().getRecordedSource());
        assertFalse(context.getRecordedSource());
    }

    public void testWriteMergedWithSingleValue() throws IOException {
        testWriteMergedWithSingleValue(randomLong());
        testWriteMergedWithSingleValue(randomDouble());
        testWriteMergedWithSingleValue(randomBoolean());
        testWriteMergedWithSingleValue(randomAlphaOfLength(5));
        testWriteMergedWithSingleValue(null);
        testWriteMergedWithSingleValue(Map.of("object_field", randomAlphaOfLength(5)));
        testWriteMergedWithSingleValue(Map.of("object_field", Map.of("nested_object_field", randomAlphaOfLength(5))));
    }

    private void testWriteMergedWithSingleValue(Object value) throws IOException {
        var map = executeWriteMergedOnRepeated(value);
        assertEquals(Arrays.asList(value, value), map.get("foo"));
    }

    public void testWriteMergedWithMultipleValues() throws IOException {
        testWriteMergedWithMultipleValues(List.of(randomLong(), randomLong()));
        testWriteMergedWithMultipleValues(List.of(randomDouble(), randomDouble()));
        testWriteMergedWithMultipleValues(List.of(randomBoolean(), randomBoolean()));
        testWriteMergedWithMultipleValues(List.of(randomAlphaOfLength(5), randomAlphaOfLength(5)));
        testWriteMergedWithMultipleValues(Arrays.asList(null, null));
        testWriteMergedWithMultipleValues(
            List.of(Map.of("object_field", randomAlphaOfLength(5)), Map.of("object_field", randomAlphaOfLength(5)))
        );
        testWriteMergedWithMultipleValues(
            List.of(
                Map.of("object_field", Map.of("nested_object_field", randomAlphaOfLength(5))),
                Map.of("object_field", Map.of("nested_object_field", randomAlphaOfLength(5)))
            )
        );
    }

    private void testWriteMergedWithMultipleValues(List<Object> value) throws IOException {
        var map = executeWriteMergedOnRepeated(value);
        var expected = Stream.of(value, value).flatMap(Collection::stream).toList();
        assertEquals(expected, map.get("foo"));
    }

    public void testWriteMergedWithMixedValues() throws IOException {
        testWriteMergedWithMixedValues(randomLong(), List.of(randomLong(), randomLong()));
        testWriteMergedWithMixedValues(randomDouble(), List.of(randomDouble(), randomDouble()));
        testWriteMergedWithMixedValues(randomBoolean(), List.of(randomBoolean(), randomBoolean()));
        testWriteMergedWithMixedValues(randomAlphaOfLength(5), List.of(randomAlphaOfLength(5), randomAlphaOfLength(5)));
        testWriteMergedWithMixedValues(null, Arrays.asList(null, null));
        testWriteMergedWithMixedValues(
            Map.of("object_field", randomAlphaOfLength(5)),
            List.of(Map.of("object_field", randomAlphaOfLength(5)), Map.of("object_field", randomAlphaOfLength(5)))
        );
        testWriteMergedWithMixedValues(
            Map.of("object_field", Map.of("nested_object_field", randomAlphaOfLength(5))),
            List.of(
                Map.of("object_field", Map.of("nested_object_field", randomAlphaOfLength(5))),
                Map.of("object_field", Map.of("nested_object_field", randomAlphaOfLength(5)))
            )
        );
    }

    private void testWriteMergedWithMixedValues(Object value, List<Object> multipleValues) throws IOException {
        var map = executeWriteMergedOnTwoEncodedValues(value, multipleValues);
        var expected = Stream.concat(Stream.of(value), multipleValues.stream()).toList();
        assertEquals(expected, map.get("foo"));
    }

    private Map<String, Object> executeWriteMergedOnRepeated(Object value) throws IOException {
        return executeWriteMergedOnTwoEncodedValues(value, value);
    }

    private Map<String, Object> executeWriteMergedOnTwoEncodedValues(Object first, Object second) throws IOException {
        var xContentType = randomFrom(XContentType.values());

        var firstEncoded = encodeSingleValue(first, xContentType);
        var secondEncoded = encodeSingleValue(second, xContentType);

        var destination = XContentFactory.contentBuilder(xContentType);
        destination.startObject();
        XContentDataHelper.writeMerged(XContentParserConfiguration.EMPTY, destination, "foo", List.of(firstEncoded, secondEncoded));
        destination.endObject();

        return XContentHelper.convertToMap(BytesReference.bytes(destination), false, xContentType).v2();
    }

    private BytesRef encodeSingleValue(Object value, XContentType xContentType) throws IOException {
        var builder = XContentFactory.contentBuilder(xContentType);
        builder.value(value);

        XContentParser parser = createParser(builder);
        parser.nextToken();
        return XContentDataHelper.encodeToken(parser);
    }
}
