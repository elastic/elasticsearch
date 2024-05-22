/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.index.mapper;

import org.elasticsearch.common.Strings;
import org.elasticsearch.common.bytes.BytesReference;
import org.elasticsearch.common.compress.CompressedXContent;
import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.xcontent.XContentBuilder;
import org.elasticsearch.xcontent.XContentFactory;
import org.elasticsearch.xcontent.XContentParser;
import org.elasticsearch.xcontent.XContentType;
import org.elasticsearch.xcontent.json.JsonXContent;

import java.io.IOException;
import java.math.BigDecimal;
import java.math.BigInteger;
import java.util.Base64;
import java.util.List;

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

            var decoded = XContentFactory.jsonBuilder();
            XContentDataHelper.decodeAndWrite(decoded, encoded);

            assertEquals("\"" + Base64.getEncoder().encodeToString(embedded.compressed()) + "\"", Strings.toString(decoded));
        }

        var encoded = XContentDataHelper.encodeXContentBuilder(builder);

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
        XContentDataHelper.decodeAndWrite(builder, XContentDataHelper.encodeToken(p));
        assertEquals(object, Strings.toString(builder));

        XContentBuilder builder2 = XContentFactory.jsonBuilder();
        builder2.humanReadable(true);
        XContentDataHelper.decodeAndWrite(builder2, XContentDataHelper.encodeXContentBuilder(builder));
        assertEquals(object, Strings.toString(builder2));
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
        assertFalse(context.getClonedSource());
        var tuple = XContentDataHelper.cloneSubContextWithParser(context);
        assertEquals(data, dataInParser(tuple.v1().parser()));
        assertEquals(data, dataInParser(tuple.v2()));
        assertTrue(tuple.v1().getClonedSource());
    }
}
