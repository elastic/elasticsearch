/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.index.mapper;

import org.apache.lucene.document.StoredField;
import org.elasticsearch.common.bytes.BytesReference;
import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.xcontent.XContentBuilder;
import org.elasticsearch.xcontent.XContentParser;
import org.elasticsearch.xcontent.XContentParserConfiguration;
import org.elasticsearch.xcontent.cbor.CborXContent;

import java.io.IOException;
import java.math.BigDecimal;
import java.math.BigInteger;
import java.util.List;
import java.util.stream.Stream;

import static org.hamcrest.Matchers.equalTo;

public class IgnoreMalformedStoredValuesTests extends ESTestCase {
    public void testIgnoreMalformedBoolean() throws IOException {
        boolean b = randomBoolean();
        XContentParser p = ignoreMalformed(b);
        assertThat(p.currentToken(), equalTo(XContentParser.Token.VALUE_BOOLEAN));
        assertThat(p.booleanValue(), equalTo(b));
    }

    public void testIgnoreMalformedString() throws IOException {
        String s = randomAlphaOfLength(5);
        XContentParser p = ignoreMalformed(s);
        assertThat(p.currentToken(), equalTo(XContentParser.Token.VALUE_STRING));
        assertThat(p.text(), equalTo(s));
    }

    public void testIgnoreMalformedInt() throws IOException {
        int i = randomInt();
        XContentParser p = ignoreMalformed(i);
        assertThat(p.currentToken(), equalTo(XContentParser.Token.VALUE_NUMBER));
        assertThat(p.numberType(), equalTo(XContentParser.NumberType.INT));
        assertThat(p.intValue(), equalTo(i));
    }

    public void testIgnoreMalformedLong() throws IOException {
        long l = randomLong();
        XContentParser p = ignoreMalformed(l);
        assertThat(p.currentToken(), equalTo(XContentParser.Token.VALUE_NUMBER));
        assertThat(p.numberType(), equalTo(XContentParser.NumberType.LONG));
        assertThat(p.longValue(), equalTo(l));
    }

    public void testIgnoreMalformedFloat() throws IOException {
        float f = randomFloat();
        XContentParser p = ignoreMalformed(f);
        assertThat(p.currentToken(), equalTo(XContentParser.Token.VALUE_NUMBER));
        assertThat(p.numberType(), equalTo(XContentParser.NumberType.FLOAT));
        assertThat(p.floatValue(), equalTo(f));
    }

    public void testIgnoreMalformedDouble() throws IOException {
        double d = randomDouble();
        XContentParser p = ignoreMalformed(d);
        assertThat(p.currentToken(), equalTo(XContentParser.Token.VALUE_NUMBER));
        assertThat(p.numberType(), equalTo(XContentParser.NumberType.DOUBLE));
        assertThat(p.doubleValue(), equalTo(d));
    }

    public void testIgnoreMalformedBigInteger() throws IOException {
        BigInteger i = randomBigInteger();
        XContentParser p = ignoreMalformed(i);
        assertThat(p.currentToken(), equalTo(XContentParser.Token.VALUE_NUMBER));
        assertThat(p.numberType(), equalTo(XContentParser.NumberType.BIG_INTEGER));
        assertThat(p.numberValue(), equalTo(i));
    }

    public void testIgnoreMalformedBigDecimal() throws IOException {
        BigDecimal d = new BigDecimal(randomBigInteger(), randomInt());
        XContentParser p = ignoreMalformed(d);
        assertThat(p.currentToken(), equalTo(XContentParser.Token.VALUE_NUMBER));
        assertThat(p.numberType(), equalTo(XContentParser.NumberType.BIG_DECIMAL));
        assertThat(p.numberValue(), equalTo(d));
    }

    public void testIgnoreMalformedBytes() throws IOException {
        byte[] b = randomByteArrayOfLength(10);
        XContentParser p = ignoreMalformed(b);
        assertThat(p.currentToken(), equalTo(XContentParser.Token.VALUE_EMBEDDED_OBJECT));
        assertThat(p.binaryValue(), equalTo(b));
    }

    private static XContentParser ignoreMalformed(Object value) throws IOException {
        String fieldName = randomAlphaOfLength(10);
        StoredField s = ignoreMalformedStoredField(value);
        Object stored = Stream.of(s.numericValue(), s.binaryValue(), s.stringValue()).filter(v -> v != null).findFirst().get();
        IgnoreMalformedStoredValues values = IgnoreMalformedStoredValues.stored(fieldName);
        values.storedFieldLoaders().forEach(e -> e.getValue().load(List.of(stored)));
        return parserFrom(values, fieldName);
    }

    private static StoredField ignoreMalformedStoredField(Object value) throws IOException {
        XContentBuilder b = CborXContent.contentBuilder();
        b.startObject().field("name", value).endObject();
        XContentParser p = CborXContent.cborXContent.createParser(XContentParserConfiguration.EMPTY, BytesReference.bytes(b).streamInput());
        assertThat(p.nextToken(), equalTo(XContentParser.Token.START_OBJECT));
        assertThat(p.nextToken(), equalTo(XContentParser.Token.FIELD_NAME));
        assertThat(p.currentName(), equalTo("name"));
        assertThat(p.nextToken().isValue(), equalTo(true));

        return IgnoreMalformedStoredValues.storedField("foo.name", p);
    }

    private static XContentParser parserFrom(IgnoreMalformedStoredValues values, String fieldName) throws IOException {
        XContentBuilder b = CborXContent.contentBuilder();
        b.startObject();
        b.field(fieldName);
        values.write(b);
        b.endObject();
        XContentParser p = CborXContent.cborXContent.createParser(XContentParserConfiguration.EMPTY, BytesReference.bytes(b).streamInput());
        assertThat(p.nextToken(), equalTo(XContentParser.Token.START_OBJECT));
        assertThat(p.nextToken(), equalTo(XContentParser.Token.FIELD_NAME));
        assertThat(p.currentName(), equalTo(fieldName));
        assertThat(p.nextToken().isValue(), equalTo(true));
        return p;
    }
}
