/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.index.mapper;

import org.apache.lucene.document.StoredField;
import org.apache.lucene.index.DirectoryReader;
import org.apache.lucene.index.LeafReader;
import org.apache.lucene.store.Directory;
import org.apache.lucene.tests.index.RandomIndexWriter;
import org.apache.lucene.util.BytesRef;
import org.elasticsearch.common.bytes.BytesReference;
import org.elasticsearch.index.IndexVersion;
import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.xcontent.XContentBuilder;
import org.elasticsearch.xcontent.XContentParser;
import org.elasticsearch.xcontent.XContentParserConfiguration;
import org.elasticsearch.xcontent.XContentType;
import org.elasticsearch.xcontent.cbor.CborXContent;

import java.io.IOException;
import java.math.BigDecimal;
import java.math.BigInteger;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.stream.Stream;

import static org.hamcrest.Matchers.equalTo;

public class IgnoreMalformedStoredValuesTests extends ESTestCase {
    public void testIgnoreMalformedBoolean() throws IOException {
        boolean b = randomBoolean();
        XContentParser p = ignoreMalformedFromStoredField(randomFrom(XContentType.values()), b);
        assertThat(p.nextToken(), equalTo(XContentParser.Token.VALUE_BOOLEAN));
        assertThat(p.booleanValue(), equalTo(b));
        assertThat(p.nextToken(), equalTo(XContentParser.Token.END_OBJECT));
    }

    public void testIgnoreMalformedString() throws IOException {
        String s = randomAlphaOfLength(5);
        XContentParser p = ignoreMalformedFromStoredField(randomFrom(XContentType.values()), s);
        assertThat(p.nextToken(), equalTo(XContentParser.Token.VALUE_STRING));
        assertThat(p.text(), equalTo(s));
        assertThat(p.nextToken(), equalTo(XContentParser.Token.END_OBJECT));
    }

    public void testIgnoreMalformedInt() throws IOException {
        int i = randomInt();
        XContentParser p = ignoreMalformedFromStoredField(randomFrom(XContentType.values()), i);
        assertThat(p.nextToken(), equalTo(XContentParser.Token.VALUE_NUMBER));
        assertThat(p.numberType(), equalTo(XContentParser.NumberType.INT));
        assertThat(p.intValue(), equalTo(i));
        assertThat(p.nextToken(), equalTo(XContentParser.Token.END_OBJECT));
    }

    public void testIgnoreMalformedLong() throws IOException {
        long l = randomLong();
        XContentParser p = ignoreMalformedFromStoredField(randomFrom(XContentType.values()), l);
        assertThat(p.nextToken(), equalTo(XContentParser.Token.VALUE_NUMBER));
        assertThat(p.numberType(), equalTo(XContentParser.NumberType.LONG));
        assertThat(p.longValue(), equalTo(l));
        assertThat(p.nextToken(), equalTo(XContentParser.Token.END_OBJECT));
    }

    public void testIgnoreMalformedFloat() throws IOException {
        float f = randomFloat();
        XContentParser p = ignoreMalformedFromStoredField(randomFrom(XContentType.SMILE, XContentType.CBOR), f);
        assertThat(p.nextToken(), equalTo(XContentParser.Token.VALUE_NUMBER));
        assertThat(p.numberType(), equalTo(XContentParser.NumberType.FLOAT));
        assertThat(p.floatValue(), equalTo(f));
        assertThat(p.nextToken(), equalTo(XContentParser.Token.END_OBJECT));
    }

    public void testIgnoreMalformedDouble() throws IOException {
        double d = randomDouble();
        XContentParser p = ignoreMalformedFromStoredField(randomFrom(XContentType.values()), d);
        assertThat(p.nextToken(), equalTo(XContentParser.Token.VALUE_NUMBER));
        assertThat(p.numberType(), equalTo(XContentParser.NumberType.DOUBLE));
        assertThat(p.doubleValue(), equalTo(d));
        assertThat(p.nextToken(), equalTo(XContentParser.Token.END_OBJECT));
    }

    public void testIgnoreMalformedBigInteger() throws IOException {
        BigInteger i = randomBigInteger();
        XContentParser p = ignoreMalformedFromStoredField(randomFrom(XContentType.SMILE, XContentType.CBOR), i);
        assertThat(p.nextToken(), equalTo(XContentParser.Token.VALUE_NUMBER));
        assertThat(p.numberType(), equalTo(XContentParser.NumberType.BIG_INTEGER));
        assertThat(p.numberValue(), equalTo(i));
        assertThat(p.nextToken(), equalTo(XContentParser.Token.END_OBJECT));
    }

    public void testIgnoreMalformedBigDecimal() throws IOException {
        BigDecimal d = new BigDecimal(randomBigInteger(), randomInt());
        XContentParser p = ignoreMalformedFromStoredField(XContentType.CBOR, d);
        assertThat(p.nextToken(), equalTo(XContentParser.Token.VALUE_NUMBER));
        assertThat(p.numberType(), equalTo(XContentParser.NumberType.BIG_DECIMAL));
        assertThat(p.numberValue(), equalTo(d));
        assertThat(p.nextToken(), equalTo(XContentParser.Token.END_OBJECT));
    }

    public void testIgnoreMalformedBytes() throws IOException {
        byte[] b = randomByteArrayOfLength(10);
        XContentParser p = ignoreMalformedFromStoredField(randomFrom(XContentType.SMILE, XContentType.CBOR), b);
        assertThat(p.nextToken(), equalTo(XContentParser.Token.VALUE_EMBEDDED_OBJECT));
        assertThat(p.binaryValue(), equalTo(b));
        assertThat(p.nextToken(), equalTo(XContentParser.Token.END_OBJECT));
    }

    public void testIgnoreMalformedObjectBoolean() throws IOException {
        boolean b = randomBoolean();
        XContentParser p = ignoreMalformedFromStoredField(randomFrom(XContentType.values()), Map.of("foo", b));
        assertThat(p.nextToken(), equalTo(XContentParser.Token.START_OBJECT));
        assertThat(p.nextToken(), equalTo(XContentParser.Token.FIELD_NAME));
        assertThat(p.currentName(), equalTo("foo"));
        assertThat(p.nextToken(), equalTo(XContentParser.Token.VALUE_BOOLEAN));
        assertThat(p.booleanValue(), equalTo(b));
        assertThat(p.nextToken(), equalTo(XContentParser.Token.END_OBJECT));
        assertThat(p.nextToken(), equalTo(XContentParser.Token.END_OBJECT));
    }

    public void testIgnoreMalformedArrayInt() throws IOException {
        int i1 = randomInt();
        int i2 = randomInt();
        int i3 = randomInt();
        XContentParser p = ignoreMalformedFromStoredField(randomFrom(XContentType.values()), List.of(i1, i2, i3));
        assertThat(p.nextToken(), equalTo(XContentParser.Token.START_ARRAY));
        assertThat(p.nextToken(), equalTo(XContentParser.Token.VALUE_NUMBER));
        assertThat(p.numberType(), equalTo(XContentParser.NumberType.INT));
        assertThat(p.intValue(), equalTo(i1));
        assertThat(p.nextToken(), equalTo(XContentParser.Token.VALUE_NUMBER));
        assertThat(p.numberType(), equalTo(XContentParser.NumberType.INT));
        assertThat(p.intValue(), equalTo(i2));
        assertThat(p.nextToken(), equalTo(XContentParser.Token.VALUE_NUMBER));
        assertThat(p.numberType(), equalTo(XContentParser.NumberType.INT));
        assertThat(p.intValue(), equalTo(i3));
        assertThat(p.nextToken(), equalTo(XContentParser.Token.END_ARRAY));
        assertThat(p.nextToken(), equalTo(XContentParser.Token.END_OBJECT));
    }

    public void testDocValuesRoundTripString() throws IOException {
        String s = randomAlphaOfLength(5);
        XContentParser p = ignoreMalformedFromDocValues(randomFrom(XContentType.values()), s);
        assertThat(p.nextToken(), equalTo(XContentParser.Token.VALUE_STRING));
        assertThat(p.text(), equalTo(s));
        assertThat(p.nextToken(), equalTo(XContentParser.Token.END_OBJECT));
    }

    public void testDocValuesRoundTripInt() throws IOException {
        int i = randomInt();
        XContentParser p = ignoreMalformedFromDocValues(randomFrom(XContentType.values()), i);
        assertThat(p.nextToken(), equalTo(XContentParser.Token.VALUE_NUMBER));
        assertThat(p.numberType(), equalTo(XContentParser.NumberType.INT));
        assertThat(p.intValue(), equalTo(i));
        assertThat(p.nextToken(), equalTo(XContentParser.Token.END_OBJECT));
    }

    public void testDocValuesRoundTripBoolean() throws IOException {
        boolean b = randomBoolean();
        XContentParser p = ignoreMalformedFromDocValues(randomFrom(XContentType.values()), b);
        assertThat(p.nextToken(), equalTo(XContentParser.Token.VALUE_BOOLEAN));
        assertThat(p.booleanValue(), equalTo(b));
        assertThat(p.nextToken(), equalTo(XContentParser.Token.END_OBJECT));
    }

    public void testDocValuesRoundTripDouble() throws IOException {
        double d = randomDouble();
        XContentParser p = ignoreMalformedFromDocValues(randomFrom(XContentType.values()), d);
        assertThat(p.nextToken(), equalTo(XContentParser.Token.VALUE_NUMBER));
        assertThat(p.numberType(), equalTo(XContentParser.NumberType.DOUBLE));
        assertThat(p.doubleValue(), equalTo(d));
        assertThat(p.nextToken(), equalTo(XContentParser.Token.END_OBJECT));
    }

    public void testDocValuesRoundTripLong() throws IOException {
        long l = randomLong();
        XContentParser p = ignoreMalformedFromDocValues(randomFrom(XContentType.values()), l);
        assertThat(p.nextToken(), equalTo(XContentParser.Token.VALUE_NUMBER));
        assertThat(p.numberType(), equalTo(XContentParser.NumberType.LONG));
        assertThat(p.longValue(), equalTo(l));
        assertThat(p.nextToken(), equalTo(XContentParser.Token.END_OBJECT));
    }

    public void testDocValuesRoundTripFloat() throws IOException {
        float f = randomFloat();
        XContentParser p = ignoreMalformedFromDocValues(randomFrom(XContentType.SMILE, XContentType.CBOR), f);
        assertThat(p.nextToken(), equalTo(XContentParser.Token.VALUE_NUMBER));
        assertThat(p.numberType(), equalTo(XContentParser.NumberType.FLOAT));
        assertThat(p.floatValue(), equalTo(f));
        assertThat(p.nextToken(), equalTo(XContentParser.Token.END_OBJECT));
    }

    public void testDocValuesRoundTripBigInteger() throws IOException {
        BigInteger i = randomBigInteger();
        XContentParser p = ignoreMalformedFromDocValues(randomFrom(XContentType.SMILE, XContentType.CBOR), i);
        assertThat(p.nextToken(), equalTo(XContentParser.Token.VALUE_NUMBER));
        assertThat(p.numberType(), equalTo(XContentParser.NumberType.BIG_INTEGER));
        assertThat(p.numberValue(), equalTo(i));
        assertThat(p.nextToken(), equalTo(XContentParser.Token.END_OBJECT));
    }

    public void testDocValuesRoundTripBigDecimal() throws IOException {
        BigDecimal d = new BigDecimal(randomBigInteger(), randomInt());
        XContentParser p = ignoreMalformedFromDocValues(XContentType.CBOR, d);
        assertThat(p.nextToken(), equalTo(XContentParser.Token.VALUE_NUMBER));
        assertThat(p.numberType(), equalTo(XContentParser.NumberType.BIG_DECIMAL));
        assertThat(p.numberValue(), equalTo(d));
        assertThat(p.nextToken(), equalTo(XContentParser.Token.END_OBJECT));
    }

    public void testDocValuesRoundTripBytes() throws IOException {
        byte[] b = randomByteArrayOfLength(10);
        XContentParser p = ignoreMalformedFromDocValues(randomFrom(XContentType.SMILE, XContentType.CBOR), b);
        assertThat(p.nextToken(), equalTo(XContentParser.Token.VALUE_EMBEDDED_OBJECT));
        assertThat(p.binaryValue(), equalTo(b));
        assertThat(p.nextToken(), equalTo(XContentParser.Token.END_OBJECT));
    }

    public void testDocValuesRoundTripObject() throws IOException {
        boolean b = randomBoolean();
        XContentParser p = ignoreMalformedFromDocValues(randomFrom(XContentType.values()), Map.of("foo", b));
        assertThat(p.nextToken(), equalTo(XContentParser.Token.START_OBJECT));
        assertThat(p.nextToken(), equalTo(XContentParser.Token.FIELD_NAME));
        assertThat(p.currentName(), equalTo("foo"));
        assertThat(p.nextToken(), equalTo(XContentParser.Token.VALUE_BOOLEAN));
        assertThat(p.booleanValue(), equalTo(b));
        assertThat(p.nextToken(), equalTo(XContentParser.Token.END_OBJECT));
        assertThat(p.nextToken(), equalTo(XContentParser.Token.END_OBJECT));
    }

    public void testDocValuesRoundTripArray() throws IOException {
        int i1 = randomInt();
        int i2 = randomInt();
        int i3 = randomInt();
        XContentParser p = ignoreMalformedFromDocValues(randomFrom(XContentType.values()), List.of(i1, i2, i3));
        assertThat(p.nextToken(), equalTo(XContentParser.Token.START_ARRAY));
        assertThat(p.nextToken(), equalTo(XContentParser.Token.VALUE_NUMBER));
        assertThat(p.numberType(), equalTo(XContentParser.NumberType.INT));
        assertThat(p.intValue(), equalTo(i1));
        assertThat(p.nextToken(), equalTo(XContentParser.Token.VALUE_NUMBER));
        assertThat(p.numberType(), equalTo(XContentParser.NumberType.INT));
        assertThat(p.intValue(), equalTo(i2));
        assertThat(p.nextToken(), equalTo(XContentParser.Token.VALUE_NUMBER));
        assertThat(p.numberType(), equalTo(XContentParser.NumberType.INT));
        assertThat(p.intValue(), equalTo(i3));
        assertThat(p.nextToken(), equalTo(XContentParser.Token.END_ARRAY));
        assertThat(p.nextToken(), equalTo(XContentParser.Token.END_OBJECT));
    }

    public void testDocValuesRoundTripMultipleValues() throws IOException {
        String s1 = randomAlphaOfLength(5);
        String s2 = randomAlphaOfLength(5);
        String fieldName = "test_field";
        String dvFieldName = IgnoreMalformedStoredValues.name(fieldName);

        XContentType type = randomFrom(XContentType.values());
        BytesRef encoded1 = encodeValue(type, s1);
        BytesRef encoded2 = encodeValue(type, s2);

        try (Directory directory = newDirectory()) {
            try (RandomIndexWriter iw = new RandomIndexWriter(random(), directory)) {
                LuceneDocument doc = new LuceneDocument();
                MultiValuedBinaryDocValuesField.SeparateCount.addToSeparateCountMultiBinaryFieldInDoc(doc, dvFieldName, encoded1, true);
                MultiValuedBinaryDocValuesField.SeparateCount.addToSeparateCountMultiBinaryFieldInDoc(doc, dvFieldName, encoded2, true);
                iw.addDocument(doc);
            }

            try (DirectoryReader reader = DirectoryReader.open(directory)) {
                LeafReader leafReader = reader.leaves().get(0).reader();
                IgnoreMalformedStoredValues values = IgnoreMalformedStoredValues.forSyntheticSource(fieldName, IndexVersion.current());
                SourceLoader.SyntheticFieldLoader.DocValuesLoader loader = values.docValuesLoader(leafReader);
                assertNotNull(loader);
                assertTrue(loader.advanceToDoc(0));
                assertThat(values.count(), equalTo(2));

                XContentBuilder b = CborXContent.contentBuilder();
                b.startObject();
                b.field(fieldName);
                b.startArray();
                values.write(b);
                b.endArray();
                b.endObject();

                XContentParser p = CborXContent.cborXContent.createParser(
                    XContentParserConfiguration.EMPTY,
                    BytesReference.bytes(b).streamInput()
                );
                assertThat(p.nextToken(), equalTo(XContentParser.Token.START_OBJECT));
                assertThat(p.nextToken(), equalTo(XContentParser.Token.FIELD_NAME));
                assertThat(p.currentName(), equalTo(fieldName));
                assertThat(p.nextToken(), equalTo(XContentParser.Token.START_ARRAY));
                assertThat(p.nextToken(), equalTo(XContentParser.Token.VALUE_STRING));
                String v1 = p.text();
                assertThat(p.nextToken(), equalTo(XContentParser.Token.VALUE_STRING));
                String v2 = p.text();
                assertThat(Set.of(v1, v2), equalTo(Set.of(s1, s2)));
                assertThat(p.nextToken(), equalTo(XContentParser.Token.END_ARRAY));
            }
        }
    }

    public void testDocValuesNoValues() throws IOException {
        String fieldName = "test_field";

        try (Directory directory = newDirectory()) {
            try (RandomIndexWriter iw = new RandomIndexWriter(random(), directory)) {
                iw.addDocument(new LuceneDocument());
            }

            try (DirectoryReader reader = DirectoryReader.open(directory)) {
                LeafReader leafReader = reader.leaves().get(0).reader();
                IgnoreMalformedStoredValues values = IgnoreMalformedStoredValues.forSyntheticSource(fieldName, IndexVersion.current());
                SourceLoader.SyntheticFieldLoader.DocValuesLoader loader = values.docValuesLoader(leafReader);
                assertNull(loader);
                assertThat(values.count(), equalTo(0));
            }
        }
    }

    private static XContentParser ignoreMalformedFromStoredField(XContentType type, Object value) throws IOException {
        String fieldName = randomAlphaOfLength(10);
        StoredField s = ignoreMalformedStoredField(type, value);
        Object stored = Stream.of(s.numericValue(), s.binaryValue(), s.stringValue()).filter(v -> v != null).findFirst().get();
        IgnoreMalformedStoredValues values = IgnoreMalformedStoredValues.stored(fieldName);
        values.storedFieldLoaders().forEach(e -> e.getValue().load(List.of(stored)));
        return parserFrom(values, fieldName);
    }

    private static StoredField ignoreMalformedStoredField(XContentType type, Object value) throws IOException {
        XContentBuilder b = XContentBuilder.builder(type.xContent());
        b.startObject().field("name", value).endObject();
        try (XContentParser p = type.xContent().createParser(XContentParserConfiguration.EMPTY, BytesReference.bytes(b).streamInput())) {
            assertThat(p.nextToken(), equalTo(XContentParser.Token.START_OBJECT));
            assertThat(p.nextToken(), equalTo(XContentParser.Token.FIELD_NAME));
            assertThat(p.currentName(), equalTo("name"));
            p.nextToken();
            return IgnoreMalformedStoredValues.storedField("foo.name", p);
        }
    }

    private XContentParser ignoreMalformedFromDocValues(XContentType type, Object value) throws IOException {
        String fieldName = randomAlphaOfLength(10);
        String dvFieldName = IgnoreMalformedStoredValues.name(fieldName);
        BytesRef encoded = encodeValue(type, value);

        XContentParser result;
        try (Directory directory = newDirectory()) {
            try (RandomIndexWriter iw = new RandomIndexWriter(random(), directory)) {
                LuceneDocument doc = new LuceneDocument();
                MultiValuedBinaryDocValuesField.SeparateCount.addToSeparateCountMultiBinaryFieldInDoc(doc, dvFieldName, encoded, true);
                iw.addDocument(doc);
            }

            try (DirectoryReader reader = DirectoryReader.open(directory)) {
                LeafReader leafReader = reader.leaves().get(0).reader();
                IgnoreMalformedStoredValues values = IgnoreMalformedStoredValues.forSyntheticSource(fieldName, IndexVersion.current());
                SourceLoader.SyntheticFieldLoader.DocValuesLoader loader = values.docValuesLoader(leafReader);
                assertNotNull(loader);
                assertTrue(loader.advanceToDoc(0));
                assertThat(values.count(), equalTo(1));

                result = parserFrom(values, fieldName);
            }
        }
        return result;
    }

    private static BytesRef encodeValue(XContentType type, Object value) throws IOException {
        XContentBuilder b = XContentBuilder.builder(type.xContent());
        b.startObject().field("name", value).endObject();
        try (XContentParser p = type.xContent().createParser(XContentParserConfiguration.EMPTY, BytesReference.bytes(b).streamInput())) {
            assertThat(p.nextToken(), equalTo(XContentParser.Token.START_OBJECT));
            assertThat(p.nextToken(), equalTo(XContentParser.Token.FIELD_NAME));
            assertThat(p.currentName(), equalTo("name"));
            p.nextToken();
            return XContentDataHelper.encodeToken(p);
        }
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
        return p;
    }
}
