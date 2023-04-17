/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.common.xcontent.builder;

import org.apache.lucene.util.BytesRef;
import org.elasticsearch.common.Strings;
import org.elasticsearch.common.bytes.BytesArray;
import org.elasticsearch.common.bytes.BytesReference;
import org.elasticsearch.common.geo.GeoPoint;
import org.elasticsearch.common.io.stream.BytesStreamOutput;
import org.elasticsearch.common.util.Maps;
import org.elasticsearch.common.xcontent.XContentElasticsearchExtension;
import org.elasticsearch.common.xcontent.XContentHelper;
import org.elasticsearch.core.PathUtils;
import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.xcontent.ToXContent;
import org.elasticsearch.xcontent.XContentBuilder;
import org.elasticsearch.xcontent.XContentFactory;
import org.elasticsearch.xcontent.XContentGenerator;
import org.elasticsearch.xcontent.XContentParser;
import org.elasticsearch.xcontent.XContentType;
import org.elasticsearch.xcontent.json.JsonXContent;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.nio.charset.StandardCharsets;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Base64;
import java.util.Calendar;
import java.util.Collections;
import java.util.Date;
import java.util.GregorianCalendar;
import java.util.HashMap;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Objects;
import java.util.Set;
import java.util.TimeZone;

import static org.hamcrest.Matchers.aMapWithSize;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.hasEntry;
import static org.hamcrest.Matchers.instanceOf;

public class XContentBuilderTests extends ESTestCase {
    public void testPrettyWithLfAtEnd() throws Exception {
        ByteArrayOutputStream os = new ByteArrayOutputStream();
        XContentGenerator generator = XContentFactory.xContent(XContentType.JSON).createGenerator(os);
        generator.usePrettyPrint();
        generator.usePrintLineFeedAtEnd();

        generator.writeStartObject();
        generator.writeStringField("test", "value");
        generator.writeEndObject();
        generator.flush();

        generator.close();
        // double close, and check there is no error...
        generator.close();

        byte[] bytes = os.toByteArray();
        assertThat((char) bytes[bytes.length - 1], equalTo('\n'));
    }

    public void testReuseJsonGenerator() throws Exception {
        ByteArrayOutputStream os = new ByteArrayOutputStream();
        XContentGenerator generator = XContentFactory.xContent(XContentType.JSON).createGenerator(os);
        generator.writeStartObject();
        generator.writeStringField("test", "value");
        generator.writeEndObject();
        generator.flush();

        assertThat(new BytesRef(os.toByteArray()), equalTo(new BytesRef("{\"test\":\"value\"}")));

        // try again...
        os.reset();
        generator.writeStartObject();
        generator.writeStringField("test", "value");
        generator.writeEndObject();
        generator.flush();
        // we get a space at the start here since it thinks we are not in the root object (fine, we will ignore it in the real code we use)
        assertThat(new BytesRef(os.toByteArray()), equalTo(new BytesRef(" {\"test\":\"value\"}")));
    }

    public void testRaw() throws IOException {
        {
            XContentBuilder xContentBuilder = XContentFactory.contentBuilder(XContentType.JSON);
            xContentBuilder.startObject();
            xContentBuilder.rawField("foo", new BytesArray("{\"test\":\"value\"}").streamInput());
            xContentBuilder.endObject();
            assertThat(Strings.toString(xContentBuilder), equalTo("""
                {"foo":{"test":"value"}}"""));
        }
        {
            XContentBuilder xContentBuilder = XContentFactory.contentBuilder(XContentType.JSON);
            xContentBuilder.startObject();
            xContentBuilder.rawField("foo", new BytesArray("{\"test\":\"value\"}").streamInput());
            xContentBuilder.rawField("foo1", new BytesArray("{\"test\":\"value\"}").streamInput());
            xContentBuilder.endObject();
            assertThat(Strings.toString(xContentBuilder), equalTo("""
                {"foo":{"test":"value"},"foo1":{"test":"value"}}"""));
        }
        {
            XContentBuilder xContentBuilder = XContentFactory.contentBuilder(XContentType.JSON);
            xContentBuilder.startObject();
            xContentBuilder.field("test", "value");
            xContentBuilder.rawField("foo", new BytesArray("{\"test\":\"value\"}").streamInput());
            xContentBuilder.endObject();
            assertThat(Strings.toString(xContentBuilder), equalTo("""
                {"test":"value","foo":{"test":"value"}}"""));
        }
        {
            XContentBuilder xContentBuilder = XContentFactory.contentBuilder(XContentType.JSON);
            xContentBuilder.startObject();
            xContentBuilder.field("test", "value");
            xContentBuilder.rawField("foo", new BytesArray("{\"test\":\"value\"}").streamInput());
            xContentBuilder.field("test1", "value1");
            xContentBuilder.endObject();
            assertThat(Strings.toString(xContentBuilder), equalTo("""
                {"test":"value","foo":{"test":"value"},"test1":"value1"}"""));
        }
        {
            XContentBuilder xContentBuilder = XContentFactory.contentBuilder(XContentType.JSON);
            xContentBuilder.startObject();
            xContentBuilder.field("test", "value");
            xContentBuilder.rawField("foo", new BytesArray("{\"test\":\"value\"}").streamInput());
            xContentBuilder.rawField("foo1", new BytesArray("{\"test\":\"value\"}").streamInput());
            xContentBuilder.field("test1", "value1");
            xContentBuilder.endObject();
            assertThat(Strings.toString(xContentBuilder), equalTo("""
                {"test":"value","foo":{"test":"value"},"foo1":{"test":"value"},"test1":"value1"}"""));
        }
    }

    public void testSimpleGenerator() throws Exception {
        XContentBuilder builder = XContentFactory.contentBuilder(XContentType.JSON);
        builder.startObject().field("test", "value").endObject();
        assertThat(Strings.toString(builder), equalTo("{\"test\":\"value\"}"));

        builder = XContentFactory.contentBuilder(XContentType.JSON);
        builder.startObject().field("test", "value").endObject();
        assertThat(Strings.toString(builder), equalTo("{\"test\":\"value\"}"));
    }

    public void testOverloadedList() throws Exception {
        XContentBuilder builder = XContentFactory.contentBuilder(XContentType.JSON);
        builder.startObject().field("test", Arrays.asList("1", "2")).endObject();
        assertThat(Strings.toString(builder), equalTo("{\"test\":[\"1\",\"2\"]}"));
    }

    public void testWritingBinaryToStream() throws Exception {
        BytesStreamOutput bos = new BytesStreamOutput();

        XContentGenerator gen = XContentFactory.xContent(XContentType.JSON).createGenerator(bos);
        gen.writeStartObject();
        gen.writeStringField("name", "something");
        gen.flush();
        bos.write(", source : { test : \"value\" }".getBytes("UTF8"));
        gen.writeStringField("name2", "something2");
        gen.writeEndObject();
        gen.close();

        String sData = bos.bytes().utf8ToString();
        assertThat(sData, equalTo("""
            {"name":"something", source : { test : "value" },"name2":"something2"}"""));
    }

    public void testByteConversion() throws Exception {
        XContentBuilder builder = XContentFactory.contentBuilder(XContentType.JSON);
        builder.startObject().field("test_name", (Byte) (byte) 120).endObject();
        assertThat(BytesReference.bytes(builder).utf8ToString(), equalTo("{\"test_name\":120}"));
    }

    public void testDateTypesConversion() throws Exception {
        Date date = new Date();
        String expectedDate = XContentElasticsearchExtension.DEFAULT_FORMATTER.format(date.toInstant());
        Calendar calendar = new GregorianCalendar(TimeZone.getTimeZone("UTC"), Locale.ROOT);
        String expectedCalendar = XContentElasticsearchExtension.DEFAULT_FORMATTER.format(calendar.toInstant());
        XContentBuilder builder = XContentFactory.contentBuilder(XContentType.JSON);
        builder.startObject().timeField("date", date).endObject();
        assertThat(Strings.toString(builder), equalTo("{\"date\":\"" + expectedDate + "\"}"));

        builder = XContentFactory.contentBuilder(XContentType.JSON);
        builder.startObject().field("calendar", calendar).endObject();
        assertThat(Strings.toString(builder), equalTo("{\"calendar\":\"" + expectedCalendar + "\"}"));

        builder = XContentFactory.contentBuilder(XContentType.JSON);
        Map<String, Object> map = new HashMap<>();
        map.put("date", date);
        builder.map(map);
        assertThat(Strings.toString(builder), equalTo("{\"date\":\"" + expectedDate + "\"}"));

        builder = XContentFactory.contentBuilder(XContentType.JSON);
        map = new HashMap<>();
        map.put("calendar", calendar);
        builder.map(map);
        assertThat(Strings.toString(builder), equalTo("{\"calendar\":\"" + expectedCalendar + "\"}"));
    }

    public void testCopyCurrentStructure() throws Exception {
        XContentBuilder builder = XContentFactory.contentBuilder(XContentType.JSON);
        builder.startObject().field("test", "test field").startObject("filter").startObject("terms");

        // up to 20k random terms
        int numTerms = randomInt(20000) + 1;
        List<String> terms = new ArrayList<>(numTerms);
        for (int i = 0; i < numTerms; i++) {
            terms.add("test" + i);
        }

        builder.field("fakefield", terms).endObject().endObject().endObject();
        XContentBuilder filterBuilder = null;
        XContentParser.Token token;

        try (XContentParser parser = createParser(JsonXContent.jsonXContent, BytesReference.bytes(builder))) {

            String currentFieldName = null;
            assertThat(parser.nextToken(), equalTo(XContentParser.Token.START_OBJECT));
            while ((token = parser.nextToken()) != XContentParser.Token.END_OBJECT) {
                if (token == XContentParser.Token.FIELD_NAME) {
                    currentFieldName = parser.currentName();
                } else if (token.isValue()) {
                    if ("test".equals(currentFieldName)) {
                        assertThat(parser.text(), equalTo("test field"));
                    }
                } else if (token == XContentParser.Token.START_OBJECT) {
                    if ("filter".equals(currentFieldName)) {
                        filterBuilder = XContentFactory.contentBuilder(parser.contentType());
                        filterBuilder.copyCurrentStructure(parser);
                    }
                }
            }
        }
        assertNotNull(filterBuilder);
        try (XContentParser parser = createParser(JsonXContent.jsonXContent, BytesReference.bytes(filterBuilder))) {
            assertThat(parser.nextToken(), equalTo(XContentParser.Token.START_OBJECT));
            assertThat(parser.nextToken(), equalTo(XContentParser.Token.FIELD_NAME));
            assertThat(parser.currentName(), equalTo("terms"));
            assertThat(parser.nextToken(), equalTo(XContentParser.Token.START_OBJECT));
            assertThat(parser.nextToken(), equalTo(XContentParser.Token.FIELD_NAME));
            assertThat(parser.currentName(), equalTo("fakefield"));
            assertThat(parser.nextToken(), equalTo(XContentParser.Token.START_ARRAY));
            int i = 0;
            while ((token = parser.nextToken()) != XContentParser.Token.END_ARRAY) {
                assertThat(parser.text(), equalTo(terms.get(i++)));
            }

            assertThat(i, equalTo(terms.size()));
        }
    }

    public void testHandlingOfPath() throws IOException {
        Path path = PathUtils.get("path");
        checkPathSerialization(path);
    }

    public void testHandlingOfPath_relative() throws IOException {
        Path path = PathUtils.get("..", "..", "path");
        checkPathSerialization(path);
    }

    public void testHandlingOfPath_absolute() throws IOException {
        Path path = createTempDir().toAbsolutePath();
        checkPathSerialization(path);
    }

    private void checkPathSerialization(Path path) throws IOException {
        XContentBuilder pathBuilder = XContentFactory.contentBuilder(XContentType.JSON);
        pathBuilder.startObject().field("file", path).endObject();

        XContentBuilder stringBuilder = XContentFactory.contentBuilder(XContentType.JSON);
        stringBuilder.startObject().field("file", path.toString()).endObject();

        assertThat(Strings.toString(pathBuilder), equalTo(Strings.toString(stringBuilder)));
    }

    public void testHandlingOfPath_StringName() throws IOException {
        Path path = PathUtils.get("path");
        String name = new String("file");

        XContentBuilder pathBuilder = XContentFactory.contentBuilder(XContentType.JSON);
        pathBuilder.startObject().field(name, path).endObject();

        XContentBuilder stringBuilder = XContentFactory.contentBuilder(XContentType.JSON);
        stringBuilder.startObject().field(name, path.toString()).endObject();

        assertThat(Strings.toString(pathBuilder), equalTo(Strings.toString(stringBuilder)));
    }

    public void testHandlingOfCollectionOfPaths() throws IOException {
        Path path = PathUtils.get("path");

        XContentBuilder pathBuilder = XContentFactory.contentBuilder(XContentType.JSON);
        pathBuilder.startObject().field("file", Arrays.asList(path)).endObject();

        XContentBuilder stringBuilder = XContentFactory.contentBuilder(XContentType.JSON);
        stringBuilder.startObject().field("file", Arrays.asList(path.toString())).endObject();

        assertThat(Strings.toString(pathBuilder), equalTo(Strings.toString(stringBuilder)));
    }

    public void testIndentIsPlatformIndependent() throws IOException {
        XContentBuilder builder = XContentFactory.contentBuilder(XContentType.JSON).prettyPrint();
        builder.startObject().field("test", "foo").startObject("foo").field("foobar", "boom").endObject().endObject();
        String string = Strings.toString(builder);
        assertEquals("""
            {
              "test" : "foo",
              "foo" : {
                "foobar" : "boom"
              }
            }""", string);

        builder = XContentFactory.contentBuilder(XContentType.YAML).prettyPrint();
        builder.startObject().field("test", "foo").startObject("foo").field("foobar", "boom").endObject().endObject();
        string = Strings.toString(builder);
        assertEquals("""
            ---
            test: "foo"
            foo:
              foobar: "boom"
            """, string);
    }

    public void testRenderGeoPoint() throws IOException {
        XContentBuilder builder = XContentFactory.contentBuilder(XContentType.JSON).prettyPrint();
        builder.startObject().field("foo").value(new GeoPoint(1, 2)).endObject();
        String string = Strings.toString(builder);
        assertEquals("""
            {
              "foo" : {
                "lat" : 1.0,
                "lon" : 2.0
              }
            }""", string.trim());
    }

    public void testWriteMapWithNullKeys() throws IOException {
        XContentBuilder builder = XContentFactory.contentBuilder(randomFrom(XContentType.values()));
        try {
            builder.map(Collections.singletonMap(null, "test"));
            fail("write map should have failed");
        } catch (IllegalArgumentException e) {
            assertThat(e.getMessage(), equalTo("Field name cannot be null"));
        }
    }

    public void testWriteMapValueWithNullKeys() throws IOException {
        XContentBuilder builder = XContentFactory.contentBuilder(randomFrom(XContentType.values()));
        try {
            builder.map(Collections.singletonMap(null, "test"));
            fail("write map should have failed");
        } catch (IllegalArgumentException e) {
            assertThat(e.getMessage(), equalTo("Field name cannot be null"));
        }
    }

    public void testWriteFieldMapWithNullKeys() throws IOException {
        XContentBuilder builder = XContentFactory.contentBuilder(randomFrom(XContentType.values()));
        try {
            builder.startObject();
            builder.field("map", Collections.singletonMap(null, "test"));
            fail("write map should have failed");
        } catch (IllegalArgumentException e) {
            assertThat(e.getMessage(), equalTo("Field name cannot be null"));
        }
    }

    public void testMissingEndObject() throws IOException {
        IllegalStateException e = expectThrows(IllegalStateException.class, () -> {
            try (XContentBuilder builder = XContentFactory.contentBuilder(randomFrom(XContentType.values()))) {
                builder.startObject();
                builder.field("foo", true);
            }
        });
        assertThat(e.getMessage(), equalTo("Failed to close the XContentBuilder"));
        assertThat(e.getCause().getMessage(), equalTo("Unclosed object or array found"));
    }

    public void testMissingEndArray() throws IOException {
        IllegalStateException e = expectThrows(IllegalStateException.class, () -> {
            try (XContentBuilder builder = XContentFactory.contentBuilder(randomFrom(XContentType.values()))) {
                builder.startObject();
                builder.startArray("foo");
                builder.value(0);
                builder.value(1);
            }
        });
        assertThat(e.getMessage(), equalTo("Failed to close the XContentBuilder"));
        assertThat(e.getCause().getMessage(), equalTo("Unclosed object or array found"));
    }

    private static class TestWritableValue {
        final Map<String, Byte> values;

        static TestWritableValue randomValue() {
            int numKeys = randomIntBetween(0, 10);
            Map<String, Byte> values = new HashMap<>();
            for (int i = 0; i < numKeys; i++) {
                values.put(randomAlphaOfLength(10), randomByte());
            }
            return new TestWritableValue(values);
        }

        TestWritableValue(Map<String, Byte> values) {
            this.values = values;
        }

        TestWritableValue(InputStream in) throws IOException {
            final int size = in.read();
            this.values = Maps.newMapWithExpectedSize(size);
            for (int i = 0; i < size; i++) {
                final int keySize = in.read();
                final String key = new String(in.readNBytes(keySize), StandardCharsets.ISO_8859_1);
                final byte value = (byte) in.read();
                values.put(key, value);
            }
        }

        public void writeTo(OutputStream os) throws IOException {
            os.write((byte) values.size());
            for (Map.Entry<String, Byte> e : values.entrySet()) {
                final String k = e.getKey();
                os.write((byte) k.length());
                os.write(k.getBytes(StandardCharsets.ISO_8859_1));
                os.write(e.getValue());
            }
        }
    }

    public void testWritableValue() throws Exception {
        Map<String, Object> expectedValues = new HashMap<>();
        final XContentBuilder builder = XContentFactory.jsonBuilder();
        builder.startObject();
        int fields = iterations(1, 10);
        for (int i = 0; i < fields; i++) {
            String field = "field-" + i;
            if (randomBoolean()) {
                final TestWritableValue value = TestWritableValue.randomValue();
                builder.directFieldAsBase64(field, value::writeTo);
                expectedValues.put(field, value);
            } else {
                Object value = randomFrom(randomInt(), randomAlphaOfLength(10));
                builder.field(field, value);
                expectedValues.put(field, value);
            }
        }
        builder.endObject();
        final BytesReference bytes = BytesReference.bytes(builder);
        final Map<String, Object> actualValues = XContentHelper.convertToMap(bytes, true).v2();
        assertThat(actualValues, aMapWithSize(fields));
        for (Map.Entry<String, Object> e : expectedValues.entrySet()) {
            if (e.getValue() instanceof final TestWritableValue expectedValue) {
                assertThat(actualValues.get(e.getKey()), instanceOf(String.class));
                final byte[] decoded = Base64.getDecoder().decode((String) actualValues.get(e.getKey()));
                final TestWritableValue actualValue = new TestWritableValue(new InputStream() {
                    int pos = 0;

                    @Override
                    public int read() {
                        Objects.checkIndex(pos, decoded.length);
                        return decoded[pos++];
                    }
                });
                assertThat(actualValue.values, equalTo(expectedValue.values));
            } else {
                assertThat(actualValues, hasEntry(e.getKey(), e.getValue()));
            }
        }
    }

    private static class NonXContentable {}

    private static class XContentable implements ToXContent {
        @Override
        public XContentBuilder toXContent(XContentBuilder builder, Params params) throws IOException {
            return null;
        }
    }

    private enum XContentableEnum {
        A,
        B,
        C
    }

    private void assertNotXContentable(Object o, Class<?> type) {
        IllegalArgumentException e = expectThrows(IllegalArgumentException.class, () -> XContentBuilder.ensureToXContentable(o));
        assertThat(e.getMessage(), equalTo("Cannot write type [" + type.getCanonicalName() + "] to x-content"));
    }

    public void testIsXContentable() throws IOException {
        assertNotXContentable(new NonXContentable(), NonXContentable.class);
    }

    public void testSetIsXContentable() throws IOException {
        XContentBuilder.ensureToXContentable(Set.of("a", "b"));
        assertNotXContentable(Set.of(new NonXContentable()), NonXContentable.class);
    }

    public void testListIsXContentable() throws IOException {
        XContentBuilder.ensureToXContentable(List.of("a", "b"));
        assertNotXContentable(List.of(new NonXContentable()), NonXContentable.class);
    }

    public void testMapIsXContentable() throws IOException {
        XContentBuilder.ensureToXContentable(Map.of("a", "b", "c", "d"));
        assertNotXContentable(Map.of("a", new NonXContentable()), NonXContentable.class);

        Map<Object, Object> nonStringKey = Map.of(1, 2);
        IllegalArgumentException e = expectThrows(IllegalArgumentException.class, () -> XContentBuilder.ensureToXContentable(nonStringKey));
        assertThat(e.getMessage(), equalTo("Cannot write non-String map key [java.lang.Integer] to x-content"));
    }

    public void testObjectArrayIsXContentable() throws IOException {
        XContentBuilder.ensureToXContentable(new Object[] { "a", "b" });
        assertNotXContentable(new Object[] { new NonXContentable() }, NonXContentable.class);
    }

    public void testIterableIsXContentable() {
        XContentBuilder.ensureToXContentable((Iterable<String>) () -> List.of("a", "b").iterator());
        assertNotXContentable((Iterable<NonXContentable>) () -> List.of(new NonXContentable()).iterator(), NonXContentable.class);
    }

    public void testPathIsXContentable() {
        XContentBuilder.ensureToXContentable(createTempDir());
    }

    public void testXContentIsXContentable() {
        XContentBuilder.ensureToXContentable(new XContentable());
    }

    public void testEnumIsXContentable() {
        XContentBuilder.ensureToXContentable(XContentableEnum.A);
    }
}
