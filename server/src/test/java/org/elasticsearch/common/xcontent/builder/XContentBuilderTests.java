/*
 * Licensed to Elasticsearch under one or more contributor
 * license agreements. See the NOTICE file distributed with
 * this work for additional information regarding copyright
 * ownership. Elasticsearch licenses this file to you under
 * the Apache License, Version 2.0 (the "License"); you may
 * not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package org.elasticsearch.common.xcontent.builder;

import org.apache.lucene.util.BytesRef;
import org.elasticsearch.common.bytes.BytesArray;
import org.elasticsearch.common.geo.GeoPoint;
import org.elasticsearch.common.io.PathUtils;
import org.elasticsearch.common.io.stream.BytesStreamOutput;
import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.common.xcontent.XContentFactory;
import org.elasticsearch.common.xcontent.XContentGenerator;
import org.elasticsearch.common.xcontent.XContentParser;
import org.elasticsearch.common.xcontent.XContentType;
import org.elasticsearch.common.xcontent.json.JsonXContent;
import org.elasticsearch.test.ESTestCase;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Calendar;
import java.util.Collections;
import java.util.Date;
import java.util.GregorianCalendar;
import java.util.HashMap;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.TimeZone;

import static org.hamcrest.Matchers.equalTo;

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
            xContentBuilder.rawField("foo", new BytesArray("{\"test\":\"value\"}"));
            xContentBuilder.endObject();
            assertThat(xContentBuilder.bytes().utf8ToString(), equalTo("{\"foo\":{\"test\":\"value\"}}"));
        }
        {
            XContentBuilder xContentBuilder = XContentFactory.contentBuilder(XContentType.JSON);
            xContentBuilder.startObject();
            xContentBuilder.rawField("foo", new BytesArray("{\"test\":\"value\"}"));
            xContentBuilder.rawField("foo1", new BytesArray("{\"test\":\"value\"}"));
            xContentBuilder.endObject();
            assertThat(xContentBuilder.bytes().utf8ToString(), equalTo("{\"foo\":{\"test\":\"value\"},\"foo1\":{\"test\":\"value\"}}"));
        }
        {
            XContentBuilder xContentBuilder = XContentFactory.contentBuilder(XContentType.JSON);
            xContentBuilder.startObject();
            xContentBuilder.field("test", "value");
            xContentBuilder.rawField("foo", new BytesArray("{\"test\":\"value\"}"));
            xContentBuilder.endObject();
            assertThat(xContentBuilder.bytes().utf8ToString(), equalTo("{\"test\":\"value\",\"foo\":{\"test\":\"value\"}}"));
        }
        {
            XContentBuilder xContentBuilder = XContentFactory.contentBuilder(XContentType.JSON);
            xContentBuilder.startObject();
            xContentBuilder.field("test", "value");
            xContentBuilder.rawField("foo", new BytesArray("{\"test\":\"value\"}"));
            xContentBuilder.field("test1", "value1");
            xContentBuilder.endObject();
            assertThat(xContentBuilder.bytes().utf8ToString(), equalTo("{\"test\":\"value\",\"foo\":{\"test\":\"value\"},\"test1\":\"value1\"}"));
        }
        {
            XContentBuilder xContentBuilder = XContentFactory.contentBuilder(XContentType.JSON);
            xContentBuilder.startObject();
            xContentBuilder.field("test", "value");
            xContentBuilder.rawField("foo", new BytesArray("{\"test\":\"value\"}"));
            xContentBuilder.rawField("foo1", new BytesArray("{\"test\":\"value\"}"));
            xContentBuilder.field("test1", "value1");
            xContentBuilder.endObject();
            assertThat(xContentBuilder.bytes().utf8ToString(), equalTo("{\"test\":\"value\",\"foo\":{\"test\":\"value\"},\"foo1\":{\"test\":\"value\"},\"test1\":\"value1\"}"));
        }
    }

    public void testSimpleGenerator() throws Exception {
        XContentBuilder builder = XContentFactory.contentBuilder(XContentType.JSON);
        builder.startObject().field("test", "value").endObject();
        assertThat(builder.string(), equalTo("{\"test\":\"value\"}"));

        builder = XContentFactory.contentBuilder(XContentType.JSON);
        builder.startObject().field("test", "value").endObject();
        assertThat(builder.string(), equalTo("{\"test\":\"value\"}"));
    }

    public void testOverloadedList() throws Exception {
        XContentBuilder builder = XContentFactory.contentBuilder(XContentType.JSON);
        builder.startObject().field("test", Arrays.asList("1", "2")).endObject();
        assertThat(builder.string(), equalTo("{\"test\":[\"1\",\"2\"]}"));
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
        assertThat(sData, equalTo("{\"name\":\"something\", source : { test : \"value\" },\"name2\":\"something2\"}"));
    }

    public void testByteConversion() throws Exception {
        XContentBuilder builder = XContentFactory.contentBuilder(XContentType.JSON);
        builder.startObject().field("test_name", (Byte)(byte)120).endObject();
        assertThat(builder.bytes().utf8ToString(), equalTo("{\"test_name\":120}"));
    }

    public void testDateTypesConversion() throws Exception {
        Date date = new Date();
        String expectedDate = XContentBuilder.DEFAULT_DATE_PRINTER.print(date.getTime());
        Calendar calendar = new GregorianCalendar(TimeZone.getTimeZone("UTC"), Locale.ROOT);
        String expectedCalendar = XContentBuilder.DEFAULT_DATE_PRINTER.print(calendar.getTimeInMillis());
        XContentBuilder builder = XContentFactory.contentBuilder(XContentType.JSON);
        builder.startObject().field("date", date).endObject();
        assertThat(builder.string(), equalTo("{\"date\":\"" + expectedDate + "\"}"));

        builder = XContentFactory.contentBuilder(XContentType.JSON);
        builder.startObject().field("calendar", calendar).endObject();
        assertThat(builder.string(), equalTo("{\"calendar\":\"" + expectedCalendar + "\"}"));

        builder = XContentFactory.contentBuilder(XContentType.JSON);
        Map<String, Object> map = new HashMap<>();
        map.put("date", date);
        builder.map(map);
        assertThat(builder.string(), equalTo("{\"date\":\"" + expectedDate + "\"}"));

        builder = XContentFactory.contentBuilder(XContentType.JSON);
        map = new HashMap<>();
        map.put("calendar", calendar);
        builder.map(map);
        assertThat(builder.string(), equalTo("{\"calendar\":\"" + expectedCalendar + "\"}"));
    }

    public void testCopyCurrentStructure() throws Exception {
        XContentBuilder builder = XContentFactory.contentBuilder(XContentType.JSON);
        builder.startObject()
                .field("test", "test field")
                .startObject("filter")
                .startObject("terms");

        // up to 20k random terms
        int numTerms = randomInt(20000) + 1;
        List<String> terms = new ArrayList<>(numTerms);
        for (int i = 0; i < numTerms; i++) {
            terms.add("test" + i);
        }

        builder.field("fakefield", terms).endObject().endObject().endObject();

        XContentParser parser = createParser(JsonXContent.jsonXContent, builder.bytes());

        XContentBuilder filterBuilder = null;
        XContentParser.Token token;
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

        assertNotNull(filterBuilder);
        parser = createParser(JsonXContent.jsonXContent, filterBuilder.bytes());
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

        assertThat(pathBuilder.string(), equalTo(stringBuilder.string()));
    }

    public void testHandlingOfPath_StringName() throws IOException {
        Path path = PathUtils.get("path");
        String name = new String("file");

        XContentBuilder pathBuilder = XContentFactory.contentBuilder(XContentType.JSON);
        pathBuilder.startObject().field(name, path).endObject();

        XContentBuilder stringBuilder = XContentFactory.contentBuilder(XContentType.JSON);
        stringBuilder.startObject().field(name, path.toString()).endObject();

        assertThat(pathBuilder.string(), equalTo(stringBuilder.string()));
    }

    public void testHandlingOfCollectionOfPaths() throws IOException {
        Path path = PathUtils.get("path");

        XContentBuilder pathBuilder = XContentFactory.contentBuilder(XContentType.JSON);
        pathBuilder.startObject().field("file", Arrays.asList(path)).endObject();

        XContentBuilder stringBuilder = XContentFactory.contentBuilder(XContentType.JSON);
        stringBuilder.startObject().field("file", Arrays.asList(path.toString())).endObject();

        assertThat(pathBuilder.string(), equalTo(stringBuilder.string()));
    }

    public void testIndentIsPlatformIndependent() throws IOException {
        XContentBuilder builder = XContentFactory.contentBuilder(XContentType.JSON).prettyPrint();
        builder.startObject().field("test","foo").startObject("foo").field("foobar", "boom").endObject().endObject();
        String string = builder.string();
        assertEquals("{\n" +
                "  \"test\" : \"foo\",\n" +
                "  \"foo\" : {\n" +
                "    \"foobar\" : \"boom\"\n" +
                "  }\n" +
                "}", string);

        builder = XContentFactory.contentBuilder(XContentType.YAML).prettyPrint();
        builder.startObject().field("test","foo").startObject("foo").field("foobar", "boom").endObject().endObject();
        string = builder.string();
        assertEquals("---\n" +
                "test: \"foo\"\n" +
                "foo:\n" +
                "  foobar: \"boom\"\n", string);
    }

    public void testRenderGeoPoint() throws IOException {
        XContentBuilder builder = XContentFactory.contentBuilder(XContentType.JSON).prettyPrint();
        builder.startObject().field("foo").value(new GeoPoint(1,2)).endObject();
        String string = builder.string();
        assertEquals("{\n" +
                "  \"foo\" : {\n" +
                "    \"lat\" : 1.0,\n" +
                "    \"lon\" : 2.0\n" +
                "  }\n" +
                "}", string.trim());
    }

    public void testWriteMapWithNullKeys() throws IOException {
        XContentBuilder builder = XContentFactory.contentBuilder(randomFrom(XContentType.values()));
        try {
            builder.map(Collections.singletonMap(null, "test"));
            fail("write map should have failed");
        } catch(IllegalArgumentException e) {
            assertThat(e.getMessage(), equalTo("Field name cannot be null"));
        }
    }

    public void testWriteMapValueWithNullKeys() throws IOException {
        XContentBuilder builder = XContentFactory.contentBuilder(randomFrom(XContentType.values()));
        try {
            builder.map(Collections.singletonMap(null, "test"));
            fail("write map should have failed");
        } catch(IllegalArgumentException e) {
            assertThat(e.getMessage(), equalTo("Field name cannot be null"));
        }
    }

    public void testWriteFieldMapWithNullKeys() throws IOException {
        XContentBuilder builder = XContentFactory.contentBuilder(randomFrom(XContentType.values()));
        try {
            builder.startObject();
            builder.field("map", Collections.singletonMap(null, "test"));
            fail("write map should have failed");
        } catch(IllegalArgumentException e) {
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
}
