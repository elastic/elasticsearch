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

package org.elasticsearch.common.xcontent;

import com.fasterxml.jackson.core.JsonGenerationException;
import com.fasterxml.jackson.core.JsonGenerator;
import com.fasterxml.jackson.core.JsonParseException;

import org.apache.lucene.util.BytesRef;
import org.apache.lucene.util.Constants;
import org.elasticsearch.ElasticsearchException;
import org.elasticsearch.cluster.metadata.IndexMetaData;
import org.elasticsearch.common.ParseField;
import org.elasticsearch.common.Strings;
import org.elasticsearch.common.bytes.BytesArray;
import org.elasticsearch.common.bytes.BytesReference;
import org.elasticsearch.common.geo.GeoPoint;
import org.elasticsearch.common.io.PathUtils;
import org.elasticsearch.common.text.Text;
import org.elasticsearch.common.unit.DistanceUnit;
import org.elasticsearch.common.xcontent.XContentParser.Token;
import org.elasticsearch.test.ESTestCase;
import org.hamcrest.Matcher;
import org.hamcrest.Matchers;
import org.joda.time.DateTime;
import org.joda.time.DateTimeZone;
import org.joda.time.Instant;
import org.joda.time.ReadableInstant;
import org.joda.time.format.DateTimeFormatter;
import org.joda.time.format.ISODateTimeFormat;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.math.BigInteger;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Calendar;
import java.util.Collections;
import java.util.Date;
import java.util.HashMap;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.concurrent.TimeUnit;

import static java.util.Collections.emptyMap;
import static java.util.Collections.singletonMap;
import static org.hamcrest.Matchers.allOf;
import static org.hamcrest.Matchers.containsString;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.instanceOf;
import static org.hamcrest.Matchers.notNullValue;
import static org.hamcrest.Matchers.startsWith;

public abstract class BaseXContentTestCase extends ESTestCase {

    protected abstract XContentType xcontentType();

    private XContentBuilder builder() throws IOException {
        return XContentBuilder.builder(xcontentType().xContent());
    }

    public void testContentType() throws IOException {
        assertThat(builder().contentType(), equalTo(xcontentType()));
    }

    public void testStartEndObject() throws IOException {
        expectUnclosedException(() -> builder().startObject().bytes());
        expectUnclosedException(() -> builder().startObject().close());
        expectUnclosedException(() -> builder().startObject().string());

        expectObjectException(() -> builder().endObject().bytes());
        expectObjectException(() -> builder().endObject().close());
        expectObjectException(() -> builder().endObject().string());

        expectValueException(() -> builder().startObject("foo").endObject());
        expectNonNullFieldException(() -> builder().startObject().startObject(null));

        assertResult("{}", () -> builder().startObject().endObject());
        assertResult("{'foo':{}}", () -> builder().startObject().startObject("foo").endObject().endObject());

        assertResult("{'foo':{'bar':{}}}", () -> builder()
                .startObject()
                    .startObject("foo")
                        .startObject("bar")
                        .endObject()
                    .endObject()
                .endObject());
    }

    public void testStartEndArray() throws IOException {
        expectUnclosedException(() -> builder().startArray().bytes());
        expectUnclosedException(() -> builder().startArray().close());
        expectUnclosedException(() -> builder().startArray().string());

        expectArrayException(() -> builder().endArray().bytes());
        expectArrayException(() -> builder().endArray().close());
        expectArrayException(() -> builder().endArray().string());

        expectValueException(() -> builder().startArray("foo").endObject());
        expectFieldException(() -> builder().startObject().startArray().endArray().endObject());
        expectNonNullFieldException(() -> builder().startObject().startArray(null).endArray().endObject());

        assertResult("{'foo':[]}", () -> builder().startObject().startArray("foo").endArray().endObject());
        assertResult("{'foo':[1,2,3]}", () -> builder()
                .startObject()
                    .startArray("foo")
                        .value(1)
                        .value(2)
                        .value(3)
                    .endArray()
                .endObject());
    }

    public void testField() throws IOException {
        expectValueException(() -> builder().field("foo").bytes());
        expectNonNullFieldException(() -> builder().field(null).bytes());
        expectUnclosedException(() -> builder().startObject().field("foo").bytes());

        assertResult("{'foo':'bar'}", () -> builder().startObject().field("foo").value("bar").endObject());
    }

    public void testNullField() throws IOException {
        expectValueException(() -> builder().nullField("foo").bytes());
        expectNonNullFieldException(() -> builder().nullField(null).bytes());
        expectUnclosedException(() -> builder().startObject().nullField("foo").bytes());

        assertResult("{'foo':null}", () -> builder().startObject().nullField("foo").endObject());
    }

    public void testNullValue() throws IOException {
        assertResult("{'foo':null}", () -> builder().startObject().field("foo").nullValue().endObject());
    }

    public void testBooleans() throws IOException {
        assertResult("{'boolean':null}", () -> builder().startObject().field("boolean", (Boolean) null).endObject());
        assertResult("{'boolean':true}", () -> builder().startObject().field("boolean", Boolean.TRUE).endObject());
        assertResult("{'boolean':false}", () -> builder().startObject().field("boolean", Boolean.FALSE).endObject());
        assertResult("{'boolean':[true,false,true]}", () -> builder().startObject().array("boolean", true, false, true).endObject());
        assertResult("{'boolean':[false,true]}", () -> builder().startObject().array("boolean", new boolean[]{false, true}).endObject());
        assertResult("{'boolean':null}", () -> builder().startObject().array("boolean", (boolean[]) null).endObject());
        assertResult("{'boolean':[]}", () -> builder().startObject().array("boolean", new boolean[]{}).endObject());
        assertResult("{'boolean':null}", () -> builder().startObject().field("boolean").value((Boolean) null).endObject());
        assertResult("{'boolean':true}", () -> builder().startObject().field("boolean").value(Boolean.TRUE).endObject());
        assertResult("{'boolean':false}", () -> builder().startObject().field("boolean").value(Boolean.FALSE).endObject());
    }

    public void testBytes() throws IOException {
        assertResult("{'byte':null}", () -> builder().startObject().field("byte", (Byte) null).endObject());
        assertResult("{'byte':0}", () -> builder().startObject().field("byte", (byte) 0).endObject());
        assertResult("{'byte':1}", () -> builder().startObject().field("byte", (byte) 1).endObject());
        assertResult("{'byte':null}", () -> builder().startObject().field("byte").value((Byte) null).endObject());
        assertResult("{'byte':0}", () -> builder().startObject().field("byte").value((byte) 0).endObject());
        assertResult("{'byte':1}", () -> builder().startObject().field("byte").value((byte) 1).endObject());
    }

    public void testDoubles() throws IOException {
        assertResult("{'double':null}", () -> builder().startObject().field("double", (Double) null).endObject());
        assertResult("{'double':42.5}", () -> builder().startObject().field("double", Double.valueOf(42.5)).endObject());
        assertResult("{'double':1.2}", () -> builder().startObject().field("double", 1.2).endObject());
        assertResult("{'double':[42.0,43.0,45]}", () -> builder().startObject().array("double", 42.0, 43.0, 45).endObject());
        assertResult("{'double':null}", () -> builder().startObject().array("double", (double[]) null).endObject());
        assertResult("{'double':[]}", () -> builder().startObject().array("double", new double[]{}).endObject());
        assertResult("{'double':null}", () -> builder().startObject().field("double").value((Double) null).endObject());
        assertResult("{'double':0.001}", () -> builder().startObject().field("double").value(0.001).endObject());
        assertResult("{'double':[1.7976931348623157E308,4.9E-324]}", () -> builder()
                .startObject()
                .array("double", new double[]{Double.MAX_VALUE, Double.MIN_VALUE})
                .endObject());
    }

    public void testFloats() throws IOException {
        assertResult("{'float':null}", () -> builder().startObject().field("float", (Float) null).endObject());
        assertResult("{'float':42.5}", () -> builder().startObject().field("float", Float.valueOf(42.5f)).endObject());
        assertResult("{'float':1.2}", () -> builder().startObject().field("float", 1.2f).endObject());
        assertResult("{'float':null}", () -> builder().startObject().array("float", (float[]) null).endObject());
        assertResult("{'float':[]}", () -> builder().startObject().array("float", new float[]{}).endObject());
        assertResult("{'float':null}", () -> builder().startObject().field("float").value((Float) null).endObject());
        assertResult("{'float':9.9E-7}", () -> builder().startObject().field("float").value(0.00000099f).endObject());
        assertResult("{'float':[42.0,43.0,45.666668]}", () -> builder()
                .startObject()
                    .array("float", 42.0f, 43.0f, 45.66666667f)
                .endObject());
        assertResult("{'float':[3.4028235E38,1.4E-45]}", () -> builder()
                .startObject()
                    .array("float", new float[]{Float.MAX_VALUE, Float.MIN_VALUE})
                .endObject());
    }

    public void testIntegers() throws IOException {
        assertResult("{'integer':null}", () -> builder().startObject().field("integer", (Integer) null).endObject());
        assertResult("{'integer':42}", () -> builder().startObject().field("integer", Integer.valueOf(42)).endObject());
        assertResult("{'integer':3}", () -> builder().startObject().field("integer", 3).endObject());
        assertResult("{'integer':[1,3,5,7,11]}", () -> builder().startObject().array("integer", 1, 3, 5, 7, 11).endObject());
        assertResult("{'integer':null}", () -> builder().startObject().array("integer", (int[]) null).endObject());
        assertResult("{'integer':[]}", () -> builder().startObject().array("integer", new int[]{}).endObject());
        assertResult("{'integer':null}", () -> builder().startObject().field("integer").value((Integer) null).endObject());
        assertResult("{'integer':42}", () -> builder().startObject().field("integer").value(42).endObject());
        assertResult("{'integer':[2147483647,-2147483648]}", () -> builder()
                .startObject()
                    .array("integer", new int[]{Integer.MAX_VALUE, Integer.MIN_VALUE})
                .endObject());
    }

    public void testLongs() throws IOException {
        assertResult("{'long':null}", () -> builder().startObject().field("long", (Long) null).endObject());
        assertResult("{'long':42}", () -> builder().startObject().field("long", Long.valueOf(42L)).endObject());
        assertResult("{'long':9223372036854775807}", () -> builder().startObject().field("long", 9_223_372_036_854_775_807L).endObject());
        assertResult("{'long':[1,3,5,7,11]}", () -> builder().startObject().array("long", 1L, 3L, 5L, 7L, 11L).endObject());
        assertResult("{'long':null}", () -> builder().startObject().array("long", (long[]) null).endObject());
        assertResult("{'long':[]}", () -> builder().startObject().array("long", new long[]{}).endObject());
        assertResult("{'long':null}", () -> builder().startObject().field("long").value((Long) null).endObject());
        assertResult("{'long':42}", () -> builder().startObject().field("long").value(42).endObject());
        assertResult("{'long':[2147483647,-2147483648]}", () -> builder()
                .startObject()
                    .array("long", new long[]{Integer.MAX_VALUE, Integer.MIN_VALUE})
                .endObject());
    }

    public void testShorts() throws IOException {
        assertResult("{'short':null}", () -> builder().startObject().field("short", (Short) null).endObject());
        assertResult("{'short':5000}", () -> builder().startObject().field("short", Short.valueOf((short) 5000)).endObject());
        assertResult("{'short':null}", () -> builder().startObject().array("short", (short[]) null).endObject());
        assertResult("{'short':[]}", () -> builder().startObject().array("short", new short[]{}).endObject());
        assertResult("{'short':null}", () -> builder().startObject().field("short").value((Short) null).endObject());
        assertResult("{'short':42}", () -> builder().startObject().field("short").value((short) 42).endObject());
        assertResult("{'short':[1,3,5,7,11]}", () -> builder()
                .startObject()
                    .array("short", (short) 1, (short) 3, (short) 5, (short) 7, (short) 11)
                .endObject());
        assertResult("{'short':[32767,-32768]}", () -> builder()
                .startObject()
                    .array("short", new short[]{Short.MAX_VALUE, Short.MIN_VALUE})
                .endObject());
    }

    public void testStrings() throws IOException {
        assertResult("{'string':null}", () -> builder().startObject().field("string", (String) null).endObject());
        assertResult("{'string':'value'}", () -> builder().startObject().field("string", "value").endObject());
        assertResult("{'string':''}", () -> builder().startObject().field("string", "").endObject());
        assertResult("{'string':null}", () -> builder().startObject().array("string", (String[]) null).endObject());
        assertResult("{'string':[]}", () -> builder().startObject().array("string", Strings.EMPTY_ARRAY).endObject());
        assertResult("{'string':null}", () -> builder().startObject().field("string").value((String) null).endObject());
        assertResult("{'string':'42'}", () -> builder().startObject().field("string").value("42").endObject());
        assertResult("{'string':['a','b','c','d']}", () -> builder()
                .startObject()
                    .array("string", "a", "b", "c", "d")
                .endObject());
    }

    public void testBinaryField() throws Exception {
        assertResult("{'binary':null}", () -> builder().startObject().field("binary", (byte[]) null).endObject());

        final byte[] randomBytes = randomBytes();
        BytesReference bytes = builder().startObject().field("binary", randomBytes).endObject().bytes();

        XContentParser parser = createParser(xcontentType().xContent(), bytes);
        assertSame(parser.nextToken(), Token.START_OBJECT);
        assertSame(parser.nextToken(), Token.FIELD_NAME);
        assertEquals(parser.currentName(), "binary");
        assertTrue(parser.nextToken().isValue());
        assertArrayEquals(randomBytes, parser.binaryValue());
        assertSame(parser.nextToken(), Token.END_OBJECT);
        assertNull(parser.nextToken());
    }

    public void testBinaryValue() throws Exception {
        assertResult("{'binary':null}", () -> builder().startObject().field("binary").value((byte[]) null).endObject());

        final byte[] randomBytes = randomBytes();
        BytesReference bytes = builder().startObject().field("binary").value(randomBytes).endObject().bytes();

        XContentParser parser = createParser(xcontentType().xContent(), bytes);
        assertSame(parser.nextToken(), Token.START_OBJECT);
        assertSame(parser.nextToken(), Token.FIELD_NAME);
        assertEquals(parser.currentName(), "binary");
        assertTrue(parser.nextToken().isValue());
        assertArrayEquals(randomBytes, parser.binaryValue());
        assertSame(parser.nextToken(), Token.END_OBJECT);
        assertNull(parser.nextToken());
    }

    public void testBinaryValueWithOffsetLength() throws Exception {
        assertResult("{'binary':null}", () -> builder().startObject().field("binary").value(null, 0, 0).endObject());

        final byte[] randomBytes = randomBytes();
        final int offset = randomIntBetween(0, randomBytes.length - 1);
        final int length = randomIntBetween(1, Math.max(1, randomBytes.length - offset - 1));

        XContentBuilder builder = builder().startObject();
        if (randomBoolean()) {
            builder.field("bin", randomBytes, offset, length);
        } else {
            builder.field("bin").value(randomBytes, offset, length);
        }
        builder.endObject();

        XContentParser parser = createParser(xcontentType().xContent(), builder.bytes());
        assertSame(parser.nextToken(), Token.START_OBJECT);
        assertSame(parser.nextToken(), Token.FIELD_NAME);
        assertEquals(parser.currentName(), "bin");
        assertTrue(parser.nextToken().isValue());
        assertArrayEquals(Arrays.copyOfRange(randomBytes, offset, offset + length), parser.binaryValue());
        assertSame(parser.nextToken(), Token.END_OBJECT);
        assertNull(parser.nextToken());
    }

    public void testBinaryUTF8() throws Exception {
        assertResult("{'utf8':null}", () -> builder().startObject().utf8Field("utf8", null).endObject());

        final BytesRef randomBytesRef = new BytesRef(randomBytes());
        XContentBuilder builder = builder().startObject();
        if (randomBoolean()) {
            builder.utf8Field("utf8", randomBytesRef);
        } else {
            builder.field("utf8").utf8Value(randomBytesRef);
        }
        builder.endObject();

        XContentParser parser = createParser(xcontentType().xContent(), builder.bytes());
        assertSame(parser.nextToken(), Token.START_OBJECT);
        assertSame(parser.nextToken(), Token.FIELD_NAME);
        assertEquals(parser.currentName(), "utf8");
        assertTrue(parser.nextToken().isValue());
        assertThat(parser.utf8Bytes().utf8ToString(), equalTo(randomBytesRef.utf8ToString()));
        assertSame(parser.nextToken(), Token.END_OBJECT);
        assertNull(parser.nextToken());
    }

    public void testText() throws Exception {
        assertResult("{'text':null}", () -> builder().startObject().field("text", (Text) null).endObject());
        assertResult("{'text':''}", () -> builder().startObject().field("text", new Text("")).endObject());
        assertResult("{'text':'foo bar'}", () -> builder().startObject().field("text", new Text("foo bar")).endObject());

        final BytesReference random = new BytesArray(randomBytes());
        XContentBuilder builder = builder().startObject().field("text", new Text(random)).endObject();

        XContentParser parser = createParser(xcontentType().xContent(), builder.bytes());
        assertSame(parser.nextToken(), Token.START_OBJECT);
        assertSame(parser.nextToken(), Token.FIELD_NAME);
        assertEquals(parser.currentName(), "text");
        assertTrue(parser.nextToken().isValue());
        assertThat(parser.utf8Bytes().utf8ToString(), equalTo(random.utf8ToString()));
        assertSame(parser.nextToken(), Token.END_OBJECT);
        assertNull(parser.nextToken());
    }

    public void testReadableInstant() throws Exception {
        assertResult("{'instant':null}", () -> builder().startObject().field("instant", (ReadableInstant) null).endObject());
        assertResult("{'instant':null}", () -> builder().startObject().field("instant").value((ReadableInstant) null).endObject());

        final DateTime t1 = new DateTime(2016, 1, 1, 0, 0, DateTimeZone.UTC);

        String expected = "{'t1':'2016-01-01T00:00:00.000Z'}";
        assertResult(expected, () -> builder().startObject().field("t1", t1).endObject());
        assertResult(expected, () -> builder().startObject().field("t1").value(t1).endObject());

        final DateTime t2 = new DateTime(2016, 12, 25, 7, 59, 42, 213, DateTimeZone.UTC);

        expected = "{'t2':'2016-12-25T07:59:42.213Z'}";
        assertResult(expected, () -> builder().startObject().field("t2", t2).endObject());
        assertResult(expected, () -> builder().startObject().field("t2").value(t2).endObject());

        final DateTimeFormatter formatter = randomFrom(ISODateTimeFormat.basicDate(), ISODateTimeFormat.dateTimeNoMillis());
        final DateTime t3 = DateTime.now();

        expected = "{'t3':'" + formatter.print(t3) + "'}";
        assertResult(expected, () -> builder().startObject().field("t3", t3, formatter).endObject());
        assertResult(expected, () -> builder().startObject().field("t3").value(t3, formatter).endObject());

        final DateTime t4 = new DateTime(randomDateTimeZone());

        expected = "{'t4':'" + formatter.print(t4) + "'}";
        assertResult(expected, () -> builder().startObject().field("t4", t4, formatter).endObject());
        assertResult(expected, () -> builder().startObject().field("t4").value(t4, formatter).endObject());

        long date = Math.abs(randomLong() % (2 * (long) 10e11)); // 1970-01-01T00:00:00Z - 2033-05-18T05:33:20.000+02:00
        final DateTime t5 = new DateTime(date, randomDateTimeZone());

        expected = "{'t5':'" + XContentBuilder.DEFAULT_DATE_PRINTER.print(t5) + "'}";
        assertResult(expected, () -> builder().startObject().field("t5", t5).endObject());
        assertResult(expected, () -> builder().startObject().field("t5").value(t5).endObject());

        expected = "{'t5':'" + formatter.print(t5) + "'}";
        assertResult(expected, () -> builder().startObject().field("t5", t5, formatter).endObject());
        assertResult(expected, () -> builder().startObject().field("t5").value(t5, formatter).endObject());

        Instant i1 = new Instant(1451606400000L); // 2016-01-01T00:00:00.000Z
        expected = "{'i1':'2016-01-01T00:00:00.000Z'}";
        assertResult(expected, () -> builder().startObject().field("i1", i1).endObject());
        assertResult(expected, () -> builder().startObject().field("i1").value(i1).endObject());

        Instant i2 = new Instant(1482652782213L); // 2016-12-25T07:59:42.213Z
        expected = "{'i2':'" + formatter.print(i2) + "'}";
        assertResult(expected, () -> builder().startObject().field("i2", i2, formatter).endObject());
        assertResult(expected, () -> builder().startObject().field("i2").value(i2, formatter).endObject());

        expectNonNullFormatterException(() -> builder().startObject().field("t3", t3, null).endObject());
        expectNonNullFormatterException(() -> builder().startObject().field("t3").value(t3, null).endObject());
    }

    public void testDate() throws Exception {
        assertResult("{'date':null}", () -> builder().startObject().field("date", (Date) null).endObject());
        assertResult("{'date':null}", () -> builder().startObject().field("date").value((Date) null).endObject());

        final Date d1 = new DateTime(2016, 1, 1, 0, 0, DateTimeZone.UTC).toDate();
        assertResult("{'d1':'2016-01-01T00:00:00.000Z'}", () -> builder().startObject().field("d1", d1).endObject());
        assertResult("{'d1':'2016-01-01T00:00:00.000Z'}", () -> builder().startObject().field("d1").value(d1).endObject());

        final Date d2 = new DateTime(2016, 12, 25, 7, 59, 42, 213, DateTimeZone.UTC).toDate();
        assertResult("{'d2':'2016-12-25T07:59:42.213Z'}", () -> builder().startObject().field("d2", d2).endObject());
        assertResult("{'d2':'2016-12-25T07:59:42.213Z'}", () -> builder().startObject().field("d2").value(d2).endObject());

        final DateTimeFormatter formatter = randomFrom(ISODateTimeFormat.basicDate(), ISODateTimeFormat.dateTimeNoMillis());
        final Date d3 = DateTime.now().toDate();

        String expected = "{'d3':'" + formatter.print(d3.getTime()) + "'}";
        assertResult(expected, () -> builder().startObject().field("d3", d3, formatter).endObject());
        assertResult(expected, () -> builder().startObject().field("d3").value(d3, formatter).endObject());

        expectNonNullFormatterException(() -> builder().startObject().field("d3", d3, null).endObject());
        expectNonNullFormatterException(() -> builder().startObject().field("d3").value(d3, null).endObject());
        expectNonNullFormatterException(() -> builder().value(null, 1L));
    }

    public void testDateField() throws Exception {
        final Date d = new DateTime(2016, 1, 1, 0, 0, DateTimeZone.UTC).toDate();

        assertResult("{'date_in_millis':1451606400000}", () -> builder()
                .startObject()
                    .dateField("date_in_millis", "date", d.getTime())
                .endObject());
        assertResult("{'date':'2016-01-01T00:00:00.000Z','date_in_millis':1451606400000}", () -> builder()
                .humanReadable(true)
                .startObject
                        ().dateField("date_in_millis", "date", d.getTime())
                .endObject());
    }

    public void testCalendar() throws Exception {
        Calendar calendar = new DateTime(2016, 1, 1, 0, 0, DateTimeZone.UTC).toCalendar(Locale.ROOT);
        assertResult("{'calendar':'2016-01-01T00:00:00.000Z'}", () -> builder()
                .startObject()
                    .field("calendar")
                    .value(calendar)
                .endObject());
    }

    public void testGeoPoint() throws Exception {
        assertResult("{'geo':null}", () -> builder().startObject().field("geo", (GeoPoint) null).endObject());
        assertResult("{'geo':{'lat':52.4267578125,'lon':13.271484375}}", () -> builder()
                .startObject()
                .   field("geo", GeoPoint.fromGeohash("u336q"))
                .endObject());
        assertResult("{'geo':{'lat':52.5201416015625,'lon':13.4033203125}}", () -> builder()
                .startObject()
                    .field("geo")
                    .value(GeoPoint.fromGeohash("u33dc1"))
                .endObject());
    }

    public void testLatLon() throws Exception {
        final String expected = "{'latlon':{'lat':13.271484375,'lon':52.4267578125}}";
        assertResult(expected, () -> builder().startObject().latlon("latlon", 13.271484375, 52.4267578125).endObject());
        assertResult(expected, () -> builder().startObject().field("latlon").latlon(13.271484375, 52.4267578125).endObject());
    }

    public void testPath() throws Exception {
        assertResult("{'path':null}", () -> builder().startObject().field("path", (Path) null).endObject());

        final Path path = PathUtils.get("first", "second", "third");
        final String expected = Constants.WINDOWS ? "{'path':'first\\\\second\\\\third'}" : "{'path':'first/second/third'}";
        assertResult(expected, () -> builder().startObject().field("path", path).endObject());
    }

    public void testObjects() throws Exception {
        Map<String, Object[]> objects = new HashMap<>();
        objects.put("{'objects':[false,true,false]}", new Object[]{false, true, false});
        objects.put("{'objects':[1,1,2,3,5,8,13]}", new Object[]{(byte) 1, (byte) 1, (byte) 2, (byte) 3, (byte) 5, (byte) 8, (byte) 13});
        objects.put("{'objects':[1.0,1.0,2.0,3.0,5.0,8.0,13.0]}", new Object[]{1.0d, 1.0d, 2.0d, 3.0d, 5.0d, 8.0d, 13.0d});
        objects.put("{'objects':[1.0,1.0,2.0,3.0,5.0,8.0,13.0]}", new Object[]{1.0f, 1.0f, 2.0f, 3.0f, 5.0f, 8.0f, 13.0f});
        objects.put("{'objects':[{'lat':45.759429931640625,'lon':4.8394775390625}]}", new Object[]{GeoPoint.fromGeohash("u05kq4k")});
        objects.put("{'objects':[1,1,2,3,5,8,13]}", new Object[]{1, 1, 2, 3, 5, 8, 13});
        objects.put("{'objects':[1,1,2,3,5,8,13]}", new Object[]{1L, 1L, 2L, 3L, 5L, 8L, 13L});
        objects.put("{'objects':[1,1,2,3,5,8]}", new Object[]{(short) 1, (short) 1, (short) 2, (short) 3, (short) 5, (short) 8});
        objects.put("{'objects':['a','b','c']}", new Object[]{"a", "b", "c"});
        objects.put("{'objects':['a','b','c']}", new Object[]{new Text("a"), new Text(new BytesArray("b")), new Text("c")});
        objects.put("{'objects':null}", null);
        objects.put("{'objects':[null,null,null]}", new Object[]{null, null, null});
        objects.put("{'objects':['OPEN','CLOSE']}", IndexMetaData.State.values());
        objects.put("{'objects':[{'f1':'v1'},{'f2':'v2'}]}", new Object[]{singletonMap("f1", "v1"), singletonMap("f2", "v2")});
        objects.put("{'objects':[[1,2,3],[4,5]]}", new Object[]{Arrays.asList(1, 2, 3), Arrays.asList(4, 5)});

        final String paths = Constants.WINDOWS ? "{'objects':['a\\\\b\\\\c','d\\\\e']}" : "{'objects':['a/b/c','d/e']}";
        objects.put(paths, new Object[]{PathUtils.get("a", "b", "c"), PathUtils.get("d", "e")});

        final DateTimeFormatter formatter = XContentBuilder.DEFAULT_DATE_PRINTER;
        final Date d1 = new DateTime(2016, 1, 1, 0, 0, DateTimeZone.UTC).toDate();
        final Date d2 = new DateTime(2015, 1, 1, 0, 0, DateTimeZone.UTC).toDate();
        objects.put("{'objects':['" + formatter.print(d1.getTime()) + "','" + formatter.print(d2.getTime()) + "']}", new Object[]{d1, d2});

        final DateTime dt1 = DateTime.now();
        final DateTime dt2 = new DateTime(2016, 12, 25, 7, 59, 42, 213, DateTimeZone.UTC);
        objects.put("{'objects':['" + formatter.print(dt1) + "','2016-12-25T07:59:42.213Z']}", new Object[]{dt1, dt2});

        final Calendar c1 = new DateTime(2012, 7, 7, 10, 23, DateTimeZone.UTC).toCalendar(Locale.ROOT);
        final Calendar c2 = new DateTime(2014, 11, 16, 19, 36, DateTimeZone.UTC).toCalendar(Locale.ROOT);
        objects.put("{'objects':['2012-07-07T10:23:00.000Z','2014-11-16T19:36:00.000Z']}", new Object[]{c1, c2});

        final ToXContent x1 = (builder, params) -> builder.startObject().field("f1", "v1").field("f2", 2).array("f3", 3, 4, 5).endObject();
        final ToXContent x2 = (builder, params) -> builder.startObject().field("f1", "v1").field("f2", x1).endObject();
        objects.put("{'objects':[{'f1':'v1','f2':2,'f3':[3,4,5]},{'f1':'v1','f2':{'f1':'v1','f2':2,'f3':[3,4,5]}}]}", new Object[]{x1, x2});

        for (Map.Entry<String, Object[]> o : objects.entrySet()) {
            final String expected = o.getKey();
            assertResult(expected, () -> builder().startObject().field("objects", o.getValue()).endObject());
            assertResult(expected, () -> builder().startObject().field("objects").value(o.getValue()).endObject());
            assertResult(expected, () -> builder().startObject().field("objects").values(o.getValue()).endObject());
            assertResult(expected, () -> builder().startObject().array("objects", o.getValue()).endObject());
        }
    }

    public void testObject() throws Exception {
        Map<String, Object> object = new HashMap<>();
        object.put("{'object':false}", Boolean.FALSE);
        object.put("{'object':13}", (byte) 13);
        object.put("{'object':5.0}", 5.0d);
        object.put("{'object':8.0}", 8.0f);
        object.put("{'object':{'lat':45.759429931640625,'lon':4.8394775390625}}", GeoPoint.fromGeohash("u05kq4k"));
        object.put("{'object':3}", 3);
        object.put("{'object':2}", 2L);
        object.put("{'object':1}", (short) 1);
        object.put("{'object':'string'}", "string");
        object.put("{'object':'a'}", new Text("a"));
        object.put("{'object':'b'}", new Text(new BytesArray("b")));
        object.put("{'object':null}", null);
        object.put("{'object':'OPEN'}", IndexMetaData.State.OPEN);
        object.put("{'object':'NM'}", DistanceUnit.NAUTICALMILES);
        object.put("{'object':{'f1':'v1'}}", singletonMap("f1", "v1"));
        object.put("{'object':{'f1':{'f2':'v2'}}}", singletonMap("f1", singletonMap("f2", "v2")));
        object.put("{'object':[1,2,3]}", Arrays.asList(1, 2, 3));

        final String path = Constants.WINDOWS ? "{'object':'a\\\\b\\\\c'}" : "{'object':'a/b/c'}";
        object.put(path, PathUtils.get("a", "b", "c"));

        final DateTimeFormatter formatter = XContentBuilder.DEFAULT_DATE_PRINTER;
        final Date d1 = new DateTime(2016, 1, 1, 0, 0, DateTimeZone.UTC).toDate();
        object.put("{'object':'" + formatter.print(d1.getTime()) + "'}", d1);

        final DateTime d2 = DateTime.now();
        object.put("{'object':'" + formatter.print(d2) + "'}", d2);

        final Calendar c1 = new DateTime(2010, 1, 1, 0, 0, DateTimeZone.UTC).toCalendar(Locale.ROOT);
        object.put("{'object':'2010-01-01T00:00:00.000Z'}", c1);

        final ToXContent x1 = (builder, params) -> builder.startObject().field("f1", "v1").field("f2", 2).array("f3", 3, 4, 5).endObject();
        final ToXContent x2 = (builder, params) -> builder.startObject().field("f1", "v1").field("f2", x1).endObject();
        object.put("{'object':{'f1':'v1','f2':{'f1':'v1','f2':2,'f3':[3,4,5]}}}", x2);

        for (Map.Entry<String, Object> o : object.entrySet()) {
            final String expected = o.getKey();
            assertResult(expected, () -> builder().humanReadable(true).startObject().field("object", o.getValue()).endObject());
            assertResult(expected, () -> builder().humanReadable(true).startObject().field("object").value(o.getValue()).endObject());
        }

        assertResult("{'objects':[null,null,null]}", () -> builder().startObject().array("objects", null, null, null).endObject());
    }

    public void testToXContent() throws Exception {
        assertResult("{'xcontent':null}", () -> builder().startObject().field("xcontent", (ToXContent) null).endObject());
        assertResult("{'xcontent':null}", () -> builder().startObject().field("xcontent").value((ToXContent) null).endObject());

        ToXContent xcontent0 = (builder, params) -> {
            builder.startObject();
            builder.field("field", "value");
            builder.array("array", "1", "2", "3");
            builder.startObject("foo");
            builder.field("bar", "baz");
            builder.endObject();
            builder.endObject();
            return builder;
        };

        assertResult("{'field':'value','array':['1','2','3'],'foo':{'bar':'baz'}}", () -> builder().value(xcontent0));
        assertResult("{'xcontent':{'field':'value','array':['1','2','3'],'foo':{'bar':'baz'}}}", () -> builder()
                .startObject()
                    .field("xcontent", xcontent0)
                .endObject());

        ToXContent xcontent1 = (builder, params) -> {
            builder.startObject();
            builder.field("field", "value");
            builder.startObject("foo");
            builder.field("bar", "baz");
            builder.endObject();
            builder.endObject();
            return builder;
        };

        ToXContent xcontent2 = (builder, params) -> {
            builder.startObject();
            builder.field("root", xcontent0);
            builder.array("childs", xcontent0, xcontent1);
            builder.endObject();
            return builder;
        };
        assertResult("{'root':{" +
                            "'field':'value'," +
                            "'array':['1','2','3']," +
                            "'foo':{'bar':'baz'}" +
                        "}," +
                        "'childs':[" +
                            "{'field':'value','array':['1','2','3'],'foo':{'bar':'baz'}}," +
                            "{'field':'value','foo':{'bar':'baz'}}" +
                        "]}", () -> builder().value(xcontent2));
    }

    public void testMap() throws Exception {
        Map<String, Map<String, ?>> maps = new HashMap<>();
        maps.put("{'map':null}", (Map) null);
        maps.put("{'map':{}}", Collections.emptyMap());
        maps.put("{'map':{'key':'value'}}", singletonMap("key", "value"));

        Map<String, Object> innerMap = new HashMap<>();
        innerMap.put("string", "value");
        innerMap.put("int", 42);
        innerMap.put("long", 42L);
        innerMap.put("long[]", new long[]{1L, 3L});
        innerMap.put("path", PathUtils.get("path", "to", "file"));
        innerMap.put("object", singletonMap("key", "value"));

        final String path = Constants.WINDOWS ? "path\\\\to\\\\file" : "path/to/file";
        maps.put("{'map':{'path':'" + path + "','string':'value','long[]':[1,3],'int':42,'long':42,'object':{'key':'value'}}}", innerMap);

        for (Map.Entry<String, Map<String, ?>> m : maps.entrySet()) {
            final String expected = m.getKey();
            assertResult(expected, () -> builder().startObject().field("map", m.getValue()).endObject());
            assertResult(expected, () -> builder().startObject().field("map").value(m.getValue()).endObject());
            assertResult(expected, () -> builder().startObject().field("map").map(m.getValue()).endObject());
        }
    }

    public void testIterable() throws Exception {
        Map<String, Iterable<?>> iterables = new HashMap<>();
        iterables.put("{'iter':null}", (Iterable) null);
        iterables.put("{'iter':[]}", Collections.emptyList());
        iterables.put("{'iter':['a','b']}", Arrays.asList("a", "b"));

        final String path = Constants.WINDOWS ? "{'iter':'path\\\\to\\\\file'}" : "{'iter':'path/to/file'}";
        iterables.put(path, PathUtils.get("path", "to", "file"));

        final String paths = Constants.WINDOWS ? "{'iter':['a\\\\b\\\\c','c\\\\d']}" : "{'iter':['a/b/c','c/d']}";
        iterables.put(paths, Arrays.asList(PathUtils.get("a", "b", "c"), PathUtils.get("c", "d")));

        for (Map.Entry<String, Iterable<?>> i : iterables.entrySet()) {
            final String expected = i.getKey();
            assertResult(expected, () -> builder().startObject().field("iter", i.getValue()).endObject());
            assertResult(expected, () -> builder().startObject().field("iter").value(i.getValue()).endObject());
        }
    }

    public void testUnknownObject() throws Exception {
        Map<String, Object> objects = new HashMap<>();
        objects.put("{'obj':50.63}", DistanceUnit.METERS.fromMeters(50.63));
        objects.put("{'obj':'MINUTES'}", TimeUnit.MINUTES);
        objects.put("{'obj':'class org.elasticsearch.common.xcontent.BaseXContentTestCase'}", BaseXContentTestCase.class);

        for (Map.Entry<String, ?> o : objects.entrySet()) {
            final String expected = o.getKey();
            assertResult(expected, () -> builder().startObject().field("obj", o.getValue()).endObject());
            assertResult(expected, () -> builder().startObject().field("obj").value(o.getValue()).endObject());
        }
    }

    public void testBasics() throws IOException {
        ByteArrayOutputStream os = new ByteArrayOutputStream();
        try (XContentGenerator generator = xcontentType().xContent().createGenerator(os)) {
            generator.writeStartObject();
            generator.writeEndObject();
        }
        byte[] data = os.toByteArray();
        assertEquals(xcontentType(), XContentFactory.xContentType(data));
    }

    public void testMissingEndObject() throws IOException {
        IOException e = expectThrows(IOException.class, () -> {
            ByteArrayOutputStream os = new ByteArrayOutputStream();
            try (XContentGenerator generator = xcontentType().xContent().createGenerator(os)) {
                generator.writeStartObject();
                generator.writeFieldName("foo");
                generator.writeNumber(2L);
            }
        });
        assertEquals(e.getMessage(), "Unclosed object or array found");
    }

    public void testMissingEndArray() throws IOException {
        IOException e = expectThrows(IOException.class, () -> {
            ByteArrayOutputStream os = new ByteArrayOutputStream();
            try (XContentGenerator generator = xcontentType().xContent().createGenerator(os)) {
                generator.writeStartArray();
                generator.writeNumber(2L);
            }
        });
        assertEquals(e.getMessage(), "Unclosed object or array found");
    }

    public void testRawField() throws Exception {
        for (boolean useStream : new boolean[]{false, true}) {
            for (XContentType xcontentType : XContentType.values()) {
                doTestRawField(xcontentType.xContent(), useStream);
            }
        }
    }

    void doTestRawField(XContent source, boolean useStream) throws Exception {
        ByteArrayOutputStream os = new ByteArrayOutputStream();
        try (XContentGenerator generator = source.createGenerator(os)) {
            generator.writeStartObject();
            generator.writeFieldName("foo");
            generator.writeNull();
            generator.writeEndObject();
        }
        final byte[] rawData = os.toByteArray();

        os = new ByteArrayOutputStream();
        try (XContentGenerator generator = xcontentType().xContent().createGenerator(os)) {
            generator.writeStartObject();
            if (useStream) {
                generator.writeRawField("bar", new ByteArrayInputStream(rawData));
            } else {
                generator.writeRawField("bar", new BytesArray(rawData));
            }
            generator.writeEndObject();
        }

        XContentParser parser = xcontentType().xContent().createParser(NamedXContentRegistry.EMPTY, os.toByteArray());
        assertEquals(Token.START_OBJECT, parser.nextToken());
        assertEquals(Token.FIELD_NAME, parser.nextToken());
        assertEquals("bar", parser.currentName());
        assertEquals(Token.START_OBJECT, parser.nextToken());
        assertEquals(Token.FIELD_NAME, parser.nextToken());
        assertEquals("foo", parser.currentName());
        assertEquals(Token.VALUE_NULL, parser.nextToken());
        assertEquals(Token.END_OBJECT, parser.nextToken());
        assertEquals(Token.END_OBJECT, parser.nextToken());
        assertNull(parser.nextToken());
    }

    public void testRawValue() throws Exception {
        for (XContentType xcontentType : XContentType.values()) {
            doTestRawValue(xcontentType.xContent());
        }
    }

    void doTestRawValue(XContent source) throws Exception {
        ByteArrayOutputStream os = new ByteArrayOutputStream();
        try (XContentGenerator generator = source.createGenerator(os)) {
            generator.writeStartObject();
            generator.writeFieldName("foo");
            generator.writeNull();
            generator.writeEndObject();
        }
        final byte[] rawData = os.toByteArray();

        os = new ByteArrayOutputStream();
        try (XContentGenerator generator = xcontentType().xContent().createGenerator(os)) {
            generator.writeRawValue(new BytesArray(rawData));
        }

        XContentParser parser = xcontentType().xContent().createParser(NamedXContentRegistry.EMPTY, os.toByteArray());
        assertEquals(Token.START_OBJECT, parser.nextToken());
        assertEquals(Token.FIELD_NAME, parser.nextToken());
        assertEquals("foo", parser.currentName());
        assertEquals(Token.VALUE_NULL, parser.nextToken());
        assertEquals(Token.END_OBJECT, parser.nextToken());
        assertNull(parser.nextToken());

        os = new ByteArrayOutputStream();
        try (XContentGenerator generator = xcontentType().xContent().createGenerator(os)) {
            generator.writeStartObject();
            generator.writeFieldName("test");
            generator.writeRawValue(new BytesArray(rawData));
            generator.writeEndObject();
        }

        parser = xcontentType().xContent().createParser(NamedXContentRegistry.EMPTY, os.toByteArray());
        assertEquals(Token.START_OBJECT, parser.nextToken());
        assertEquals(Token.FIELD_NAME, parser.nextToken());
        assertEquals("test", parser.currentName());
        assertEquals(Token.START_OBJECT, parser.nextToken());
        assertEquals(Token.FIELD_NAME, parser.nextToken());
        assertEquals("foo", parser.currentName());
        assertEquals(Token.VALUE_NULL, parser.nextToken());
        assertEquals(Token.END_OBJECT, parser.nextToken());
        assertEquals(Token.END_OBJECT, parser.nextToken());
        assertNull(parser.nextToken());

    }

    protected void doTestBigInteger(JsonGenerator generator, ByteArrayOutputStream os) throws Exception {
        // Big integers cannot be handled explicitly, but if some values happen to be big ints,
        // we can still call parser.map() and get the bigint value so that eg. source filtering
        // keeps working
        BigInteger bigInteger = BigInteger.valueOf(Long.MAX_VALUE).add(BigInteger.ONE);
        generator.writeStartObject();
        generator.writeFieldName("foo");
        generator.writeString("bar");
        generator.writeFieldName("bigint");
        generator.writeNumber(bigInteger);
        generator.writeEndObject();
        generator.flush();
        byte[] serialized = os.toByteArray();

        XContentParser parser = xcontentType().xContent().createParser(NamedXContentRegistry.EMPTY, serialized);
        Map<String, Object> map = parser.map();
        assertEquals("bar", map.get("foo"));
        assertEquals(bigInteger, map.get("bigint"));
    }

    public void testEnsureNameNotNull() {
        IllegalArgumentException e = expectThrows(IllegalArgumentException.class, () -> XContentBuilder.ensureNameNotNull(null));
        assertThat(e.getMessage(), containsString("Field name cannot be null"));
    }

    public void testFormatterNameNotNull() {
        IllegalArgumentException e = expectThrows(IllegalArgumentException.class, () -> XContentBuilder.ensureFormatterNotNull(null));
        assertThat(e.getMessage(), containsString("DateTimeFormatter cannot be null"));
    }

    public void testEnsureNotNull() {
        IllegalArgumentException e = expectThrows(IllegalArgumentException.class, () -> XContentBuilder.ensureNotNull(null, "message"));
        assertThat(e.getMessage(), containsString("message"));

        XContentBuilder.ensureNotNull("foo", "No exception must be thrown");
    }

    public void testEnsureNoSelfReferences() throws IOException {
        XContentBuilder.ensureNoSelfReferences(emptyMap());
        XContentBuilder.ensureNoSelfReferences(null);

        Map<String, Object> map = new HashMap<>();
        map.put("field", map);

        IllegalArgumentException e = expectThrows(IllegalArgumentException.class, () -> builder().map(map));
        assertThat(e.getMessage(), containsString("Object has already been built and is self-referencing itself"));
    }

    /**
     * Test that the same map written multiple times do not trigger the self-reference check in
     * {@link XContentBuilder#ensureNoSelfReferences(Object)}
     */
    public void testRepeatedMapsAndNoSelfReferences() throws Exception {
        Map<String, Object> mapB = singletonMap("b", "B");
        Map<String, Object> mapC = singletonMap("c", "C");
        Map<String, Object> mapD = singletonMap("d", "D");
        Map<String, Object> mapA = new HashMap<>();
        mapA.put("a", 0);
        mapA.put("b1", mapB);
        mapA.put("b2", mapB);
        mapA.put("c", Arrays.asList(mapC, mapC));
        mapA.put("d1", mapD);
        mapA.put("d2", singletonMap("d3", mapD));

        final String expected =
                "{'map':{'b2':{'b':'B'},'a':0,'c':[{'c':'C'},{'c':'C'}],'d1':{'d':'D'},'d2':{'d3':{'d':'D'}},'b1':{'b':'B'}}}";

        assertResult(expected, () -> builder().startObject().field("map", mapA).endObject());
        assertResult(expected, () -> builder().startObject().field("map").value(mapA).endObject());
        assertResult(expected, () -> builder().startObject().field("map").map(mapA).endObject());
    }

    public void testSelfReferencingMapsOneLevel() throws IOException {
        Map<String, Object> map0 = new HashMap<>();
        Map<String, Object> map1 = new HashMap<>();

        map0.put("foo", 0);
        map0.put("map1", map1); // map 0 -> map 1

        map1.put("bar", 1);
        map1.put("map0", map0); // map 1 -> map 0 loop

        IllegalArgumentException e = expectThrows(IllegalArgumentException.class, () -> builder().map(map0));
        assertThat(e.getMessage(), containsString("Object has already been built and is self-referencing itself"));
    }

    public void testSelfReferencingMapsTwoLevels() throws IOException {
        Map<String, Object> map0 = new HashMap<>();
        Map<String, Object> map1 = new HashMap<>();
        Map<String, Object> map2 = new HashMap<>();

        map0.put("foo", 0);
        map0.put("map1", map1); // map 0 -> map 1

        map1.put("bar", 1);
        map1.put("map2", map2); // map 1 -> map 2

        map2.put("baz", 2);
        map2.put("map0", map0); // map 2 -> map 0 loop

        IllegalArgumentException e = expectThrows(IllegalArgumentException.class, () -> builder().map(map0));
        assertThat(e.getMessage(), containsString("Object has already been built and is self-referencing itself"));
    }

    public void testSelfReferencingObjectsArray() throws IOException {
        Object[] values = new Object[3];
        values[0] = 0;
        values[1] = 1;
        values[2] = values;

        IllegalArgumentException e = expectThrows(IllegalArgumentException.class, () -> builder()
                .startObject()
                .field("field", values)
                .endObject());
        assertThat(e.getMessage(), containsString("Object has already been built and is self-referencing itself"));

        e = expectThrows(IllegalArgumentException.class, () -> builder()
                .startObject()
                .array("field", values)
                .endObject());
        assertThat(e.getMessage(), containsString("Object has already been built and is self-referencing itself"));
    }

    public void testSelfReferencingIterable() throws IOException {
        List<Object> values = new ArrayList<>();
        values.add("foo");
        values.add("bar");
        values.add(values);

        IllegalArgumentException e = expectThrows(IllegalArgumentException.class, () -> builder()
                .startObject()
                .field("field", (Iterable) values)
                .endObject());
        assertThat(e.getMessage(), containsString("Object has already been built and is self-referencing itself"));
    }

    public void testSelfReferencingIterableOneLevel() throws IOException {
        Map<String, Object> map = new HashMap<>();
        map.put("foo", 0);
        map.put("bar", 1);

        Iterable<Object> values = Arrays.asList("one", "two", map);
        map.put("baz", values);

        IllegalArgumentException e = expectThrows(IllegalArgumentException.class, () -> builder()
                .startObject()
                .field("field", (Iterable) values)
                .endObject());
        assertThat(e.getMessage(), containsString("Object has already been built and is self-referencing itself"));
    }

    public void testSelfReferencingIterableTwoLevels() throws IOException {
        Map<String, Object> map0 = new HashMap<>();
        Map<String, Object> map1 = new HashMap<>();
        Map<String, Object> map2 = new HashMap<>();

        List<Object> it1 = new ArrayList<>();

        map0.put("foo", 0);
        map0.put("it1", (Iterable<?>) it1); // map 0 -> it1

        it1.add(map1);
        it1.add(map2); // it 1 -> map 1, map 2

        map2.put("baz", 2);
        map2.put("map0", map0); // map 2 -> map 0 loop

        IllegalArgumentException e = expectThrows(IllegalArgumentException.class, () -> builder().map(map0));
        assertThat(e.getMessage(), containsString("Object has already been built and is self-referencing itself"));
    }

    public void testChecksForDuplicates() throws Exception {
        assumeTrue("Test only makes sense if XContent parser has strict duplicate checks enabled",
            XContent.isStrictDuplicateDetectionEnabled());

        XContentBuilder builder = builder()
                .startObject()
                    .field("key", 1)
                    .field("key", 2)
                .endObject();

        JsonParseException pex = expectThrows(JsonParseException.class, () -> createParser(builder).map());
        assertThat(pex.getMessage(), startsWith("Duplicate field 'key'"));
    }

    public void testNamedObject() throws IOException {
        Object test1 = new Object();
        Object test2 = new Object();
        NamedXContentRegistry registry = new NamedXContentRegistry(Arrays.asList(
                new NamedXContentRegistry.Entry(Object.class, new ParseField("test1"), p -> test1),
                new NamedXContentRegistry.Entry(Object.class, new ParseField("test2", "deprecated"), p -> test2),
                new NamedXContentRegistry.Entry(Object.class, new ParseField("str"), p -> p.text())));
        XContentBuilder b = XContentBuilder.builder(xcontentType().xContent());
        b.value("test");
        XContentParser p = xcontentType().xContent().createParser(registry, b.bytes());
        assertEquals(test1, p.namedObject(Object.class, "test1", null));
        assertEquals(test2, p.namedObject(Object.class, "test2", null));
        assertEquals(test2, p.namedObject(Object.class, "deprecated", null));
        assertWarnings("Deprecated field [deprecated] used, expected [test2] instead");
        {
            p.nextToken();
            assertEquals("test", p.namedObject(Object.class, "str", null));
            NamedXContentRegistry.UnknownNamedObjectException e = expectThrows(NamedXContentRegistry.UnknownNamedObjectException.class,
                    () -> p.namedObject(Object.class, "unknown", null));
            assertEquals("Unknown Object [unknown]", e.getMessage());
            assertEquals("java.lang.Object", e.getCategoryClass());
            assertEquals("unknown", e.getName());
        }
        {
            Exception e = expectThrows(ElasticsearchException.class, () -> p.namedObject(String.class, "doesn't matter", null));
            assertEquals("Unknown namedObject category [java.lang.String]", e.getMessage());
        }
        {
            XContentParser emptyRegistryParser = xcontentType().xContent().createParser(NamedXContentRegistry.EMPTY, new byte[] {});
            Exception e = expectThrows(ElasticsearchException.class,
                    () -> emptyRegistryParser.namedObject(String.class, "doesn't matter", null));
            assertEquals("namedObject is not supported for this parser", e.getMessage());
        }
    }

    private static void expectUnclosedException(ThrowingRunnable runnable) {
        IllegalStateException e = expectThrows(IllegalStateException.class, runnable);
        assertThat(e.getMessage(), containsString("Failed to close the XContentBuilder"));
        assertThat(e.getCause(), allOf(notNullValue(), instanceOf(IOException.class)));
        assertThat(e.getCause().getMessage(), containsString("Unclosed object or array found"));
    }

    private static void expectValueException(ThrowingRunnable runnable) {
        JsonGenerationException e = expectThrows(JsonGenerationException.class, runnable);
        assertThat(e.getMessage(), containsString("expecting a value"));
    }

    private static void expectFieldException(ThrowingRunnable runnable) {
        JsonGenerationException e = expectThrows(JsonGenerationException.class, runnable);
        assertThat(e.getMessage(), containsString("expecting field name"));
    }

    private static void expectNonNullFieldException(ThrowingRunnable runnable) {
        IllegalArgumentException e = expectThrows(IllegalArgumentException.class, runnable);
        assertThat(e.getMessage(), containsString("Field name cannot be null"));
    }

    private static void expectNonNullFormatterException(ThrowingRunnable runnable) {
        IllegalArgumentException e = expectThrows(IllegalArgumentException.class, runnable);
        assertThat(e.getMessage(), containsString("DateTimeFormatter cannot be null"));
    }

    private static void expectObjectException(ThrowingRunnable runnable) {
        JsonGenerationException e = expectThrows(JsonGenerationException.class, runnable);
        assertThat(e.getMessage(), containsString("Current context not Object"));
    }

    private static void expectArrayException(ThrowingRunnable runnable) {
        JsonGenerationException e = expectThrows(JsonGenerationException.class, runnable);
        assertThat(e.getMessage(), containsString("Current context not Array"));
    }

    public static Matcher<String> equalToJson(String json) {
        return Matchers.equalTo(json.replace("'", "\""));
    }

    private static void assertResult(String expected, Builder builder) throws IOException {
        // Build the XContentBuilder, convert its bytes to JSON and check it matches
        assertThat(XContentHelper.convertToJson(builder.build().bytes(), randomBoolean()), equalToJson(expected));
    }

    private static byte[] randomBytes() throws Exception {
        return randomUnicodeOfLength(scaledRandomIntBetween(10, 1000)).getBytes("UTF-8");
    }

    @FunctionalInterface
    private interface Builder {
        XContentBuilder build() throws IOException;
    }
}
