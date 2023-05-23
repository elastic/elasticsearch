/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.common.xcontent;

import org.apache.lucene.util.BytesRef;
import org.apache.lucene.util.Constants;
import org.elasticsearch.cluster.metadata.IndexMetadata;
import org.elasticsearch.common.Strings;
import org.elasticsearch.common.bytes.BytesArray;
import org.elasticsearch.common.bytes.BytesReference;
import org.elasticsearch.common.geo.GeoPoint;
import org.elasticsearch.common.text.Text;
import org.elasticsearch.common.unit.DistanceUnit;
import org.elasticsearch.core.PathUtils;
import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.xcontent.NamedObjectNotFoundException;
import org.elasticsearch.xcontent.NamedXContentRegistry;
import org.elasticsearch.xcontent.ParseField;
import org.elasticsearch.xcontent.ToXContent;
import org.elasticsearch.xcontent.ToXContentObject;
import org.elasticsearch.xcontent.XContent;
import org.elasticsearch.xcontent.XContentBuilder;
import org.elasticsearch.xcontent.XContentFactory;
import org.elasticsearch.xcontent.XContentGenerationException;
import org.elasticsearch.xcontent.XContentGenerator;
import org.elasticsearch.xcontent.XContentParseException;
import org.elasticsearch.xcontent.XContentParser;
import org.elasticsearch.xcontent.XContentParser.Token;
import org.elasticsearch.xcontent.XContentParserConfiguration;
import org.elasticsearch.xcontent.XContentType;
import org.hamcrest.Matcher;
import org.hamcrest.Matchers;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.math.BigDecimal;
import java.math.BigInteger;
import java.nio.file.Path;
import java.time.DayOfWeek;
import java.time.Duration;
import java.time.LocalDate;
import java.time.LocalDateTime;
import java.time.LocalTime;
import java.time.Month;
import java.time.MonthDay;
import java.time.OffsetDateTime;
import java.time.OffsetTime;
import java.time.Period;
import java.time.Year;
import java.time.ZoneOffset;
import java.time.ZonedDateTime;
import java.time.format.DateTimeFormatter;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Calendar;
import java.util.Collections;
import java.util.Date;
import java.util.GregorianCalendar;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;

import static java.util.Collections.emptyMap;
import static java.util.Collections.singletonMap;
import static org.hamcrest.Matchers.allOf;
import static org.hamcrest.Matchers.containsInAnyOrder;
import static org.hamcrest.Matchers.containsString;
import static org.hamcrest.Matchers.endsWith;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.instanceOf;
import static org.hamcrest.Matchers.notNullValue;

public abstract class BaseXContentTestCase extends ESTestCase {

    protected abstract XContentType xcontentType();

    protected XContentBuilder builder() throws IOException {
        return XContentBuilder.builder(xcontentType().xContent());
    }

    public void testContentType() throws IOException {
        assertThat(builder().contentType(), equalTo(xcontentType()));
    }

    public void testStartEndObject() throws IOException {
        expectUnclosedException(() -> BytesReference.bytes(builder().startObject()));
        expectUnclosedException(() -> builder().startObject().close());
        expectUnclosedException(() -> Strings.toString(builder().startObject()));

        expectObjectException(() -> BytesReference.bytes(builder().endObject()));
        expectObjectException(() -> builder().endObject().close());
        expectObjectException(() -> Strings.toString(builder().endObject()));

        expectValueException(() -> builder().startObject("foo").endObject());
        expectNonNullFieldException(() -> builder().startObject().startObject(null));

        assertResult("{}", () -> builder().startObject().endObject());
        assertResult("{'foo':{}}", () -> builder().startObject().startObject("foo").endObject().endObject());

        assertResult(
            "{'foo':{'bar':{}}}",
            () -> builder().startObject().startObject("foo").startObject("bar").endObject().endObject().endObject()
        );
    }

    public void testStartEndArray() throws IOException {
        expectUnclosedException(() -> BytesReference.bytes(builder().startArray()));
        expectUnclosedException(() -> builder().startArray().close());
        expectUnclosedException(() -> Strings.toString(builder().startArray()));

        expectArrayException(() -> BytesReference.bytes(builder().endArray()));
        expectArrayException(() -> builder().endArray().close());
        expectArrayException(() -> Strings.toString(builder().endArray()));

        expectValueException(() -> builder().startArray("foo").endObject());
        expectFieldException(() -> builder().startObject().startArray().endArray().endObject());
        expectNonNullFieldException(() -> builder().startObject().startArray(null).endArray().endObject());

        assertResult("{'foo':[]}", () -> builder().startObject().startArray("foo").endArray().endObject());
        assertResult("{'foo':[1,2,3]}", () -> builder().startObject().startArray("foo").value(1).value(2).value(3).endArray().endObject());
    }

    public void testField() throws IOException {
        expectValueException(() -> BytesReference.bytes(builder().field("foo")));
        expectNonNullFieldException(() -> BytesReference.bytes(builder().field(null)));
        expectUnclosedException(() -> BytesReference.bytes(builder().startObject().field("foo")));

        assertResult("{'foo':'bar'}", () -> builder().startObject().field("foo").value("bar").endObject());
    }

    public void testNullField() throws IOException {
        expectValueException(() -> BytesReference.bytes(builder().nullField("foo")));
        expectNonNullFieldException(() -> BytesReference.bytes(builder().nullField(null)));
        expectUnclosedException(() -> BytesReference.bytes(builder().startObject().nullField("foo")));

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
        assertResult("{'boolean':[false,true]}", () -> builder().startObject().array("boolean", new boolean[] { false, true }).endObject());
        assertResult(
            "{'boolean':[false,true]}",
            () -> builder().startObject().field("boolean").value(new boolean[] { false, true }).endObject()
        );
        assertResult("{'boolean':null}", () -> builder().startObject().array("boolean", (boolean[]) null).endObject());
        assertResult("{'boolean':[]}", () -> builder().startObject().array("boolean", new boolean[] {}).endObject());
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
        assertResult("{'double':[]}", () -> builder().startObject().array("double", new double[] {}).endObject());
        assertResult("{'double':null}", () -> builder().startObject().field("double").value((Double) null).endObject());
        assertResult("{'double':0.001}", () -> builder().startObject().field("double").value(0.001).endObject());
        assertResult(
            "{'double':[1.7976931348623157E308,4.9E-324]}",
            () -> builder().startObject().array("double", new double[] { Double.MAX_VALUE, Double.MIN_VALUE }).endObject()
        );
    }

    public void testFloats() throws IOException {
        assertResult("{'float':null}", () -> builder().startObject().field("float", (Float) null).endObject());
        assertResult("{'float':42.5}", () -> builder().startObject().field("float", Float.valueOf(42.5f)).endObject());
        assertResult("{'float':1.2}", () -> builder().startObject().field("float", 1.2f).endObject());
        assertResult("{'float':null}", () -> builder().startObject().array("float", (float[]) null).endObject());
        assertResult("{'float':[]}", () -> builder().startObject().array("float", new float[] {}).endObject());
        assertResult("{'float':null}", () -> builder().startObject().field("float").value((Float) null).endObject());
        assertResult("{'float':9.9E-7}", () -> builder().startObject().field("float").value(0.00000099f).endObject());
        assertResult(
            "{'float':[42.0,43.0,45.666668]}",
            () -> builder().startObject().array("float", 42.0f, 43.0f, 45.66666667f).endObject()
        );
        assertResult(
            "{'float':[3.4028235E38,1.4E-45]}",
            () -> builder().startObject().array("float", new float[] { Float.MAX_VALUE, Float.MIN_VALUE }).endObject()
        );
    }

    public void testIntegers() throws IOException {
        assertResult("{'integer':null}", () -> builder().startObject().field("integer", (Integer) null).endObject());
        assertResult("{'integer':42}", () -> builder().startObject().field("integer", Integer.valueOf(42)).endObject());
        assertResult("{'integer':3}", () -> builder().startObject().field("integer", 3).endObject());
        assertResult("{'integer':[1,3,5,7,11]}", () -> builder().startObject().array("integer", 1, 3, 5, 7, 11).endObject());
        assertResult("{'integer':null}", () -> builder().startObject().array("integer", (int[]) null).endObject());
        assertResult("{'integer':[]}", () -> builder().startObject().array("integer", new int[] {}).endObject());
        assertResult("{'integer':null}", () -> builder().startObject().field("integer").value((Integer) null).endObject());
        assertResult("{'integer':42}", () -> builder().startObject().field("integer").value(42).endObject());
        assertResult(
            "{'integer':[2147483647,-2147483648]}",
            () -> builder().startObject().array("integer", new int[] { Integer.MAX_VALUE, Integer.MIN_VALUE }).endObject()
        );
    }

    public void testLongs() throws IOException {
        assertResult("{'long':null}", () -> builder().startObject().field("long", (Long) null).endObject());
        assertResult("{'long':42}", () -> builder().startObject().field("long", Long.valueOf(42L)).endObject());
        assertResult("{'long':9223372036854775807}", () -> builder().startObject().field("long", 9_223_372_036_854_775_807L).endObject());
        assertResult("{'long':[1,3,5,7,11]}", () -> builder().startObject().array("long", 1L, 3L, 5L, 7L, 11L).endObject());
        assertResult("{'long':null}", () -> builder().startObject().array("long", (long[]) null).endObject());
        assertResult("{'long':[]}", () -> builder().startObject().array("long", new long[] {}).endObject());
        assertResult("{'long':null}", () -> builder().startObject().field("long").value((Long) null).endObject());
        assertResult("{'long':42}", () -> builder().startObject().field("long").value(42).endObject());
        assertResult(
            "{'long':[2147483647,-2147483648]}",
            () -> builder().startObject().array("long", new long[] { Integer.MAX_VALUE, Integer.MIN_VALUE }).endObject()
        );
    }

    public void testShorts() throws IOException {
        assertResult("{'short':null}", () -> builder().startObject().field("short", (Short) null).endObject());
        assertResult("{'short':5000}", () -> builder().startObject().field("short", Short.valueOf((short) 5000)).endObject());
        assertResult("{'short':null}", () -> builder().startObject().array("short", (short[]) null).endObject());
        assertResult("{'short':[]}", () -> builder().startObject().array("short", new short[] {}).endObject());
        assertResult("{'short':null}", () -> builder().startObject().field("short").value((Short) null).endObject());
        assertResult("{'short':42}", () -> builder().startObject().field("short").value((short) 42).endObject());
        assertResult(
            "{'short':[1,3,5,7,11]}",
            () -> builder().startObject().array("short", (short) 1, (short) 3, (short) 5, (short) 7, (short) 11).endObject()
        );
        assertResult(
            "{'short':[32767,-32768]}",
            () -> builder().startObject().array("short", new short[] { Short.MAX_VALUE, Short.MIN_VALUE }).endObject()
        );
    }

    public void testBigIntegers() throws Exception {
        assertResult("{'bigint':null}", () -> builder().startObject().field("bigint", (BigInteger) null).endObject());
        assertResult("{'bigint':[]}", () -> builder().startObject().array("bigint", new BigInteger[] {}).endObject());

        BigInteger bigInteger = BigInteger.valueOf(Long.MAX_VALUE).add(BigInteger.ONE);
        String result = "{'bigint':" + bigInteger.toString() + "}";
        assertResult(result, () -> builder().startObject().field("bigint", bigInteger).endObject());

        result = "{'bigint':[" + bigInteger.toString() + "," + bigInteger.toString() + "," + bigInteger.toString() + "]}";
        assertResult(result, () -> builder().startObject().array("bigint", bigInteger, bigInteger, bigInteger).endObject());
    }

    public void testBigDecimals() throws Exception {
        assertResult("{'bigdecimal':null}", () -> builder().startObject().field("bigdecimal", (BigInteger) null).endObject());
        assertResult("{'bigdecimal':[]}", () -> builder().startObject().array("bigdecimal", new BigInteger[] {}).endObject());

        BigDecimal bigDecimal = new BigDecimal("234.43");
        String result = "{'bigdecimal':" + bigDecimal.toString() + "}";
        assertResult(result, () -> builder().startObject().field("bigdecimal", bigDecimal).endObject());

        result = "{'bigdecimal':[" + bigDecimal.toString() + "," + bigDecimal.toString() + "," + bigDecimal.toString() + "]}";
        assertResult(result, () -> builder().startObject().array("bigdecimal", bigDecimal, bigDecimal, bigDecimal).endObject());
    }

    public void testStrings() throws IOException {
        assertResult("{'string':null}", () -> builder().startObject().field("string", (String) null).endObject());
        assertResult("{'string':'value'}", () -> builder().startObject().field("string", "value").endObject());
        assertResult("{'string':''}", () -> builder().startObject().field("string", "").endObject());
        assertResult("{'string':null}", () -> builder().startObject().array("string", (String[]) null).endObject());
        assertResult("{'string':[]}", () -> builder().startObject().array("string", Strings.EMPTY_ARRAY).endObject());
        assertResult("{'string':null}", () -> builder().startObject().field("string").value((String) null).endObject());
        assertResult("{'string':'42'}", () -> builder().startObject().field("string").value("42").endObject());
        assertResult("{'string':['a','b','c','d']}", () -> builder().startObject().array("string", "a", "b", "c", "d").endObject());
    }

    public void testBinaryField() throws Exception {
        assertResult("{'binary':null}", () -> builder().startObject().field("binary", (byte[]) null).endObject());

        final byte[] randomBytes = randomBytes();
        BytesReference bytes = BytesReference.bytes(builder().startObject().field("binary", randomBytes).endObject());

        try (XContentParser parser = createParser(xcontentType().xContent(), bytes)) {
            assertSame(parser.nextToken(), Token.START_OBJECT);
            assertSame(parser.nextToken(), Token.FIELD_NAME);
            assertEquals(parser.currentName(), "binary");
            assertTrue(parser.nextToken().isValue());
            assertArrayEquals(randomBytes, parser.binaryValue());
            assertSame(parser.nextToken(), Token.END_OBJECT);
            assertNull(parser.nextToken());
        }
    }

    public void testBinaryValue() throws Exception {
        assertResult("{'binary':null}", () -> builder().startObject().field("binary").value((byte[]) null).endObject());

        final byte[] randomBytes = randomBytes();
        BytesReference bytes = BytesReference.bytes(builder().startObject().field("binary").value(randomBytes).endObject());

        try (XContentParser parser = createParser(xcontentType().xContent(), bytes)) {
            assertSame(parser.nextToken(), Token.START_OBJECT);
            assertSame(parser.nextToken(), Token.FIELD_NAME);
            assertEquals(parser.currentName(), "binary");
            assertTrue(parser.nextToken().isValue());
            assertArrayEquals(randomBytes, parser.binaryValue());
            assertSame(parser.nextToken(), Token.END_OBJECT);
            assertNull(parser.nextToken());
        }
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

        try (XContentParser parser = createParser(xcontentType().xContent(), BytesReference.bytes(builder))) {
            assertSame(parser.nextToken(), Token.START_OBJECT);
            assertSame(parser.nextToken(), Token.FIELD_NAME);
            assertEquals(parser.currentName(), "bin");
            assertTrue(parser.nextToken().isValue());
            assertArrayEquals(Arrays.copyOfRange(randomBytes, offset, offset + length), parser.binaryValue());
            assertSame(parser.nextToken(), Token.END_OBJECT);
            assertNull(parser.nextToken());
        }
    }

    public void testBinaryUTF8() throws Exception {
        assertResult("{'utf8':null}", () -> builder().startObject().nullField("utf8").endObject());

        final BytesRef randomBytesRef = new BytesRef(randomBytes());
        XContentBuilder builder = builder().startObject();
        builder.field("utf8").utf8Value(randomBytesRef.bytes, randomBytesRef.offset, randomBytesRef.length);
        builder.endObject();

        try (XContentParser parser = createParser(xcontentType().xContent(), BytesReference.bytes(builder))) {
            assertSame(parser.nextToken(), Token.START_OBJECT);
            assertSame(parser.nextToken(), Token.FIELD_NAME);
            assertEquals(parser.currentName(), "utf8");
            assertTrue(parser.nextToken().isValue());
            assertThat(new BytesRef(parser.charBuffer()).utf8ToString(), equalTo(randomBytesRef.utf8ToString()));
            assertSame(parser.nextToken(), Token.END_OBJECT);
            assertNull(parser.nextToken());
        }
    }

    public void testText() throws Exception {
        assertResult("{'text':null}", () -> builder().startObject().field("text", (Text) null).endObject());
        assertResult("{'text':''}", () -> builder().startObject().field("text", new Text("")).endObject());
        assertResult("{'text':'foo bar'}", () -> builder().startObject().field("text", new Text("foo bar")).endObject());

        final BytesReference random = new BytesArray(randomBytes());
        XContentBuilder builder = builder().startObject().field("text", new Text(random)).endObject();

        try (XContentParser parser = createParser(xcontentType().xContent(), BytesReference.bytes(builder))) {
            assertSame(parser.nextToken(), Token.START_OBJECT);
            assertSame(parser.nextToken(), Token.FIELD_NAME);
            assertEquals(parser.currentName(), "text");
            assertTrue(parser.nextToken().isValue());
            assertThat(new BytesRef(parser.charBuffer()).utf8ToString(), equalTo(random.utf8ToString()));
            assertSame(parser.nextToken(), Token.END_OBJECT);
            assertNull(parser.nextToken());
        }
    }

    public void testDate() throws Exception {
        assertResult("{'date':null}", () -> builder().startObject().timeField("date", (Date) null).endObject());
        assertResult("{'date':null}", () -> builder().startObject().field("date").timeValue((Date) null).endObject());

        final Date d1 = Date.from(ZonedDateTime.of(2016, 1, 1, 0, 0, 0, 0, ZoneOffset.UTC).toInstant());
        assertResult("{'d1':'2016-01-01T00:00:00.000Z'}", () -> builder().startObject().timeField("d1", d1).endObject());
        assertResult("{'d1':'2016-01-01T00:00:00.000Z'}", () -> builder().startObject().field("d1").timeValue(d1).endObject());

        final Date d2 = Date.from(ZonedDateTime.of(2016, 12, 25, 7, 59, 42, 213000000, ZoneOffset.UTC).toInstant());
        assertResult("{'d2':'2016-12-25T07:59:42.213Z'}", () -> builder().startObject().timeField("d2", d2).endObject());
        assertResult("{'d2':'2016-12-25T07:59:42.213Z'}", () -> builder().startObject().field("d2").timeValue(d2).endObject());
    }

    public void testDateField() throws Exception {
        final Date d = Date.from(ZonedDateTime.of(2016, 1, 1, 0, 0, 0, 0, ZoneOffset.UTC).toInstant());

        assertResult(
            "{'date_in_millis':1451606400000}",
            () -> builder().startObject().timeField("date_in_millis", "date", d.getTime()).endObject()
        );
        assertResult(
            "{'date':'2016-01-01T00:00:00.000Z','date_in_millis':1451606400000}",
            () -> builder().humanReadable(true).startObject().timeField("date_in_millis", "date", d.getTime()).endObject()
        );
    }

    public void testCalendar() throws Exception {
        Calendar calendar = GregorianCalendar.from(ZonedDateTime.of(2016, 1, 1, 0, 0, 0, 0, ZoneOffset.UTC));
        assertResult(
            "{'calendar':'2016-01-01T00:00:00.000Z'}",
            () -> builder().startObject().field("calendar").timeValue(calendar).endObject()
        );
    }

    public void testJavaTime() throws Exception {
        final ZonedDateTime d1 = ZonedDateTime.of(2016, 1, 1, 0, 0, 0, 0, ZoneOffset.UTC);

        // ZonedDateTime
        assertResult("{'date':null}", () -> builder().startObject().timeField("date", (ZonedDateTime) null).endObject());
        assertResult("{'date':null}", () -> builder().startObject().field("date").timeValue((ZonedDateTime) null).endObject());
        assertResult("{'date':null}", () -> builder().startObject().field("date", (ZonedDateTime) null).endObject());
        assertResult("{'d1':'2016-01-01T00:00:00.000Z'}", () -> builder().startObject().timeField("d1", d1).endObject());
        assertResult("{'d1':'2016-01-01T00:00:00.000Z'}", () -> builder().startObject().field("d1").timeValue(d1).endObject());
        assertResult("{'d1':'2016-01-01T00:00:00.000Z'}", () -> builder().startObject().field("d1", d1).endObject());

        // Instant
        assertResult("{'date':null}", () -> builder().startObject().timeField("date", (java.time.Instant) null).endObject());
        assertResult("{'date':null}", () -> builder().startObject().field("date").timeValue((java.time.Instant) null).endObject());
        assertResult("{'date':null}", () -> builder().startObject().field("date", (java.time.Instant) null).endObject());
        assertResult("{'d1':'2016-01-01T00:00:00.000Z'}", () -> builder().startObject().timeField("d1", d1.toInstant()).endObject());
        assertResult("{'d1':'2016-01-01T00:00:00.000Z'}", () -> builder().startObject().field("d1").timeValue(d1.toInstant()).endObject());
        assertResult("{'d1':'2016-01-01T00:00:00.000Z'}", () -> builder().startObject().field("d1", d1.toInstant()).endObject());

        // LocalDateTime (no time zone)
        assertResult("{'date':null}", () -> builder().startObject().timeField("date", (LocalDateTime) null).endObject());
        assertResult("{'date':null}", () -> builder().startObject().field("date").timeValue((LocalDateTime) null).endObject());
        assertResult("{'date':null}", () -> builder().startObject().field("date", (LocalDateTime) null).endObject());
        assertResult("{'d1':'2016-01-01T00:00:00.000Z'}", () -> builder().startObject().timeField("d1", d1.toLocalDateTime()).endObject());
        assertResult(
            "{'d1':'2016-01-01T00:00:00.000Z'}",
            () -> builder().startObject().field("d1").timeValue(d1.toLocalDateTime()).endObject()
        );
        assertResult("{'d1':'2016-01-01T00:00:00.000Z'}", () -> builder().startObject().field("d1", d1.toLocalDateTime()).endObject());

        // LocalDate (no time, no time zone)
        assertResult("{'date':null}", () -> builder().startObject().timeField("date", (LocalDate) null).endObject());
        assertResult("{'date':null}", () -> builder().startObject().field("date").timeValue((LocalDate) null).endObject());
        assertResult("{'date':null}", () -> builder().startObject().field("date", (LocalDate) null).endObject());
        assertResult("{'d1':'2016-01-01'}", () -> builder().startObject().timeField("d1", d1.toLocalDate()).endObject());
        assertResult("{'d1':'2016-01-01'}", () -> builder().startObject().field("d1").timeValue(d1.toLocalDate()).endObject());
        assertResult("{'d1':'2016-01-01'}", () -> builder().startObject().field("d1", d1.toLocalDate()).endObject());

        // LocalTime (no date, no time zone)
        assertResult("{'date':null}", () -> builder().startObject().timeField("date", (LocalTime) null).endObject());
        assertResult("{'date':null}", () -> builder().startObject().field("date").timeValue((LocalTime) null).endObject());
        assertResult("{'date':null}", () -> builder().startObject().field("date", (LocalTime) null).endObject());
        assertResult("{'d1':'00:00:00.000'}", () -> builder().startObject().timeField("d1", d1.toLocalTime()).endObject());
        assertResult("{'d1':'00:00:00.000'}", () -> builder().startObject().field("d1").timeValue(d1.toLocalTime()).endObject());
        assertResult("{'d1':'00:00:00.000'}", () -> builder().startObject().field("d1", d1.toLocalTime()).endObject());
        final ZonedDateTime d2 = ZonedDateTime.of(2016, 1, 1, 7, 59, 23, 123_000_000, ZoneOffset.UTC);
        assertResult("{'d1':'07:59:23.123'}", () -> builder().startObject().timeField("d1", d2.toLocalTime()).endObject());
        assertResult("{'d1':'07:59:23.123'}", () -> builder().startObject().field("d1").timeValue(d2.toLocalTime()).endObject());
        assertResult("{'d1':'07:59:23.123'}", () -> builder().startObject().field("d1", d2.toLocalTime()).endObject());

        // OffsetDateTime
        assertResult("{'date':null}", () -> builder().startObject().timeField("date", (OffsetDateTime) null).endObject());
        assertResult("{'date':null}", () -> builder().startObject().field("date").timeValue((OffsetDateTime) null).endObject());
        assertResult("{'date':null}", () -> builder().startObject().field("date", (OffsetDateTime) null).endObject());
        assertResult("{'d1':'2016-01-01T00:00:00.000Z'}", () -> builder().startObject().field("d1", d1.toOffsetDateTime()).endObject());
        assertResult("{'d1':'2016-01-01T00:00:00.000Z'}", () -> builder().startObject().timeField("d1", d1.toOffsetDateTime()).endObject());
        assertResult(
            "{'d1':'2016-01-01T00:00:00.000Z'}",
            () -> builder().startObject().field("d1").timeValue(d1.toOffsetDateTime()).endObject()
        );
        // also test with a date that has a real offset
        OffsetDateTime offsetDateTime = d1.withZoneSameLocal(ZoneOffset.ofHours(5)).toOffsetDateTime();
        assertResult("{'d1':'2016-01-01T00:00:00.000+05:00'}", () -> builder().startObject().field("d1", offsetDateTime).endObject());
        assertResult("{'d1':'2016-01-01T00:00:00.000+05:00'}", () -> builder().startObject().timeField("d1", offsetDateTime).endObject());
        assertResult(
            "{'d1':'2016-01-01T00:00:00.000+05:00'}",
            () -> builder().startObject().field("d1").timeValue(offsetDateTime).endObject()
        );

        // OffsetTime
        assertResult("{'date':null}", () -> builder().startObject().timeField("date", (OffsetTime) null).endObject());
        assertResult("{'date':null}", () -> builder().startObject().field("date").timeValue((OffsetTime) null).endObject());
        assertResult("{'date':null}", () -> builder().startObject().field("date", (OffsetTime) null).endObject());
        final OffsetTime offsetTime = d2.toOffsetDateTime().toOffsetTime();
        assertResult("{'o':'07:59:23.123Z'}", () -> builder().startObject().timeField("o", offsetTime).endObject());
        assertResult("{'o':'07:59:23.123Z'}", () -> builder().startObject().field("o").timeValue(offsetTime).endObject());
        assertResult("{'o':'07:59:23.123Z'}", () -> builder().startObject().field("o", offsetTime).endObject());
        // also test with a date that has a real offset
        final OffsetTime zonedOffsetTime = offsetTime.withOffsetSameLocal(ZoneOffset.ofHours(5));
        assertResult("{'o':'07:59:23.123+05:00'}", () -> builder().startObject().timeField("o", zonedOffsetTime).endObject());
        assertResult("{'o':'07:59:23.123+05:00'}", () -> builder().startObject().field("o").timeValue(zonedOffsetTime).endObject());
        assertResult("{'o':'07:59:23.123+05:00'}", () -> builder().startObject().field("o", zonedOffsetTime).endObject());

        // DayOfWeek enum, not a real time value, but might be used in scripts
        assertResult("{'dayOfWeek':null}", () -> builder().startObject().field("dayOfWeek", (DayOfWeek) null).endObject());
        DayOfWeek dayOfWeek = randomFrom(DayOfWeek.values());
        assertResult("{'dayOfWeek':'" + dayOfWeek + "'}", () -> builder().startObject().field("dayOfWeek", dayOfWeek).endObject());

        // Month
        Month month = randomFrom(Month.values());
        assertResult("{'m':null}", () -> builder().startObject().field("m", (Month) null).endObject());
        assertResult("{'m':'" + month + "'}", () -> builder().startObject().field("m", month).endObject());

        // MonthDay
        MonthDay monthDay = MonthDay.of(month, randomIntBetween(1, 28));
        assertResult("{'m':null}", () -> builder().startObject().field("m", (MonthDay) null).endObject());
        assertResult("{'m':'" + monthDay + "'}", () -> builder().startObject().field("m", monthDay).endObject());

        // Year
        Year year = Year.of(randomIntBetween(0, 2300));
        assertResult("{'y':null}", () -> builder().startObject().field("y", (Year) null).endObject());
        assertResult("{'y':'" + year + "'}", () -> builder().startObject().field("y", year).endObject());

        // Duration
        Duration duration = Duration.ofSeconds(randomInt(100000));
        assertResult("{'d':null}", () -> builder().startObject().field("d", (Duration) null).endObject());
        assertResult("{'d':'" + duration + "'}", () -> builder().startObject().field("d", duration).endObject());

        // Period
        Period period = Period.ofDays(randomInt(1000));
        assertResult("{'p':null}", () -> builder().startObject().field("p", (Period) null).endObject());
        assertResult("{'p':'" + period + "'}", () -> builder().startObject().field("p", period).endObject());
    }

    public void testGeoPoint() throws Exception {
        assertResult("{'geo':null}", () -> builder().startObject().field("geo", (GeoPoint) null).endObject());
        assertResult(
            "{'geo':{'lat':52.4267578125,'lon':13.271484375}}",
            () -> builder().startObject().field("geo", GeoPoint.fromGeohash("u336q")).endObject()
        );
        assertResult(
            "{'geo':{'lat':52.5201416015625,'lon':13.4033203125}}",
            () -> builder().startObject().field("geo").value(GeoPoint.fromGeohash("u33dc1")).endObject()
        );
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
        objects.put("{'objects':[false,true,false]}", new Object[] { false, true, false });
        objects.put("{'objects':[1,1,2,3,5,8,13]}", new Object[] { (byte) 1, (byte) 1, (byte) 2, (byte) 3, (byte) 5, (byte) 8, (byte) 13 });
        objects.put("{'objects':[1.0,1.0,2.0,3.0,5.0,8.0,13.0]}", new Object[] { 1.0d, 1.0d, 2.0d, 3.0d, 5.0d, 8.0d, 13.0d });
        objects.put("{'objects':[1.0,1.0,2.0,3.0,5.0,8.0,13.0]}", new Object[] { 1.0f, 1.0f, 2.0f, 3.0f, 5.0f, 8.0f, 13.0f });
        objects.put("{'objects':[{'lat':45.759429931640625,'lon':4.8394775390625}]}", new Object[] { GeoPoint.fromGeohash("u05kq4k") });
        objects.put("{'objects':[1,1,2,3,5,8,13]}", new Object[] { 1, 1, 2, 3, 5, 8, 13 });
        objects.put("{'objects':[1,1,2,3,5,8,13]}", new Object[] { 1L, 1L, 2L, 3L, 5L, 8L, 13L });
        objects.put("{'objects':[1,1,2,3,5,8]}", new Object[] { (short) 1, (short) 1, (short) 2, (short) 3, (short) 5, (short) 8 });
        objects.put("{'objects':['a','b','c']}", new Object[] { "a", "b", "c" });
        objects.put("{'objects':['a','b','c']}", new Object[] { new Text("a"), new Text(new BytesArray("b")), new Text("c") });
        objects.put("{'objects':null}", null);
        objects.put("{'objects':[null,null,null]}", new Object[] { null, null, null });
        objects.put("{'objects':['OPEN','CLOSE']}", IndexMetadata.State.values());
        objects.put("{'objects':[{'f1':'v1'},{'f2':'v2'}]}", new Object[] { singletonMap("f1", "v1"), singletonMap("f2", "v2") });
        objects.put("{'objects':[[1,2,3],[4,5]]}", new Object[] { Arrays.asList(1, 2, 3), Arrays.asList(4, 5) });

        final String paths = Constants.WINDOWS ? "{'objects':['a\\\\b\\\\c','d\\\\e']}" : "{'objects':['a/b/c','d/e']}";
        objects.put(paths, new Object[] { PathUtils.get("a", "b", "c"), PathUtils.get("d", "e") });

        final DateTimeFormatter formatter = DateTimeFormatter.ISO_INSTANT;
        final Date d1 = Date.from(ZonedDateTime.of(2016, 1, 1, 0, 0, 0, 0, ZoneOffset.UTC).toInstant());
        final Date d2 = Date.from(ZonedDateTime.of(2015, 1, 1, 0, 0, 0, 0, ZoneOffset.UTC).toInstant());
        objects.put("{'objects':['2016-01-01T00:00:00.000Z','2015-01-01T00:00:00.000Z']}", new Object[] { d1, d2 });

        final Calendar c1 = GregorianCalendar.from(ZonedDateTime.of(2012, 7, 7, 10, 23, 0, 0, ZoneOffset.UTC));
        final Calendar c2 = GregorianCalendar.from(ZonedDateTime.of(2014, 11, 16, 19, 36, 0, 0, ZoneOffset.UTC));
        objects.put("{'objects':['2012-07-07T10:23:00.000Z','2014-11-16T19:36:00.000Z']}", new Object[] { c1, c2 });

        final ToXContent x1 = (builder, params) -> builder.startObject().field("f1", "v1").field("f2", 2).array("f3", 3, 4, 5).endObject();
        final ToXContent x2 = (builder, params) -> builder.startObject().field("f1", "v1").field("f2", x1).endObject();
        objects.put(
            "{'objects':[{'f1':'v1','f2':2,'f3':[3,4,5]},{'f1':'v1','f2':{'f1':'v1','f2':2,'f3':[3,4,5]}}]}",
            new Object[] { x1, x2 }
        );

        for (Map.Entry<String, Object[]> o : objects.entrySet()) {
            final String expected = o.getKey();
            assertResult(expected, () -> builder().startObject().field("objects", o.getValue()).endObject());
            assertResult(expected, () -> builder().startObject().field("objects").value(o.getValue()).endObject());
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
        object.put("{'object':'OPEN'}", IndexMetadata.State.OPEN);
        object.put("{'object':'NM'}", DistanceUnit.NAUTICALMILES);
        object.put("{'object':{'f1':'v1'}}", singletonMap("f1", "v1"));
        object.put("{'object':{'f1':{'f2':'v2'}}}", singletonMap("f1", singletonMap("f2", "v2")));
        object.put("{'object':[1,2,3]}", Arrays.asList(1, 2, 3));

        final String path = Constants.WINDOWS ? "{'object':'a\\\\b\\\\c'}" : "{'object':'a/b/c'}";
        object.put(path, PathUtils.get("a", "b", "c"));

        final Date d1 = Date.from(ZonedDateTime.of(2016, 1, 1, 0, 0, 0, 0, ZoneOffset.UTC).toInstant());
        object.put("{'object':'" + "2016-01-01T00:00:00.000Z" + "'}", d1);

        final Calendar c1 = GregorianCalendar.from(ZonedDateTime.of(2010, 1, 1, 0, 0, 0, 0, ZoneOffset.UTC));
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
        assertResult(
            "{'xcontent':{'field':'value','array':['1','2','3'],'foo':{'bar':'baz'}}}",
            () -> builder().startObject().field("xcontent", xcontent0).endObject()
        );

        ToXContentObject xcontent1 = (builder, params) -> {
            builder.startObject();
            builder.field("field", "value");
            builder.startObject("foo");
            builder.field("bar", "baz");
            builder.endObject();
            builder.endObject();
            return builder;
        };

        ToXContentObject xcontent2 = (builder, params) -> {
            builder.startObject();
            builder.field("root", xcontent0);
            builder.array("childs", xcontent0, xcontent1);
            builder.endObject();
            return builder;
        };
        assertResult(XContentHelper.stripWhitespace("""
            {
               "root": {
                 "field": "value",
                 "array": [ "1", "2", "3" ],
                 "foo": {
                   "bar": "baz"
                 }
               },
               "childs": [
                 {
                   "field": "value",
                   "array": [ "1", "2", "3" ],
                   "foo": {
                     "bar": "baz"
                   }
                 },
                 {
                   "field": "value",
                   "foo": {
                     "bar": "baz"
                   }
                 }
               ]
             }"""), () -> builder().value(xcontent2));
    }

    public void testMap() throws Exception {
        Map<String, Map<String, ?>> maps = new HashMap<>();
        maps.put("{'map':null}", (Map<String, ?>) null);
        maps.put("{'map':{}}", Collections.emptyMap());
        maps.put("{'map':{'key':'value'}}", singletonMap("key", "value"));

        Map<String, Object> innerMap = new HashMap<>();
        innerMap.put("string", "value");
        innerMap.put("int", 42);
        innerMap.put("long", 42L);
        innerMap.put("long[]", new long[] { 1L, 3L });
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
        iterables.put("{'iter':null}", (Iterable<?>) null);
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
        for (boolean useStream : new boolean[] { false, true }) {
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
                generator.writeRawField("bar", new BytesArray(rawData).streamInput());
            }
            generator.writeEndObject();
        }

        try (XContentParser parser = xcontentType().xContent().createParser(XContentParserConfiguration.EMPTY, os.toByteArray())) {
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
            generator.writeRawValue(new BytesArray(rawData).streamInput(), source.type());
        }

        try (XContentParser parser = xcontentType().xContent().createParser(XContentParserConfiguration.EMPTY, os.toByteArray())) {
            assertEquals(Token.START_OBJECT, parser.nextToken());
            assertEquals(Token.FIELD_NAME, parser.nextToken());
            assertEquals("foo", parser.currentName());
            assertEquals(Token.VALUE_NULL, parser.nextToken());
            assertEquals(Token.END_OBJECT, parser.nextToken());
            assertNull(parser.nextToken());
        }

        final AtomicBoolean closed = new AtomicBoolean(false);
        os = new ByteArrayOutputStream() {
            @Override
            public void close() {
                closed.set(true);
            }
        };
        try (XContentGenerator generator = xcontentType().xContent().createGenerator(os)) {
            generator.writeStartObject();
            generator.writeFieldName("test");
            generator.writeRawValue(new BytesArray(rawData).streamInput(), source.type());
            assertFalse("Generator should not have closed the output stream", closed.get());
            generator.writeEndObject();
        }

        try (XContentParser parser = xcontentType().xContent().createParser(XContentParserConfiguration.EMPTY, os.toByteArray())) {
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

    }

    protected void doTestBigInteger(XContentGenerator generator, ByteArrayOutputStream os) throws Exception {
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

        try (XContentParser parser = xcontentType().xContent().createParser(XContentParserConfiguration.EMPTY, serialized)) {
            Map<String, Object> map = parser.map();
            assertEquals("bar", map.get("foo"));
            assertEquals(bigInteger, map.get("bigint"));
        }
    }

    public void testEnsureNameNotNull() {
        IllegalArgumentException e = expectThrows(IllegalArgumentException.class, () -> XContentBuilder.ensureNameNotNull(null));
        assertThat(e.getMessage(), containsString("Field name cannot be null"));
    }

    public void testEnsureNotNull() {
        IllegalArgumentException e = expectThrows(IllegalArgumentException.class, () -> XContentBuilder.ensureNotNull(null, "message"));
        assertThat(e.getMessage(), containsString("message"));

        XContentBuilder.ensureNotNull("foo", "No exception must be thrown");
    }

    public void testEnsureNoSelfReferences() throws IOException {
        builder().map(emptyMap());
        builder().map(null);

        Map<String, Object> map = new HashMap<>();
        map.put("field", map);

        IllegalArgumentException e = expectThrows(IllegalArgumentException.class, () -> builder().map(map));
        assertThat(e.getMessage(), containsString("Iterable object is self-referencing itself"));
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
        assertThat(e.getMessage(), containsString("Iterable object is self-referencing itself"));
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
        assertThat(e.getMessage(), containsString("Iterable object is self-referencing itself"));
    }

    public void testSelfReferencingObjectsArray() throws IOException {
        Object[] values = new Object[3];
        values[0] = 0;
        values[1] = 1;
        values[2] = values;

        IllegalArgumentException e = expectThrows(
            IllegalArgumentException.class,
            () -> builder().startObject().field("field", values).endObject()
        );
        assertThat(e.getMessage(), containsString("Iterable object is self-referencing itself"));

        e = expectThrows(IllegalArgumentException.class, () -> builder().startObject().array("field", values).endObject());
        assertThat(e.getMessage(), containsString("Iterable object is self-referencing itself"));
    }

    public void testSelfReferencingIterable() throws IOException {
        List<Object> values = new ArrayList<>();
        values.add("foo");
        values.add("bar");
        values.add(values);

        IllegalArgumentException e = expectThrows(
            IllegalArgumentException.class,
            () -> builder().startObject().field("field", values).endObject()
        );
        assertThat(e.getMessage(), containsString("Iterable object is self-referencing itself"));
    }

    public void testSelfReferencingIterableOneLevel() throws IOException {
        Map<String, Object> map = new HashMap<>();
        map.put("foo", 0);
        map.put("bar", 1);

        Iterable<Object> values = Arrays.asList("one", "two", map);
        map.put("baz", values);

        IllegalArgumentException e = expectThrows(
            IllegalArgumentException.class,
            () -> builder().startObject().field("field", values).endObject()
        );
        assertThat(e.getMessage(), containsString("Iterable object is self-referencing itself"));
    }

    public void testSelfReferencingIterableTwoLevels() throws IOException {
        Map<String, Object> map0 = new HashMap<>();
        Map<String, Object> map1 = new HashMap<>();
        Map<String, Object> map2 = new HashMap<>();

        List<Object> it1 = new ArrayList<>();

        map0.put("foo", 0);
        map0.put("it1", it1); // map 0 -> it1

        it1.add(map1);
        it1.add(map2); // it 1 -> map 1, map 2

        map2.put("baz", 2);
        map2.put("map0", map0); // map 2 -> map 0 loop

        IllegalArgumentException e = expectThrows(IllegalArgumentException.class, () -> builder().map(map0));
        assertThat(e.getMessage(), containsString("Iterable object is self-referencing itself"));
    }

    public void testChecksForDuplicates() throws Exception {
        XContentBuilder builder = builder().startObject().field("key", 1).field("key", 2).endObject();
        try (XContentParser xParser = createParser(builder)) {
            XContentParseException pex = expectThrows(XContentParseException.class, () -> xParser.map());
            assertThat(pex.getMessage(), containsString("Duplicate field 'key'"));
        }
    }

    public void testAllowsDuplicates() throws Exception {
        XContentBuilder builder = builder().startObject().field("key", 1).field("key", 2).endObject();
        try (XContentParser xParser = createParser(builder)) {
            xParser.allowDuplicateKeys(true);
            assertThat(xParser.map(), equalTo(Collections.singletonMap("key", 2)));
        }
    }

    public void testNamedObject() throws IOException {
        Object test1 = new Object();
        Object test2 = new Object();
        NamedXContentRegistry registry = new NamedXContentRegistry(
            Arrays.asList(
                new NamedXContentRegistry.Entry(Object.class, new ParseField("test1"), p -> test1),
                new NamedXContentRegistry.Entry(Object.class, new ParseField("test2", "deprecated"), p -> test2),
                new NamedXContentRegistry.Entry(Object.class, new ParseField("str"), p -> p.text())
            )
        );
        XContentBuilder b = XContentBuilder.builder(xcontentType().xContent());
        b.value("test");
        try (
            XContentParser p = xcontentType().xContent()
                .createParser(registry, LoggingDeprecationHandler.INSTANCE, BytesReference.bytes(b).streamInput())
        ) {
            assertEquals(test1, p.namedObject(Object.class, "test1", null));
            assertEquals(test2, p.namedObject(Object.class, "test2", null));
            assertEquals(test2, p.namedObject(Object.class, "deprecated", null));
            assertWarnings("Deprecated field [deprecated] used, expected [test2] instead");
            p.nextToken();
            assertEquals("test", p.namedObject(Object.class, "str", null));
            {
                NamedObjectNotFoundException e = expectThrows(
                    NamedObjectNotFoundException.class,
                    () -> p.namedObject(Object.class, "unknown", null)
                );
                assertThat(e.getMessage(), endsWith("unknown field [unknown]"));
                assertThat(e.getCandidates(), containsInAnyOrder("test1", "test2", "deprecated", "str"));
            }
            {
                Exception e = expectThrows(XContentParseException.class, () -> p.namedObject(String.class, "doesn't matter", null));
                assertEquals("unknown named object category [java.lang.String]", e.getMessage());
            }
        }
        try (
            XContentParser emptyRegistryParser = xcontentType().xContent().createParser(XContentParserConfiguration.EMPTY, new byte[] {})
        ) {
            Exception e = expectThrows(
                XContentParseException.class,
                () -> emptyRegistryParser.namedObject(String.class, "doesn't matter", null)
            );
            assertEquals("named objects are not supported for this parser", e.getMessage());
        }

    }

    private static void expectUnclosedException(ThrowingRunnable runnable) {
        IllegalStateException e = expectThrows(IllegalStateException.class, runnable);
        assertThat(e.getMessage(), containsString("Failed to close the XContentBuilder"));
        assertThat(e.getCause(), allOf(notNullValue(), instanceOf(IOException.class)));
        assertThat(e.getCause().getMessage(), containsString("Unclosed object or array found"));
    }

    private static void expectValueException(ThrowingRunnable runnable) {
        XContentGenerationException e = expectThrows(XContentGenerationException.class, runnable);
        assertThat(e.getMessage(), containsString("expecting a value"));
    }

    private static void expectFieldException(ThrowingRunnable runnable) {
        XContentGenerationException e = expectThrows(XContentGenerationException.class, runnable);
        assertThat(e.getMessage(), containsString("expecting field name"));
    }

    private static void expectNonNullFieldException(ThrowingRunnable runnable) {
        IllegalArgumentException e = expectThrows(IllegalArgumentException.class, runnable);
        assertThat(e.getMessage(), containsString("Field name cannot be null"));
    }

    private static void expectObjectException(ThrowingRunnable runnable) {
        XContentGenerationException e = expectThrows(XContentGenerationException.class, runnable);
        assertThat(e.getMessage(), containsString("Current context not Object"));
    }

    private static void expectArrayException(ThrowingRunnable runnable) {
        XContentGenerationException e = expectThrows(XContentGenerationException.class, runnable);
        assertThat(e.getMessage(), containsString("Current context not Array"));
    }

    public static Matcher<String> equalToJson(String json) {
        return Matchers.equalTo(json.replace("'", "\""));
    }

    private static void assertResult(String expected, Builder builder) throws IOException {
        // Build the XContentBuilder, convert its bytes to JSON and check it matches
        assertThat(XContentHelper.convertToJson(BytesReference.bytes(builder.build()), randomBoolean()), equalToJson(expected));
    }

    private static byte[] randomBytes() throws Exception {
        return randomUnicodeOfLength(scaledRandomIntBetween(10, 1000)).getBytes("UTF-8");
    }

    @FunctionalInterface
    private interface Builder {
        XContentBuilder build() throws IOException;
    }
}
