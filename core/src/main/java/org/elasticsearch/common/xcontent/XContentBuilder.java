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

import org.apache.lucene.util.BytesRef;
import org.elasticsearch.common.bytes.BytesReference;
import org.elasticsearch.common.geo.GeoPoint;
import org.elasticsearch.common.io.stream.BytesStream;
import org.elasticsearch.common.io.stream.BytesStreamOutput;
import org.elasticsearch.common.lease.Releasable;
import org.elasticsearch.common.text.Text;
import org.elasticsearch.common.unit.ByteSizeValue;
import org.elasticsearch.common.unit.TimeValue;
import org.joda.time.DateTimeZone;
import org.joda.time.ReadableInstant;
import org.joda.time.format.DateTimeFormatter;
import org.joda.time.format.ISODateTimeFormat;

import java.io.Flushable;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.nio.file.Path;
import java.util.Arrays;
import java.util.Calendar;
import java.util.Collections;
import java.util.Date;
import java.util.HashMap;
import java.util.IdentityHashMap;
import java.util.Locale;
import java.util.Map;
import java.util.Objects;
import java.util.Set;
import java.util.concurrent.TimeUnit;

/**
 * A utility to build XContent (ie json).
 */
public final class XContentBuilder implements Releasable, Flushable {

    /**
     * Create a new {@link XContentBuilder} using the given {@link XContent} content.
     * <p>
     * The builder uses an internal {@link BytesStreamOutput} output stream to build the content.
     * </p>
     *
     * @param xContent the {@link XContent}
     * @return a new {@link XContentBuilder}
     * @throws IOException if an {@link IOException} occurs while building the content
     */
    public static XContentBuilder builder(XContent xContent) throws IOException {
        return new XContentBuilder(xContent, new BytesStreamOutput());
    }

    /**
     * Create a new {@link XContentBuilder} using the given {@link XContent} content and some inclusive and/or exclusive filters.
     * <p>
     * The builder uses an internal {@link BytesStreamOutput} output stream to build the content. When both exclusive and
     * inclusive filters are provided, the underlying builder will first use exclusion filters to remove fields and then will check the
     * remaining fields against the inclusive filters.
     * <p>
     *
     * @param xContent the {@link XContent}
     * @param includes the inclusive filters: only fields and objects that match the inclusive filters will be written to the output.
     * @param excludes the exclusive filters: only fields and objects that don't match the exclusive filters will be written to the output.
     * @throws IOException if an {@link IOException} occurs while building the content
     */
    public static XContentBuilder builder(XContent xContent, Set<String> includes, Set<String> excludes) throws IOException {
        return new XContentBuilder(xContent, new BytesStreamOutput(), includes, excludes);
    }

    public static final DateTimeFormatter DEFAULT_DATE_PRINTER = ISODateTimeFormat.dateTime().withZone(DateTimeZone.UTC);

    private static final Map<Class<?>, Writer> WRITERS;
    static {
        Map<Class<?>, Writer> writers = new HashMap<>();
        writers.put(Boolean.class, (b, v) -> b.value((Boolean) v));
        writers.put(Byte.class, (b, v) -> b.value((Byte) v));
        writers.put(byte[].class, (b, v) -> b.value((byte[]) v));
        writers.put(BytesRef.class, (b, v) -> b.binaryValue((BytesRef) v));
        writers.put(Date.class, (b, v) -> b.value((Date) v));
        writers.put(Double.class, (b, v) -> b.value((Double) v));
        writers.put(double[].class, (b, v) -> b.values((double[]) v));
        writers.put(Float.class, (b, v) -> b.value((Float) v));
        writers.put(float[].class, (b, v) -> b.values((float[]) v));
        writers.put(GeoPoint.class, (b, v) -> b.value((GeoPoint) v));
        writers.put(Integer.class, (b, v) -> b.value((Integer) v));
        writers.put(int[].class, (b, v) -> b.values((int[]) v));
        writers.put(Long.class, (b, v) -> b.value((Long) v));
        writers.put(long[].class, (b, v) -> b.values((long[]) v));
        writers.put(Short.class, (b, v) -> b.value((Short) v));
        writers.put(short[].class, (b, v) -> b.values((short[]) v));
        writers.put(String.class, (b, v) -> b.value((String) v));
        writers.put(String[].class, (b, v) -> b.values((String[]) v));
        writers.put(Text.class, (b, v) -> b.value((Text) v));

        WRITERS = Collections.unmodifiableMap(writers);
    }

    @FunctionalInterface
    private interface Writer {
        void write(XContentBuilder builder, Object value) throws IOException;
    }

    /**
     * XContentGenerator used to build the XContent object
     */
    private final XContentGenerator generator;

    /**
     * Output stream to which the built object is written
     */
    private final OutputStream bos;

    /**
     * When this flag is set to true, some types of values are written in a format easier to read for a human.
     */
    private boolean humanReadable = false;

    /**
     * Constructs a new builder using the provided XContent and an OutputStream. Make sure
     * to call {@link #close()} when the builder is done with.
     */
    public XContentBuilder(XContent xContent, OutputStream bos) throws IOException {
        this(xContent, bos, Collections.emptySet(), Collections.emptySet());
    }

    /**
     * Constructs a new builder using the provided XContent, an OutputStream and
     * some filters. If filters are specified, only those values matching a
     * filter will be written to the output stream. Make sure to call
     * {@link #close()} when the builder is done with.
     */
    public XContentBuilder(XContent xContent, OutputStream bos, Set<String> includes) throws IOException {
        this(xContent, bos, includes, Collections.emptySet());
    }

    /**
     * Creates a new builder using the provided XContent, output stream and some inclusive and/or exclusive filters. When both exclusive and
     * inclusive filters are provided, the underlying builder will first use exclusion filters to remove fields and then will check the
     * remaining fields against the inclusive filters.
     * <p>
     * Make sure to call {@link #close()} when the builder is done with.
     *
     * @param os       the output stream
     * @param includes the inclusive filters: only fields and objects that match the inclusive filters will be written to the output.
     * @param excludes the exclusive filters: only fields and objects that don't match the exclusive filters will be written to the output.
     */
    public XContentBuilder(XContent xContent, OutputStream os, Set<String> includes, Set<String> excludes) throws IOException {
        this.bos = os;
        this.generator = xContent.createGenerator(bos, includes, excludes);
    }

    public XContentType contentType() {
        return generator.contentType();
    }

    public XContentBuilder prettyPrint() {
        generator.usePrettyPrint();
        return this;
    }

    public boolean isPrettyPrint() {
        return generator.isPrettyPrint();
    }

    /**
     * Indicate that the current {@link XContentBuilder} must write a line feed ("\n")
     * at the end of the built object.
     * <p>
     * This only applies for JSON XContent type. It has no effect for other types.
     */
    public XContentBuilder lfAtEnd() {
        generator.usePrintLineFeedAtEnd();
        return this;
    }

    /**
     * Set the "human readable" flag. Once set, some types of values are written in a
     * format easier to read for a human.
     */
    public XContentBuilder humanReadable(boolean humanReadable) {
        this.humanReadable = humanReadable;
        return this;
    }

    /**
     * @return the value of the "human readable" flag. When the value is equal to true,
     * some types of values are written in a format easier to read for a human.
     */
    public boolean humanReadable() {
        return this.humanReadable;
    }

    ////////////////////////////////////////////////////////////////////////////
    // Structure (object, array, field, null values...)
    //////////////////////////////////

    public XContentBuilder startObject() throws IOException {
        generator.writeStartObject();
        return this;
    }

    public XContentBuilder startObject(String name) throws IOException {
        return field(name).startObject();
    }

    public XContentBuilder endObject() throws IOException {
        generator.writeEndObject();
        return this;
    }

    public XContentBuilder startArray() throws IOException {
        generator.writeStartArray();
        return this;
    }

    public XContentBuilder startArray(String name) throws IOException {
        return field(name).startArray();
    }

    public XContentBuilder endArray() throws IOException {
        generator.writeEndArray();
        return this;
    }

    public XContentBuilder field(String name) throws IOException {
        ensureNameNotNull(name);
        generator.writeFieldName(name);
        return this;
    }

    public XContentBuilder nullField(String name) throws IOException {
        ensureNameNotNull(name);
        generator.writeNullField(name);
        return this;
    }

    public XContentBuilder nullValue() throws IOException {
        generator.writeNull();
        return this;
    }

    ////////////////////////////////////////////////////////////////////////////
    // Boolean
    //////////////////////////////////

    public XContentBuilder field(String name, Boolean value) throws IOException {
        return (value == null) ? nullField(name) : field(name, value.booleanValue());
    }

    public XContentBuilder field(String name, boolean value) throws IOException {
        ensureNameNotNull(name);
        generator.writeBooleanField(name, value);
        return this;
    }

    public XContentBuilder array(String name, boolean[] values) throws IOException {
        return field(name).values(values);
    }

    private XContentBuilder values(boolean[] values) throws IOException {
        if (values == null) {
            return nullValue();
        }
        startArray();
        for (boolean b : values) {
            value(b);
        }
        endArray();
        return this;
    }

    public XContentBuilder value(Boolean value) throws IOException {
        return (value == null) ? nullValue() : value(value.booleanValue());
    }

    public XContentBuilder value(boolean value) throws IOException {
        generator.writeBoolean(value);
        return this;
    }

    ////////////////////////////////////////////////////////////////////////////
    // Byte
    //////////////////////////////////

    public XContentBuilder field(String name, Byte value) throws IOException {
        return (value == null) ? nullField(name) : field(name, value.byteValue());
    }

    public XContentBuilder field(String name, byte value) throws IOException {
        return field(name).value(value);
    }

    public XContentBuilder value(Byte value) throws IOException {
        return (value == null) ? nullValue() : value(value.byteValue());
    }

    public XContentBuilder value(byte value) throws IOException {
        generator.writeNumber(value);
        return this;
    }

    ////////////////////////////////////////////////////////////////////////////
    // Double
    //////////////////////////////////

    public XContentBuilder field(String name, Double value) throws IOException {
        return (value == null) ? nullField(name) : field(name, value.doubleValue());
    }

    public XContentBuilder field(String name, double value) throws IOException {
        ensureNameNotNull(name);
        generator.writeNumberField(name, value);
        return this;
    }

    public XContentBuilder array(String name, double[] values) throws IOException {
        return field(name).values(values);
    }

    private XContentBuilder values(double[] values) throws IOException {
        if (values == null) {
            return nullValue();
        }
        startArray();
        for (double b : values) {
            value(b);
        }
        endArray();
        return this;
    }

    public XContentBuilder value(Double value) throws IOException {
        return (value == null) ? nullValue() : value(value.doubleValue());
    }

    public XContentBuilder value(double value) throws IOException {
        generator.writeNumber(value);
        return this;
    }

    ////////////////////////////////////////////////////////////////////////////
    // Float
    //////////////////////////////////

    public XContentBuilder field(String name, Float value) throws IOException {
        return (value == null) ? nullField(name) : field(name, value.floatValue());
    }

    public XContentBuilder field(String name, float value) throws IOException {
        ensureNameNotNull(name);
        generator.writeNumberField(name, value);
        return this;
    }

    public XContentBuilder array(String name, float[] values) throws IOException {
        return field(name).values(values);
    }

    private XContentBuilder values(float[] values) throws IOException {
        if (values == null) {
            return nullValue();
        }
        startArray();
        for (float f : values) {
            value(f);
        }
        endArray();
        return this;
    }

    public XContentBuilder value(Float value) throws IOException {
        return (value == null) ? nullValue() : value(value.floatValue());
    }

    public XContentBuilder value(float value) throws IOException {
        generator.writeNumber(value);
        return this;
    }

    ////////////////////////////////////////////////////////////////////////////
    // Integer
    //////////////////////////////////

    public XContentBuilder field(String name, Integer value) throws IOException {
        return (value == null) ? nullField(name) : field(name, value.intValue());
    }

    public XContentBuilder field(String name, int value) throws IOException {
        ensureNameNotNull(name);
        generator.writeNumberField(name, value);
        return this;
    }

    public XContentBuilder array(String name, int[] values) throws IOException {
        return field(name).values(values);
    }

    private XContentBuilder values(int[] values) throws IOException {
        if (values == null) {
            return nullValue();
        }
        startArray();
        for (int i : values) {
            value(i);
        }
        endArray();
        return this;
    }

    public XContentBuilder value(Integer value) throws IOException {
        return (value == null) ? nullValue() : value(value.intValue());
    }

    public XContentBuilder value(int value) throws IOException {
        generator.writeNumber(value);
        return this;
    }

    ////////////////////////////////////////////////////////////////////////////
    // Long
    //////////////////////////////////

    public XContentBuilder field(String name, Long value) throws IOException {
        return (value == null) ? nullField(name) : field(name, value.longValue());
    }

    public XContentBuilder field(String name, long value) throws IOException {
        ensureNameNotNull(name);
        generator.writeNumberField(name, value);
        return this;
    }

    public XContentBuilder array(String name, long[] values) throws IOException {
        return field(name).values(values);
    }

    private XContentBuilder values(long[] values) throws IOException {
        if (values == null) {
            return nullValue();
        }
        startArray();
        for (long l : values) {
            value(l);
        }
        endArray();
        return this;
    }

    public XContentBuilder value(Long value) throws IOException {
        return (value == null) ? nullValue() : value(value.longValue());
    }

    public XContentBuilder value(long value) throws IOException {
        generator.writeNumber(value);
        return this;
    }

    ////////////////////////////////////////////////////////////////////////////
    // Short
    //////////////////////////////////

    public XContentBuilder field(String name, Short value) throws IOException {
        return (value == null) ? nullField(name) : field(name, value.shortValue());
    }

    public XContentBuilder field(String name, short value) throws IOException {
        return field(name).value(value);
    }

    public XContentBuilder array(String name, short[] values) throws IOException {
        return field(name).values(values);
    }

    private XContentBuilder values(short[] values) throws IOException {
        if (values == null) {
            return nullValue();
        }
        startArray();
        for (short s : values) {
            value(s);
        }
        endArray();
        return this;
    }

    public XContentBuilder value(Short value) throws IOException {
        return (value == null) ? nullValue() : value(value.shortValue());
    }

    public XContentBuilder value(short value) throws IOException {
        generator.writeNumber(value);
        return this;
    }

    ////////////////////////////////////////////////////////////////////////////
    // String
    //////////////////////////////////

    public XContentBuilder field(String name, String value) throws IOException {
        if (value == null) {
            return nullField(name);
        }
        ensureNameNotNull(name);
        generator.writeStringField(name, value);
        return this;
    }

    public XContentBuilder array(String name, String... values) throws IOException {
        return field(name).values(values);
    }

    private XContentBuilder values(String[] values) throws IOException {
        if (values == null) {
            return nullValue();
        }
        startArray();
        for (String s : values) {
            value(s);
        }
        endArray();
        return this;
    }

    public XContentBuilder value(String value) throws IOException {
        if (value == null) {
            return nullValue();
        }
        generator.writeString(value);
        return this;
    }

    ////////////////////////////////////////////////////////////////////////////
    // Binary
    //////////////////////////////////

    public XContentBuilder field(String name, byte[] value) throws IOException {
        if (value == null) {
            return nullField(name);
        }
        ensureNameNotNull(name);
        generator.writeBinaryField(name, value);
        return this;
    }

    public XContentBuilder value(byte[] value) throws IOException {
        if (value == null) {
            return nullValue();
        }
        generator.writeBinary(value);
        return this;
    }

    public XContentBuilder field(String name, byte[] value, int offset, int length) throws IOException {
        return field(name).value(value, offset, length);
    }

    public XContentBuilder value(byte[] value, int offset, int length) throws IOException {
        if (value == null) {
            return nullValue();
        }
        generator.writeBinary(value, offset, length);
        return this;
    }

    /**
     * Writes the binary content of the given {@link BytesRef}.
     *
     * Use {@link org.elasticsearch.common.xcontent.XContentParser#binaryValue()} to read the value back
     */
    public XContentBuilder field(String name, BytesRef value) throws IOException {
        return field(name).binaryValue(value);
    }

    /**
     * Writes the binary content of the given {@link BytesRef} as UTF-8 bytes.
     *
     * Use {@link XContentParser#utf8Bytes()} to read the value back
     */
    public XContentBuilder utf8Field(String name, BytesRef value) throws IOException {
        return field(name).utf8Value(value);
    }

    /**
     * Writes the binary content of the given {@link BytesRef}.
     *
     * Use {@link org.elasticsearch.common.xcontent.XContentParser#binaryValue()} to read the value back
     */
    public XContentBuilder binaryValue(BytesRef value) throws IOException {
        if (value == null) {
            return nullValue();
        }
        value(value.bytes, value.offset, value.length);
        return this;
    }

    /**
     * Writes the binary content of the given {@link BytesRef} as UTF-8 bytes.
     *
     * Use {@link XContentParser#utf8Bytes()} to read the value back
     */
    public XContentBuilder utf8Value(BytesRef value) throws IOException {
        if (value == null) {
            return nullValue();
        }
        generator.writeUTF8String(value.bytes, value.offset, value.length);
        return this;
    }

    /**
     * Writes the binary content of the given {@link BytesReference}.
     *
     * Use {@link org.elasticsearch.common.xcontent.XContentParser#binaryValue()} to read the value back
     */
    public XContentBuilder field(String name, BytesReference value) throws IOException {
        return field(name).value(value);
    }

    /**
     * Writes the binary content of the given {@link BytesReference}.
     *
     * Use {@link org.elasticsearch.common.xcontent.XContentParser#binaryValue()} to read the value back
     */
    public XContentBuilder value(BytesReference value) throws IOException {
        return (value == null) ? nullValue() : binaryValue(value.toBytesRef());
    }

    ////////////////////////////////////////////////////////////////////////////
    // Text
    //////////////////////////////////

    public XContentBuilder field(String name, Text value) throws IOException {
        return field(name).value(value);
    }

    public XContentBuilder value(Text value) throws IOException {
        if (value == null) {
            return nullValue();
        } else if (value.hasString()) {
            return value(value.string());
        } else {
            // TODO: TextBytesOptimization we can use a buffer here to convert it? maybe add a
            // request to jackson to support InputStream as well?
            return utf8Value(value.bytes().toBytesRef());
        }
    }

    ////////////////////////////////////////////////////////////////////////////
    // Date
    //////////////////////////////////

    public XContentBuilder field(String name, ReadableInstant value) throws IOException {
        return field(name).value(value);
    }

    public XContentBuilder field(String name, ReadableInstant value, DateTimeFormatter formatter) throws IOException {
        return field(name).value(value, formatter);
    }

    public XContentBuilder value(ReadableInstant value) throws IOException {
        return value(value, DEFAULT_DATE_PRINTER);
    }

    public XContentBuilder value(ReadableInstant value, DateTimeFormatter formatter) throws IOException {
        if (value == null) {
            return nullValue();
        }
        ensureFormatterNotNull(formatter);
        return value(formatter.print(value));
    }

    public XContentBuilder field(String name, Date value) throws IOException {
        return field(name).value(value);
    }

    public XContentBuilder field(String name, Date value, DateTimeFormatter formatter) throws IOException {
        return field(name).value(value, formatter);
    }

    public XContentBuilder value(Date value) throws IOException {
        return value(value, DEFAULT_DATE_PRINTER);
    }

    public XContentBuilder value(Date value, DateTimeFormatter formatter) throws IOException {
        if (value == null) {
            return nullValue();
        }
        return value(formatter, value.getTime());
    }

    public XContentBuilder dateField(String name, String readableName, long value) throws IOException {
        if (humanReadable) {
            field(readableName).value(DEFAULT_DATE_PRINTER, value);
        }
        field(name, value);
        return this;
    }

    XContentBuilder value(Calendar value) throws IOException {
        if (value == null) {
            return nullValue();
        }
        return value(DEFAULT_DATE_PRINTER, value.getTimeInMillis());
    }

    XContentBuilder value(DateTimeFormatter formatter, long value) throws IOException {
        ensureFormatterNotNull(formatter);
        return value(formatter.print(value));
    }

    ////////////////////////////////////////////////////////////////////////////
    // GeoPoint & LatLon
    //////////////////////////////////

    public XContentBuilder field(String name, GeoPoint value) throws IOException {
        return field(name).value(value);
    }

    public XContentBuilder value(GeoPoint value) throws IOException {
        if (value == null) {
            return nullValue();
        }
        return latlon(value.getLat(), value.getLon());
    }

    public XContentBuilder latlon(String name, double lat, double lon) throws IOException {
        return field(name).latlon(lat, lon);
    }

    public XContentBuilder latlon(double lat, double lon) throws IOException {
        return startObject().field("lat", lat).field("lon", lon).endObject();
    }

    ////////////////////////////////////////////////////////////////////////////
    // Path
    //////////////////////////////////

    public XContentBuilder value(Path value) throws IOException {
        if (value == null) {
            return nullValue();
        }
        return value(value.toString());
    }

    ////////////////////////////////////////////////////////////////////////////
    // Objects
    //
    // These methods are used when the type of value is unknown. It tries to fallback
    // on typed methods and use Object.toString() as a last resort. Always prefer using
    // typed methods over this.
    //////////////////////////////////

    public XContentBuilder field(String name, Object value) throws IOException {
        return field(name).value(value);
    }

    public XContentBuilder array(String name, Object... values) throws IOException {
        return field(name).values(values);
    }

    XContentBuilder values(Object[] values) throws IOException {
        if (values == null) {
            return nullValue();
        }

        // checks that the array of object does not contain references to itself because
        // iterating over entries will cause a stackoverflow error
        ensureNoSelfReferences(values);

        startArray();
        for (Object o : values) {
            value(o);
        }
        endArray();
        return this;
    }

    public XContentBuilder value(Object value) throws IOException {
        unknownValue(value);
        return this;
    }

    private void unknownValue(Object value) throws IOException {
        if (value == null) {
            nullValue();
            return;
        }
        Writer writer = WRITERS.get(value.getClass());
        if (writer != null) {
            writer.write(this, value);
        } else if (value instanceof Path) {
            //Path implements Iterable<Path> and causes endless recursion and a StackOverFlow if treated as an Iterable here
            value((Path) value);
        } else if (value instanceof Map) {
            map((Map) value);
        } else if (value instanceof Iterable) {
            value((Iterable<?>) value);
        } else if (value instanceof Object[]) {
            values((Object[]) value);
        } else if (value instanceof Calendar) {
            value((Calendar) value);
        } else if (value instanceof ReadableInstant) {
            value((ReadableInstant) value);
        } else if (value instanceof BytesReference) {
            value((BytesReference) value);
        } else if (value instanceof ToXContent) {
            value((ToXContent) value);
        } else {
            // This is a "value" object (like enum, DistanceUnit, etc) just toString() it
            // (yes, it can be misleading when toString a Java class, but really, jackson should be used in that case)
            value(Objects.toString(value));
        }
    }

    ////////////////////////////////////////////////////////////////////////////
    // ToXContent
    //////////////////////////////////

    public XContentBuilder field(String name, ToXContent value) throws IOException {
        return field(name).value(value);
    }

    public XContentBuilder field(String name, ToXContent value, ToXContent.Params params) throws IOException {
        return field(name).value(value, params);
    }

    private XContentBuilder value(ToXContent value) throws IOException {
        return value(value, ToXContent.EMPTY_PARAMS);
    }

    private XContentBuilder value(ToXContent value, ToXContent.Params params) throws IOException {
        if (value == null) {
            return nullValue();
        }
        value.toXContent(this, params);
        return this;
    }

    ////////////////////////////////////////////////////////////////////////////
    // Maps & Iterable
    //////////////////////////////////

    public XContentBuilder field(String name, Map<String, Object> values) throws IOException {
        return field(name).map(values);
    }

    public XContentBuilder map(Map<String, ?> values) throws IOException {
        if (values == null) {
            return nullValue();
        }

        // checks that the map does not contain references to itself because
        // iterating over map entries will cause a stackoverflow error
        ensureNoSelfReferences(values);

        startObject();
        for (Map.Entry<String, ?> value : values.entrySet()) {
            field(value.getKey());
            unknownValue(value.getValue());
        }
        endObject();
        return this;
    }

    public XContentBuilder field(String name, Iterable<?> values) throws IOException {
        return field(name).value(values);
    }

    private XContentBuilder value(Iterable<?> values) throws IOException {
        if (values == null) {
            return nullValue();
        }

        if (values instanceof Path) {
            //treat as single value
            value((Path) values);
        } else {
            // checks that the iterable does not contain references to itself because
            // iterating over entries will cause a stackoverflow error
            ensureNoSelfReferences(values);

            startArray();
            for (Object value : values) {
                unknownValue(value);
            }
            endArray();
        }
        return this;
    }

    ////////////////////////////////////////////////////////////////////////////
    // Misc.
    //////////////////////////////////

    public XContentBuilder timeValueField(String rawFieldName, String readableFieldName, TimeValue timeValue) throws IOException {
        if (humanReadable) {
            field(readableFieldName, timeValue.toString());
        }
        field(rawFieldName, timeValue.millis());
        return this;
    }

    public XContentBuilder timeValueField(String rawFieldName, String readableFieldName, long rawTime) throws IOException {
        if (humanReadable) {
            field(readableFieldName, new TimeValue(rawTime).toString());
        }
        field(rawFieldName, rawTime);
        return this;
    }

    public XContentBuilder timeValueField(String rawFieldName, String readableFieldName, long rawTime, TimeUnit timeUnit) throws
            IOException {
        if (humanReadable) {
            field(readableFieldName, new TimeValue(rawTime, timeUnit).toString());
        }
        field(rawFieldName, rawTime);
        return this;
    }


    public XContentBuilder percentageField(String rawFieldName, String readableFieldName, double percentage) throws IOException {
        if (humanReadable) {
            field(readableFieldName, String.format(Locale.ROOT, "%1.1f%%", percentage));
        }
        field(rawFieldName, percentage);
        return this;
    }

    public XContentBuilder byteSizeField(String rawFieldName, String readableFieldName, ByteSizeValue byteSizeValue) throws IOException {
        if (humanReadable) {
            field(readableFieldName, byteSizeValue.toString());
        }
        field(rawFieldName, byteSizeValue.getBytes());
        return this;
    }

    public XContentBuilder byteSizeField(String rawFieldName, String readableFieldName, long rawSize) throws IOException {
        if (humanReadable) {
            field(readableFieldName, new ByteSizeValue(rawSize).toString());
        }
        field(rawFieldName, rawSize);
        return this;
    }

    ////////////////////////////////////////////////////////////////////////////
    // Raw fields
    //////////////////////////////////

    /**
     * Writes a raw field with the value taken from the bytes in the stream
     * @deprecated use {@link #rawField(String, InputStream, XContentType)} to avoid content type auto-detection
     */
    @Deprecated
    public XContentBuilder rawField(String name, InputStream value) throws IOException {
        generator.writeRawField(name, value);
        return this;
    }

    /**
     * Writes a raw field with the value taken from the bytes in the stream
     */
    public XContentBuilder rawField(String name, InputStream value, XContentType contentType) throws IOException {
        generator.writeRawField(name, value, contentType);
        return this;
    }

    /**
     * Writes a raw field with the given bytes as the value
     * @deprecated use {@link #rawField(String name, BytesReference, XContentType)} to avoid content type auto-detection
     */
    @Deprecated
    public XContentBuilder rawField(String name, BytesReference value) throws IOException {
        generator.writeRawField(name, value);
        return this;
    }

    /**
     * Writes a raw field with the given bytes as the value
     */
    public XContentBuilder rawField(String name, BytesReference value, XContentType contentType) throws IOException {
        generator.writeRawField(name, value, contentType);
        return this;
    }

    /**
     * Writes a value with the source coming directly from the bytes
     * @deprecated use {@link #rawValue(BytesReference, XContentType)} to avoid content type auto-detection
     */
    @Deprecated
    public XContentBuilder rawValue(BytesReference value) throws IOException {
        generator.writeRawValue(value);
        return this;
    }

    /**
     * Writes a value with the source coming directly from the bytes
     */
    public XContentBuilder rawValue(BytesReference value, XContentType contentType) throws IOException {
        generator.writeRawValue(value, contentType);
        return this;
    }

    public XContentBuilder copyCurrentStructure(XContentParser parser) throws IOException {
        generator.copyCurrentStructure(parser);
        return this;
    }

    @Override
    public void flush() throws IOException {
        generator.flush();
    }

    @Override
    public void close() {
        try {
            generator.close();
        } catch (IOException e) {
            throw new IllegalStateException("Failed to close the XContentBuilder", e);
        }
    }

    public XContentGenerator generator() {
        return this.generator;
    }

    public BytesReference bytes() {
        close();
        return ((BytesStream) bos).bytes();
    }

    /**
     * Returns a string representation of the builder (only applicable for text based xcontent).
     */
    public String string() throws IOException {
        return bytes().utf8ToString();
    }

    static void ensureNameNotNull(String name) {
        ensureNotNull(name, "Field name cannot be null");
    }

    static void ensureFormatterNotNull(DateTimeFormatter formatter) {
        ensureNotNull(formatter, "DateTimeFormatter cannot be null");
    }

    static void ensureNotNull(Object value, String message) {
        if (value == null) {
            throw new IllegalArgumentException(message);
        }
    }

    static void ensureNoSelfReferences(Object value) {
        ensureNoSelfReferences(value, Collections.newSetFromMap(new IdentityHashMap<>()));
    }

    private static void ensureNoSelfReferences(final Object value, final Set<Object> ancestors) {
        if (value != null) {

            Iterable<?> it;
            if (value instanceof Map) {
                it = ((Map) value).values();
            } else if ((value instanceof Iterable) && (value instanceof Path == false)) {
                it = (Iterable) value;
            } else if (value instanceof Object[]) {
                it = Arrays.asList((Object[]) value);
            } else {
                return;
            }

            if (ancestors.add(value) == false) {
                throw new IllegalArgumentException("Object has already been built and is self-referencing itself");
            }
            for (Object o : it) {
                ensureNoSelfReferences(o, ancestors);
            }
            ancestors.remove(value);
        }
    }
}
