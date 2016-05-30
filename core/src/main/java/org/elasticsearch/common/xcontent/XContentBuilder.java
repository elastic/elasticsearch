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

import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.math.BigDecimal;
import java.math.RoundingMode;
import java.nio.charset.StandardCharsets;
import java.nio.file.Path;
import java.util.Calendar;
import java.util.Collections;
import java.util.Date;
import java.util.HashMap;
import java.util.Locale;
import java.util.Map;
import java.util.concurrent.TimeUnit;

import org.apache.lucene.util.BytesRef;
import org.elasticsearch.common.bytes.BytesArray;
import org.elasticsearch.common.bytes.BytesReference;
import org.elasticsearch.common.geo.GeoPoint;
import org.elasticsearch.common.io.BytesStream;
import org.elasticsearch.common.io.stream.BytesStreamOutput;
import org.elasticsearch.common.lease.Releasable;
import org.elasticsearch.common.text.Text;
import org.elasticsearch.common.unit.ByteSizeValue;
import org.elasticsearch.common.unit.TimeValue;
import org.joda.time.DateTimeZone;
import org.joda.time.ReadableInstant;
import org.joda.time.format.DateTimeFormatter;
import org.joda.time.format.ISODateTimeFormat;

/**
 * A utility to build XContent (ie json).
 */
public final class XContentBuilder implements BytesStream, Releasable {

    public final static DateTimeFormatter defaultDatePrinter = ISODateTimeFormat.dateTime().withZone(DateTimeZone.UTC);

    public static XContentBuilder builder(XContent xContent) throws IOException {
        return new XContentBuilder(xContent, new BytesStreamOutput());
    }

    public static XContentBuilder builder(XContent xContent, String[] filters) throws IOException {
        return new XContentBuilder(xContent, new BytesStreamOutput(), filters);
    }

    public static XContentBuilder builder(XContent xContent, String[] filters, boolean inclusive) throws IOException {
        return new XContentBuilder(xContent, new BytesStreamOutput(), filters, inclusive);
    }

    private XContentGenerator generator;

    private final OutputStream bos;

    private boolean humanReadable = false;

    /**
     * Constructs a new builder using the provided xcontent and an OutputStream. Make sure
     * to call {@link #close()} when the builder is done with.
     */
    public XContentBuilder(XContent xContent, OutputStream bos) throws IOException {
        this(xContent, bos, null);
    }

    /**
     * Constructs a new builder using the provided xcontent, an OutputStream and
     * some filters. If filters are specified, only those values matching a
     * filter will be written to the output stream. Make sure to call
     * {@link #close()} when the builder is done with.
     */
    public XContentBuilder(XContent xContent, OutputStream bos, String[] filters) throws IOException {
        this(xContent, bos, filters, true);
    }

    /**
     * Constructs a new builder using the provided xcontent, an OutputStream and
     * some filters. If {@code filters} are specified and {@code inclusive} is
     * true, only those values matching a filter will be written to the output
     * stream. If {@code inclusive} is false, those matching will be excluded.
     * Make sure to call {@link #close()} when the builder is done with.
     */
    public XContentBuilder(XContent xContent, OutputStream bos, String[] filters, boolean inclusive) throws IOException {
        this.bos = bos;
        this.generator = xContent.createGenerator(bos, filters, inclusive);
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

    public XContentBuilder lfAtEnd() {
        generator.usePrintLineFeedAtEnd();
        return this;
    }

    public XContentBuilder humanReadable(boolean humanReadable) {
        this.humanReadable = humanReadable;
        return this;
    }

    public boolean humanReadable() {
        return this.humanReadable;
    }

    public XContentBuilder field(String name, ToXContent xContent) throws IOException {
        field(name);
        xContent.toXContent(this, ToXContent.EMPTY_PARAMS);
        return this;
    }

    public XContentBuilder field(String name, ToXContent xContent, ToXContent.Params params) throws IOException {
        field(name);
        xContent.toXContent(this, params);
        return this;
    }

    public XContentBuilder startObject(String name) throws IOException {
        field(name);
        startObject();
        return this;
    }

    public XContentBuilder startObject() throws IOException {
        generator.writeStartObject();
        return this;
    }

    public XContentBuilder endObject() throws IOException {
        generator.writeEndObject();
        return this;
    }

    public XContentBuilder array(String name, String... values) throws IOException {
        startArray(name);
        for (String value : values) {
            value(value);
        }
        endArray();
        return this;
    }

    public XContentBuilder array(String name, Object... values) throws IOException {
        startArray(name);
        for (Object value : values) {
            value(value);
        }
        endArray();
        return this;
    }

    public XContentBuilder startArray(String name) throws IOException {
        field(name);
        startArray();
        return this;
    }

    public XContentBuilder startArray() throws IOException {
        generator.writeStartArray();
        return this;
    }

    public XContentBuilder endArray() throws IOException {
        generator.writeEndArray();
        return this;
    }

    public XContentBuilder field(String name) throws IOException {
        if (name == null) {
            throw new IllegalArgumentException("field name cannot be null");
        }
        generator.writeFieldName(name);
        return this;
    }

    public XContentBuilder field(String name, char[] value, int offset, int length) throws IOException {
        field(name);
        if (value == null) {
            generator.writeNull();
        } else {
            generator.writeString(value, offset, length);
        }
        return this;
    }

    public XContentBuilder field(String name, String value) throws IOException {
        field(name);
        if (value == null) {
            generator.writeNull();
        } else {
            generator.writeString(value);
        }
        return this;
    }

    public XContentBuilder field(String name, Integer value) throws IOException {
        field(name);
        if (value == null) {
            generator.writeNull();
        } else {
            generator.writeNumber(value.intValue());
        }
        return this;
    }

    public XContentBuilder field(String name, int value) throws IOException {
        field(name);
        generator.writeNumber(value);
        return this;
    }

    public XContentBuilder field(String name, Long value) throws IOException {
        field(name);
        if (value == null) {
            generator.writeNull();
        } else {
            generator.writeNumber(value.longValue());
        }
        return this;
    }

    public XContentBuilder field(String name, long value) throws IOException {
        field(name);
        generator.writeNumber(value);
        return this;
    }

    public XContentBuilder field(String name, Float value) throws IOException {
        field(name);
        if (value == null) {
            generator.writeNull();
        } else {
            generator.writeNumber(value.floatValue());
        }
        return this;
    }

    public XContentBuilder field(String name, float value) throws IOException {
        field(name);
        generator.writeNumber(value);
        return this;
    }

    public XContentBuilder field(String name, Double value) throws IOException {
        field(name);
        if (value == null) {
            generator.writeNull();
        } else {
            generator.writeNumber(value);
        }
        return this;
    }

    public XContentBuilder field(String name, double value) throws IOException {
        field(name);
        generator.writeNumber(value);
        return this;
    }

    public XContentBuilder field(String name, BigDecimal value) throws IOException {
        return field(name, value, value.scale(), RoundingMode.HALF_UP, true);
    }

    public XContentBuilder field(String name, BigDecimal value, int scale, RoundingMode rounding, boolean toDouble) throws IOException {
        field(name);
        if (value == null) {
            generator.writeNull();
        } else {
            if (toDouble) {
                try {
                    generator.writeNumber(value.setScale(scale, rounding).doubleValue());
                } catch (ArithmeticException e) {
                    generator.writeString(value.toEngineeringString());
                }
            } else {
                generator.writeString(value.toEngineeringString());
            }
        }
        return this;
    }

    /**
     * Writes the binary content of the given BytesRef
     * Use {@link org.elasticsearch.common.xcontent.XContentParser#binaryValue()} to read the value back
     */
    public XContentBuilder field(String name, BytesRef value) throws IOException {
        field(name);
        generator.writeBinary(value.bytes, value.offset, value.length);
        return this;
    }

    /**
     * Writes the binary content of the given BytesReference
     * Use {@link org.elasticsearch.common.xcontent.XContentParser#binaryValue()} to read the value back
     */
    public XContentBuilder field(String name, BytesReference value) throws IOException {
        field(name);
        if (!value.hasArray()) {
            value = value.toBytesArray();
        }
        generator.writeBinary(value.array(), value.arrayOffset(), value.length());
        return this;
    }

    /**
     * Writes the binary content of the given BytesRef as UTF-8 bytes
     * Use {@link XContentParser#utf8Bytes()} to read the value back
     */
    public XContentBuilder utf8Field(String name, BytesRef value) throws IOException {
        field(name);
        generator.writeUTF8String(value.bytes, value.offset, value.length);
        return this;
    }

    public XContentBuilder field(String name, Text value) throws IOException {
        field(name);
        if (value.hasBytes() && value.bytes().hasArray()) {
            generator.writeUTF8String(value.bytes().array(), value.bytes().arrayOffset(), value.bytes().length());
            return this;
        }
        if (value.hasString()) {
            generator.writeString(value.string());
            return this;
        }
        // TODO: TextBytesOptimization we can use a buffer here to convert it? maybe add a request to jackson to support InputStream as well?
        BytesArray bytesArray = value.bytes().toBytesArray();
        generator.writeUTF8String(bytesArray.array(), bytesArray.arrayOffset(), bytesArray.length());
        return this;
    }

    public XContentBuilder field(String name, byte[] value, int offset, int length) throws IOException {
        field(name);
        generator.writeBinary(value, offset, length);
        return this;
    }

    public XContentBuilder field(String name, Map<String, Object> value) throws IOException {
        field(name);
        value(value);
        return this;
    }

    public XContentBuilder field(String name, Iterable<?> value) throws IOException {
        if (value instanceof Path) {
            //treat Paths as single value
            field(name);
            value(value);
        } else {
            startArray(name);
            for (Object o : value) {
                value(o);
            }
            endArray();
        }
        return this;
    }

    public XContentBuilder field(String name, boolean... value) throws IOException {
        startArray(name);
        for (boolean o : value) {
            value(o);
        }
        endArray();
        return this;
    }

    public XContentBuilder field(String name, String... value) throws IOException {
        startArray(name);
        for (String o : value) {
            value(o);
        }
        endArray();
        return this;
    }

    public XContentBuilder field(String name, Object... value) throws IOException {
        startArray(name);
        for (Object o : value) {
            value(o);
        }
        endArray();
        return this;
    }

    public XContentBuilder field(String name, int... value) throws IOException {
        startArray(name);
        for (Object o : value) {
            value(o);
        }
        endArray();
        return this;
    }

    public XContentBuilder field(String name, long... value) throws IOException {
        startArray(name);
        for (Object o : value) {
            value(o);
        }
        endArray();
        return this;
    }

    public XContentBuilder field(String name, float... value) throws IOException {
        startArray(name);
        for (Object o : value) {
            value(o);
        }
        endArray();
        return this;
    }

    public XContentBuilder field(String name, double... value) throws IOException {
        startArray(name);
        for (Object o : value) {
            value(o);
        }
        endArray();
        return this;
    }

    public XContentBuilder field(String name, Object value) throws IOException {
        field(name);
        writeValue(value);
        return this;
    }

    public XContentBuilder value(Object value) throws IOException {
        writeValue(value);
        return this;
    }

    public XContentBuilder field(String name, boolean value) throws IOException {
        field(name);
        generator.writeBoolean(value);
        return this;
    }

    public XContentBuilder field(String name, byte[] value) throws IOException {
        field(name);
        if (value == null) {
            generator.writeNull();
        } else {
            generator.writeBinary(value);
        }
        return this;
    }

    public XContentBuilder field(String name, ReadableInstant date) throws IOException {
        field(name);
        return value(date);
    }

    public XContentBuilder field(String name, ReadableInstant date, DateTimeFormatter formatter) throws IOException {
        field(name);
        return value(date, formatter);
    }

    public XContentBuilder field(String name, Date date) throws IOException {
        field(name);
        return value(date);
    }

    public XContentBuilder field(String name, Date date, DateTimeFormatter formatter) throws IOException {
        field(name);
        return value(date, formatter);
    }

    public XContentBuilder nullField(String name) throws IOException {
        generator.writeNullField(name);
        return this;
    }

    public XContentBuilder nullValue() throws IOException {
        generator.writeNull();
        return this;
    }

    public XContentBuilder rawField(String fieldName, InputStream content) throws IOException {
        generator.writeRawField(fieldName, content);
        return this;
    }

    public XContentBuilder rawField(String fieldName, BytesReference content) throws IOException {
        generator.writeRawField(fieldName, content);
        return this;
    }

    public XContentBuilder rawValue(BytesReference content) throws IOException {
        generator.writeRawValue(content);
        return this;
    }

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

    public XContentBuilder dateValueField(String rawFieldName, String readableFieldName, long rawTimestamp) throws IOException {
        if (humanReadable) {
            field(readableFieldName, defaultDatePrinter.print(rawTimestamp));
        }
        field(rawFieldName, rawTimestamp);
        return this;
    }

    public XContentBuilder byteSizeField(String rawFieldName, String readableFieldName, ByteSizeValue byteSizeValue) throws IOException {
        if (humanReadable) {
            field(readableFieldName, byteSizeValue.toString());
        }
        field(rawFieldName, byteSizeValue.bytes());
        return this;
    }

    public XContentBuilder byteSizeField(String rawFieldName, String readableFieldName, long rawSize) throws IOException {
        if (humanReadable) {
            field(readableFieldName, new ByteSizeValue(rawSize).toString());
        }
        field(rawFieldName, rawSize);
        return this;
    }

    public XContentBuilder percentageField(String rawFieldName, String readableFieldName, double percentage) throws IOException {
        if (humanReadable) {
            field(readableFieldName, String.format(Locale.ROOT, "%1.1f%%", percentage));
        }
        field(rawFieldName, percentage);
        return this;
    }

    public XContentBuilder value(Boolean value) throws IOException {
        if (value == null) {
            return nullValue();
        }
        return value(value.booleanValue());
    }

    public XContentBuilder value(boolean value) throws IOException {
        generator.writeBoolean(value);
        return this;
    }

    public XContentBuilder value(ReadableInstant date) throws IOException {
        return value(date, defaultDatePrinter);
    }

    public XContentBuilder value(ReadableInstant date, DateTimeFormatter dateTimeFormatter) throws IOException {
        if (date == null) {
            return nullValue();
        }
        return value(dateTimeFormatter.print(date));
    }

    public XContentBuilder value(Date date) throws IOException {
        return value(date, defaultDatePrinter);
    }

    public XContentBuilder value(Date date, DateTimeFormatter dateTimeFormatter) throws IOException {
        if (date == null) {
            return nullValue();
        }
        return value(dateTimeFormatter.print(date.getTime()));
    }

    public XContentBuilder value(Integer value) throws IOException {
        if (value == null) {
            return nullValue();
        }
        return value(value.intValue());
    }

    public XContentBuilder value(int value) throws IOException {
        generator.writeNumber(value);
        return this;
    }

    public XContentBuilder value(Long value) throws IOException {
        if (value == null) {
            return nullValue();
        }
        return value(value.longValue());
    }

    public XContentBuilder value(long value) throws IOException {
        generator.writeNumber(value);
        return this;
    }

    public XContentBuilder value(Float value) throws IOException {
        if (value == null) {
            return nullValue();
        }
        return value(value.floatValue());
    }

    public XContentBuilder value(float value) throws IOException {
        generator.writeNumber(value);
        return this;
    }

    public XContentBuilder value(Double value) throws IOException {
        if (value == null) {
            return nullValue();
        }
        return value(value.doubleValue());
    }

    public XContentBuilder value(double value) throws IOException {
        generator.writeNumber(value);
        return this;
    }

    public XContentBuilder value(String value) throws IOException {
        if (value == null) {
            return nullValue();
        }
        generator.writeString(value);
        return this;
    }

    public XContentBuilder value(byte[] value) throws IOException {
        if (value == null) {
            return nullValue();
        }
        generator.writeBinary(value);
        return this;
    }

    public XContentBuilder value(byte[] value, int offset, int length) throws IOException {
        if (value == null) {
            return nullValue();
        }
        generator.writeBinary(value, offset, length);
        return this;
    }

    /**
     * Writes the binary content of the given BytesRef
     * Use {@link org.elasticsearch.common.xcontent.XContentParser#binaryValue()} to read the value back
     */
    public XContentBuilder value(BytesRef value) throws IOException {
        if (value == null) {
            return nullValue();
        }
        generator.writeBinary(value.bytes, value.offset, value.length);
        return this;
    }

    /**
     * Writes the binary content of the given BytesReference
     * Use {@link org.elasticsearch.common.xcontent.XContentParser#binaryValue()} to read the value back
     */
    public XContentBuilder value(BytesReference value) throws IOException {
        if (value == null) {
            return nullValue();
        }
        if (!value.hasArray()) {
            value = value.toBytesArray();
        }
        generator.writeBinary(value.array(), value.arrayOffset(), value.length());
        return this;
    }

    public XContentBuilder value(Text value) throws IOException {
        if (value == null) {
            return nullValue();
        }
        if (value.hasBytes() && value.bytes().hasArray()) {
            generator.writeUTF8String(value.bytes().array(), value.bytes().arrayOffset(), value.bytes().length());
            return this;
        }
        if (value.hasString()) {
            generator.writeString(value.string());
            return this;
        }
        BytesArray bytesArray = value.bytes().toBytesArray();
        generator.writeUTF8String(bytesArray.array(), bytesArray.arrayOffset(), bytesArray.length());
        return this;
    }

    public XContentBuilder map(Map<String, ?> map) throws IOException {
        if (map == null) {
            return nullValue();
        }
        writeMap(map);
        return this;
    }

    public XContentBuilder value(Map<String, Object> map) throws IOException {
        if (map == null) {
            return nullValue();
        }
        writeMap(map);
        return this;
    }

    public XContentBuilder value(Iterable<?> value) throws IOException {
        if (value == null) {
            return nullValue();
        }
        if (value instanceof Path) {
            //treat as single value
            writeValue(value);
        } else {
            startArray();
            for (Object o : value) {
                value(o);
            }
            endArray();
        }
        return this;
    }

    public XContentBuilder latlon(String name, double lat, double lon) throws IOException {
        return startObject(name).field("lat", lat).field("lon", lon).endObject();
    }

    public XContentBuilder latlon(double lat, double lon) throws IOException {
        return startObject().field("lat", lat).field("lon", lon).endObject();
    }

    public XContentBuilder copyCurrentStructure(XContentParser parser) throws IOException {
        generator.copyCurrentStructure(parser);
        return this;
    }

    public XContentBuilder flush() throws IOException {
        generator.flush();
        return this;
    }

    @Override
    public void close() {
        try {
            generator.close();
        } catch (IOException e) {
            throw new IllegalStateException("failed to close the XContentBuilder", e);
        }
    }

    public XContentGenerator generator() {
        return this.generator;
    }

    @Override
    public BytesReference bytes() {
        close();
        return ((BytesStream) bos).bytes();
    }

    /**
     * Returns a string representation of the builder (only applicable for text based xcontent).
     */
    public String string() throws IOException {
        close();
        BytesArray bytesArray = bytes().toBytesArray();
        return new String(bytesArray.array(), bytesArray.arrayOffset(), bytesArray.length(), StandardCharsets.UTF_8);
    }


    private void writeMap(Map<String, ?> map) throws IOException {
        generator.writeStartObject();

        for (Map.Entry<String, ?> entry : map.entrySet()) {
            field(entry.getKey());
            Object value = entry.getValue();
            if (value == null) {
                generator.writeNull();
            } else {
                writeValue(value);
            }
        }
        generator.writeEndObject();
    }

    @FunctionalInterface
    interface Writer {
        void write(XContentGenerator g, Object v) throws IOException;
    }

    private final static Map<Class<?>, Writer> MAP;

    static {
        Map<Class<?>, Writer> map = new HashMap<>();
        map.put(String.class, (g, v) -> g.writeString((String) v));
        map.put(Integer.class, (g, v) -> g.writeNumber((Integer) v));
        map.put(Long.class, (g, v) -> g.writeNumber((Long) v));
        map.put(Float.class, (g, v) -> g.writeNumber((Float) v));
        map.put(Double.class, (g, v) -> g.writeNumber((Double) v));
        map.put(Byte.class, (g, v) -> g.writeNumber((Byte) v));
        map.put(Short.class, (g, v) -> g.writeNumber((Short) v));
        map.put(Boolean.class, (g, v) -> g.writeBoolean((Boolean) v));
        map.put(GeoPoint.class, (g, v) -> {
            g.writeStartObject();
            g.writeNumberField("lat", ((GeoPoint) v).lat());
            g.writeNumberField("lon", ((GeoPoint) v).lon());
            g.writeEndObject();
        });
        map.put(int[].class, (g, v) -> {
            g.writeStartArray();
            for (int item : (int[]) v) {
                g.writeNumber(item);
            }
            g.writeEndArray();
        });
        map.put(long[].class, (g, v) -> {
            g.writeStartArray();
            for (long item : (long[]) v) {
                g.writeNumber(item);
            }
            g.writeEndArray();
        });
        map.put(float[].class, (g, v) -> {
            g.writeStartArray();
            for (float item : (float[]) v) {
                g.writeNumber(item);
            }
            g.writeEndArray();
        });
        map.put(double[].class, (g, v) -> {
            g.writeStartArray();
            for (double item : (double[])v) {
                g.writeNumber(item);
            }
            g.writeEndArray();
        });
        map.put(byte[].class, (g, v) -> g.writeBinary((byte[]) v));
        map.put(short[].class, (g, v) -> {
            g.writeStartArray();
            for (short item : (short[])v) {
                g.writeNumber(item);
            }
            g.writeEndArray();
        });
        map.put(BytesRef.class, (g, v) -> {
            BytesRef bytes = (BytesRef) v;
            g.writeBinary(bytes.bytes, bytes.offset, bytes.length);
        });
        map.put(Text.class, (g, v) -> {
            Text text = (Text) v;
            if (text.hasBytes() && text.bytes().hasArray()) {
                g.writeUTF8String(text.bytes().array(), text.bytes().arrayOffset(), text.bytes().length());
            } else if (text.hasString()) {
                g.writeString(text.string());
            } else {
                BytesArray bytesArray = text.bytes().toBytesArray();
                g.writeUTF8String(bytesArray.array(), bytesArray.arrayOffset(), bytesArray.length());
            }
        });
        MAP = Collections.unmodifiableMap(map);
    }

    private void writeValue(Object value) throws IOException {
        if (value == null) {
            generator.writeNull();
            return;
        }
        Class<?> type = value.getClass();
        Writer writer = MAP.get(type);
        if (writer != null) {
            writer.write(generator, value);
        } else if (value instanceof Map) {
            writeMap((Map) value);
        } else if (value instanceof Path) {
            //Path implements Iterable<Path> and causes endless recursion and a StackOverFlow if treated as an Iterable here
            generator.writeString(value.toString());
        } else if (value instanceof Iterable) {
            writeIterable((Iterable<?>) value);
        } else if (value instanceof Object[]) {
            writeObjectArray((Object[]) value);
        } else if (value instanceof Date) {
            generator.writeString(XContentBuilder.defaultDatePrinter.print(((Date) value).getTime()));
        } else if (value instanceof Calendar) {
            generator.writeString(XContentBuilder.defaultDatePrinter.print((((Calendar) value)).getTimeInMillis()));
        } else if (value instanceof ReadableInstant) {
            generator.writeString(XContentBuilder.defaultDatePrinter.print((((ReadableInstant) value)).getMillis()));
        } else if (value instanceof BytesReference) {
            writeBytesReference((BytesReference) value);
        } else if (value instanceof ToXContent) {
            ((ToXContent) value).toXContent(this, ToXContent.EMPTY_PARAMS);
        } else {
            // if this is a "value" object, like enum, DistanceUnit, ..., just toString it
            // yea, it can be misleading when toString a Java class, but really, jackson should be used in that case
            generator.writeString(value.toString());
            //throw new ElasticsearchIllegalArgumentException("type not supported for generic value conversion: " + type);
        }
    }

    private void writeBytesReference(BytesReference value) throws IOException {
        BytesReference bytes = value;
        if (!bytes.hasArray()) {
            bytes = bytes.toBytesArray();
        }
        generator.writeBinary(bytes.array(), bytes.arrayOffset(), bytes.length());
    }

    private void writeIterable(Iterable<?> value) throws IOException {
        generator.writeStartArray();
        for (Object v : value) {
            writeValue(v);
        }
        generator.writeEndArray();
    }

    private void writeObjectArray(Object[] value) throws IOException {
        generator.writeStartArray();
        for (Object v : value) {
            writeValue(v);
        }
        generator.writeEndArray();
    }

}
