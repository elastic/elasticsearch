/*
 * Licensed to ElasticSearch and Shay Banon under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership. ElasticSearch licenses this
 * file to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
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

import com.google.common.base.Charsets;
import org.elasticsearch.common.Nullable;
import org.elasticsearch.common.Strings;
import org.elasticsearch.common.bytes.BytesArray;
import org.elasticsearch.common.bytes.BytesReference;
import org.elasticsearch.common.io.BytesStream;
import org.elasticsearch.common.io.FastByteArrayOutputStream;
import org.elasticsearch.common.text.Text;
import org.elasticsearch.common.xcontent.support.XContentMapConverter;
import org.joda.time.DateTimeZone;
import org.joda.time.ReadableInstant;
import org.joda.time.format.DateTimeFormatter;
import org.joda.time.format.ISODateTimeFormat;

import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.util.Date;
import java.util.Map;

/**
 *
 */
public final class XContentBuilder implements BytesStream {

    public static enum FieldCaseConversion {
        /**
         * No came conversion will occur.
         */
        NONE,
        /**
         * Camel Case will be converted to Underscore casing.
         */
        UNDERSCORE,
        /**
         * Underscore will be converted to Camel case conversion.
         */
        CAMELCASE
    }

    public final static DateTimeFormatter defaultDatePrinter = ISODateTimeFormat.dateTime().withZone(DateTimeZone.UTC);

    protected static FieldCaseConversion globalFieldCaseConversion = FieldCaseConversion.NONE;

    public static void globalFieldCaseConversion(FieldCaseConversion globalFieldCaseConversion) {
        XContentBuilder.globalFieldCaseConversion = globalFieldCaseConversion;
    }

    /**
     * Constructs a new builder using a fresh {@link FastByteArrayOutputStream}.
     */
    public static XContentBuilder builder(XContent xContent) throws IOException {
        return new XContentBuilder(xContent, new FastByteArrayOutputStream());
    }


    private XContentGenerator generator;

    private final OutputStream bos;

    private final Object payload;

    private FieldCaseConversion fieldCaseConversion = globalFieldCaseConversion;

    private StringBuilder cachedStringBuilder;


    /**
     * Constructs a new builder using the provided xcontent and an OutputStream. Make sure
     * to call {@link #close()} when the builder is done with.
     */
    public XContentBuilder(XContent xContent, OutputStream bos) throws IOException {
        this(xContent, bos, null);
    }

    /**
     * Constructs a new builder using the provided xcontent and an OutputStream. Make sure
     * to call {@link #close()} when the builder is done with.
     */
    public XContentBuilder(XContent xContent, OutputStream bos, @Nullable Object payload) throws IOException {
        this.bos = bos;
        this.generator = xContent.createGenerator(bos);
        this.payload = payload;
    }

    public XContentBuilder fieldCaseConversion(FieldCaseConversion fieldCaseConversion) {
        this.fieldCaseConversion = fieldCaseConversion;
        return this;
    }

    public XContentType contentType() {
        return generator.contentType();
    }

    public XContentBuilder prettyPrint() {
        generator.usePrettyPrint();
        return this;
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

    public XContentBuilder startObject(String name, FieldCaseConversion conversion) throws IOException {
        field(name, conversion);
        startObject();
        return this;
    }

    public XContentBuilder startObject(XContentBuilderString name) throws IOException {
        field(name);
        startObject();
        return this;
    }

    public XContentBuilder startObject(XContentBuilderString name, FieldCaseConversion conversion) throws IOException {
        field(name, conversion);
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

    public XContentBuilder array(XContentBuilderString name, String... values) throws IOException {
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

    public XContentBuilder array(XContentBuilderString name, Object... values) throws IOException {
        startArray(name);
        for (Object value : values) {
            value(value);
        }
        endArray();
        return this;
    }

    public XContentBuilder startArray(String name, FieldCaseConversion conversion) throws IOException {
        field(name, conversion);
        startArray();
        return this;
    }

    public XContentBuilder startArray(String name) throws IOException {
        field(name);
        startArray();
        return this;
    }

    public XContentBuilder startArray(XContentBuilderString name) throws IOException {
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

    public XContentBuilder field(XContentBuilderString name) throws IOException {
        if (fieldCaseConversion == FieldCaseConversion.UNDERSCORE) {
            generator.writeFieldName(name.underscore());
        } else if (fieldCaseConversion == FieldCaseConversion.CAMELCASE) {
            generator.writeFieldName(name.camelCase());
        } else {
            generator.writeFieldName(name.underscore());
        }
        return this;
    }

    public XContentBuilder field(XContentBuilderString name, FieldCaseConversion conversion) throws IOException {
        if (conversion == FieldCaseConversion.UNDERSCORE) {
            generator.writeFieldName(name.underscore());
        } else if (conversion == FieldCaseConversion.CAMELCASE) {
            generator.writeFieldName(name.camelCase());
        } else {
            generator.writeFieldName(name.underscore());
        }
        return this;
    }

    public XContentBuilder field(String name) throws IOException {
        if (fieldCaseConversion == FieldCaseConversion.UNDERSCORE) {
            if (cachedStringBuilder == null) {
                cachedStringBuilder = new StringBuilder();
            }
            name = Strings.toUnderscoreCase(name, cachedStringBuilder);
        } else if (fieldCaseConversion == FieldCaseConversion.CAMELCASE) {
            if (cachedStringBuilder == null) {
                cachedStringBuilder = new StringBuilder();
            }
            name = Strings.toCamelCase(name, cachedStringBuilder);
        }
        generator.writeFieldName(name);
        return this;
    }

    public XContentBuilder field(String name, FieldCaseConversion conversion) throws IOException {
        if (conversion == FieldCaseConversion.UNDERSCORE) {
            if (cachedStringBuilder == null) {
                cachedStringBuilder = new StringBuilder();
            }
            name = Strings.toUnderscoreCase(name, cachedStringBuilder);
        } else if (conversion == FieldCaseConversion.CAMELCASE) {
            if (cachedStringBuilder == null) {
                cachedStringBuilder = new StringBuilder();
            }
            name = Strings.toCamelCase(name, cachedStringBuilder);
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

    public XContentBuilder field(XContentBuilderString name, char[] value, int offset, int length) throws IOException {
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

    public XContentBuilder field(String name, String value, FieldCaseConversion conversion) throws IOException {
        field(name, conversion);
        if (value == null) {
            generator.writeNull();
        } else {
            generator.writeString(value);
        }
        return this;
    }

    public XContentBuilder field(XContentBuilderString name, String value) throws IOException {
        field(name);
        if (value == null) {
            generator.writeNull();
        } else {
            generator.writeString(value);
        }
        return this;
    }

    public XContentBuilder field(XContentBuilderString name, String value, FieldCaseConversion conversion) throws IOException {
        field(name, conversion);
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

    public XContentBuilder field(XContentBuilderString name, Integer value) throws IOException {
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

    public XContentBuilder field(XContentBuilderString name, int value) throws IOException {
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

    public XContentBuilder field(XContentBuilderString name, Long value) throws IOException {
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

    public XContentBuilder field(XContentBuilderString name, long value) throws IOException {
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

    public XContentBuilder field(XContentBuilderString name, Float value) throws IOException {
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

    public XContentBuilder field(XContentBuilderString name, float value) throws IOException {
        field(name);
        generator.writeNumber(value);
        return this;
    }

    public XContentBuilder field(String name, Double value) throws IOException {
        field(name);
        if (value == null) {
            generator.writeNull();
        } else {
            generator.writeNumber(value.doubleValue());
        }
        return this;
    }

    public XContentBuilder field(XContentBuilderString name, Double value) throws IOException {
        field(name);
        if (value == null) {
            generator.writeNull();
        } else {
            generator.writeNumber(value.doubleValue());
        }
        return this;
    }

    public XContentBuilder field(String name, double value) throws IOException {
        field(name);
        generator.writeNumber(value);
        return this;
    }

    public XContentBuilder field(XContentBuilderString name, double value) throws IOException {
        field(name);
        generator.writeNumber(value);
        return this;
    }

    public XContentBuilder field(String name, BytesReference value) throws IOException {
        field(name);
        if (!value.hasArray()) {
            value = value.toBytesArray();
        }
        generator.writeBinary(value.array(), value.arrayOffset(), value.length());
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

    public XContentBuilder field(XContentBuilderString name, Map<String, Object> value) throws IOException {
        field(name);
        value(value);
        return this;
    }

    public XContentBuilder field(String name, Iterable value) throws IOException {
        startArray(name);
        for (Object o : value) {
            value(o);
        }
        endArray();
        return this;
    }

    public XContentBuilder field(XContentBuilderString name, Iterable value) throws IOException {
        startArray(name);
        for (Object o : value) {
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


    public XContentBuilder field(XContentBuilderString name, String... value) throws IOException {
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

    public XContentBuilder field(XContentBuilderString name, Object... value) throws IOException {
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

    public XContentBuilder field(XContentBuilderString name, int... value) throws IOException {
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

    public XContentBuilder field(XContentBuilderString name, long... value) throws IOException {
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

    public XContentBuilder field(XContentBuilderString name, float... value) throws IOException {
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

    public XContentBuilder field(XContentBuilderString name, double... value) throws IOException {
        startArray(name);
        for (Object o : value) {
            value(o);
        }
        endArray();
        return this;
    }

    public XContentBuilder field(String name, Object value) throws IOException {
        if (value == null) {
            nullField(name);
            return this;
        }
        Class type = value.getClass();
        if (type == String.class) {
            field(name, (String) value);
        } else if (type == Float.class) {
            field(name, ((Float) value).floatValue());
        } else if (type == Double.class) {
            field(name, ((Double) value).doubleValue());
        } else if (type == Integer.class) {
            field(name, ((Integer) value).intValue());
        } else if (type == Long.class) {
            field(name, ((Long) value).longValue());
        } else if (type == Short.class) {
            field(name, ((Short) value).shortValue());
        } else if (type == Byte.class) {
            field(name, ((Byte) value).byteValue());
        } else if (type == Boolean.class) {
            field(name, ((Boolean) value).booleanValue());
        } else if (value instanceof Date) {
            field(name, (Date) value);
        } else if (type == byte[].class) {
            field(name, (byte[]) value);
        } else if (value instanceof ReadableInstant) {
            field(name, (ReadableInstant) value);
        } else if (value instanceof Map) {
            //noinspection unchecked
            field(name, (Map<String, Object>) value);
        } else if (value instanceof Iterable) {
            field(name, (Iterable) value);
        } else if (value instanceof Object[]) {
            field(name, (Object[]) value);
        } else if (value instanceof int[]) {
            field(name, (int[]) value);
        } else if (value instanceof long[]) {
            field(name, (long[]) value);
        } else if (value instanceof float[]) {
            field(name, (float[]) value);
        } else if (value instanceof double[]) {
            field(name, (double[]) value);
        } else if (value instanceof ToXContent) {
            field(name, (ToXContent) value);
        } else {
            field(name, value.toString());
        }
        return this;
    }

    public XContentBuilder field(XContentBuilderString name, Object value) throws IOException {
        if (value == null) {
            nullField(name);
            return this;
        }
        Class type = value.getClass();
        if (type == String.class) {
            field(name, (String) value);
        } else if (type == Float.class) {
            field(name, ((Float) value).floatValue());
        } else if (type == Double.class) {
            field(name, ((Double) value).doubleValue());
        } else if (type == Integer.class) {
            field(name, ((Integer) value).intValue());
        } else if (type == Long.class) {
            field(name, ((Long) value).longValue());
        } else if (type == Short.class) {
            field(name, ((Short) value).shortValue());
        } else if (type == Byte.class) {
            field(name, ((Byte) value).byteValue());
        } else if (type == Boolean.class) {
            field(name, ((Boolean) value).booleanValue());
        } else if (value instanceof Date) {
            field(name, (Date) value);
        } else if (type == byte[].class) {
            field(name, (byte[]) value);
        } else if (value instanceof ReadableInstant) {
            field(name, (ReadableInstant) value);
        } else if (value instanceof Map) {
            //noinspection unchecked
            field(name, (Map<String, Object>) value);
        } else if (value instanceof Iterable) {
            field(name, (Iterable) value);
        } else if (value instanceof Object[]) {
            field(name, (Object[]) value);
        } else if (value instanceof int[]) {
            field(name, (int[]) value);
        } else if (value instanceof long[]) {
            field(name, (long[]) value);
        } else if (value instanceof float[]) {
            field(name, (float[]) value);
        } else if (value instanceof double[]) {
            field(name, (double[]) value);
        } else if (value instanceof BytesReference) {
            field(name, (BytesReference) value);
        } else if (value instanceof Text) {
            field(name, (Text) value);
        } else {
            field(name, value.toString());
        }
        return this;
    }

    public XContentBuilder value(Object value) throws IOException {
        if (value == null) {
            return nullValue();
        }
        Class type = value.getClass();
        if (type == String.class) {
            value((String) value);
        } else if (type == Float.class) {
            value(((Float) value).floatValue());
        } else if (type == Double.class) {
            value(((Double) value).doubleValue());
        } else if (type == Integer.class) {
            value(((Integer) value).intValue());
        } else if (type == Long.class) {
            value(((Long) value).longValue());
        } else if (type == Short.class) {
            value(((Short) value).shortValue());
        } else if (type == Byte.class) {
            value(((Byte) value).byteValue());
        } else if (type == Boolean.class) {
            value((Boolean) value);
        } else if (type == byte[].class) {
            value((byte[]) value);
        } else if (value instanceof Date) {
            value((Date) value);
        } else if (value instanceof ReadableInstant) {
            value((ReadableInstant) value);
        } else if (value instanceof BytesReference) {
            value((BytesReference) value);
        } else if (value instanceof Text) {
            value((Text) value);
        } else if (value instanceof Map) {
            //noinspection unchecked
            value((Map<String, Object>) value);
        } else if (value instanceof Iterable) {
            value((Iterable) value);
        } else {
            throw new IOException("Type not allowed [" + type + "]");
        }
        return this;
    }

    public XContentBuilder field(String name, boolean value) throws IOException {
        field(name);
        generator.writeBoolean(value);
        return this;
    }

    public XContentBuilder field(XContentBuilderString name, boolean value) throws IOException {
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

    public XContentBuilder field(XContentBuilderString name, byte[] value) throws IOException {
        field(name);
        return value(value);
    }

    public XContentBuilder field(String name, ReadableInstant date) throws IOException {
        field(name);
        return value(date);
    }

    public XContentBuilder field(XContentBuilderString name, ReadableInstant date) throws IOException {
        field(name);
        return value(date);
    }

    public XContentBuilder field(String name, ReadableInstant date, DateTimeFormatter formatter) throws IOException {
        field(name);
        return value(date, formatter);
    }

    public XContentBuilder field(XContentBuilderString name, ReadableInstant date, DateTimeFormatter formatter) throws IOException {
        field(name);
        return value(date, formatter);
    }

    public XContentBuilder field(String name, Date date) throws IOException {
        field(name);
        return value(date);
    }

    public XContentBuilder field(XContentBuilderString name, Date date) throws IOException {
        field(name);
        return value(date);
    }

    public XContentBuilder field(String name, Date date, DateTimeFormatter formatter) throws IOException {
        field(name);
        return value(date, formatter);
    }

    public XContentBuilder field(XContentBuilderString name, Date date, DateTimeFormatter formatter) throws IOException {
        field(name);
        return value(date, formatter);
    }

    public XContentBuilder nullField(String name) throws IOException {
        generator.writeNullField(name);
        return this;
    }

    public XContentBuilder nullField(XContentBuilderString name) throws IOException {
        field(name);
        generator.writeNull();
        return this;
    }

    public XContentBuilder nullValue() throws IOException {
        generator.writeNull();
        return this;
    }

    public XContentBuilder rawField(String fieldName, byte[] content) throws IOException {
        generator.writeRawField(fieldName, content, bos);
        return this;
    }

    public XContentBuilder rawField(String fieldName, byte[] content, int offset, int length) throws IOException {
        generator.writeRawField(fieldName, content, offset, length, bos);
        return this;
    }

    public XContentBuilder rawField(String fieldName, InputStream content) throws IOException {
        generator.writeRawField(fieldName, content, bos);
        return this;
    }

    public XContentBuilder rawField(String fieldName, BytesReference content) throws IOException {
        generator.writeRawField(fieldName, content, bos);
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

    public XContentBuilder map(Map<String, Object> map) throws IOException {
        if (map == null) {
            return nullValue();
        }
        XContentMapConverter.writeMap(generator, map);
        return this;
    }

    public XContentBuilder value(Map<String, Object> map) throws IOException {
        if (map == null) {
            return nullValue();
        }
        XContentMapConverter.writeMap(generator, map);
        return this;
    }

    public XContentBuilder value(Iterable value) throws IOException {
        if (value == null) {
            return nullValue();
        }
        startArray();
        for (Object o : value) {
            value(o);
        }
        endArray();
        return this;
    }

    public XContentBuilder copyCurrentStructure(XContentParser parser) throws IOException {
        generator.copyCurrentStructure(parser);
        return this;
    }

    public XContentBuilder flush() throws IOException {
        generator.flush();
        return this;
    }

    public void close() {
        try {
            generator.close();
        } catch (IOException e) {
            // ignore
        }
    }

    @Nullable
    public Object payload() {
        return this.payload;
    }

    public OutputStream stream() {
        return this.bos;
    }

    @Override
    public BytesReference bytes() {
        close();
        return ((BytesStream) bos).bytes();
    }

    /**
     * Returns the actual stream used.
     */
    public BytesStream bytesStream() throws IOException {
        close();
        return (BytesStream) bos;
    }

    /**
     * Returns a string representation of the builder (only applicable for text based xcontent).
     * <p/>
     * <p>Only applicable when the builder is constructed with {@link FastByteArrayOutputStream}.
     */
    public String string() throws IOException {
        close();
        BytesArray bytesArray = bytes().toBytesArray();
        return new String(bytesArray.array(), bytesArray.arrayOffset(), bytesArray.length(), Charsets.UTF_8);
    }
}
