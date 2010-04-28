/*
 * Licensed to Elastic Search and Shay Banon under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership. Elastic Search licenses this
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

package org.elasticsearch.util.xcontent.builder;

import org.elasticsearch.util.Strings;
import org.elasticsearch.util.xcontent.XContentGenerator;
import org.elasticsearch.util.xcontent.XContentType;
import org.elasticsearch.util.xcontent.support.XContentMapConverter;
import org.joda.time.DateTimeZone;
import org.joda.time.ReadableInstant;
import org.joda.time.format.DateTimeFormatter;
import org.joda.time.format.ISODateTimeFormat;

import java.io.IOException;
import java.util.Date;
import java.util.Map;

/**
 * @author kimchy (shay.banon)
 */
public abstract class XContentBuilder<T extends XContentBuilder> {

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

    private final static DateTimeFormatter defaultDatePrinter = ISODateTimeFormat.dateTime().withZone(DateTimeZone.UTC);

    protected static FieldCaseConversion globalFieldCaseConversion = FieldCaseConversion.NONE;

    public static void globalFieldCaseConversion(FieldCaseConversion globalFieldCaseConversion) {
        XContentBuilder.globalFieldCaseConversion = globalFieldCaseConversion;
    }

    protected XContentGenerator generator;

    protected T builder;

    protected FieldCaseConversion fieldCaseConversion = globalFieldCaseConversion;

    protected StringBuilder cachedStringBuilder = new StringBuilder();

    public T fieldCaseConversion(FieldCaseConversion fieldCaseConversion) {
        this.fieldCaseConversion = fieldCaseConversion;
        return builder;
    }

    public XContentType contentType() {
        return generator.contentType();
    }

    public T prettyPrint() {
        generator.usePrettyPrint();
        return builder;
    }

    public T map(Map<String, Object> map) throws IOException {
        XContentMapConverter.writeMap(generator, map);
        return builder;
    }

    public T startObject(String name) throws IOException {
        field(name);
        startObject();
        return builder;
    }

    public T startObject() throws IOException {
        generator.writeStartObject();
        return builder;
    }

    public T endObject() throws IOException {
        generator.writeEndObject();
        return builder;
    }

    public T array(String name, String... values) throws IOException {
        startArray(name);
        for (String value : values) {
            value(value);
        }
        endArray();
        return builder;
    }

    public T array(String name, Object... values) throws IOException {
        startArray(name);
        for (Object value : values) {
            value(value);
        }
        endArray();
        return builder;
    }

    public T startArray(String name) throws IOException {
        field(name);
        startArray();
        return builder;
    }

    public T startArray() throws IOException {
        generator.writeStartArray();
        return builder;
    }

    public T endArray() throws IOException {
        generator.writeEndArray();
        return builder;
    }

    public T field(String name) throws IOException {
        if (fieldCaseConversion == FieldCaseConversion.UNDERSCORE) {
            name = Strings.toUnderscoreCase(name, cachedStringBuilder);
        } else if (fieldCaseConversion == FieldCaseConversion.CAMELCASE) {
            name = Strings.toCamelCase(name, cachedStringBuilder);
        }
        generator.writeFieldName(name);
        return builder;
    }

    public T field(String name, char[] value, int offset, int length) throws IOException {
        field(name);
        if (value == null) {
            generator.writeNull();
        } else {
            generator.writeString(value, offset, length);
        }
        return builder;
    }

    public T field(String name, String value) throws IOException {
        field(name);
        if (value == null) {
            generator.writeNull();
        } else {
            generator.writeString(value);
        }
        return builder;
    }

    public T field(String name, Integer value) throws IOException {
        return field(name, value.intValue());
    }

    public T field(String name, int value) throws IOException {
        field(name);
        generator.writeNumber(value);
        return builder;
    }

    public T field(String name, Long value) throws IOException {
        return field(name, value.longValue());
    }

    public T field(String name, long value) throws IOException {
        field(name);
        generator.writeNumber(value);
        return builder;
    }

    public T field(String name, Float value) throws IOException {
        return field(name, value.floatValue());
    }

    public T field(String name, float value) throws IOException {
        field(name);
        generator.writeNumber(value);
        return builder;
    }


    public T field(String name, Double value) throws IOException {
        return field(name, value.doubleValue());
    }

    public T field(String name, double value) throws IOException {
        field(name);
        generator.writeNumber(value);
        return builder;
    }

    public T field(String name, Object value) throws IOException {
        if (value == null) {
            nullField(name);
            return builder;
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
        } else if (type == Boolean.class) {
            field(name, ((Boolean) value).booleanValue());
        } else if (type == Date.class) {
            field(name, (Date) value);
        } else if (type == byte[].class) {
            field(name, (byte[]) value);
        } else if (value instanceof ReadableInstant) {
            field(name, (ReadableInstant) value);
        } else {
            field(name, value.toString());
        }
        return builder;
    }

    public T field(String name, boolean value) throws IOException {
        field(name);
        generator.writeBoolean(value);
        return builder;
    }

    public T field(String name, byte[] value) throws IOException {
        field(name);
        generator.writeBinary(value);
        return builder;
    }

    public T field(String name, ReadableInstant date) throws IOException {
        field(name);
        return value(date);
    }

    public T field(String name, ReadableInstant date, DateTimeFormatter formatter) throws IOException {
        field(name);
        return value(date, formatter);
    }

    public T field(String name, Date date) throws IOException {
        field(name);
        return value(date);
    }

    public T field(String name, Date date, DateTimeFormatter formatter) throws IOException {
        field(name);
        return value(date, formatter);
    }

    public T nullField(String name) throws IOException {
        generator.writeNullField(name);
        return builder;
    }

    public T nullValue() throws IOException {
        generator.writeNull();
        return builder;
    }

    public T rawField(String fieldName, byte[] content) throws IOException {
        generator.writeRawFieldStart(fieldName);
        return raw(content);
    }

    public abstract T raw(byte[] content) throws IOException;

    public T value(Boolean value) throws IOException {
        return value(value.booleanValue());
    }

    public T value(boolean value) throws IOException {
        generator.writeBoolean(value);
        return builder;
    }

    public T value(ReadableInstant date) throws IOException {
        return value(date, defaultDatePrinter);
    }

    public T value(ReadableInstant date, DateTimeFormatter dateTimeFormatter) throws IOException {
        return value(dateTimeFormatter.print(date));
    }

    public T value(Date date) throws IOException {
        return value(date, defaultDatePrinter);
    }

    public T value(Date date, DateTimeFormatter dateTimeFormatter) throws IOException {
        return value(dateTimeFormatter.print(date.getTime()));
    }

    public T value(Integer value) throws IOException {
        return value(value.intValue());
    }

    public T value(int value) throws IOException {
        generator.writeNumber(value);
        return builder;
    }

    public T value(Long value) throws IOException {
        return value(value.longValue());
    }

    public T value(long value) throws IOException {
        generator.writeNumber(value);
        return builder;
    }

    public T value(Float value) throws IOException {
        return value(value.floatValue());
    }

    public T value(float value) throws IOException {
        generator.writeNumber(value);
        return builder;
    }

    public T value(Double value) throws IOException {
        return value(value.doubleValue());
    }

    public T value(double value) throws IOException {
        generator.writeNumber(value);
        return builder;
    }

    public T value(String value) throws IOException {
        generator.writeString(value);
        return builder;
    }

    public T value(byte[] value) throws IOException {
        generator.writeBinary(value);
        return builder;
    }

    public T value(Object value) throws IOException {
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
        } else if (type == Boolean.class) {
            value((Boolean) value);
        } else if (type == byte[].class) {
            value((byte[]) value);
        } else if (type == Date.class) {
            value((Date) value);
        } else if (value instanceof ReadableInstant) {
            value((ReadableInstant) value);
        } else {
            throw new IOException("Type not allowed [" + type + "]");
        }
        return builder;
    }

    public T flush() throws IOException {
        generator.flush();
        return builder;
    }

    public void close() {
        try {
            generator.close();
        } catch (IOException e) {
            // ignore
        }
    }

    public abstract T reset() throws IOException;

    public abstract byte[] unsafeBytes() throws IOException;

    public abstract int unsafeBytesLength() throws IOException;

    public abstract byte[] copiedBytes() throws IOException;

    public abstract String string() throws IOException;
}
