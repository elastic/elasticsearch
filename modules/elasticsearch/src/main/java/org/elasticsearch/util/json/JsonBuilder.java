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

package org.elasticsearch.util.json;

import org.elasticsearch.util.concurrent.NotThreadSafe;
import org.joda.time.DateTimeZone;
import org.joda.time.ReadableInstant;
import org.joda.time.format.DateTimeFormatter;
import org.joda.time.format.ISODateTimeFormat;

import java.io.IOException;
import java.util.Date;

/**
 * @author kimchy (Shay Banon)
 */
@NotThreadSafe
public abstract class JsonBuilder<T extends JsonBuilder> {

    private final static DateTimeFormatter defaultDatePrinter = ISODateTimeFormat.dateTime().withZone(DateTimeZone.UTC);

    protected org.codehaus.jackson.JsonGenerator generator;

    protected T builder;

    public static StringJsonBuilder stringJsonBuilder() throws IOException {
        return StringJsonBuilder.Cached.cached();
    }

    public static BinaryJsonBuilder binaryJsonBuilder() throws IOException {
        return BinaryJsonBuilder.Cached.cached();
    }

    public T prettyPrint() {
        generator.useDefaultPrettyPrinter();
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
        generator.writeFieldName(name);
        return builder;
    }

    public T field(String name, char[] value, int offset, int length) throws IOException {
        generator.writeFieldName(name);
        if (value == null) {
            generator.writeNull();
        } else {
            generator.writeString(value, offset, length);
        }
        return builder;
    }

    public T field(String name, String value) throws IOException {
        generator.writeFieldName(name);
        if (value == null) {
            generator.writeNull();
        } else {
            generator.writeString(value);
        }
        return builder;
    }

    public T field(String name, int value) throws IOException {
        generator.writeFieldName(name);
        generator.writeNumber(value);
        return builder;
    }

    public T field(String name, long value) throws IOException {
        generator.writeFieldName(name);
        generator.writeNumber(value);
        return builder;
    }

    public T field(String name, float value) throws IOException {
        generator.writeFieldName(name);
        generator.writeNumber(value);
        return builder;
    }

    public T field(String name, double value) throws IOException {
        generator.writeFieldName(name);
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
        } else {
            field(name, value.toString());
        }
        return builder;
    }

    public T field(String name, boolean value) throws IOException {
        generator.writeFieldName(name);
        generator.writeBoolean(value);
        return builder;
    }

    public T field(String name, byte[] value) throws IOException {
        generator.writeFieldName(name);
        generator.writeBinary(value);
        return builder;
    }

    public T field(String name, ReadableInstant date) throws IOException {
        generator.writeFieldName(name);
        return date(date);
    }

    public T field(String name, ReadableInstant date, DateTimeFormatter formatter) throws IOException {
        generator.writeFieldName(name);
        return date(date, formatter);
    }

    public T field(String name, Date date) throws IOException {
        generator.writeFieldName(name);
        return date(date);
    }

    public T field(String name, Date date, DateTimeFormatter formatter) throws IOException {
        generator.writeFieldName(name);
        return date(date, formatter);
    }

    public T nullField(String name) throws IOException {
        generator.writeNullField(name);
        return builder;
    }

    public T binary(byte[] bytes) throws IOException {
        generator.writeBinary(bytes);
        return builder;
    }

    public T raw(String json) throws IOException {
        generator.writeRaw(json);
        return builder;
    }

    public abstract T raw(byte[] json) throws IOException;

    public T string(String value) throws IOException {
        generator.writeString(value);
        return builder;
    }

    public T number(int value) throws IOException {
        generator.writeNumber(value);
        return builder;
    }

    public T number(long value) throws IOException {
        generator.writeNumber(value);
        return builder;
    }

    public T number(double value) throws IOException {
        generator.writeNumber(value);
        return builder;
    }

    public T number(Integer value) throws IOException {
        generator.writeNumber(value.intValue());
        return builder;
    }

    public T number(Long value) throws IOException {
        generator.writeNumber(value.longValue());
        return builder;
    }

    public T number(Float value) throws IOException {
        generator.writeNumber(value.floatValue());
        return builder;
    }

    public T number(Double value) throws IOException {
        generator.writeNumber(value.doubleValue());
        return builder;
    }

    public T bool(boolean value) throws IOException {
        generator.writeBoolean(value);
        return builder;
    }

    public T date(ReadableInstant date) throws IOException {
        return date(date, defaultDatePrinter);
    }

    public T date(ReadableInstant date, DateTimeFormatter dateTimeFormatter) throws IOException {
        string(dateTimeFormatter.print(date));
        return builder;
    }

    public T date(Date date) throws IOException {
        return date(date, defaultDatePrinter);
    }

    public T date(Date date, DateTimeFormatter dateTimeFormatter) throws IOException {
        string(dateTimeFormatter.print(date.getTime()));
        return builder;
    }

    public T value(Object value) throws IOException {
        Class type = value.getClass();
        if (type == String.class) {
            string((String) value);
        } else if (type == Float.class) {
            number(((Float) value).floatValue());
        } else if (type == Double.class) {
            number(((Double) value).doubleValue());
        } else if (type == Integer.class) {
            number(((Integer) value).intValue());
        } else if (type == Long.class) {
            number(((Long) value).longValue());
        } else if (type == Boolean.class) {
            bool((Boolean) value);
        } else if (type == byte[].class) {
            binary((byte[]) value);
        } else {
            throw new IOException("Type not allowed [" + type + "]");
        }
        return builder;
    }

    public T flush() throws IOException {
        generator.flush();
        return builder;
    }

    public abstract T reset() throws IOException;

    public abstract byte[] unsafeBytes() throws IOException;

    public abstract int unsafeBytesLength() throws IOException;

    public abstract byte[] copiedBytes() throws IOException;

    public void close() {
        try {
            generator.close();
        } catch (IOException e) {
            // ignore
        }
    }
}
