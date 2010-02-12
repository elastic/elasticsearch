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

import org.apache.lucene.util.UnicodeUtil;
import org.codehaus.jackson.JsonFactory;
import org.elasticsearch.ElasticSearchException;
import org.elasticsearch.util.concurrent.NotThreadSafe;
import org.elasticsearch.util.io.FastCharArrayWriter;

import java.io.IOException;

/**
 * @author kimchy (Shay Banon)
 */
@NotThreadSafe
public class JsonBuilder {

    /**
     * A thread local based cache of {@link JsonBuilder}.
     */
    public static class Cached {

        private JsonBuilder generator;

        public Cached(JsonBuilder generator) {
            this.generator = generator;
        }

        private static final ThreadLocal<Cached> cache = new ThreadLocal<Cached>() {
            @Override protected Cached initialValue() {
                try {
                    return new Cached(new JsonBuilder());
                } catch (IOException e) {
                    throw new ElasticSearchException("Failed to create json generator", e);
                }
            }
        };

        /**
         * Returns the cached thread local generator, with its internal {@link StringBuilder} cleared.
         */
        public static JsonBuilder cached() throws IOException {
            Cached cached = cache.get();
            cached.generator.reset();
            return cached.generator;
        }

        public static JsonBuilder cachedNoReset() {
            Cached cached = cache.get();
            return cached.generator;
        }
    }

    public static JsonBuilder cached() throws IOException {
        return Cached.cached();
    }


    private final FastCharArrayWriter writer;

    private final JsonFactory factory;

    private org.codehaus.jackson.JsonGenerator generator;

    final UnicodeUtil.UTF8Result utf8Result = new UnicodeUtil.UTF8Result();

    public JsonBuilder() throws IOException {
        this(Jackson.defaultJsonFactory());
    }

    public JsonBuilder(JsonFactory factory) throws IOException {
        this.writer = new FastCharArrayWriter();
        this.factory = factory;
        this.generator = factory.createJsonGenerator(writer);
    }

    public JsonBuilder prettyPrint() {
        generator.useDefaultPrettyPrinter();
        return this;
    }

    public JsonBuilder startJsonp(String callback) throws IOException {
        flush();
        writer.append(callback).append('(');
        return this;
    }

    public JsonBuilder endJsonp() throws IOException {
        flush();
        writer.append(");");
        return this;
    }

    public JsonBuilder startObject(String name) throws IOException {
        field(name);
        startObject();
        return this;
    }

    public JsonBuilder startObject() throws IOException {
        generator.writeStartObject();
        return this;
    }

    public JsonBuilder endObject() throws IOException {
        generator.writeEndObject();
        return this;
    }

    public JsonBuilder startArray(String name) throws IOException {
        field(name);
        startArray();
        return this;
    }

    public JsonBuilder startArray() throws IOException {
        generator.writeStartArray();
        return this;
    }

    public JsonBuilder endArray() throws IOException {
        generator.writeEndArray();
        return this;
    }

    public JsonBuilder field(String name) throws IOException {
        generator.writeFieldName(name);
        return this;
    }

    public JsonBuilder field(String name, char[] value, int offset, int length) throws IOException {
        generator.writeFieldName(name);
        if (value == null) {
            generator.writeNull();
        } else {
            generator.writeString(value, offset, length);
        }
        return this;
    }

    public JsonBuilder field(String name, String value) throws IOException {
        generator.writeFieldName(name);
        if (value == null) {
            generator.writeNull();
        } else {
            generator.writeString(value);
        }
        return this;
    }

    public JsonBuilder field(String name, int value) throws IOException {
        generator.writeFieldName(name);
        generator.writeNumber(value);
        return this;
    }

    public JsonBuilder field(String name, long value) throws IOException {
        generator.writeFieldName(name);
        generator.writeNumber(value);
        return this;
    }

    public JsonBuilder field(String name, float value) throws IOException {
        generator.writeFieldName(name);
        generator.writeNumber(value);
        return this;
    }

    public JsonBuilder field(String name, double value) throws IOException {
        generator.writeFieldName(name);
        generator.writeNumber(value);
        return this;
    }

    public JsonBuilder field(String name, Object value) throws IOException {
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
        } else if (type == Boolean.class) {
            field(name, ((Boolean) value).booleanValue());
        } else {
            field(name, value.toString());
        }
        return this;
    }

    public JsonBuilder field(String name, boolean value) throws IOException {
        generator.writeFieldName(name);
        generator.writeBoolean(value);
        return this;
    }

    public JsonBuilder field(String name, byte[] value) throws IOException {
        generator.writeFieldName(name);
        generator.writeBinary(value);
        return this;
    }

    public JsonBuilder nullField(String name) throws IOException {
        generator.writeNullField(name);
        return this;
    }

    public JsonBuilder binary(byte[] bytes) throws IOException {
        generator.writeBinary(bytes);
        return this;
    }

    public JsonBuilder raw(String json) throws IOException {
        generator.writeRaw(json);
        return this;
    }

    public JsonBuilder string(String value) throws IOException {
        generator.writeString(value);
        return this;
    }

    public JsonBuilder number(int value) throws IOException {
        generator.writeNumber(value);
        return this;
    }

    public JsonBuilder number(long value) throws IOException {
        generator.writeNumber(value);
        return this;
    }

    public JsonBuilder number(double value) throws IOException {
        generator.writeNumber(value);
        return this;
    }

    public JsonBuilder number(Integer value) throws IOException {
        generator.writeNumber(value.intValue());
        return this;
    }

    public JsonBuilder number(Long value) throws IOException {
        generator.writeNumber(value.longValue());
        return this;
    }

    public JsonBuilder number(Float value) throws IOException {
        generator.writeNumber(value.floatValue());
        return this;
    }

    public JsonBuilder number(Double value) throws IOException {
        generator.writeNumber(value.doubleValue());
        return this;
    }

    public JsonBuilder bool(boolean value) throws IOException {
        generator.writeBoolean(value);
        return this;
    }

    public JsonBuilder value(Object value) throws IOException {
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
        return this;
    }

    public JsonBuilder flush() throws IOException {
        generator.flush();
        return this;
    }

    public JsonBuilder reset() throws IOException {
        writer.reset();
        generator = factory.createJsonGenerator(writer);
        return this;
    }

    public String string() throws IOException {
        flush();
        return writer.toStringTrim();
    }

    /**
     * Returns the byte[] that represents the utf8 of the json written up until now.
     * Note, the result is shared within this instance, so copy the byte array if needed
     * or use {@link #utf8copied()}.
     */
    public UnicodeUtil.UTF8Result utf8() throws IOException {
        flush();

        // ignore whitepsaces
        int st = 0;
        int len = writer.size();
        char[] val = writer.unsafeCharArray();

        while ((st < len) && (val[st] <= ' ')) {
            st++;
            len--;
        }
        while ((st < len) && (val[len - 1] <= ' ')) {
            len--;
        }

        UnicodeUtil.UTF16toUTF8(val, st, len, utf8Result);

        return utf8Result;
    }

    /**
     * Returns a copied byte[] that represnts the utf8 o fthe json written up until now.
     */
    public byte[] utf8copied() throws IOException {
        utf8();
        byte[] result = new byte[utf8Result.length];
        System.arraycopy(utf8Result.result, 0, result, 0, utf8Result.length);
        return result;
    }

    public void close() {
        try {
            generator.close();
        } catch (IOException e) {
            // ignore
        }
    }
}
