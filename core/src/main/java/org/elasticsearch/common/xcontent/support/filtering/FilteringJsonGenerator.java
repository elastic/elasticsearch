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

package org.elasticsearch.common.xcontent.support.filtering;

import com.fasterxml.jackson.core.Base64Variant;
import com.fasterxml.jackson.core.JsonGenerator;
import com.fasterxml.jackson.core.JsonParser;
import com.fasterxml.jackson.core.SerializableString;
import org.elasticsearch.common.Strings;
import org.elasticsearch.common.bytes.BytesReference;
import org.elasticsearch.common.xcontent.json.BaseJsonGenerator;

import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.math.BigDecimal;
import java.math.BigInteger;
import java.util.ArrayDeque;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Queue;

/**
 * A FilteringJsonGenerator uses antpath-like filters to include/exclude fields when writing XContent streams.
 *
 * When writing a XContent stream, this class instantiates (or reuses) a FilterContext instance for each
 * field (or property) that must be generated. This filter context is used to check if the field/property must be
 * written according to the current list of XContentFilter filters.
 */
public class FilteringJsonGenerator extends BaseJsonGenerator {

    /**
     * List of previous contexts
     * (MAX_CONTEXTS contexts are kept around in order to be reused)
     */
    private Queue<FilterContext> contexts = new ArrayDeque<>();
    private static final int MAX_CONTEXTS = 10;

    /**
     * Current filter context
     */
    private FilterContext context;

    public FilteringJsonGenerator(JsonGenerator generator, String[] filters) {
        super(generator);

        List<String[]> builder = new ArrayList<>();
        if (filters != null) {
            for (String filter : filters) {
                String[] matcher = Strings.delimitedListToStringArray(filter, ".");
                if (matcher != null) {
                    builder.add(matcher);
                }
            }
        }

        // Creates a root context that matches all filtering rules
        this.context = get(null, null, Collections.unmodifiableList(builder));
    }

    /**
     * Get a new context instance (and reset it if needed)
     */
    private FilterContext get(String property, FilterContext parent) {
        FilterContext ctx = contexts.poll();
        if (ctx == null) {
            ctx = new FilterContext(property, parent);
        } else {
            ctx.reset(property, parent);
        }
        return ctx;
    }

    /**
     * Get a new context instance (and reset it if needed)
     */
    private FilterContext get(String property, FilterContext context, List<String[]> matchings) {
        FilterContext ctx = get(property, context);
        if (matchings != null) {
            for (String[] matching : matchings) {
                ctx.addMatching(matching);
            }
        }
        return ctx;
    }

    /**
     * Adds a context instance to the pool in order to reuse it if needed
     */
    private void put(FilterContext ctx) {
        if (contexts.size() <= MAX_CONTEXTS) {
            contexts.offer(ctx);
        }
    }

    @Override
    public void writeStartArray() throws IOException {
        context.initArray();
        if (context.include()) {
            super.writeStartArray();
        }
    }

    @Override
    public void writeStartArray(int size) throws IOException {
        context.initArray();
        if (context.include()) {
            super.writeStartArray(size);
        }
    }

    @Override
    public void writeEndArray() throws IOException {
        // Case of array of objects
        if (context.isArrayOfObject()) {
            // Release current context and go one level up
            FilterContext parent = context.parent();
            put(context);
            context = parent;
        }

        if (context.include()) {
            super.writeEndArray();
        }
    }

    @Override
    public void writeStartObject() throws IOException {
        // Case of array of objects
        if (context.isArray()) {
            // Get a context for the anonymous object
            context = get(null, context, context.matchings());
            context.initArrayOfObject();
        }

        if (!context.isArrayOfObject()) {
            context.initObject();
        }

        if (context.include()) {
            super.writeStartObject();
        }

        context = get(null, context);
    }

    @Override
    public void writeEndObject() throws IOException {
        if (!context.isRoot()) {
            // Release current context and go one level up
            FilterContext parent = context.parent();
            put(context);
            context = parent;
        }

        if (context.include()) {
            super.writeEndObject();
        }
    }

    @Override
    public void writeFieldName(String name) throws IOException {
        context.reset(name);

        if (context.include()) {
            // Ensure that the full path to the field is written
            context.writePath(delegate);
            super.writeFieldName(name);
        }
    }

    @Override
    public void writeFieldName(SerializableString name) throws IOException {
        context.reset(name.getValue());

        if (context.include()) {
            // Ensure that the full path to the field is written
            context.writePath(delegate);
            super.writeFieldName(name);
        }
    }

    @Override
    public void writeString(String text) throws IOException {
        if (context.include()) {
            super.writeString(text);
        }
    }

    @Override
    public void writeString(char[] text, int offset, int len) throws IOException {
        if (context.include()) {
            super.writeString(text, offset, len);
        }
    }

    @Override
    public void writeString(SerializableString text) throws IOException {
        if (context.include()) {
            super.writeString(text);
        }
    }

    @Override
    public void writeRawUTF8String(byte[] text, int offset, int length) throws IOException {
        if (context.include()) {
            super.writeRawUTF8String(text, offset, length);
        }
    }

    @Override
    public void writeUTF8String(byte[] text, int offset, int length) throws IOException {
        if (context.include()) {
            super.writeUTF8String(text, offset, length);
        }
    }

    @Override
    public void writeRaw(String text) throws IOException {
        if (context.include()) {
            super.writeRaw(text);
        }
    }

    @Override
    public void writeRaw(String text, int offset, int len) throws IOException {
        if (context.include()) {
            super.writeRaw(text, offset, len);
        }
    }

    @Override
    public void writeRaw(SerializableString raw) throws IOException {
        if (context.include()) {
            super.writeRaw(raw);
        }
    }

    @Override
    public void writeRaw(char[] text, int offset, int len) throws IOException {
        if (context.include()) {
            super.writeRaw(text, offset, len);
        }
    }

    @Override
    public void writeRaw(char c) throws IOException {
        if (context.include()) {
            super.writeRaw(c);
        }
    }

    @Override
    public void writeRawValue(String text) throws IOException {
        if (context.include()) {
            super.writeRawValue(text);
        }
    }

    @Override
    public void writeRawValue(String text, int offset, int len) throws IOException {
        if (context.include()) {
            super.writeRawValue(text, offset, len);
        }
    }

    @Override
    public void writeRawValue(char[] text, int offset, int len) throws IOException {
        if (context.include()) {
            super.writeRawValue(text, offset, len);
        }
    }

    @Override
    public void writeBinary(Base64Variant b64variant, byte[] data, int offset, int len) throws IOException {
        if (context.include()) {
            super.writeBinary(b64variant, data, offset, len);
        }
    }

    @Override
    public int writeBinary(Base64Variant b64variant, InputStream data, int dataLength) throws IOException {
        if (context.include()) {
            return super.writeBinary(b64variant, data, dataLength);
        }
        return 0;
    }

    @Override
    public void writeNumber(short v) throws IOException {
        if (context.include()) {
            super.writeNumber(v);
        }
    }

    @Override
    public void writeNumber(int v) throws IOException {
        if (context.include()) {
            super.writeNumber(v);
        }
    }

    @Override
    public void writeNumber(long v) throws IOException {
        if (context.include()) {
            super.writeNumber(v);
        }
    }

    @Override
    public void writeNumber(BigInteger v) throws IOException {
        if (context.include()) {
            super.writeNumber(v);
        }
    }

    @Override
    public void writeNumber(double v) throws IOException {
        if (context.include()) {
            super.writeNumber(v);
        }
    }

    @Override
    public void writeNumber(float v) throws IOException {
        if (context.include()) {
            super.writeNumber(v);
        }
    }

    @Override
    public void writeNumber(BigDecimal v) throws IOException {
        if (context.include()) {
            super.writeNumber(v);
        }
    }

    @Override
    public void writeNumber(String encodedValue) throws IOException, UnsupportedOperationException {
        if (context.include()) {
            super.writeNumber(encodedValue);
        }
    }

    @Override
    public void writeBoolean(boolean state) throws IOException {
        if (context.include()) {
            super.writeBoolean(state);
        }
    }

    @Override
    public void writeNull() throws IOException {
        if (context.include()) {
            super.writeNull();
        }
    }

    @Override
    public void copyCurrentEvent(JsonParser jp) throws IOException {
        if (context.include()) {
            super.copyCurrentEvent(jp);
        }
    }

    @Override
    public void copyCurrentStructure(JsonParser jp) throws IOException {
        if (context.include()) {
            super.copyCurrentStructure(jp);
        }
    }

    @Override
    protected void writeRawValue(byte[] content, OutputStream bos) throws IOException {
        if (context.include()) {
            super.writeRawValue(content, bos);
        }
    }

    @Override
    protected void writeRawValue(byte[] content, int offset, int length, OutputStream bos) throws IOException {
        if (context.include()) {
            super.writeRawValue(content, offset, length, bos);
        }
    }

    @Override
    protected void writeRawValue(InputStream content, OutputStream bos) throws IOException {
        if (context.include()) {
            super.writeRawValue(content, bos);
        }
    }

    @Override
    protected void writeRawValue(BytesReference content, OutputStream bos) throws IOException {
        if (context.include()) {
            super.writeRawValue(content, bos);
        }
    }

    @Override
    public void close() throws IOException {
        contexts.clear();
        super.close();
    }
}
