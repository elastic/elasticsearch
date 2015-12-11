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

package org.elasticsearch.common.xcontent.json;

import com.fasterxml.jackson.core.JsonGenerator;
import com.fasterxml.jackson.core.JsonStreamContext;
import com.fasterxml.jackson.core.base.GeneratorBase;
import com.fasterxml.jackson.core.filter.FilteringGeneratorDelegate;
import com.fasterxml.jackson.core.io.SerializedString;
import com.fasterxml.jackson.core.util.DefaultIndenter;
import com.fasterxml.jackson.core.util.DefaultPrettyPrinter;
import org.elasticsearch.common.bytes.BytesArray;
import org.elasticsearch.common.bytes.BytesReference;
import org.elasticsearch.common.io.Streams;
import org.elasticsearch.common.util.CollectionUtils;
import org.elasticsearch.common.xcontent.*;
import org.elasticsearch.common.xcontent.support.filtering.FilterPathBasedFilter;

import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;

/**
 *
 */
public class JsonXContentGenerator implements XContentGenerator {

    /** Generator used to write content **/
    protected final JsonGenerator generator;

    /**
     * Reference to base generator because
     * writing raw values needs a specific method call.
     */
    private final GeneratorBase base;

    /**
     * Reference to filtering generator because
     * writing an empty object '{}' when everything is filtered
     * out needs a specific treatment
     */
    private final FilteringGeneratorDelegate filter;

    private boolean writeLineFeedAtEnd;
    private static final SerializedString LF = new SerializedString("\n");
    private static final DefaultPrettyPrinter.Indenter INDENTER = new DefaultIndenter("  ", LF.getValue());

    public JsonXContentGenerator(JsonGenerator jsonGenerator, String... filters) {
        if (jsonGenerator instanceof GeneratorBase) {
            this.base = (GeneratorBase) jsonGenerator;
        } else {
            this.base = null;
        }

        if (CollectionUtils.isEmpty(filters)) {
            this.generator = jsonGenerator;
            this.filter = null;
        } else {
            this.filter = new FilteringGeneratorDelegate(jsonGenerator, new FilterPathBasedFilter(filters), true, true);
            this.generator = this.filter;
        }
    }

    @Override
    public XContentType contentType() {
        return XContentType.JSON;
    }

    @Override
    public final void usePrettyPrint() {
        generator.setPrettyPrinter(new DefaultPrettyPrinter().withObjectIndenter(INDENTER));
    }

    @Override
    public void usePrintLineFeedAtEnd() {
        writeLineFeedAtEnd = true;
    }

    @Override
    public void writeStartArray() throws IOException {
        generator.writeStartArray();
    }

    @Override
    public void writeEndArray() throws IOException {
        generator.writeEndArray();
    }

    protected boolean isFiltered() {
        return filter != null;
    }

    protected boolean inRoot() {
        if (isFiltered()) {
            JsonStreamContext context = filter.getFilterContext();
            return ((context != null) && (context.inRoot() && context.getCurrentName() == null));
        }
        return false;
    }

    @Override
    public void writeStartObject() throws IOException {
        if (isFiltered() && inRoot()) {
            // Bypass generator to always write the root start object
            filter.getDelegate().writeStartObject();
            return;
        }
        generator.writeStartObject();
    }

    @Override
    public void writeEndObject() throws IOException {
        if (isFiltered() && inRoot()) {
            // Bypass generator to always write the root end object
            filter.getDelegate().writeEndObject();
            return;
        }
        generator.writeEndObject();
    }

    @Override
    public void writeFieldName(String name) throws IOException {
        generator.writeFieldName(name);
    }

    @Override
    public void writeFieldName(XContentString name) throws IOException {
        generator.writeFieldName(name);
    }

    @Override
    public void writeString(String text) throws IOException {
        generator.writeString(text);
    }

    @Override
    public void writeString(char[] text, int offset, int len) throws IOException {
        generator.writeString(text, offset, len);
    }

    @Override
    public void writeUTF8String(byte[] text, int offset, int length) throws IOException {
        generator.writeUTF8String(text, offset, length);
    }

    @Override
    public void writeBinary(byte[] data, int offset, int len) throws IOException {
        generator.writeBinary(data, offset, len);
    }

    @Override
    public void writeBinary(byte[] data) throws IOException {
        generator.writeBinary(data);
    }

    @Override
    public void writeNumber(int v) throws IOException {
        generator.writeNumber(v);
    }

    @Override
    public void writeNumber(long v) throws IOException {
        generator.writeNumber(v);
    }

    @Override
    public void writeNumber(double d) throws IOException {
        generator.writeNumber(d);
    }

    @Override
    public void writeNumber(float f) throws IOException {
        generator.writeNumber(f);
    }

    @Override
    public void writeBoolean(boolean state) throws IOException {
        generator.writeBoolean(state);
    }

    @Override
    public void writeNull() throws IOException {
        generator.writeNull();
    }

    @Override
    public void writeStringField(String fieldName, String value) throws IOException {
        generator.writeStringField(fieldName, value);
    }

    @Override
    public void writeStringField(XContentString fieldName, String value) throws IOException {
        generator.writeFieldName(fieldName);
        generator.writeString(value);
    }

    @Override
    public void writeBooleanField(String fieldName, boolean value) throws IOException {
        generator.writeBooleanField(fieldName, value);
    }

    @Override
    public void writeBooleanField(XContentString fieldName, boolean value) throws IOException {
        generator.writeFieldName(fieldName);
        generator.writeBoolean(value);
    }

    @Override
    public void writeNullField(String fieldName) throws IOException {
        generator.writeNullField(fieldName);
    }

    @Override
    public void writeNullField(XContentString fieldName) throws IOException {
        generator.writeFieldName(fieldName);
        generator.writeNull();
    }

    @Override
    public void writeNumberField(String fieldName, int value) throws IOException {
        generator.writeNumberField(fieldName, value);
    }

    @Override
    public void writeNumberField(XContentString fieldName, int value) throws IOException {
        generator.writeFieldName(fieldName);
        generator.writeNumber(value);
    }

    @Override
    public void writeNumberField(String fieldName, long value) throws IOException {
        generator.writeNumberField(fieldName, value);
    }

    @Override
    public void writeNumberField(XContentString fieldName, long value) throws IOException {
        generator.writeFieldName(fieldName);
        generator.writeNumber(value);
    }

    @Override
    public void writeNumberField(String fieldName, double value) throws IOException {
        generator.writeNumberField(fieldName, value);
    }

    @Override
    public void writeNumberField(XContentString fieldName, double value) throws IOException {
        generator.writeFieldName(fieldName);
        generator.writeNumber(value);
    }

    @Override
    public void writeNumberField(String fieldName, float value) throws IOException {
        generator.writeNumberField(fieldName, value);
    }

    @Override
    public void writeNumberField(XContentString fieldName, float value) throws IOException {
        generator.writeFieldName(fieldName);
        generator.writeNumber(value);
    }

    @Override
    public void writeBinaryField(String fieldName, byte[] data) throws IOException {
        generator.writeBinaryField(fieldName, data);
    }

    @Override
    public void writeBinaryField(XContentString fieldName, byte[] value) throws IOException {
        generator.writeFieldName(fieldName);
        generator.writeBinary(value);
    }

    @Override
    public void writeArrayFieldStart(String fieldName) throws IOException {
        generator.writeArrayFieldStart(fieldName);
    }

    @Override
    public void writeArrayFieldStart(XContentString fieldName) throws IOException {
        generator.writeFieldName(fieldName);
        generator.writeStartArray();
    }

    @Override
    public void writeObjectFieldStart(String fieldName) throws IOException {
        generator.writeObjectFieldStart(fieldName);
    }

    @Override
    public void writeObjectFieldStart(XContentString fieldName) throws IOException {
        generator.writeFieldName(fieldName);
        generator.writeStartObject();
    }

    private void writeStartRaw(String fieldName) throws IOException {
        writeFieldName(fieldName);
        generator.writeRaw(':');
    }

    public void writeEndRaw() {
        assert base != null : "JsonGenerator should be of instance GeneratorBase but was: " + generator.getClass();
        if (base != null) {
            base.getOutputContext().writeValue();
        }
    }

    @Override
    public void writeRawField(String fieldName, byte[] content, OutputStream bos) throws IOException {
        writeRawField(fieldName, new BytesArray(content), bos);
    }

    @Override
    public void writeRawField(String fieldName, byte[] content, int offset, int length, OutputStream bos) throws IOException {
        writeRawField(fieldName, new BytesArray(content, offset, length), bos);
    }

    @Override
    public void writeRawField(String fieldName, InputStream content, OutputStream bos, XContentType contentType) throws IOException {
        if (isFiltered() || (contentType != contentType())) {
            // When the current generator is filtered (ie filter != null)
            // or the content is in a different format than the current generator,
            // we need to copy the whole structure so that it will be correctly
            // filtered or converted
            try (XContentParser parser = XContentFactory.xContent(contentType).createParser(content)) {
                parser.nextToken();
                writeFieldName(fieldName);
                copyCurrentStructure(parser);
            }
        } else {
            writeStartRaw(fieldName);
            flush();
            Streams.copy(content, bos);
            writeEndRaw();
        }
    }

    @Override
    public final void writeRawField(String fieldName, BytesReference content, OutputStream bos) throws IOException {
        XContentType contentType = XContentFactory.xContentType(content);
        if (contentType != null) {
            if (isFiltered() || (contentType != contentType())) {
                // When the current generator is filtered (ie filter != null)
                // or the content is in a different format than the current generator,
                // we need to copy the whole structure so that it will be correctly
                // filtered or converted
                copyRawField(fieldName, content, contentType.xContent());
            } else {
                // Otherwise, the generator is not filtered and has the same type: we can potentially optimize the write
                writeObjectRaw(fieldName, content, bos);
            }
        } else {
            writeFieldName(fieldName);
            // we could potentially optimize this to not rely on exception logic...
            String sValue = content.toUtf8();
            try {
                writeNumber(Long.parseLong(sValue));
            } catch (NumberFormatException e) {
                try {
                    writeNumber(Double.parseDouble(sValue));
                } catch (NumberFormatException e1) {
                    writeString(sValue);
                }
            }
        }
    }

    protected void writeObjectRaw(String fieldName, BytesReference content, OutputStream bos) throws IOException {
        writeStartRaw(fieldName);
        flush();
        content.writeTo(bos);
        writeEndRaw();
    }

    protected void copyRawField(String fieldName, BytesReference content, XContent xContent) throws IOException {
        XContentParser parser = null;
        try {
            if (content.hasArray()) {
                parser = xContent.createParser(content.array(), content.arrayOffset(), content.length());
            } else {
                parser = xContent.createParser(content.streamInput());
            }
            if (fieldName != null) {
                writeFieldName(fieldName);
            }
            copyCurrentStructure(parser);
        } finally {
            if (parser != null) {
                parser.close();
            }
        }
    }

    @Override
    public void copyCurrentStructure(XContentParser parser) throws IOException {
        // the start of the parser
        if (parser.currentToken() == null) {
            parser.nextToken();
        }
        if (parser instanceof JsonXContentParser) {
            generator.copyCurrentStructure(((JsonXContentParser) parser).parser);
        } else {
            XContentHelper.copyCurrentStructure(this, parser);
        }
    }

    @Override
    public void flush() throws IOException {
        generator.flush();
    }

    @Override
    public void close() throws IOException {
        if (generator.isClosed()) {
            return;
        }
        if (writeLineFeedAtEnd) {
            flush();
            generator.writeRaw(LF);
        }
        generator.close();
    }
}
