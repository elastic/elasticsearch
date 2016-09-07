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
import com.fasterxml.jackson.core.json.JsonWriteContext;
import com.fasterxml.jackson.core.util.DefaultIndenter;
import com.fasterxml.jackson.core.util.DefaultPrettyPrinter;
import com.fasterxml.jackson.core.util.JsonGeneratorDelegate;
import org.elasticsearch.common.bytes.BytesReference;
import org.elasticsearch.common.io.Streams;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.xcontent.XContent;
import org.elasticsearch.common.xcontent.XContentFactory;
import org.elasticsearch.common.xcontent.XContentGenerator;
import org.elasticsearch.common.xcontent.XContentHelper;
import org.elasticsearch.common.xcontent.XContentParser;
import org.elasticsearch.common.xcontent.XContentType;
import org.elasticsearch.common.xcontent.support.filtering.FilterPathBasedFilter;

import java.io.BufferedInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.util.Collections;
import java.util.Objects;
import java.util.Set;

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

    private final OutputStream os;

    private boolean writeLineFeedAtEnd;
    private static final SerializedString LF = new SerializedString("\n");
    private static final DefaultPrettyPrinter.Indenter INDENTER = new DefaultIndenter("  ", LF.getValue());
    private boolean prettyPrint = false;

    public JsonXContentGenerator(JsonGenerator jsonGenerator, OutputStream os) {
        this(jsonGenerator, os, Collections.emptySet(), Collections.emptySet());
    }

    public JsonXContentGenerator(JsonGenerator jsonGenerator, OutputStream os, Set<String> includes, Set<String> excludes) {
        Objects.requireNonNull(includes, "Including filters must not be null");
        Objects.requireNonNull(excludes, "Excluding filters must not be null");
        this.os = os;
        if (jsonGenerator instanceof GeneratorBase) {
            this.base = (GeneratorBase) jsonGenerator;
        } else {
            this.base = null;
        }

        JsonGenerator generator = jsonGenerator;

        boolean hasExcludes = excludes.isEmpty() == false;
        if (hasExcludes) {
            generator = new FilteringGeneratorDelegate(generator, new FilterPathBasedFilter(excludes, false), true, true);
        }

        boolean hasIncludes = includes.isEmpty() == false;
        if (hasIncludes) {
            generator = new FilteringGeneratorDelegate(generator, new FilterPathBasedFilter(includes, true), true, true);
        }

        if (hasExcludes || hasIncludes) {
            this.filter = (FilteringGeneratorDelegate) generator;
        } else {
            this.filter = null;
        }
        this.generator = generator;
    }

    @Override
    public XContentType contentType() {
        return XContentType.JSON;
    }

    @Override
    public final void usePrettyPrint() {
        generator.setPrettyPrinter(new DefaultPrettyPrinter().withObjectIndenter(INDENTER).withArrayIndenter(INDENTER));
        prettyPrint = true;
    }

    @Override
    public boolean isPrettyPrint() {
        return this.prettyPrint;
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

    private boolean isFiltered() {
        return filter != null;
    }

    private JsonGenerator getLowLevelGenerator() {
        if (isFiltered()) {
            JsonGenerator delegate = filter.getDelegate();
            if (delegate instanceof JsonGeneratorDelegate) {
                // In case of combined inclusion and exclusion filters, we have one and only one another delegating level
                delegate = ((JsonGeneratorDelegate) delegate).getDelegate();
                assert delegate instanceof JsonGeneratorDelegate == false;
            }
            return delegate;
        }
        return generator;
    }

    private boolean inRoot() {
        JsonStreamContext context = generator.getOutputContext();
        return ((context != null) && (context.inRoot() && context.getCurrentName() == null));
    }

    @Override
    public void writeStartObject() throws IOException {
        if (inRoot()) {
            // Use the low level generator to write the startObject so that the root
            // start object is always written even if a filtered generator is used
            getLowLevelGenerator().writeStartObject();
            return;
        }
        generator.writeStartObject();
    }

    @Override
    public void writeEndObject() throws IOException {
        if (inRoot()) {
            // Use the low level generator to write the startObject so that the root
            // start object is always written even if a filtered generator is used
            getLowLevelGenerator().writeEndObject();
            return;
        }
        generator.writeEndObject();
    }

    @Override
    public void writeFieldName(String name) throws IOException {
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
    public void writeBooleanField(String fieldName, boolean value) throws IOException {
        generator.writeBooleanField(fieldName, value);
    }

    @Override
    public void writeNullField(String fieldName) throws IOException {
        generator.writeNullField(fieldName);
    }

    @Override
    public void writeNumberField(String fieldName, int value) throws IOException {
        generator.writeNumberField(fieldName, value);
    }

    @Override
    public void writeNumberField(String fieldName, long value) throws IOException {
        generator.writeNumberField(fieldName, value);
    }

    @Override
    public void writeNumberField(String fieldName, double value) throws IOException {
        generator.writeNumberField(fieldName, value);
    }

    @Override
    public void writeNumberField(String fieldName, float value) throws IOException {
        generator.writeNumberField(fieldName, value);
    }

    @Override
    public void writeBinaryField(String fieldName, byte[] data) throws IOException {
        generator.writeBinaryField(fieldName, data);
    }

    @Override
    public void writeArrayFieldStart(String fieldName) throws IOException {
        generator.writeArrayFieldStart(fieldName);
    }

    @Override
    public void writeObjectFieldStart(String fieldName) throws IOException {
        generator.writeObjectFieldStart(fieldName);
    }

    private void writeStartRaw(String fieldName) throws IOException {
        writeFieldName(fieldName);
        generator.writeRaw(':');
    }

    public void writeEndRaw() {
        assert base != null : "JsonGenerator should be of instance GeneratorBase but was: " + generator.getClass();
        if (base != null) {
            JsonStreamContext context = base.getOutputContext();
            assert (context instanceof JsonWriteContext) : "Expected an instance of JsonWriteContext but was: " + context.getClass();
            ((JsonWriteContext) context).writeValue();
        }
    }

    @Override
    public void writeRawField(String fieldName, InputStream content) throws IOException {
        if (content.markSupported() == false) {
            // needed for the XContentFactory.xContentType call
            content = new BufferedInputStream(content);
        }
        XContentType contentType = XContentFactory.xContentType(content);
        if (contentType == null) {
            throw new IllegalArgumentException("Can't write raw bytes whose xcontent-type can't be guessed");
        }
        if (mayWriteRawData(contentType) == false) {
            try (XContentParser parser = XContentFactory.xContent(contentType).createParser(content)) {
                parser.nextToken();
                writeFieldName(fieldName);
                copyCurrentStructure(parser);
            }
        } else {
            writeStartRaw(fieldName);
            flush();
            Streams.copy(content, os);
            writeEndRaw();
        }
    }

    @Override
    public final void writeRawField(String fieldName, BytesReference content) throws IOException {
        XContentType contentType = XContentFactory.xContentType(content);
        if (contentType == null) {
            throw new IllegalArgumentException("Can't write raw bytes whose xcontent-type can't be guessed");
        }
        if (mayWriteRawData(contentType) == false) {
            writeFieldName(fieldName);
            copyRawValue(content, contentType.xContent());
        } else {
            writeStartRaw(fieldName);
            flush();
            content.writeTo(os);
            writeEndRaw();
        }
    }

    @Override
    public final void writeRawValue(BytesReference content) throws IOException {
        XContentType contentType = XContentFactory.xContentType(content);
        if (contentType == null) {
            throw new IllegalArgumentException("Can't write raw bytes whose xcontent-type can't be guessed");
        }
        if (mayWriteRawData(contentType) == false) {
            copyRawValue(content, contentType.xContent());
        } else {
            if (generator.getOutputContext().getCurrentName() != null) {
                // If we've just started a field we'll need to add the separator
                generator.writeRaw(':');
            }
            flush();
            content.writeTo(os);
            writeEndRaw();
        }
    }

    private boolean mayWriteRawData(XContentType contentType) {
        // When the current generator is filtered (ie filter != null)
        // or the content is in a different format than the current generator,
        // we need to copy the whole structure so that it will be correctly
        // filtered or converted
        return supportsRawWrites()
                && isFiltered() == false
                && contentType == contentType()
                && prettyPrint == false;
    }

    /** Whether this generator supports writing raw data directly */
    protected boolean supportsRawWrites() {
        return true;
    }

    protected void copyRawValue(BytesReference content, XContent xContent) throws IOException {
        try (StreamInput input = content.streamInput();
             XContentParser parser = xContent.createParser(input)) {
            copyCurrentStructure(parser);
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
        JsonStreamContext context = generator.getOutputContext();
        if ((context != null) && (context.inRoot() ==  false)) {
            throw new IOException("unclosed object or array found");
        }
        if (writeLineFeedAtEnd) {
            flush();
            // Bypass generator to always write the line feed
            getLowLevelGenerator().writeRaw(LF);
        }
        generator.close();
    }

}
