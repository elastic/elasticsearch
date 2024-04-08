/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.xcontent.provider.json;

import com.fasterxml.jackson.core.JsonGenerationException;
import com.fasterxml.jackson.core.JsonGenerator;
import com.fasterxml.jackson.core.JsonStreamContext;
import com.fasterxml.jackson.core.base.GeneratorBase;
import com.fasterxml.jackson.core.filter.FilteringGeneratorDelegate;
import com.fasterxml.jackson.core.io.SerializedString;
import com.fasterxml.jackson.core.json.JsonWriteContext;
import com.fasterxml.jackson.core.util.DefaultIndenter;
import com.fasterxml.jackson.core.util.DefaultPrettyPrinter;
import com.fasterxml.jackson.core.util.JsonGeneratorDelegate;

import org.elasticsearch.core.CheckedConsumer;
import org.elasticsearch.core.Streams;
import org.elasticsearch.xcontent.XContent;
import org.elasticsearch.xcontent.XContentFactory;
import org.elasticsearch.xcontent.XContentGenerationException;
import org.elasticsearch.xcontent.XContentGenerator;
import org.elasticsearch.xcontent.XContentParser;
import org.elasticsearch.xcontent.XContentParserConfiguration;
import org.elasticsearch.xcontent.XContentType;
import org.elasticsearch.xcontent.provider.filtering.FilterPathBasedFilter;

import java.io.BufferedInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.math.BigDecimal;
import java.math.BigInteger;
import java.util.Objects;
import java.util.Set;

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

    public JsonXContentGenerator(JsonGenerator baseJsonGenerator, OutputStream os, Set<String> includes, Set<String> excludes) {
        Objects.requireNonNull(includes, "Including filters must not be null");
        Objects.requireNonNull(excludes, "Excluding filters must not be null");
        this.os = os;
        if (baseJsonGenerator instanceof GeneratorBase) {
            this.base = (GeneratorBase) baseJsonGenerator;
        } else {
            this.base = null;
        }

        JsonGenerator jsonGenerator = baseJsonGenerator;

        boolean hasExcludes = excludes.isEmpty() == false;
        if (hasExcludes) {
            jsonGenerator = new FilteringGeneratorDelegate(jsonGenerator, new FilterPathBasedFilter(excludes, false), true, true);
        }

        boolean hasIncludes = includes.isEmpty() == false;
        if (hasIncludes) {
            jsonGenerator = new FilteringGeneratorDelegate(jsonGenerator, new FilterPathBasedFilter(includes, true), true, true);
        }

        if (hasExcludes || hasIncludes) {
            this.filter = (FilteringGeneratorDelegate) jsonGenerator;
        } else {
            this.filter = null;
        }
        this.generator = jsonGenerator;
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
        try {
            if (inRoot()) {
                // Use the low level generator to write the startObject so that the root
                // start object is always written even if a filtered generator is used
                getLowLevelGenerator().writeEndObject();
                return;
            }
            generator.writeEndObject();
        } catch (JsonGenerationException e) {
            throw new XContentGenerationException(e);
        }
    }

    @Override
    public void writeStartArray() throws IOException {
        try {
            generator.writeStartArray();
        } catch (JsonGenerationException e) {
            throw new XContentGenerationException(e);
        }
    }

    @Override
    public void writeEndArray() throws IOException {
        try {
            generator.writeEndArray();
        } catch (JsonGenerationException e) {
            throw new XContentGenerationException(e);
        }
    }

    @Override
    public void writeFieldName(String name) throws IOException {
        try {
            generator.writeFieldName(name);
        } catch (JsonGenerationException e) {
            throw new XContentGenerationException(e);
        }
    }

    @Override
    public void writeNull() throws IOException {
        try {
            generator.writeNull();
        } catch (JsonGenerationException e) {
            throw new XContentGenerationException(e);
        }
    }

    @Override
    public void writeNullField(String name) throws IOException {
        try {
            generator.writeNullField(name);
        } catch (JsonGenerationException e) {
            throw new XContentGenerationException(e);
        }
    }

    @Override
    public void writeBooleanField(String name, boolean value) throws IOException {
        try {
            generator.writeBooleanField(name, value);
        } catch (JsonGenerationException e) {
            throw new XContentGenerationException(e);
        }
    }

    @Override
    public void writeBoolean(boolean value) throws IOException {
        try {
            generator.writeBoolean(value);
        } catch (JsonGenerationException e) {
            throw new XContentGenerationException(e);
        }
    }

    @Override
    public void writeNumberField(String name, double value) throws IOException {
        try {
            generator.writeNumberField(name, value);
        } catch (JsonGenerationException e) {
            throw new XContentGenerationException(e);
        }
    }

    @Override
    public void writeNumber(double value) throws IOException {
        try {
            generator.writeNumber(value);
        } catch (JsonGenerationException e) {
            throw new XContentGenerationException(e);
        }
    }

    @Override
    public void writeNumberField(String name, float value) throws IOException {
        try {
            generator.writeNumberField(name, value);
        } catch (JsonGenerationException e) {
            throw new XContentGenerationException(e);
        }
    }

    @Override
    public void writeNumber(float value) throws IOException {
        try {
            generator.writeNumber(value);
        } catch (JsonGenerationException e) {
            throw new XContentGenerationException(e);
        }
    }

    @Override
    public void writeNumberField(String name, int value) throws IOException {
        try {
            generator.writeNumberField(name, value);
        } catch (JsonGenerationException e) {
            throw new XContentGenerationException(e);
        }
    }

    @Override
    public void writeNumberField(String name, BigInteger value) throws IOException {
        // as jackson's JsonGenerator doesn't have this method for BigInteger
        // we have to implement it ourselves
        try {
            generator.writeFieldName(name);
            generator.writeNumber(value);
        } catch (JsonGenerationException e) {
            throw new XContentGenerationException(e);
        }
    }

    @Override
    public void writeNumberField(String name, BigDecimal value) throws IOException {
        try {
            generator.writeNumberField(name, value);
        } catch (JsonGenerationException e) {
            throw new XContentGenerationException(e);
        }
    }

    @Override
    public void writeNumber(int value) throws IOException {
        try {
            generator.writeNumber(value);
        } catch (JsonGenerationException e) {
            throw new XContentGenerationException(e);
        }
    }

    @Override
    public void writeNumberField(String name, long value) throws IOException {
        try {
            generator.writeNumberField(name, value);
        } catch (JsonGenerationException e) {
            throw new XContentGenerationException(e);
        }
    }

    @Override
    public void writeNumber(long value) throws IOException {
        try {
            generator.writeNumber(value);
        } catch (JsonGenerationException e) {
            throw new XContentGenerationException(e);
        }
    }

    @Override
    public void writeNumber(short value) throws IOException {
        try {
            generator.writeNumber(value);
        } catch (JsonGenerationException e) {
            throw new XContentGenerationException(e);
        }
    }

    @Override
    public void writeNumber(BigInteger value) throws IOException {
        try {
            generator.writeNumber(value);
        } catch (JsonGenerationException e) {
            throw new XContentGenerationException(e);
        }
    }

    @Override
    public void writeNumber(BigDecimal value) throws IOException {
        try {
            generator.writeNumber(value);
        } catch (JsonGenerationException e) {
            throw new XContentGenerationException(e);
        }
    }

    @Override
    public void writeStringField(String name, String value) throws IOException {
        try {
            generator.writeStringField(name, value);
        } catch (JsonGenerationException e) {
            throw new XContentGenerationException(e);
        }
    }

    @Override
    public void writeString(String value) throws IOException {
        try {
            generator.writeString(value);
        } catch (JsonGenerationException e) {
            throw new XContentGenerationException(e);
        }
    }

    @Override
    public void writeStringArray(String[] array) throws IOException {
        try {
            if (isFiltered()) {
                // filtered serialization does not work correctly with the bulk array serializer, so we need to fall back to serializing
                // the array one-by-one
                // TODO: this can probably be removed after upgrading Jackson to 2.15.1 or later, see
                // https://github.com/FasterXML/jackson-core/issues/1023
                writeStringArrayFiltered(array);
            } else {
                generator.writeArray(array, 0, array.length);
            }
        } catch (JsonGenerationException e) {
            throw new XContentGenerationException(e);
        }
    }

    private void writeStringArrayFiltered(String[] array) throws IOException {
        writeStartArray();
        for (String s : array) {
            writeString(s);
        }
        writeEndArray();
    }

    @Override
    public void writeString(char[] value, int offset, int len) throws IOException {
        try {
            generator.writeString(value, offset, len);
        } catch (JsonGenerationException e) {
            throw new XContentGenerationException(e);
        }
    }

    @Override
    public void writeUTF8String(byte[] value, int offset, int length) throws IOException {
        try {
            generator.writeUTF8String(value, offset, length);
        } catch (JsonGenerationException e) {
            throw new XContentGenerationException(e);
        }
    }

    @Override
    public void writeBinaryField(String name, byte[] value) throws IOException {
        try {
            generator.writeBinaryField(name, value);
        } catch (JsonGenerationException e) {
            throw new XContentGenerationException(e);
        }
    }

    @Override
    public void writeBinary(byte[] value) throws IOException {
        try {
            generator.writeBinary(value);
        } catch (JsonGenerationException e) {
            throw new XContentGenerationException(e);
        }
    }

    @Override
    public void writeBinary(byte[] value, int offset, int len) throws IOException {
        try {
            generator.writeBinary(value, offset, len);
        } catch (JsonGenerationException e) {
            throw new XContentGenerationException(e);
        }
    }

    private void writeStartRaw(String name) throws IOException {
        try {
            writeFieldName(name);
            generator.writeRaw(':');
        } catch (JsonGenerationException e) {
            throw new XContentGenerationException(e);
        }
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
    public void writeRawField(String name, InputStream content) throws IOException {
        if (content.markSupported() == false) {
            // needed for the XContentFactory.xContentType call
            content = new BufferedInputStream(content);
        }
        XContentType contentType = XContentFactory.xContentType(content);
        if (contentType == null) {
            throw new IllegalArgumentException("Can't write raw bytes whose xcontent-type can't be guessed");
        }
        writeRawField(name, content, contentType);
    }

    @Override
    public void writeRawField(String name, InputStream content, XContentType contentType) throws IOException {
        if (mayWriteRawData(contentType) == false) {
            try (XContentParser parser = XContentFactory.xContent(contentType).createParser(XContentParserConfiguration.EMPTY, content)) {
                parser.nextToken();
                writeFieldName(name);
                copyCurrentStructure(parser);
            }
        } else {
            writeStartRaw(name);
            flush();
            Streams.copy(content, os);
            writeEndRaw();
        }
    }

    @Override
    public void writeRawValue(InputStream stream, XContentType xContentType) throws IOException {
        if (mayWriteRawData(xContentType) == false) {
            copyRawValue(stream, xContentType.xContent());
        } else {
            if (generator.getOutputContext().getCurrentName() != null) {
                // If we've just started a field we'll need to add the separator
                generator.writeRaw(':');
            }
            flush();
            Streams.copy(stream, os, false);
            writeEndRaw();
        }
    }

    @Override
    public void writeRawValue(String value) throws IOException {
        try {
            if (supportsRawWrites()) {
                generator.writeRawValue(value);
            } else {
                // fallback to a regular string for formats that don't allow writing the value as is
                generator.writeString(value);
            }
        } catch (JsonGenerationException e) {
            throw new XContentGenerationException(e);
        }
    }

    private boolean mayWriteRawData(XContentType contentType) {
        // When the current generator is filtered (ie filter != null)
        // or the content is in a different format than the current generator,
        // we need to copy the whole structure so that it will be correctly
        // filtered or converted
        return supportsRawWrites() && isFiltered() == false && contentType == contentType() && prettyPrint == false;
    }

    /** Whether this generator supports writing raw data directly */
    protected boolean supportsRawWrites() {
        return true;
    }

    protected void copyRawValue(InputStream stream, XContent xContent) throws IOException {
        try (XContentParser parser = xContent.createParser(XContentParserConfiguration.EMPTY, stream)) {
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
            copyCurrentStructure(this, parser);
        }
    }

    /**
     * Low level implementation detail of {@link XContentGenerator#copyCurrentStructure(XContentParser)}.
     */
    private static void copyCurrentStructure(XContentGenerator destination, XContentParser parser) throws IOException {
        XContentParser.Token token = parser.currentToken();

        // Let's handle field-name separately first
        if (token == XContentParser.Token.FIELD_NAME) {
            destination.writeFieldName(parser.currentName());
            token = parser.nextToken();
            // fall-through to copy the associated value
        }

        switch (token) {
            case START_ARRAY -> {
                destination.writeStartArray();
                while (parser.nextToken() != XContentParser.Token.END_ARRAY) {
                    copyCurrentStructure(destination, parser);
                }
                destination.writeEndArray();
            }
            case START_OBJECT -> {
                destination.writeStartObject();
                while (parser.nextToken() != XContentParser.Token.END_OBJECT) {
                    copyCurrentStructure(destination, parser);
                }
                destination.writeEndObject();
            }
            default -> // others are simple:
                destination.copyCurrentEvent(parser);
        }
    }

    @Override
    public void writeDirectField(String name, CheckedConsumer<OutputStream, IOException> writer) throws IOException {
        writeStartRaw(name);
        flush();
        writer.accept(os);
        flush();
        writeEndRaw();
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
        if ((context != null) && (context.inRoot() == false)) {
            throw new IOException("Unclosed object or array found");
        }
        if (writeLineFeedAtEnd) {
            flush();
            // Bypass generator to always write the line feed
            getLowLevelGenerator().writeRaw(LF);
        }
        generator.close();
    }

    @Override
    public boolean isClosed() {
        return generator.isClosed();
    }
}
