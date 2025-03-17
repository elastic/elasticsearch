/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.arrow;

import com.fasterxml.jackson.core.JsonGenerationException;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.util.TokenBuffer;

import org.elasticsearch.core.CheckedConsumer;
import org.elasticsearch.xcontent.XContentFactory;
import org.elasticsearch.xcontent.XContentGenerationException;
import org.elasticsearch.xcontent.XContentGenerator;
import org.elasticsearch.xcontent.XContentParser;
import org.elasticsearch.xcontent.XContentParserConfiguration;
import org.elasticsearch.xcontent.XContentType;

import java.io.BufferedInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.math.BigDecimal;
import java.math.BigInteger;

/**
 * A buffer of {@code XContent} events that can be replayed as an {@code XContentParser}. Useful to create synthetic
 * JSON documents that are fed to existing JSON parsers.
 */
public class XContentBuffer implements XContentGenerator {

    /** Buffer used to write content **/
    private final TokenBuffer generator;

    public XContentBuffer() {
        this.generator = new TokenBuffer(new ObjectMapper(), false);
    }

    /**
     * Return this buffer as an {@code XContent} parser. Events can be added to the buffer while events are
     * consumed from the parser, but these appends are not thread-safe.
     */
    public XContentParser asParser() {
        return new ArrowJsonXContentParser(XContentParserConfiguration.EMPTY, this.generator.asParser());
    }

    @Override
    public XContentType contentType() {
        return null;
    }

    @Override
    public final void usePrettyPrint() {}

    @Override
    public boolean isPrettyPrint() {
        return false;
    }

    @Override
    public void usePrintLineFeedAtEnd() {}

    @Override
    public void writeStartObject() throws IOException {
        try {
            generator.writeStartObject();
        } catch (JsonGenerationException e) {
            throw new XContentGenerationException(e);
        }
    }

    @Override
    public void writeEndObject() throws IOException {
        try {
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
            generator.writeArray(array, 0, array.length);
        } catch (JsonGenerationException e) {
            throw new XContentGenerationException(e);
        }
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
        writeFieldName(name);
        writeRawValue(content, contentType);
    }

    @Override
    public void writeRawValue(InputStream content, XContentType contentType) throws IOException {
        try (XContentParser parser = XContentFactory.xContent(contentType).createParser(XContentParserConfiguration.EMPTY, content)) {
            parser.nextToken();
            copyCurrentStructure(parser);
        }
    }

    @Override
    public void writeRawValue(String value) throws IOException {
        try {
            generator.writeRawValue(value);
        } catch (JsonGenerationException e) {
            throw new XContentGenerationException(e);
        }
    }

    @Override
    public void copyCurrentStructure(XContentParser parser) throws IOException {
        // the start of the parser
        if (parser.currentToken() == null) {
            parser.nextToken();
        }
        copyCurrentStructure(this, parser);
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
        throw new UnsupportedOperationException("writeDirectField is not supported");
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
        generator.close();
    }

    @Override
    public boolean isClosed() {
        return generator.isClosed();
    }
}
