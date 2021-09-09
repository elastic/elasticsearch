/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.common.xcontent;

import org.elasticsearch.core.CheckedConsumer;

import java.io.Closeable;
import java.io.Flushable;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.math.BigDecimal;
import java.math.BigInteger;

public interface XContentGenerator extends Closeable, Flushable {

    XContentType contentType();

    void usePrettyPrint();

    boolean isPrettyPrint();

    void usePrintLineFeedAtEnd();

    void writeStartObject() throws IOException;

    void writeEndObject() throws IOException;

    void writeStartArray() throws IOException;

    void writeEndArray() throws IOException;

    void writeFieldName(String name) throws IOException;

    void writeNull() throws IOException;

    void writeNullField(String name) throws IOException;

    void writeBooleanField(String name, boolean value) throws IOException;

    void writeBoolean(boolean value) throws IOException;

    void writeNumberField(String name, double value) throws IOException;

    void writeNumber(double value) throws IOException;

    void writeNumberField(String name, float value) throws IOException;

    void writeNumber(float value) throws IOException;

    void writeNumberField(String name, int value) throws IOException;

    void writeNumber(int value) throws IOException;

    void writeNumberField(String name, long value) throws IOException;

    void writeNumber(long value) throws IOException;

    void writeNumber(short value) throws IOException;

    void writeNumber(BigInteger value) throws IOException;

    void writeNumberField(String name, BigInteger value) throws IOException;

    void writeNumber(BigDecimal value) throws IOException;

    void writeNumberField(String name, BigDecimal value) throws IOException;

    void writeStringField(String name, String value) throws IOException;

    void writeString(String value) throws IOException;

    void writeString(char[] text, int offset, int len) throws IOException;

    void writeUTF8String(byte[] value, int offset, int length) throws IOException;

    void writeBinaryField(String name, byte[] value) throws IOException;

    void writeBinary(byte[] value) throws IOException;

    void writeBinary(byte[] value, int offset, int length) throws IOException;

    /**
     * Writes a raw field with the value taken from the bytes in the stream
     * @deprecated use {@link #writeRawField(String, InputStream, XContentType)} to avoid content type auto-detection
     */
    @Deprecated
    void writeRawField(String name, InputStream value) throws IOException;

    /**
     * Writes a raw field with the value taken from the bytes in the stream
     */
    void writeRawField(String name, InputStream value, XContentType xContentType) throws IOException;

    /**
     * Writes a raw value taken from the bytes in the stream
     */
    void writeRawValue(InputStream value, XContentType xContentType) throws IOException;

    void copyCurrentStructure(XContentParser parser) throws IOException;

    /**
     * Write a field whose value is written directly to the output stream. As the content is copied as is,
     * the writer must a valid XContent value (e.g., string is properly escaped and quoted)
     */
    void writeDirectField(String name, CheckedConsumer<OutputStream, IOException> writer) throws IOException;

    default void copyCurrentEvent(XContentParser parser) throws IOException {
        switch (parser.currentToken()) {
            case START_OBJECT:
                writeStartObject();
                break;
            case END_OBJECT:
                writeEndObject();
                break;
            case START_ARRAY:
                writeStartArray();
                break;
            case END_ARRAY:
                writeEndArray();
                break;
            case FIELD_NAME:
                writeFieldName(parser.currentName());
                break;
            case VALUE_STRING:
                if (parser.hasTextCharacters()) {
                    writeString(parser.textCharacters(), parser.textOffset(), parser.textLength());
                } else {
                    writeString(parser.text());
                }
                break;
            case VALUE_NUMBER:
                switch (parser.numberType()) {
                    case INT:
                        writeNumber(parser.intValue());
                        break;
                    case LONG:
                        writeNumber(parser.longValue());
                        break;
                    case FLOAT:
                        writeNumber(parser.floatValue());
                        break;
                    case DOUBLE:
                        writeNumber(parser.doubleValue());
                        break;
                }
                break;
            case VALUE_BOOLEAN:
                writeBoolean(parser.booleanValue());
                break;
            case VALUE_NULL:
                writeNull();
                break;
            case VALUE_EMBEDDED_OBJECT:
                writeBinary(parser.binaryValue());
        }
    }

    /**
     * Returns {@code true} if this XContentGenerator has been closed. A closed generator can not do any more output.
     */
    boolean isClosed();

}
