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

package org.elasticsearch.common.xcontent;

import org.elasticsearch.common.bytes.BytesReference;

import java.io.Closeable;
import java.io.Flushable;
import java.io.IOException;
import java.io.InputStream;

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
     * Writes a raw field with the given bytes as the value
     * @deprecated use {@link #writeRawField(String, BytesReference, XContentType)} to avoid content type auto-detection
     */
    @Deprecated
    void writeRawField(String name, BytesReference value) throws IOException;

    /**
     * Writes a raw field with the given bytes as the value
     */
    void writeRawField(String name, BytesReference value, XContentType xContentType) throws IOException;

    /**
     * Writes a value with the source coming directly from the bytes
     * @deprecated use {@link #writeRawValue(BytesReference, XContentType)} to avoid content type auto-detection
     */
    @Deprecated
    void writeRawValue(BytesReference value) throws IOException;

    /**
     * Writes a value with the source coming directly from the bytes
     */
    void writeRawValue(BytesReference value, XContentType xContentType) throws IOException;

    void copyCurrentStructure(XContentParser parser) throws IOException;

    /**
     * Returns {@code true} if this XContentGenerator has been closed. A closed generator can not do any more output.
     */
    boolean isClosed();

}
