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

package org.elasticsearch.util.xcontent.json;

import org.codehaus.jackson.JsonGenerator;
import org.elasticsearch.util.xcontent.XContentGenerator;
import org.elasticsearch.util.xcontent.XContentType;

import java.io.IOException;
import java.math.BigDecimal;
import java.math.BigInteger;

/**
 * @author kimchy (shay.banon)
 */
public class JsonXContentGenerator implements XContentGenerator {

    private final JsonGenerator generator;

    public JsonXContentGenerator(JsonGenerator generator) {
        this.generator = generator;
    }

    @Override public XContentType contentType() {
        return XContentType.JSON;
    }

    @Override public void usePrettyPrint() {
        generator.useDefaultPrettyPrinter();
    }

    @Override public void writeStartArray() throws IOException {
        generator.writeStartArray();
    }

    @Override public void writeEndArray() throws IOException {
        generator.writeEndArray();
    }

    @Override public void writeStartObject() throws IOException {
        generator.writeStartObject();
    }

    @Override public void writeEndObject() throws IOException {
        generator.writeEndObject();
    }

    @Override public void writeFieldName(String name) throws IOException {
        generator.writeFieldName(name);
    }

    @Override public void writeString(String text) throws IOException {
        generator.writeString(text);
    }

    @Override public void writeString(char[] text, int offset, int len) throws IOException {
        generator.writeString(text, offset, len);
    }

    @Override public void writeBinary(byte[] data, int offset, int len) throws IOException {
        generator.writeBinary(data, offset, len);
    }

    @Override public void writeBinary(byte[] data) throws IOException {
        generator.writeBinary(data);
    }

    @Override public void writeNumber(int v) throws IOException {
        generator.writeNumber(v);
    }

    @Override public void writeNumber(long v) throws IOException {
        generator.writeNumber(v);
    }

    @Override public void writeNumber(BigInteger v) throws IOException {
        generator.writeNumber(v);
    }

    @Override public void writeNumber(double d) throws IOException {
        generator.writeNumber(d);
    }

    @Override public void writeNumber(float f) throws IOException {
        generator.writeNumber(f);
    }

    @Override public void writeNumber(BigDecimal dec) throws IOException {
        generator.writeNumber(dec);
    }

    @Override public void writeBoolean(boolean state) throws IOException {
        generator.writeBoolean(state);
    }

    @Override public void writeNull() throws IOException {
        generator.writeNull();
    }

    @Override public void writeStringField(String fieldName, String value) throws IOException {
        generator.writeStringField(fieldName, value);
    }

    @Override public void writeBooleanField(String fieldName, boolean value) throws IOException {
        generator.writeBooleanField(fieldName, value);
    }

    @Override public void writeNullField(String fieldName) throws IOException {
        generator.writeNullField(fieldName);
    }

    @Override public void writeNumberField(String fieldName, int value) throws IOException {
        generator.writeNumberField(fieldName, value);
    }

    @Override public void writeNumberField(String fieldName, long value) throws IOException {
        generator.writeNumberField(fieldName, value);
    }

    @Override public void writeNumberField(String fieldName, double value) throws IOException {
        generator.writeNumberField(fieldName, value);
    }

    @Override public void writeNumberField(String fieldName, float value) throws IOException {
        generator.writeNumberField(fieldName, value);
    }

    @Override public void writeNumberField(String fieldName, BigDecimal value) throws IOException {
        generator.writeNumberField(fieldName, value);
    }

    @Override public void writeBinaryField(String fieldName, byte[] data) throws IOException {
        generator.writeBinaryField(fieldName, data);
    }

    @Override public void writeArrayFieldStart(String fieldName) throws IOException {
        generator.writeArrayFieldStart(fieldName);
    }

    @Override public void writeObjectFieldStart(String fieldName) throws IOException {
        generator.writeObjectFieldStart(fieldName);
    }

    @Override public void flush() throws IOException {
        generator.flush();
    }

    @Override public void close() throws IOException {
        generator.close();
    }
}
