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

package org.elasticsearch.common.xcontent.javautil;

import org.elasticsearch.common.xcontent.XContentGenerator;
import org.elasticsearch.common.xcontent.XContentParser;
import org.elasticsearch.common.xcontent.XContentType;

import java.io.IOException;
import java.io.InputStream;
import java.math.BigDecimal;
import java.math.BigInteger;
import java.nio.charset.StandardCharsets;
import java.util.*;
import java.util.stream.Collectors;
import java.util.stream.Stream;

/**
 * Plain Java Map and List XContentGenerator.
 */
public class JavaUtilXContentGenerator implements XContentGenerator {

    public static String toCamelCase(String text) {
        if (text.matches( "([a-z]+[a-zA-Z0-9]+)+" )) {
            return text;
        }
        String bactrianCamel = Stream.of(text.split("[^a-zA-Z0-9]"))
            .map(v -> v.substring(0, 1).toUpperCase() + v.substring(1).toLowerCase())
            .collect(Collectors.joining());
        return bactrianCamel.toLowerCase().substring(0, 1) + bactrianCamel.substring(1);
    }

    private Stack<Object> stack = new Stack<Object>();
    private Boolean closed = false;
    private Object lastPopped = null;
    public Boolean forceCamelCase = false;

    public Object getResult() throws Exception {
        if (stack.size() > 0) {
            throw new Exception("JavaUtilXContentGenerator has unfinished objects or arrays on stack.");
        }
        return lastPopped;
    }

    private Object pop() {
        lastPopped = stack.pop();
        return lastPopped;
    }

    public XContentType contentType() {
        return XContentType.JSON;
    }

    public void flush() throws IOException {}

    public void close() throws IOException {
        closed = true;
    }

    public void usePrettyPrint() {}

    public boolean isPrettyPrint() {
        return true;
    }

    public void usePrintLineFeedAtEnd() {}

    public void writeStartObject() throws IOException  {
        // System.out.println("writeStartObject");
        Map<String, Object> map = new LinkedHashMap<String, Object>();
        if (stack.size() > 0) {
            if (target() instanceof String) {
                String name = (String) pop();
                targetMap().put(name, map);
            } else if (target() instanceof List) {
                targetList().add(map);
            }
        }
        stack.push(map);
    }

    public void writeEndObject() {
        // System.out.println("writeEndObject");
        pop();
    }

    public void writeStartArray() throws IOException {
        // System.out.println("writeStartArray");
        List<Object> list = new ArrayList<Object>();
        if ((stack.size() > 0) && (target() instanceof String)) {
            String name = (String) pop();
            targetMap().put(name, list);
        }
        stack.push(list);
    }

    public void writeEndArray() {
        // System.out.println("writeEndArray");
        pop();
    }

    private Object target() throws IOException {
        // System.out.println("target");
        if (stack.size() < 1) {
            throw new IOException("No insertion target on stack.");
        }
        return stack.get(stack.size() - 1);
    }

    @SuppressWarnings("unchecked")
    private Map<String, Object> targetMap() throws IOException {
        // System.out.println("targetMap");
        if (stack.size() < 1) {
            throw new IOException("No insertion target on stack.");
        }
        Object map = stack.get(stack.size() - 1);
        if (!(map instanceof Map<?,?>)) {
            throw new IOException("Need to create writeStartObject() to add fields to.");
        }
        return (Map<String, Object>) map;
    }

    @SuppressWarnings("unchecked")
    private List<Object> targetList() throws IOException {
        // System.out.println("targetList");
        if (stack.size() < 1) {
            throw new IOException("No insertion target on stack.");
        }
        Object list = stack.get(stack.size() - 1);
        if (!(list instanceof List<?>)) {
            throw new IOException("Need to create writeStartArray() to add elements to it.");
        }
        return (List<Object>) list;
    }

    private void writeAnyField(String name, Object value) throws IOException {
        // System.out.println("writeAnyField " + name + " " + value);
        if (forceCamelCase) {
            name = toCamelCase(name);
        }
        // System.out.println("name " + name);
        targetMap().put(name, value);
    }

    private void writeAnyValue(Object value) throws IOException {
        // System.out.println("writeAnyValue " + value);
        if (target() instanceof String) {
            String name = (String) pop();
            targetMap().put(name, value);
            return;
        }

        if (target() instanceof List) {
            targetList().add(value);
            return;
        }

        throw new IOException("Cannot add value to stack top.");
    }

    public void writeFieldName(String name) throws IOException {
        // System.out.println("writeFieldName " + name);
        if (!(target() instanceof Map)) {
            throw new IOException("Need to create writeStartObject() to add fields to.");
        }
        if (forceCamelCase) {
            name = toCamelCase(name);
        }
        stack.push(name);
    }

    public void writeNull() throws IOException {
        writeAnyValue(null);
    }

    public void writeNullField(String name) throws IOException {
        writeAnyField(name, null);
    }

    public void writeBooleanField(String name, boolean value) throws IOException {
        writeAnyField(name, value);
    }

    public void writeBoolean(boolean value) throws IOException {
        writeAnyValue(value);
    }

    public void writeNumberField(String name, double value) throws IOException {
        writeAnyField(name, value);
    }

    public void writeNumber(double value) throws IOException {
        writeAnyValue(value);
    }

    public void writeNumberField(String name, float value) throws IOException {
        writeAnyField(name, value);
    }

    public void writeNumber(float value) throws IOException {
        writeAnyValue(value);
    }

    public void writeNumberField(String name, int value) throws IOException {
        writeAnyField(name, value);
    }

    public void writeNumber(int value) throws IOException {
        writeAnyValue(value);
    }

    public void writeNumberField(String name, long value) throws IOException {
        writeAnyField(name, value);
    }

    public void writeNumber(long value) throws IOException {
        writeAnyValue(value);
    }

    public void writeNumber(short value) throws IOException {
        writeAnyValue(value);
    }

    public void writeNumber(BigInteger value) throws IOException {
        writeAnyValue(value);
    }

    public void writeNumberField(String name, BigInteger value) throws IOException {
        writeAnyField(name, value);
    }

    public void writeNumber(BigDecimal value) throws IOException {
        writeAnyValue(value);
    }

    public void writeNumberField(String name, BigDecimal value) throws IOException {
        writeAnyField(name, value);
    }

    public void writeStringField(String name, String value) throws IOException {
        writeAnyField(name, value);
    }

    public void writeString(String value) throws IOException {
        writeAnyValue(value);
    }

    public void writeString(char[] text, int offset, int len) throws IOException {
        // System.out.println("writeString (char[]) " + text + " " + offset + " " + len);
        String str = new String(text);
        writeAnyValue(str.substring(offset, offset + len));
    }

    public void writeUTF8String(byte[] value, int offset, int length) throws IOException {
        // System.out.println("writeUTF8String " + value + " " + offset + " " + length);
        writeAnyValue("writeUTF8String_NOT_IMPLEMENTED");
    }

    public void writeBinaryField(String name, byte[] value) throws IOException {
        // System.out.println("writeBinaryField " + name + " " + value);
        writeAnyField(name, new String(value, StandardCharsets.US_ASCII));
    }

    public void writeBinary(byte[] value) throws IOException {
        // System.out.println("writeBinary " + value);
//        writeAnyValue(new String(value, StandardCharsets.UTF_8));
        // System.out.println("writeBinary not implemented");
    }

    public void writeBinary(byte[] value, int offset, int length) throws IOException {
        // System.out.println("writeBinary " + value + " " + offset + " " + length);
//        writeAnyValue("writeBinary_NOT_IMPLEMENTED");
        // System.out.println("writeBinary [2] not implemented");
    }

    public void writeRawField(String name, InputStream value) throws IOException {
        // System.out.println("writeRawField " + name + " " + value);
        java.util.Scanner scanner = new java.util.Scanner(value).useDelimiter("\\A");
        String str = scanner.hasNext() ? scanner.next() : "";
        writeAnyField(name, str);
    }

    /**
     * Writes a raw field with the value taken from the bytes in the stream
     */
    public void writeRawField(String name, InputStream value, XContentType xContentType) throws IOException {
        // System.out.println("writeRawField [2] " + name + " " + value);
        writeRawField(name, value);
    }

    @Deprecated
    public void writeRawValue(InputStream value, XContentType xContentType) throws IOException {
        // System.out.println("writeRawValue " + value + " " + xContentType);
        java.util.Scanner scanner = new java.util.Scanner(value).useDelimiter("\\A");
        String str = scanner.hasNext() ? scanner.next() : "";
        writeAnyValue(str);
    }

    public void copyCurrentStructure(XContentParser parser) throws IOException {
        // System.out.println("copyCurrentStructure " + parser);
        copyCurrentEvent(parser);
    }

    public void copyCurrentEvent(XContentParser parser) throws IOException {
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
    public boolean isClosed() {
        return closed;
    }
}
