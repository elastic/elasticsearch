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

/**
 * Plain Java Map and List XContentGenerator.
 */
public class JavaUtilXContentGenerator implements XContentGenerator {
    private Stack<Object> stack = new Stack<Object>();
    private Boolean closed = false;
    private Object lastPopped = null;

    public Object getResult() {
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
        Map<String, Object> map = new LinkedHashMap<String, Object>();
        if ((stack.size() > 0) && (target() instanceof String)) {
            String name = (String) pop();
            targetMap().put(name, map);
        }
        stack.push(map);
    }

    public void writeEndObject() {
        pop();
    }

    public void writeStartArray() throws IOException {
        List<Object> list = new ArrayList<Object>();
        if ((stack.size() > 0) && (target() instanceof String)) {
            String name = (String) pop();
            targetMap().put(name, list);
        }
        stack.push(list);
    }

    public void writeEndArray() {
        pop();
    }

    private Object target() throws IOException {
        if (stack.size() < 1) {
            throw new IOException("No insertion target on stack.");
        }
        return stack.get(stack.size() - 1);
    }

    @SuppressWarnings("unchecked")
    private Map<String, Object> targetMap() throws IOException {
        if (stack.size() < 1) {
            throw new IOException("No insertion target on stack.");
        }
        Object map = stack.get(stack.size() - 1);
        System.out.println("ON STACK: " + map);
        if (!(map instanceof Map<?,?>)) {
            throw new IOException("Need to create writeStartObject() to add fields to.");
        }
        return (Map<String, Object>) map;
    }

    @SuppressWarnings("unchecked")
    private List<Object> targetList() throws IOException {
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
        targetMap().put(name, value);
    }

    private void writeAnyValue(Object value) throws IOException {
        if (target() instanceof String) {
            String name = (String) pop();
            targetMap().put(name, value);
            return;
        }

        targetList().add(value);
    }

    public void writeFieldName(String name) throws IOException {
        if (!(target() instanceof Map)) {
            throw new IOException("Need to create writeStartObject() to add fields to.");
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
        writeAnyValue("NOT_IMPLEMENTED");
    }

    public void writeUTF8String(byte[] value, int offset, int length) throws IOException {
        writeAnyValue("NOT_IMPLEMENTED");
    }

    public void writeBinaryField(String name, byte[] value) throws IOException {
        writeAnyField(name, new String(value, StandardCharsets.UTF_8));
    }

    public void writeBinary(byte[] value) throws IOException {
        writeAnyValue("NOT_IMPLEMENTED");
    }

    public void writeBinary(byte[] value, int offset, int length) throws IOException {
        writeAnyValue("NOT_IMPLEMENTED");
    }

    public void writeRawField(String name, InputStream value) throws IOException {
        java.util.Scanner scanner = new java.util.Scanner(value).useDelimiter("\\A");
        String str = scanner.hasNext() ? scanner.next() : "";
        writeAnyField(name, str);
    }

    /**
     * Writes a raw field with the value taken from the bytes in the stream
     */
    public void writeRawField(String name, InputStream value, XContentType xContentType) throws IOException {
        writeRawField(name, value);
    }

    public void writeRawValue(InputStream value, XContentType xContentType) throws IOException {
        java.util.Scanner scanner = new java.util.Scanner(value).useDelimiter("\\A");
        String str = scanner.hasNext() ? scanner.next() : "";
        writeAnyValue(str);
    }

    public void copyCurrentStructure(XContentParser parser) throws IOException {
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
