/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.ingest;

import org.apache.lucene.util.BytesRef;
import org.elasticsearch.xcontent.DeprecationHandler;
import org.elasticsearch.xcontent.NamedXContentRegistry;
import org.elasticsearch.xcontent.Text;
import org.elasticsearch.xcontent.XContentLocation;
import org.elasticsearch.xcontent.XContentString;
import org.elasticsearch.xcontent.XContentType;
import org.elasticsearch.xcontent.support.AbstractXContentParser;

import java.io.IOException;
import java.io.OutputStream;
import java.math.BigDecimal;
import java.math.BigInteger;
import java.nio.CharBuffer;
import java.util.Arrays;
import java.util.Base64;
import java.util.List;

/**
 * Simplified XContentParser for flattened ESON structures.
 *
 * This parser assumes the ESON has been flattened using ESONSource.flatten(),
 * which means all nested structures are expanded into a single linear key array.
 *
 * The parser performs a single iteration through the key array, using a stack
 * to track container boundaries and maintain proper state.
 */
public class ESONXContentParser extends AbstractXContentParser {

    private final ESONSource.Values values;
    private final XContentType xContentType;

    // Key array iteration state
    private final List<ESONEntry> keyArray;
    private int currentIndex = 0;

    // Current token state
    private Token currentToken = null;
    private Token nextToken = null;
    private ESONEntry currentEntry = null;
    private Object currentValue = null;

    private final IntStack containerStack = new IntStack();

    private boolean closed = false;

    public ESONXContentParser(
        ESONFlat esonFlat,
        NamedXContentRegistry registry,
        DeprecationHandler deprecationHandler,
        XContentType xContentType
    ) {
        super(registry, deprecationHandler);
        this.values = esonFlat.values();
        this.xContentType = xContentType;

        this.keyArray = esonFlat.keys();
    }

    @Override
    public XContentType contentType() {
        return xContentType;
    }

    @Override
    public void allowDuplicateKeys(boolean allowDuplicateKeys) {
        // ESON already handles this during parsing
    }

    @Override
    public Token nextToken() throws IOException {
        if (currentToken == Token.FIELD_NAME) {
            currentToken = nextToken;
            nextToken = null;
            return currentToken;
        } else if (currentToken != null && containerStack.isEmpty() == false) {
            int stackValue = containerStack.currentStackValue();
            int remainingFields = IntStack.fieldsRemaining(stackValue);
            if (remainingFields > 0) {
                currentEntry = keyArray.get(currentIndex);
                currentValue = null;
                containerStack.updateRemainingFields(stackValue - 1);
                ++currentIndex;

                byte type = currentEntry.type();
                // Optimize for common JSON value types first
                Token token;
                if (type < ESONEntry.TYPE_OBJECT) {
                    // Primitive values (most common in JSON data)
                    // Order by frequency: strings, numbers, booleans, nulls
                    if (type == ESONEntry.STRING) {
                        token = Token.VALUE_STRING;
                    } else if (type >= ESONEntry.TYPE_INT && type <= ESONEntry.TYPE_DOUBLE) {
                        token = Token.VALUE_NUMBER;
                    } else if (type == ESONEntry.TYPE_TRUE || type == ESONEntry.TYPE_FALSE) {
                        token = Token.VALUE_BOOLEAN;
                    } else if (type == ESONEntry.TYPE_NULL) {
                        token = Token.VALUE_NULL;
                    } else {
                        token = TOKEN_LOOKUP[type];  // Rare types
                    }
                } else {
                    // Container types (less common)
                    newContainer(type);
                    token = (type == ESONEntry.TYPE_OBJECT) ? Token.START_OBJECT : Token.START_ARRAY;
                }

                if (IntStack.isObject(stackValue)) {
                    nextToken = token;
                    return currentToken = Token.FIELD_NAME;
                } else {
                    return currentToken = token;
                }
            } else {
                // End of container
                containerStack.popContainer();
                return currentToken = IntStack.isObject(stackValue) ? Token.END_OBJECT : Token.END_ARRAY;
            }
        }

        if (closed) {
            return null;
        }

        if (currentToken == null) {
            // First token logic
            currentEntry = keyArray.get(currentIndex);
            currentValue = null;
            containerStack.pushObject(currentEntry.offsetOrCount());
            currentIndex++;
            return currentToken = Token.START_OBJECT;
        }

        return null;
    }

    private void newContainer(byte type) {
        if (type == ESONEntry.TYPE_OBJECT) {
            containerStack.pushObject(currentEntry.offsetOrCount());
        } else {
            containerStack.pushArray(currentEntry.offsetOrCount());
        }
    }

    private static final Token[] TOKEN_LOOKUP = new Token[16];

    static {
        TOKEN_LOOKUP[ESONEntry.TYPE_OBJECT] = Token.START_OBJECT;
        TOKEN_LOOKUP[ESONEntry.TYPE_ARRAY] = Token.START_ARRAY;
        TOKEN_LOOKUP[ESONEntry.TYPE_NULL] = Token.VALUE_NULL;
        TOKEN_LOOKUP[ESONEntry.TYPE_TRUE] = Token.VALUE_BOOLEAN;
        TOKEN_LOOKUP[ESONEntry.TYPE_FALSE] = Token.VALUE_BOOLEAN;
        TOKEN_LOOKUP[ESONEntry.TYPE_INT] = Token.VALUE_NUMBER;
        TOKEN_LOOKUP[ESONEntry.TYPE_LONG] = Token.VALUE_NUMBER;
        TOKEN_LOOKUP[ESONEntry.TYPE_FLOAT] = Token.VALUE_NUMBER;
        TOKEN_LOOKUP[ESONEntry.TYPE_DOUBLE] = Token.VALUE_NUMBER;
        TOKEN_LOOKUP[ESONEntry.BIG_INTEGER] = Token.VALUE_NUMBER;
        TOKEN_LOOKUP[ESONEntry.BIG_DECIMAL] = Token.VALUE_NUMBER;
        TOKEN_LOOKUP[ESONEntry.STRING] = Token.VALUE_STRING;
        TOKEN_LOOKUP[ESONEntry.BINARY] = Token.VALUE_EMBEDDED_OBJECT;
    }

    // Helper method to materialize the current value on demand
    private Object getCurrentValue() {
        // TODO: Could probably optimize to not box all the numbers
        if (currentValue == null) {
            currentValue = materializeValue();
        }
        return currentValue;
    }

    private Object materializeValue() {
        ESONSource.Value type = this.currentEntry.value();
        if (type == null || type == ESONSource.ConstantValue.NULL) {
            return null;
        } else if (type == ESONSource.ConstantValue.FALSE || type == ESONSource.ConstantValue.TRUE) {
            return type == ESONSource.ConstantValue.TRUE;
        } else if (type instanceof ESONSource.FixedValue fixed) {
            return fixed.getValue(values);
        } else if (type instanceof ESONSource.VariableValue var) {
            return var.getValue(values);
        }

        throw new IllegalStateException("Cannot materialize type: " + type.getClass());
    }

    @Override
    public void skipChildren() throws IOException {
        if (currentToken == Token.START_OBJECT || currentToken == Token.START_ARRAY) {
            int depth = 1;
            while (depth > 0) {
                Token token = nextToken();
                if (token == null) {
                    break;
                }
                if (token == Token.START_OBJECT || token == Token.START_ARRAY) {
                    depth++;
                } else if (token == Token.END_OBJECT || token == Token.END_ARRAY) {
                    depth--;
                }
            }
        }
    }

    @Override
    public Token currentToken() {
        return currentToken;
    }

    @Override
    public String currentName() throws IOException {
        if (currentToken == Token.FIELD_NAME) {
            // TODO: Hack due to 0 length strings being considered null right now
            return currentEntry.key() == null ? "" : currentEntry.key();
        }
        // When on a value token, return the field name if in an object
        if (containerStack.isEmpty() == false) {
            return currentEntry.key();
        }
        return null;
    }

    @Override
    public String text() throws IOException {
        if (currentToken.isValue() == false) {
            throwOnNoText();
        }
        Object value = getCurrentValue();
        return value.toString();
    }

    @Override
    public XContentString optimizedText() throws IOException {
        if (currentToken.isValue() == false) {
            throwOnNoText();
        }
        // For strings, try to access raw bytes directly without materializing the string
        if (currentEntry.value() instanceof ESONSource.VariableValue varValue && varValue.type() == ESONEntry.STRING) {
            BytesRef bytesRef = ESONSource.Values.readByteSlice(values.data(), varValue.position());
            // TODO: Fix Length
            return new Text(new XContentString.UTF8Bytes(bytesRef.bytes, bytesRef.offset, bytesRef.length), bytesRef.length);
        }

        // Fallback: materialize value and convert to bytes
        Object value = getCurrentValue();
        return new Text(value.toString());
    }

    private void throwOnNoText() {
        throw new IllegalArgumentException("Expected text at " + getTokenLocation() + " but found " + currentToken());
    }

    @Override
    public boolean optimizedTextToStream(OutputStream out) throws IOException {
        if (currentToken.isValue() == false) {
            throwOnNoText();
        }
        // For strings, try to write raw bytes directly without materializing the string
        if (currentEntry.value() instanceof ESONSource.VariableValue varValue && varValue.type() == ESONEntry.STRING) {
            try {
                BytesRef bytesRef = ESONSource.Values.readByteSlice(values.data(), varValue.position());
                out.write(bytesRef.bytes, bytesRef.offset, bytesRef.length);
                // TODO: Can optimize more. Just not sure if this method needs to stay.
                return true;
            } catch (Exception e) {
                // Fall back to materialized string
            }
        }

        // Fallback: materialize value and convert to bytes
        Object value = getCurrentValue();
        if (value instanceof String str) {
            byte[] utf8Bytes = str.getBytes(java.nio.charset.StandardCharsets.UTF_8);
            out.write(utf8Bytes);
            return true;
        }
        return false;
    }

    @Override
    public CharBuffer charBuffer() throws IOException {
        return CharBuffer.wrap(text());
    }

    @Override
    public Object objectText() throws IOException {
        getCurrentValue();
        return currentValue;
    }

    @Override
    public Object objectBytes() throws IOException {
        getCurrentValue();
        return currentValue;
    }

    @Override
    public boolean hasTextCharacters() {
        return false; // We use string representation
    }

    @Override
    public char[] textCharacters() throws IOException {
        return text().toCharArray();
    }

    @Override
    public int textLength() throws IOException {
        return text().length();
    }

    @Override
    public int textOffset() throws IOException {
        return 0;
    }

    @Override
    public Number numberValue() throws IOException {
        getCurrentValue();
        if (currentValue instanceof Number num) {
            return num;
        }
        throw new IllegalStateException("Current token is not a number value");
    }

    @Override
    public NumberType numberType() throws IOException {
        getCurrentValue();
        if (currentValue instanceof Integer) {
            return NumberType.INT;
        } else if (currentValue instanceof Long) {
            return NumberType.LONG;
        } else if (currentValue instanceof Float) {
            return NumberType.FLOAT;
        } else if (currentValue instanceof Double) {
            return NumberType.DOUBLE;
        } else if (currentValue instanceof BigInteger) {
            return NumberType.BIG_INTEGER;
        } else if (currentValue instanceof BigDecimal) {
            return NumberType.BIG_DECIMAL;
        }
        throw new IllegalStateException("Current token is not a number value");
    }

    @Override
    protected boolean doBooleanValue() throws IOException {
        getCurrentValue();
        if (currentValue instanceof Boolean bool) {
            return bool;
        }
        throw new IllegalStateException("Current token is not a boolean value");
    }

    @Override
    protected short doShortValue() throws IOException {
        getCurrentValue();
        if (currentValue instanceof Number num) {
            return num.shortValue();
        } else {
            // TODO: Improve handling
            return Short.parseShort(text());
        }
    }

    @Override
    protected int doIntValue() throws IOException {
        getCurrentValue();
        if (currentValue instanceof Number num) {
            return num.intValue();
        } else {
            // TODO: Improve handling
            return Integer.parseInt(text());
        }
    }

    @Override
    protected long doLongValue() throws IOException {
        getCurrentValue();
        if (currentValue instanceof Number num) {
            return num.longValue();
        } else {
            // TODO: Improve handling
            return Long.parseLong(text());
        }
    }

    @Override
    protected float doFloatValue() throws IOException {
        getCurrentValue();
        if (currentValue instanceof Number num) {
            return num.floatValue();
        } else {
            // TODO: Improve handling
            return Float.parseFloat(text());
        }
    }

    @Override
    protected double doDoubleValue() throws IOException {
        getCurrentValue();
        if (currentValue instanceof Number num) {
            return num.doubleValue();
        } else {
            // TODO: Improve handling
            return Double.parseDouble(text());
        }
    }

    @Override
    public byte[] binaryValue() throws IOException {
        getCurrentValue();
        if (currentValue instanceof byte[] bytes) {
            return bytes;
        } else {
            // TODO: Research correct approach
            return Base64.getDecoder().decode(currentValue.toString());
        }
    }

    @Override
    public XContentLocation getTokenLocation() {
        return new XContentLocation(0, 0);
    }

    @Override
    public boolean isClosed() {
        return closed;
    }

    @Override
    public void close() {
        closed = true;
    }

    private static class IntStack {
        // Bit layout: [container_type:1][count:31]
        private int[] containerStack = new int[16];
        private int stackTop = -1;

        private static final int CONTAINER_TYPE_MASK = 0x80000000;  // Top bit
        private static final int COUNT_MASK = 0x7FFFFFFF;           // Bottom 31 bits

        private void pushArray(int count) {
            if (++stackTop >= containerStack.length) {
                growStack();
            }
            containerStack[stackTop] = count | CONTAINER_TYPE_MASK;
        }

        private void pushObject(int count) {
            if (++stackTop >= containerStack.length) {
                growStack();
            }
            containerStack[stackTop] = count;
        }

        private void growStack() {
            containerStack = Arrays.copyOf(containerStack, containerStack.length << 1);
        }

        private int currentStackValue() {
            return containerStack[stackTop];
        }

        private static boolean isObject(int value) {
            return (value & CONTAINER_TYPE_MASK) == 0;
        }

        private static int fieldsRemaining(int value) {
            return value & COUNT_MASK;
        }

        private void updateRemainingFields(int stackValue) {
            containerStack[stackTop] = stackValue;
        }

        private boolean isEmpty() {
            return stackTop == -1;
        }

        // Pop
        private void popContainer() {
            stackTop--;
        }
    }
}
