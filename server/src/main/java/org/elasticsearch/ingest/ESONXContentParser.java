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
import java.util.Base64;

/**
 * Simplified XContentParser for flattened ESON structures.
 *
 * This parser assumes the ESON has been flattened using ESONSource.flatten(),
 * which means all nested structures are expanded into a single linear key array.
 *
 * The parser performs a single iteration through the key array, using a stack
 * to track container boundaries and maintain proper state.
 */
public abstract class ESONXContentParser extends AbstractXContentParser {

    private final ESONSource.Values values;
    private final XContentType xContentType;

    // Current token state
    private Token currentToken = null;
    private Token nextToken = null;
    private ESONEntry currentEntry = null;
    private Object currentValue = null;

    protected final ESONStack containerStack = new ESONStack();

    private boolean closed = false;

    public ESONXContentParser(
        ESONSource.Values values,
        NamedXContentRegistry registry,
        DeprecationHandler deprecationHandler,
        XContentType xContentType
    ) {
        super(registry, deprecationHandler);
        this.values = values;
        this.xContentType = xContentType;
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
            return returnFieldValue();
        } else if (currentToken != null && containerStack.isEmpty() == false) {
            return advanceInContainer();
        } else {
            return handleInitial();
        }

    }

    private Token returnFieldValue() {
        currentToken = nextToken;
        nextToken = null;
        return currentToken;
    }

    private Token advanceInContainer() throws IOException {
        int stackValue = containerStack.currentStackValue();
        int remainingFields = ESONStack.fieldsRemaining(stackValue);
        if (remainingFields > 0) {
            currentEntry = nextEntry();
            currentValue = null;
            containerStack.updateRemainingFields(stackValue - 1);

            byte type = currentEntry.type();
            final Token token = switch (type) {
                case ESONEntry.STRING -> Token.VALUE_STRING;
                case ESONEntry.TYPE_INT, ESONEntry.TYPE_LONG, ESONEntry.TYPE_FLOAT, ESONEntry.TYPE_DOUBLE, ESONEntry.BIG_INTEGER,
                    ESONEntry.BIG_DECIMAL -> Token.VALUE_NUMBER;
                case ESONEntry.TYPE_NULL -> Token.VALUE_NULL;
                case ESONEntry.TYPE_TRUE, ESONEntry.TYPE_FALSE -> Token.VALUE_BOOLEAN;
                case ESONEntry.TYPE_OBJECT -> Token.START_OBJECT;
                case ESONEntry.TYPE_ARRAY -> Token.START_ARRAY;
                case ESONEntry.BINARY -> Token.VALUE_EMBEDDED_OBJECT;
                default -> throw new IllegalArgumentException("Unknown type: " + type);
            };
            if (token == Token.START_OBJECT || token == Token.START_ARRAY) {
                newContainer(type);
            }
            // token = TOKEN_LOOKUP[type];

            if (ESONStack.isObject(stackValue)) {
                nextToken = token;
                return currentToken = Token.FIELD_NAME;
            } else {
                return currentToken = token;
            }
        } else {
            // End of container
            containerStack.popContainer();
            return currentToken = ESONStack.isObject(stackValue) ? Token.END_OBJECT : Token.END_ARRAY;
        }
    }

    private Token handleInitial() throws IOException {
        if (closed) {
            return null;
        }

        if (currentToken == null) {
            // First token logic
            currentEntry = nextEntry();
            currentValue = null;
            containerStack.pushObject(currentEntry.offsetOrCount());
            return currentToken = Token.START_OBJECT;
        }
        return null;
    }

    protected abstract ESONEntry nextEntry() throws IOException;

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

    // TODO: Optimize Skipping for bytes vs. fields
    @Override
    public void skipChildren() throws IOException {
        if (currentToken != Token.START_OBJECT && currentToken != Token.START_ARRAY) {
            return;
        }

        Token endToken = (currentToken == Token.START_OBJECT) ? Token.END_OBJECT : Token.END_ARRAY;

        int targetDepth = containerStack.depth() - 1;
        while (containerStack.depth() > targetDepth) {
            int stackValue = containerStack.currentStackValue();
            if (ESONStack.fieldsRemaining(stackValue) == 0) {
                containerStack.popContainer();
            } else {
                containerStack.updateRemainingFields(stackValue - 1);
                ESONEntry entry = nextEntry();

                byte type = entry.type();
                if (type == ESONEntry.TYPE_OBJECT) {
                    containerStack.pushObject(entry.offsetOrCount());
                } else if (type == ESONEntry.TYPE_ARRAY) {
                    containerStack.pushArray(entry.offsetOrCount());
                }
            }
        }

        currentToken = endToken;
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

}
