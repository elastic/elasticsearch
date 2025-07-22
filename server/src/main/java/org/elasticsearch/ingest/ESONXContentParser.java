/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.ingest;

import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.xcontent.DeprecationHandler;
import org.elasticsearch.xcontent.NamedXContentRegistry;
import org.elasticsearch.xcontent.Text;
import org.elasticsearch.xcontent.XContentLocation;
import org.elasticsearch.xcontent.XContentString;
import org.elasticsearch.xcontent.XContentType;
import org.elasticsearch.xcontent.support.AbstractXContentParser;

import java.io.IOException;
import java.io.OutputStream;
import java.nio.CharBuffer;
import java.util.ArrayDeque;
import java.util.Deque;
import java.util.Iterator;
import java.util.List;
import java.util.Map;

/**
 * XContentParser implementation for ESON (Elastic SON) data structures.
 *
 * This parser converts a pre-parsed ESON tree structure back into a streaming
 * parser interface, enabling seamless integration with existing XContent APIs.
 *
 * Key optimizations:
 * 1. Lazy value materialization - Values are only computed when accessed
 * 2. Direct UTF-8 byte access - Strings can be accessed as raw bytes without conversion
 * 3. Type-aware token emission - Token types determined from ESON metadata
 * 4. Zero-copy string operations when possible
 *
 * Uses the enhanced ESON API methods:
 * - entrySet(false) - iterate over raw types without materializing values
 * - iterator(false) - iterate over raw array elements without materialization
 */
public class ESONXContentParser extends AbstractXContentParser {

    private final ESONSource.ESONObject root;
    private final ESONSource.Values values;
    private final XContentType xContentType;

    // Parsing state
    private Token currentToken;
    private String currentFieldName;
    private Object currentValue; // Lazily computed value
    private ESONSource.Type currentEsonType; // Raw ESON type
    private boolean valueComputed = false; // Track if currentValue has been materialized

    // Stack to handle nested objects/arrays
    private final Deque<ParseContext> contextStack = new ArrayDeque<>();

    // Current parse context
    private ParseContext currentContext;

    private boolean closed = false;

    private static class ParseContext {
        private final Type type;
        private final Iterator<?> iterator;
        private final boolean isMutation;
        boolean expectingValue = false;

        // For objects - use non-materializing iterator
        ParseContext(ESONSource.ESONObject obj) {
            this.type = Type.OBJECT;
            this.iterator = obj.entrySet(false).iterator();
            this.isMutation = false;
        }

        // For arrays - use non-materializing iterator
        ParseContext(ESONSource.ESONArray arr) {
            this.type = Type.ARRAY;
            this.iterator = arr.iterator(false);
            this.isMutation = false;
        }

        ParseContext(Map<String, Object> map) {
            this.type = Type.OBJECT;
            this.iterator = map.entrySet().iterator();
            this.isMutation = true;
        }

        ParseContext(List<Object> list) {
            this.type = Type.ARRAY;
            this.iterator = list.iterator();
            this.isMutation = true;
        }

        enum Type {
            OBJECT,
            ARRAY
        }
    }

    public ESONXContentParser(
        ESONSource.ESONObject root,
        NamedXContentRegistry registry,
        DeprecationHandler deprecationHandler,
        XContentType xContentType
    ) {
        super(registry, deprecationHandler);
        this.root = root;
        this.values = root.objectValues().get();
        // Start with the root object context
        this.currentContext = new ParseContext(root);
        contextStack.addFirst(currentContext);
        this.xContentType = xContentType;
        this.currentToken = null; // Will be set to START_OBJECT on first nextToken()
    }

    @Override
    public XContentType contentType() {
        return xContentType;
    }

    @Override
    public void allowDuplicateKeys(boolean allowDuplicateKeys) {
        // ESON already handles this during parsing, so this is a no-op
    }

    @Override
    public Token nextToken() throws IOException {
        if (closed) {
            return null;
        }

        if (currentToken == null) {
            // First call - return START_OBJECT for root
            currentToken = Token.START_OBJECT;
            return currentToken;
        }

        return advanceToken();
    }

    private Token advanceToken() {
        if (currentContext == null) {
            return null; // End of document
        }

        if (currentContext.type == ParseContext.Type.OBJECT) {
            return advanceObjectToken();
        } else {
            return advanceArrayToken();
        }
    }

    private Token advanceObjectToken() {
        if (currentContext.expectingValue) {
            // We just emitted a field name, now emit the value
            currentContext.expectingValue = false;
            return emitValue(currentEsonType);
        } else {
            // Try to get next field - now working with raw types
            @SuppressWarnings("unchecked")
            Iterator<Map.Entry<String, Object>> objIter = (Iterator<Map.Entry<String, Object>>) currentContext.iterator;

            if (objIter.hasNext()) {
                // Reset value computation state
                currentValue = null;
                valueComputed = false;

                Map.Entry<String, Object> entry = objIter.next();
                currentFieldName = entry.getKey();
                if (currentContext.isMutation) {
                    currentEsonType = new ESONSource.Mutation(entry.getValue());
                } else {
                    currentEsonType = (ESONSource.Type) entry.getValue();
                }

                currentContext.expectingValue = true;
                currentToken = Token.FIELD_NAME;
                return currentToken;
            } else {
                // End of object
                return endCurrentContext(Token.END_OBJECT);
            }
        }
    }

    private Token advanceArrayToken() {
        @SuppressWarnings("unchecked")
        Iterator<Object> arrIter = (Iterator<Object>) currentContext.iterator;

        if (arrIter.hasNext()) {
            // Reset value computation state
            currentValue = null;
            valueComputed = false;

            Object next = arrIter.next();
            if (currentContext.isMutation) {
                currentEsonType = new ESONSource.Mutation(next);
            } else {
                currentEsonType = (ESONSource.Type) next;
            }

            return emitValue(currentEsonType);
        } else {
            // End of array
            return endCurrentContext(Token.END_ARRAY);
        }
    }

    private Token endCurrentContext(Token endToken) {
        contextStack.removeFirst(); // Pop current context
        currentContext = contextStack.peekFirst();
        currentToken = endToken;
        return currentToken;
    }

    @SuppressWarnings("unchecked")
    private Token emitValue(ESONSource.Type esonType) {
        if (esonType == null) {
            currentToken = Token.VALUE_NULL;
        } else if (esonType instanceof ESONSource.ESONObject obj) {
            // Push new object context
            currentContext = new ParseContext(obj);
            contextStack.addFirst(currentContext);
            currentToken = Token.START_OBJECT;
        } else if (esonType instanceof ESONSource.ESONArray arr) {
            // Push new array context
            currentContext = new ParseContext(arr);
            contextStack.addFirst(currentContext);
            currentToken = Token.START_ARRAY;
        } else if (esonType instanceof ESONSource.FixedValue fixedVal) {
            currentToken = Token.VALUE_NUMBER;
            if (fixedVal.valueType() == ESONSource.ValueType.BOOLEAN) {
                currentToken = Token.VALUE_BOOLEAN;
            }
        } else if (esonType instanceof ESONSource.VariableValue varVal) {
            if (varVal.valueType() == ESONSource.ValueType.STRING) {
                currentToken = Token.VALUE_STRING;
            } else if (varVal.valueType() == ESONSource.ValueType.BINARY) {
                currentToken = Token.VALUE_EMBEDDED_OBJECT;
            }
        } else if (esonType instanceof ESONSource.Mutation mutation) {
            // Handle mutations by checking the contained object type
            Object mutatedValue = mutation.object();
            if (mutatedValue == null) {
                currentToken = Token.VALUE_NULL;
            } else if (mutatedValue instanceof String) {
                currentToken = Token.VALUE_STRING;
            } else if (mutatedValue instanceof Number) {
                currentToken = Token.VALUE_NUMBER;
            } else if (mutatedValue instanceof Boolean) {
                currentToken = Token.VALUE_BOOLEAN;
            } else if (mutatedValue instanceof byte[]) {
                currentToken = Token.VALUE_EMBEDDED_OBJECT;
            } else if (mutatedValue instanceof Map) {
                currentContext = new ParseContext((Map<String, Object>) mutatedValue);
                contextStack.addFirst(currentContext);
                currentToken = Token.START_OBJECT;
            } else if (mutatedValue instanceof List) {
                currentContext = new ParseContext((List<Object>) mutatedValue);
                contextStack.addFirst(currentContext);
                currentToken = Token.START_ARRAY;
            } else {
                // TODO: Fix. This is because we have a variety of custom writers. We would need to expose those.
                currentToken = Token.VALUE_STRING;
            }
        } else {
            throw new IllegalStateException("Unknown ESON type: " + esonType.getClass());
        }

        return currentToken;
    }

    // Helper method to materialize the current value on demand
    private Object getCurrentValue() {
        // TODO: Could probably optimize to not box all the numbers
        if (valueComputed == false) {
            currentValue = materializeValue();
            valueComputed = true;
        }
        return currentValue;
    }

    // Helper method to materialize a value from an ESON type
    private Object materializeValue() {
        ESONSource.Type esonType = currentEsonType;
        if (esonType == null) {
            return null;
        } else if (esonType instanceof ESONSource.ESONObject obj) {
            return obj;
        } else if (esonType instanceof ESONSource.ESONArray arr) {
            return arr;
        } else if (esonType instanceof ESONSource.FixedValue fixedVal) {
            return fixedVal.getValue(values);
        } else if (esonType instanceof ESONSource.VariableValue varVal) {
            return varVal.getValue(values);
        } else if (esonType instanceof ESONSource.Mutation mutation) {
            return mutation.object();
        } else {
            throw new IllegalStateException("Unknown ESON type: " + esonType.getClass());
        }
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
        return currentFieldName;
    }

    @Override
    public String text() throws IOException {
        Object value = getCurrentValue();
        return value.toString();
    }

    @Override
    public XContentString optimizedText() throws IOException {
        // For strings, try to access raw bytes directly without materializing the string
        if (currentEsonType instanceof ESONSource.VariableValue varValue && varValue.valueType() == ESONSource.ValueType.STRING) {
            // Return XContentString with direct byte access (lazy string conversion)
            byte[] rawBytes;
            int offset;
            int length = varValue.length();
            if (values.data().hasArray()) {
                rawBytes = values.data().array();
                offset = values.data().arrayOffset() + varValue.position();
            } else {
                rawBytes = new byte[Math.toIntExact(length)];
                offset = 0;
                StreamInput streamInput = values.data().streamInput();
                streamInput.skip(varValue.position());
                streamInput.read(rawBytes);
            }
            return new Text(new XContentString.UTF8Bytes(rawBytes, offset, length));
        }

        // Fallback: materialize value and convert to bytes
        Object value = getCurrentValue();
        return new Text(value.toString());
    }

    @Override
    public boolean optimizedText(OutputStream out) throws IOException {
        // For strings, try to write raw bytes directly without materializing the string
        if (currentEsonType instanceof ESONSource.VariableValue varValue && varValue.valueType() == ESONSource.ValueType.STRING) {
            try {
                // TODO: Can optimize more. Just not sure if this method needs to stay.
                if (values.data().hasArray()) {
                    // Write directly from the raw bytes
                    byte[] rawBytes = values.data().array();
                    int offset = values.data().arrayOffset() + varValue.position();
                    out.write(rawBytes, offset, varValue.length());
                    return true;
                }
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
        }
        throw new IllegalStateException("Current token is not a number value");
    }

    @Override
    protected int doIntValue() throws IOException {
        getCurrentValue();
        if (currentValue instanceof Number num) {
            return num.intValue();
        }
        throw new IllegalStateException("Current token is not a number value");
    }

    @Override
    protected long doLongValue() throws IOException {
        getCurrentValue();
        if (currentValue instanceof Number num) {
            return num.longValue();
        }
        throw new IllegalStateException("Current token is not a number value");
    }

    @Override
    protected float doFloatValue() throws IOException {
        getCurrentValue();
        if (currentValue instanceof Number num) {
            return num.floatValue();
        }
        throw new IllegalStateException("Current token is not a number value");
    }

    @Override
    protected double doDoubleValue() throws IOException {
        getCurrentValue();
        if (currentValue instanceof Number num) {
            return num.doubleValue();
        }
        throw new IllegalStateException("Current token is not a number value");
    }

    @Override
    public byte[] binaryValue() throws IOException {
        getCurrentValue();
        if (currentValue instanceof byte[] bytes) {
            return bytes;
        }
        throw new IllegalStateException("Current token is not a binary value");
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
