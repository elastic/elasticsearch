/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.ingest;

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
        final Type type;
        final Iterator<?> iterator;
        final Object rawObject; // Keep reference to raw ESON object for optimization
        boolean expectingValue = false;

        // For objects - use non-materializing iterator
        ParseContext(ESONSource.ESONObject obj) {
            this.type = Type.OBJECT;
            this.iterator = obj.entrySet(false).iterator();
            this.rawObject = obj;
        }

        // For arrays - use non-materializing iterator
        ParseContext(ESONSource.ESONArray arr) {
            this.type = Type.ARRAY;
            this.iterator = arr.iterator(false);
            this.rawObject = arr;
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
        // Start with the root object context
        this.currentContext = new ParseContext(root);
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
            contextStack.addFirst(currentContext);
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
            Iterator<Map.Entry<String, ESONSource.Type>> objIter = (Iterator<Map.Entry<String, ESONSource.Type>>) currentContext.iterator;

            if (objIter.hasNext()) {
                Map.Entry<String, ESONSource.Type> entry = objIter.next();
                currentFieldName = entry.getKey();
                currentEsonType = entry.getValue();

                // Reset value computation state
                currentValue = null;
                valueComputed = false;

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
        Iterator<ESONSource.Type> arrIter = (Iterator<ESONSource.Type>) currentContext.iterator;

        if (arrIter.hasNext()) {
            currentEsonType = arrIter.next();

            // Reset value computation state
            currentValue = null;
            valueComputed = false;

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

    private Token emitValue(ESONSource.Type esonType) {
        if (esonType == null) {
            currentToken = Token.VALUE_NULL;
        } else if (esonType instanceof ESONSource.ESONObject obj) {
            // Push new object context
            contextStack.addFirst(currentContext);
            currentContext = new ParseContext(obj);
            currentToken = Token.START_OBJECT;
        } else if (esonType instanceof ESONSource.ESONArray arr) {
            // Push new array context
            contextStack.addFirst(currentContext);
            currentContext = new ParseContext(arr);
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
            } else if (mutatedValue instanceof ESONSource.ESONObject obj) {
                contextStack.addFirst(currentContext);
                currentContext = new ParseContext(obj);
                currentToken = Token.START_OBJECT;
            } else if (mutatedValue instanceof ESONSource.ESONArray arr) {
                contextStack.addFirst(currentContext);
                currentContext = new ParseContext(arr);
                currentToken = Token.START_ARRAY;
            } else {
                throw new IllegalStateException("Unknown mutation value type: " + mutatedValue.getClass());
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
            return fixedVal.getValue(getValuesFromContext());
        } else if (esonType instanceof ESONSource.VariableValue varVal) {
            return varVal.getValue(getValuesFromContext());
        } else if (esonType instanceof ESONSource.Mutation mutation) {
            return mutation.object();
        } else {
            throw new IllegalStateException("Unknown ESON type: " + esonType.getClass());
        }
    }

    // Helper to get Values instance from current context
    private ESONSource.Values getValuesFromContext() {
        if (currentContext != null && currentContext.rawObject instanceof ESONSource.ESONObject esonObj) {
            return esonObj.objectValues().get();
        } else if (currentContext != null && currentContext.rawObject instanceof ESONSource.ESONArray esonArr) {
            return esonArr.arrayValues().get();
        }

        // Try to find an ancestor context with Values (arrays inherit from parent objects)
        for (ParseContext ctx : contextStack) {
            if (ctx.rawObject instanceof ESONSource.ESONObject esonObj) {
                return esonObj.objectValues().get();
            } else if (ctx.rawObject instanceof ESONSource.ESONArray esonArr) {
                return esonArr.arrayValues().get();
            }
        }

        throw new IllegalStateException("Cannot find Values instance in context stack");
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
        if (value instanceof String str) {
            return str;
        }
        throw new IllegalStateException("Current token is not a string value");
    }

    @Override
    public XContentString optimizedText() throws IOException {
        // For strings, try to access raw bytes directly without materializing the string
        if (currentEsonType instanceof ESONSource.VariableValue varValue && varValue.valueType() == ESONSource.ValueType.STRING) {
            try {
                ESONSource.Values values = getValuesFromContext();
                if (values.data().hasArray()) {
                    // Return XContentString with direct byte access (lazy string conversion)
                    byte[] rawBytes = values.data().array();
                    int offset = values.data().arrayOffset() + varValue.position();
                    return new Text(new XContentString.UTF8Bytes(rawBytes, offset, varValue.length()));
                }
            } catch (Exception e) {
                // Fall back to materialized string
            }
        }

        // Fallback: materialize value and convert to bytes
        Object value = getCurrentValue();
        if (value instanceof String str) {
            return new Text(str);
        }
        throw new IllegalStateException("Current token is not a string value");
    }

    @Override
    public boolean optimizedText(OutputStream out) throws IOException {
        // For strings, try to write raw bytes directly without materializing the string
        if (currentEsonType instanceof ESONSource.VariableValue varValue && varValue.valueType() == ESONSource.ValueType.STRING) {

            try {
                ESONSource.Values values = getValuesFromContext();
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
