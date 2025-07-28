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
import java.util.Base64;
import java.util.Deque;
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

    private final ESONSource.ESONObject root;
    private final ESONSource.Values values;
    private final XContentType xContentType;

    // Key array iteration state
    private final List<ESONSource.KeyEntry> keyArray;
    private int currentIndex = 0;

    // Current token state
    private Token currentToken = null;
    private String currentFieldName = null;
    private ESONSource.Type currentType = null;
    private Object currentValue = null;
    private boolean valueComputed = false;

    // Container tracking
    private final Deque<ContainerContext> containerStack = new ArrayDeque<>();

    private boolean closed = false;

    /**
     * Tracks the state of containers (objects/arrays) as we parse
     */
    private static class ContainerContext {
        enum Type {
            OBJECT,
            ARRAY
        }

        final Type type;
        int fieldsRemaining;

        ContainerContext(Type type, int fieldCount) {
            this.type = type;
            this.fieldsRemaining = fieldCount;
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
        this.values = root.objectValues();
        this.xContentType = xContentType;

        // Convert to array for efficient indexed access
        this.keyArray = root.getKeyArray();
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
        if (closed) {
            return null;
        }

        // Clear value state from previous token
        currentValue = null;
        valueComputed = false;

        int size = keyArray.size();

        // First token - start root object
        if (currentToken == null) {
            if (currentIndex >= size) {
                return null;
            }
            ESONSource.ObjectEntry rootEntry = (ESONSource.ObjectEntry) keyArray.get(currentIndex);
            containerStack.push(new ContainerContext(ContainerContext.Type.OBJECT, rootEntry.fieldCount));
            currentIndex++;
            currentToken = Token.START_OBJECT;
            return currentToken;
        }

        // Check if we've finished parsing
        if (containerStack.isEmpty() && currentIndex >= size) {
            return null;
        }

        // Emit END tokens for completed containers
        if (containerStack.isEmpty() == false && containerStack.peek().fieldsRemaining == 0) {
            ContainerContext ctx = containerStack.pop();
            currentToken = ctx.type == ContainerContext.Type.OBJECT ? Token.END_OBJECT : Token.END_ARRAY;
            return currentToken;
        }

        // If we've exhausted the array but still have containers, close them
        if (currentIndex >= size) {
            ContainerContext ctx = containerStack.pop();
            currentToken = ctx.type == ContainerContext.Type.OBJECT ? Token.END_OBJECT : Token.END_ARRAY;
            return currentToken;
        }

        // Process next entry
        ESONSource.KeyEntry entry = keyArray.get(currentIndex);
        ContainerContext currentContainer = containerStack.peek();

        if (currentContainer == null) {
            // We've finished everything
            return null;
        }

        // Handle based on container type
        if (currentContainer.type == ContainerContext.Type.OBJECT) {
            // In object: emit field name, next call will emit value
            if (currentToken == Token.FIELD_NAME) {
                // We just emitted a field name, now emit the value
                return emitValue(entry);
            } else {
                // Emit field name
                currentFieldName = entry.key();
                currentToken = Token.FIELD_NAME;
                return currentToken;
            }
        } else {
            // In array: directly emit value
            return emitValue(entry);
        }
    }

    private Token emitValue(ESONSource.KeyEntry entry) throws IOException {
        ContainerContext currentContainer = containerStack.peek();

        if (entry instanceof ESONSource.ObjectEntry objEntry) {
            // Starting a nested object
            containerStack.push(new ContainerContext(ContainerContext.Type.OBJECT, objEntry.fieldCount));
            currentIndex++;
            if (currentContainer != null) {
                currentContainer.fieldsRemaining--;
            }
            currentToken = Token.START_OBJECT;
            return currentToken;

        } else if (entry instanceof ESONSource.ArrayEntry arrEntry) {
            // Starting a nested array
            containerStack.push(new ContainerContext(ContainerContext.Type.ARRAY, arrEntry.elementCount));
            currentIndex++;
            if (currentContainer != null) {
                currentContainer.fieldsRemaining--;
            }
            currentToken = Token.START_ARRAY;
            return currentToken;

        } else if (entry instanceof ESONSource.FieldEntry fieldEntry) {
            // Simple value
            currentType = fieldEntry.type;
            currentIndex++;
            if (currentContainer != null) {
                currentContainer.fieldsRemaining--;
            }
            currentToken = determineValueToken(currentType);
            return currentToken;
        }

        throw new IllegalStateException("Unknown entry type: " + entry.getClass());
    }

    private Token determineValueToken(ESONSource.Type type) {
        if (type == null || type == ESONSource.NullValue.INSTANCE) {
            return Token.VALUE_NULL;
        } else if (type instanceof ESONSource.Mutation mutation) {
            return determineTokenFromObject(mutation.object());
        } else if (type instanceof ESONSource.FixedValue fixed) {
            return switch (fixed.valueType()) {
                case INT, LONG -> Token.VALUE_NUMBER;
                case FLOAT, DOUBLE -> Token.VALUE_NUMBER;
                case BOOLEAN -> Token.VALUE_BOOLEAN;
                default -> throw new IllegalStateException("Unknown fixed value type: " + fixed.valueType());
            };
        } else if (type instanceof ESONSource.VariableValue var) {
            return switch (var.valueType()) {
                case STRING -> Token.VALUE_STRING;
                case BINARY -> Token.VALUE_EMBEDDED_OBJECT;
                default -> throw new IllegalStateException("Unknown variable value type: " + var.valueType());
            };
        }

        throw new IllegalStateException("Unknown type: " + type.getClass());
    }

    private Token determineTokenFromObject(Object obj) {
        if (obj == null) {
            return Token.VALUE_NULL;
        } else if (obj instanceof String) {
            return Token.VALUE_STRING;
        } else if (obj instanceof Number) {
            return Token.VALUE_NUMBER;
        } else if (obj instanceof Boolean) {
            return Token.VALUE_BOOLEAN;
        } else if (obj instanceof byte[]) {
            return Token.VALUE_EMBEDDED_OBJECT;
        } else {
            // TODO: Fix. This is because we have a variety of custom writers. We would need to expose those.
            return Token.VALUE_STRING;
        }
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

    private Object materializeValue() {
        ESONSource.Type type = this.currentType;
        if (type == null || type == ESONSource.NullValue.INSTANCE) {
            return null;
        } else if (type instanceof ESONSource.Mutation mutation) {
            return mutation.object();
        } else if (type instanceof ESONSource.FixedValue fixed) {
            return fixed.getValue(values);
        } else if (type instanceof ESONSource.VariableValue var) {
            return var.getValue(values);
        } else if (type instanceof ESONSource.ESONObject obj) {
            return obj;
        } else if (type instanceof ESONSource.ESONArray arr) {
            return arr;
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
            return currentFieldName;
        }
        // When on a value token, return the field name if in an object
        ContainerContext ctx = containerStack.peek();
        if (ctx != null && ctx.type == ContainerContext.Type.OBJECT && currentFieldName != null) {
            return currentFieldName;
        }
        return null;
    }

    @Override
    public String text() throws IOException {
        Object value = getCurrentValue();
        return value.toString();
    }

    @Override
    public XContentString optimizedText() throws IOException {
        // For strings, try to access raw bytes directly without materializing the string
        if (currentType instanceof ESONSource.VariableValue varValue && varValue.valueType() == ESONSource.ValueType.STRING) {
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

            // TODO: Fix
            return new Text(new XContentString.UTF8Bytes(rawBytes, offset, length), length);
        }

        // Fallback: materialize value and convert to bytes
        Object value = getCurrentValue();
        return new Text(value.toString());
    }

    @Override
    public boolean optimizedTextToStream(OutputStream out) throws IOException {
        // For strings, try to write raw bytes directly without materializing the string
        if (currentType instanceof ESONSource.VariableValue varValue && varValue.valueType() == ESONSource.ValueType.STRING) {
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
