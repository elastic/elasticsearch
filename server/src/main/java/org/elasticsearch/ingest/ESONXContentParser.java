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

    private final ESONSource.Values values;
    private final XContentType xContentType;

    // Key array iteration state
    private final List<ESONEntry> keyArray;
    private int currentIndex = 0;

    // Current token state
    private Token currentToken = null;
    private String currentFieldName = null;
    private ESONSource.Value currentType = null;
    private Object currentValue = null;
    private boolean valueComputed = false;

    // Container tracking
    private final Deque<ContainerContext> containerStack = new ArrayDeque<>();

    private boolean closed = false;

    /**
     * Tracks the state of containers (objects/arrays) as we parse
     */
    private static class ContainerContext {
        final byte type;
        int fieldsRemaining;

        ContainerContext(byte type, int fieldCount) {
            this.type = type;
            this.fieldsRemaining = fieldCount;
        }
    }

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
        if (closed) {
            return null;
        }

        // Clear value state from previous token
        currentValue = null;
        valueComputed = false;

        int size = keyArray.size();

        // First token - start root object
        if (currentToken == null) {
            assert size >= currentIndex;
            ESONEntry.ObjectEntry rootEntry = (ESONEntry.ObjectEntry) keyArray.get(currentIndex);
            containerStack.push(new ContainerContext(ESONEntry.TYPE_OBJECT, rootEntry.offsetOrCount()));
            currentIndex++;
            currentToken = Token.START_OBJECT;
            return currentToken;
        }

        // Check if we've finished parsing
        if (containerStack.isEmpty()) {
            return null;
        }

        if (containerStack.peek().fieldsRemaining == 0) {
            ContainerContext ctx = containerStack.pop();
            currentToken = ctx.type == ESONEntry.TYPE_OBJECT ? Token.END_OBJECT : Token.END_ARRAY;
            return currentToken;
        }

        assert size > currentIndex;

        // Process next entry
        ESONEntry entry = keyArray.get(currentIndex);
        ContainerContext currentContainer = containerStack.peek();
        assert currentContainer != null;

        // Handle based on container type
        if (currentContainer.type == ESONEntry.TYPE_OBJECT && currentToken != Token.FIELD_NAME) {
            currentFieldName = entry.key();
            currentToken = Token.FIELD_NAME;
            return currentToken;
        } else {
            // In array or object value
            return emitValue(entry);
        }
    }

    private Token emitValue(ESONEntry entry) {
        ContainerContext currentContainer = containerStack.peek();
        assert currentContainer != null;
        currentContainer.fieldsRemaining--;
        currentIndex++;

        if (entry.type() == ESONEntry.TYPE_OBJECT || entry.type() == ESONEntry.TYPE_ARRAY) {
            containerStack.push(new ContainerContext(entry.type(), entry.offsetOrCount()));
        } else {
            currentType = ((ESONEntry.FieldEntry) entry).value;
        }

        currentToken = switch (entry.type()) {
            case ESONEntry.TYPE_OBJECT -> Token.START_OBJECT;
            case ESONEntry.TYPE_ARRAY -> Token.START_ARRAY;
            case ESONEntry.TYPE_NULL -> Token.VALUE_NULL;
            case ESONEntry.TYPE_TRUE, ESONEntry.TYPE_FALSE -> Token.VALUE_BOOLEAN;
            case ESONEntry.TYPE_INT, ESONEntry.TYPE_LONG, ESONEntry.TYPE_FLOAT, ESONEntry.TYPE_DOUBLE, ESONEntry.BIG_INTEGER,
                ESONEntry.BIG_DECIMAL -> Token.VALUE_NUMBER;
            case ESONEntry.STRING -> Token.VALUE_STRING;
            case ESONEntry.BINARY -> Token.VALUE_EMBEDDED_OBJECT;
            default -> throw new IllegalStateException("Unexpected entry type: " + entry.type());
        };
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

    private Object materializeValue() {
        ESONSource.Value type = this.currentType;
        if (type == null || type == ESONSource.ConstantValue.NULL) {
            return null;
        } else if (type == ESONSource.ConstantValue.FALSE || type == ESONSource.ConstantValue.TRUE) {
            return type == ESONSource.ConstantValue.TRUE;
        } else if (type instanceof ESONSource.Mutation mutation) {
            return mutation.object();
        } else if (type instanceof ESONSource.FixedValue fixed) {
            return fixed.getValue(values);
        } else if (type instanceof ESONSource.VariableValue var) {
            return var.getValue(values);
        } else if (type instanceof ESONIndexed.ESONObject obj) {
            return obj;
        } else if (type instanceof ESONIndexed.ESONArray arr) {
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
        if (ctx != null && ctx.type == ESONEntry.TYPE_OBJECT && currentFieldName != null) {
            return currentFieldName;
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
        if (currentType instanceof ESONSource.VariableValue varValue && varValue.type() == ESONEntry.STRING) {
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
        if (currentType instanceof ESONSource.VariableValue varValue && varValue.type() == ESONEntry.STRING) {
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
