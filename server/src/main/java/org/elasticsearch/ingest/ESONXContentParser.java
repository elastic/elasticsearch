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

/**
 * Simplified XContentParser for flattened ESON structures.
 *
 * This parser assumes the ESON has been flattened using ESONSource.flatten(),
 * which means all nested structures are expanded into a single linear key array.
 *
 * The parser performs a single iteration through the key array, maintaining state
 * about the current position and context.
 */
public class ESONXContentParser extends AbstractXContentParser {

    private final ESONSource.ESONObject root;
    private final ESONSource.Values values;
    private final XContentType xContentType;

    // Key array iteration state
    private final ESONSource.KeyEntry[] keyArray;
    private int currentIndex = -1;

    // Current token state
    private Token currentToken = null;
    private String currentFieldName = null;
    private ESONSource.Type currentType = null;
    private Object currentValue = null;
    private boolean valueComputed = false;

    // Track container depths for proper END_OBJECT/END_ARRAY emission
    private int objectDepth = 0;
    private int arrayDepth = 0;

    // State machine flags
    private boolean expectingFieldName = false;
    private boolean expectingValue = false;

    private boolean closed = false;

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
        this.keyArray = root.getKeyArray().toArray(new ESONSource.KeyEntry[0]);
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

        // First token is always START_OBJECT for root
        if (currentToken == null) {
            currentIndex = 0;
            objectDepth = 1;
            currentToken = Token.START_OBJECT;
            expectingFieldName = true;
            return currentToken;
        }

        return advanceToken();
    }

    private Token advanceToken() throws IOException {
        // Check if we need to emit END tokens
        if (currentIndex >= keyArray.length) {
            return emitEndTokens();
        }

        ESONSource.KeyEntry entry = keyArray[currentIndex];

        // Handle based on entry type
        if (entry instanceof ESONSource.ObjectEntry objEntry) {
            return handleObjectEntry(objEntry);
        } else if (entry instanceof ESONSource.ArrayEntry arrEntry) {
            return handleArrayEntry(arrEntry);
        } else if (entry instanceof ESONSource.FieldEntry fieldEntry) {
            return handleFieldEntry(fieldEntry);
        }

        throw new IllegalStateException("Unknown entry type: " + entry.getClass());
    }

    private Token handleObjectEntry(ESONSource.ObjectEntry objEntry) throws IOException {
        if (expectingValue) {
            // We're entering a nested object as a value
            expectingValue = false;
            objectDepth++;
            currentIndex++;
            expectingFieldName = true;
            currentToken = Token.START_OBJECT;
        } else if (expectingFieldName && objEntry.fieldCount == 0) {
            // Empty object - immediately emit END_OBJECT
            currentIndex++;
            objectDepth--;
            currentToken = Token.END_OBJECT;

            // Determine what we're expecting next
            if (objectDepth > 0) {
                expectingFieldName = true;
            } else if (arrayDepth > 0) {
                expectingValue = false; // Arrays don't have field names
            }
        } else if (expectingFieldName) {
            // Move to first field of object
            currentIndex++;
            return advanceToken();
        } else {
            // We've finished processing fields, emit END_OBJECT
            objectDepth--;
            currentToken = Token.END_OBJECT;

            if (objectDepth > 0) {
                expectingFieldName = true;
            } else if (arrayDepth > 0) {
                expectingValue = false;
            }
        }

        return currentToken;
    }

    private Token handleArrayEntry(ESONSource.ArrayEntry arrEntry) throws IOException {
        if (expectingValue) {
            // We're entering a nested array as a value
            expectingValue = false;
            arrayDepth++;
            currentIndex++;
            currentToken = Token.START_ARRAY;
        } else if (expectingFieldName == false && expectingValue == false && arrEntry.elementCount == 0) {
            // Empty array - immediately emit END_ARRAY
            currentIndex++;
            arrayDepth--;
            currentToken = Token.END_ARRAY;

            if (objectDepth > 0) {
                expectingFieldName = true;
            }
        } else if (expectingFieldName == false && expectingValue == false) {
            // Move to first element of array
            currentIndex++;
            return advanceToken();
        } else {
            // We've finished processing elements, emit END_ARRAY
            arrayDepth--;
            currentToken = Token.END_ARRAY;

            if (objectDepth > 0) {
                expectingFieldName = true;
            } else if (arrayDepth > 0) {
                expectingValue = false;
            }
        }

        return currentToken;
    }

    private Token handleFieldEntry(ESONSource.FieldEntry fieldEntry) throws IOException {
        if (expectingFieldName || (arrayDepth > 0 && expectingValue == false)) {
            // This is either a field in an object or we need to process the value
            if (fieldEntry.key != null) {
                // Object field - emit field name
                currentFieldName = fieldEntry.key;
                currentType = fieldEntry.type;
                expectingFieldName = false;
                expectingValue = true;
                currentIndex++;
                currentToken = Token.FIELD_NAME;
            } else {
                // Array element - directly emit value
                currentType = fieldEntry.type;
                currentValue = null;
                valueComputed = false;
                currentIndex++;
                currentToken = determineValueToken(currentType);
                expectingValue = false;
            }
        } else if (expectingValue) {
            // Emit the value
            currentValue = null;
            valueComputed = false;
            expectingValue = false;
            currentToken = determineValueToken(currentType);

            // Check if we need to move to next field/element
            if (objectDepth > 0) {
                expectingFieldName = true;
            }

            // For arrays, we stay in value mode
        }

        return currentToken;
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
        } else if (type instanceof ESONSource.ESONObject) {
            expectingValue = true;
            return Token.START_OBJECT;
        } else if (type instanceof ESONSource.ESONArray) {
            expectingValue = true;
            return Token.START_ARRAY;
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
        }

        throw new IllegalStateException("Unknown object type for token: " + obj.getClass());
    }

    private Token emitEndTokens() {
        // Emit remaining END tokens
        if (arrayDepth > 0) {
            arrayDepth--;
            currentToken = Token.END_ARRAY;
        } else if (objectDepth > 0) {
            objectDepth--;
            currentToken = Token.END_OBJECT;
        } else {
            currentToken = null; // End of document
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

    private Object materializeValue() {
        ESONSource.Type esonType = currentType;
        if (esonType == null || esonType == ESONSource.NullValue.INSTANCE) {
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
