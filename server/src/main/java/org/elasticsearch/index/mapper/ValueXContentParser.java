/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.index.mapper;

import org.elasticsearch.xcontent.DeprecationHandler;
import org.elasticsearch.xcontent.NamedXContentRegistry;
import org.elasticsearch.xcontent.XContentLocation;
import org.elasticsearch.xcontent.XContentType;
import org.elasticsearch.xcontent.support.AbstractXContentParser;

import java.io.IOException;
import java.math.BigDecimal;
import java.math.BigInteger;
import java.nio.CharBuffer;

public final class ValueXContentParser extends AbstractXContentParser {

    private final XContentLocation originalLocationOffset;

    private boolean closed;
    private Object value;
    private String currentName;
    private Token currentToken;

    public ValueXContentParser(XContentLocation originalLocationOffset, Object value, String currentName, Token currentToken) {
        super(NamedXContentRegistry.EMPTY, DeprecationHandler.IGNORE_DEPRECATIONS);
        this.originalLocationOffset = originalLocationOffset;
        this.value = value;
        this.currentName = currentName;
        this.currentToken = currentToken;
    }

    @Override
    protected boolean doBooleanValue() throws IOException {
        if (value instanceof Boolean aBoolean) {
            return aBoolean;
        } else {
            throw new IllegalStateException("Cannot get boolean value for the current token " + currentToken());
        }
    }

    @Override
    protected short doShortValue() throws IOException {
        return numberValue().shortValue();
    }

    @Override
    protected int doIntValue() throws IOException {
        return numberValue().intValue();
    }

    @Override
    protected long doLongValue() throws IOException {
        return numberValue().longValue();
    }

    @Override
    protected float doFloatValue() throws IOException {
        return numberValue().floatValue();
    }

    @Override
    protected double doDoubleValue() throws IOException {
        return numberValue().doubleValue();
    }

    @Override
    public XContentType contentType() {
        return XContentType.JSON;
    }

    @Override
    public void allowDuplicateKeys(boolean allowDuplicateKeys) {
        throw new UnsupportedOperationException("Allowing duplicate keys is not possible for maps");
    }

    @Override
    public Token nextToken() throws IOException {
        currentToken = null;
        return null;
    }

    @Override
    public void skipChildren() throws IOException {
        currentToken = null;
    }

    @Override
    public Token currentToken() {
        return currentToken;
    }

    @Override
    public String currentName() throws IOException {
        if (currentToken != null) {
            return currentName;
        } else {
            return null;
        }
    }

    @Override
    public String text() throws IOException {
        if (currentToken() == Token.VALUE_STRING || currentToken() == Token.VALUE_NUMBER || currentToken() == Token.VALUE_BOOLEAN) {
            return value.toString();
        } else {
            return null;
        }
    }

    @Override
    public CharBuffer charBuffer() throws IOException {
        throw new UnsupportedOperationException("use text() instead");
    }

    @Override
    public Object objectText() throws IOException {
        throw new UnsupportedOperationException("use text() instead");
    }

    @Override
    public Object objectBytes() throws IOException {
        throw new UnsupportedOperationException("use text() instead");
    }

    @Override
    public boolean hasTextCharacters() {
        return false;
    }

    @Override
    public char[] textCharacters() throws IOException {
        throw new UnsupportedOperationException("use text() instead");
    }

    @Override
    public int textLength() throws IOException {
        throw new UnsupportedOperationException("use text() instead");
    }

    @Override
    public int textOffset() throws IOException {
        throw new UnsupportedOperationException("use text() instead");
    }

    @Override
    public Number numberValue() throws IOException {
        if (currentToken() == Token.VALUE_NUMBER) {
            return (Number) value;
        } else {
            throw new IllegalStateException("Cannot get numeric value for the current token " + currentToken());
        }
    }

    @Override
    public NumberType numberType() throws IOException {
        Number number = numberValue();
        if (number instanceof Integer) {
            return NumberType.INT;
        } else if (number instanceof BigInteger) {
            return NumberType.BIG_INTEGER;
        } else if (number instanceof Long) {
            return NumberType.LONG;
        } else if (number instanceof Float) {
            return NumberType.FLOAT;
        } else if (number instanceof Double) {
            return NumberType.DOUBLE;
        } else if (number instanceof BigDecimal) {
            return NumberType.BIG_DECIMAL;
        }
        throw new IllegalStateException("No matching token for number_type [" + number.getClass() + "]");
    }

    @Override
    public byte[] binaryValue() throws IOException {
        if (value instanceof byte[] bytes) {
            return bytes;
        } else {
            throw new IllegalStateException("Cannot get binary value for the current token " + currentToken());
        }
    }

    @Override
    public XContentLocation getTokenLocation() {
        return originalLocationOffset;
    }

    @Override
    public boolean isClosed() {
        return closed;
    }

    @Override
    public void close() throws IOException {
        closed = true;
    }

}
