/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.common.xcontent;

import org.elasticsearch.core.CheckedFunction;
import org.elasticsearch.core.RestApiVersion;

import java.io.IOException;
import java.nio.CharBuffer;
import java.util.List;
import java.util.Map;
import java.util.function.Supplier;

/**
 * Wrapper for a XContentParser that makes a single object/array look like a complete document.
 *
 * The wrapper prevents the parsing logic to consume tokens outside of the wrapped object as well
 * as skipping to the end of the object in case of a parsing error. The wrapper is intended to be
 * used for parsing objects that should be ignored if they are malformed.
 */
public class XContentSubParser implements XContentParser {

    private final XContentParser parser;
    private int level;
    private boolean closed;

    public XContentSubParser(XContentParser parser) {
        this.parser = parser;
        if (parser.currentToken() != Token.START_OBJECT && parser.currentToken() != Token.START_ARRAY) {
            throw new IllegalStateException("The sub parser has to be created on the start of an object or array");
        }
        level = 1;
    }

    @Override
    public XContentType contentType() {
        return parser.contentType();
    }

    @Override
    public void allowDuplicateKeys(boolean allowDuplicateKeys) {
        parser.allowDuplicateKeys(allowDuplicateKeys);
    }

    @Override
    public Token nextToken() throws IOException {
        if (level > 0) {
            Token token = parser.nextToken();
            if (token == Token.START_OBJECT || token == Token.START_ARRAY) {
                level++;
            } else if (token == Token.END_OBJECT || token == Token.END_ARRAY) {
                level--;
            }
            return token;
        } else {
            return null; // we have reached the end of the wrapped object
        }
    }

    @Override
    public void skipChildren() throws IOException {
        Token token = parser.currentToken();
        if (token != Token.START_OBJECT && token != Token.START_ARRAY) {
            // skip if not starting on an object or an array
            return;
        }
        int backToLevel = level - 1;
        while (nextToken() != null) {
            if (level <= backToLevel) {
                return;
            }
        }
    }

    @Override
    public Token currentToken() {
        return parser.currentToken();
    }

    @Override
    public String currentName() throws IOException {
        return parser.currentName();
    }

    @Override
    public Map<String, Object> map() throws IOException {
        return parser.map();
    }

    @Override
    public Map<String, Object> mapOrdered() throws IOException {
        return parser.mapOrdered();
    }

    @Override
    public Map<String, String> mapStrings() throws IOException {
        return parser.mapStrings();
    }

    @Override
    public <T> Map<String, T> map(
            Supplier<Map<String, T>> mapFactory, CheckedFunction<XContentParser, T, IOException> mapValueParser) throws IOException {
        return parser.map(mapFactory, mapValueParser);
    }

    @Override
    public List<Object> list() throws IOException {
        return parser.list();
    }

    @Override
    public List<Object> listOrderedMap() throws IOException {
        return parser.listOrderedMap();
    }

    @Override
    public String text() throws IOException {
        return parser.text();
    }

    @Override
    public String textOrNull() throws IOException {
        return parser.textOrNull();
    }

    @Override
    public CharBuffer charBufferOrNull() throws IOException {
        return parser.charBufferOrNull();
    }

    @Override
    public CharBuffer charBuffer() throws IOException {
        return parser.charBuffer();
    }

    @Override
    public Object objectText() throws IOException {
        return parser.objectText();
    }

    @Override
    public Object objectBytes() throws IOException {
        return parser.objectBytes();
    }

    @Override
    public boolean hasTextCharacters() {
        return parser.hasTextCharacters();
    }

    @Override
    public char[] textCharacters() throws IOException {
        return parser.textCharacters();
    }

    @Override
    public int textLength() throws IOException {
        return parser.textLength();
    }

    @Override
    public int textOffset() throws IOException {
        return parser.textOffset();
    }

    @Override
    public Number numberValue() throws IOException {
        return parser.numberValue();
    }

    @Override
    public NumberType numberType() throws IOException {
        return parser.numberType();
    }

    @Override
    public short shortValue(boolean coerce) throws IOException {
        return parser.shortValue(coerce);
    }

    @Override
    public int intValue(boolean coerce) throws IOException {
        return parser.intValue(coerce);
    }

    @Override
    public long longValue(boolean coerce) throws IOException {
        return parser.longValue(coerce);
    }

    @Override
    public float floatValue(boolean coerce) throws IOException {
        return parser.floatValue(coerce);
    }

    @Override
    public double doubleValue(boolean coerce) throws IOException {
        return parser.doubleValue();
    }

    @Override
    public short shortValue() throws IOException {
        return parser.shortValue();
    }

    @Override
    public int intValue() throws IOException {
        return parser.intValue();
    }

    @Override
    public long longValue() throws IOException {
        return parser.longValue();
    }

    @Override
    public float floatValue() throws IOException {
        return parser.floatValue();
    }

    @Override
    public double doubleValue() throws IOException {
        return parser.doubleValue();
    }

    @Override
    public boolean isBooleanValue() throws IOException {
        return parser.isBooleanValue();
    }

    @Override
    public boolean booleanValue() throws IOException {
        return parser.booleanValue();
    }

    @Override
    public byte[] binaryValue() throws IOException {
        return parser.binaryValue();
    }

    @Override
    public XContentLocation getTokenLocation() {
        return parser.getTokenLocation();
    }

    @Override
    public <T> T namedObject(Class<T> categoryClass, String name, Object context) throws IOException {
        return parser.namedObject(categoryClass, name, context);
    }

    @Override
    public NamedXContentRegistry getXContentRegistry() {
        return parser.getXContentRegistry();
    }

    @Override
    public boolean isClosed() {
        return closed;
    }

    @Override
    public RestApiVersion getRestApiVersion() {
        return parser.getRestApiVersion();
    }

    @Override
    public DeprecationHandler getDeprecationHandler() {
        return parser.getDeprecationHandler();
    }

    @Override
    public void close() throws IOException {
        if (closed == false) {
            closed = true;
            while (true) {
                if (nextToken() == null) {
                    return;
                }
            }
        }
    }
}
