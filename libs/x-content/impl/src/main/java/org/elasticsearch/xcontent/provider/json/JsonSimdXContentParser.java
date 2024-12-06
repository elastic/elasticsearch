/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.xcontent.provider.json;

import org.elasticsearch.xcontent.XContentLocation;
import org.elasticsearch.xcontent.XContentParseException;
import org.elasticsearch.xcontent.XContentParserConfiguration;
import org.elasticsearch.xcontent.XContentType;
import org.elasticsearch.xcontent.support.AbstractXContentParser;
import org.simdjson.JsonValue;

import java.io.IOException;
import java.nio.CharBuffer;
import java.util.Iterator;
import java.util.Map;
import java.util.Stack;

public class JsonSimdXContentParser extends AbstractXContentParser {
    private final Stack<State> stack;
    private State state;

    public JsonSimdXContentParser(XContentParserConfiguration config, JsonValue initialState) {
        super(config.registry(), config.deprecationHandler(), config.restApiVersion());

        this.stack = new Stack<>();
        this.state = new State.InObject(initialState.objectIterator());
    }

    private sealed interface State permits State.InArray, State.InObject {
        Token nextToken();

        Token currentToken();

        boolean doBooleanValue();

        String text();

        State stepIntoObject();

        State stepIntoArray();

        final class InArray implements State {
            private final Iterator<JsonValue> iterator;
            private JsonValue current;
            private Mode mode;

            enum Mode {
                START,
                ELEMENT,
                END,
                NULL
            }

            InArray(Iterator<JsonValue> iterator) {
                this.iterator = iterator;
                this.mode = Mode.START;
            }

            @Override
            public Token nextToken() {
                switch (mode) {
                    case START, ELEMENT -> advance();
                    case END -> mode = Mode.NULL;
                    case NULL -> {
                    }
                }

                return currentToken();
            }

            private void advance() {
                if (iterator.hasNext()) {
                    current = iterator.next();
                    mode = Mode.ELEMENT;
                } else {
                    mode = Mode.END;
                }
            }

            @Override
            public Token currentToken() {
                return switch (mode) {
                    case START -> Token.START_ARRAY;
                    case ELEMENT -> getToken(current);
                    case END -> Token.END_ARRAY;
                    case NULL -> null;
                };
            }

            @Override
            public boolean doBooleanValue() {
                if (mode != Mode.ELEMENT) {
                    throw new XContentParseException(XContentLocation.UNKNOWN, "not a boolean");
                }

                return State.doBooleanValue(current);
            }

            @Override
            public String text() {
                if (mode == Mode.ELEMENT) {
                    return current.asString();
                }

                throw new XContentParseException(XContentLocation.UNKNOWN, "not test");
            }

            @Override
            public State stepIntoObject() {
                assert mode == Mode.ELEMENT && current.isObject() : "not an object";
                return new InObject(current.objectIterator());
            }

            @Override
            public State stepIntoArray() {
                assert mode == Mode.ELEMENT && current.isArray() : "not an array";
                return new InArray(current.arrayIterator());
            }
        }

        final class InObject implements State {
            private final Iterator<Map.Entry<String, JsonValue>> iterator;
            private String currentName;
            private JsonValue currentValue;
            private Mode mode;

            enum Mode {
                START,
                NAME,
                VALUE,
                END,
                NULL
            }

            InObject(Iterator<Map.Entry<String, JsonValue>> iterator) {
                this.iterator = iterator;
                this.mode = Mode.START;
            }

            @Override
            public Token nextToken() {
                switch (mode) {
                    case START, VALUE -> advance();
                    case NAME -> mode = Mode.VALUE;
                    case END -> mode = Mode.NULL;
                    case NULL -> {
                    }
                }

                return currentToken();
            }

            private void advance() {
                if (iterator.hasNext()) {
                    var entry = iterator.next();
                    currentName = entry.getKey();
                    currentValue = entry.getValue();
                    mode = Mode.NAME;
                } else {
                    mode = Mode.END;
                }
            }

            @Override
            public Token currentToken() {
                return switch (mode) {
                    case START -> Token.START_OBJECT;
                    case NAME -> Token.FIELD_NAME;
                    case VALUE -> getToken(currentValue);
                    case END -> Token.END_OBJECT;
                    case NULL -> null;
                };
            }

            @Override
            public boolean doBooleanValue() {
                if (mode != Mode.VALUE) {
                    throw new XContentParseException(XContentLocation.UNKNOWN, "not a boolean");
                }

                return State.doBooleanValue(currentValue);
            }

            @Override
            public String text() {
                if (mode == Mode.NAME) {
                    return currentName;
                }
                if (mode == Mode.VALUE) {
                    return currentValue.asString();
                }

                throw new XContentParseException(XContentLocation.UNKNOWN, "not test");
            }

            @Override
            public State stepIntoObject() {
                assert mode == Mode.VALUE && currentValue.isObject() : "not an object";
                return new InObject(currentValue.objectIterator());
            }

            @Override
            public State stepIntoArray() {
                assert mode == Mode.VALUE && currentValue.isArray() : "not an array";
                return new InArray(currentValue.arrayIterator());
            }
        }

        static Token getToken(JsonValue value) {
            if (value.isObject()) {
                return Token.START_OBJECT;
            }
            if (value.isArray()) {
                return Token.START_ARRAY;
            }
            if (value.isString()) {
                return Token.VALUE_STRING;
            }
            if (value.isLong() || value.isDouble()) {
                return Token.VALUE_NUMBER;
            }
            if (value.isBoolean()) {
                return Token.VALUE_BOOLEAN;
            }
            // TODO VALUE_EMBEDDED_OBJECT
            if (value.isNull()) {
                return Token.VALUE_NULL;
            }

            throw new IllegalStateException("unexpected state of JSON parser");
        }

        static boolean doBooleanValue(JsonValue value) {
            if (value.isBoolean()) {
                return value.asBoolean();
            }

            throw new XContentParseException(XContentLocation.UNKNOWN, "not a boolean");
        }
    }

    @Override
    protected boolean doBooleanValue() {
        return state.doBooleanValue();
    }

    @Override
    protected short doShortValue() throws IOException {
        // Can be implemented, does not matter now.
        throw new UnsupportedOperationException();
    }

    @Override
    protected int doIntValue() throws IOException {
        // Can be implemented, does not matter now.
        throw new UnsupportedOperationException();
    }

    @Override
    protected long doLongValue() throws IOException {
        // Can be implemented, does not matter now.
        throw new UnsupportedOperationException();
    }

    @Override
    protected float doFloatValue() throws IOException {
        // Can be implemented, does not matter now.
        throw new UnsupportedOperationException();
    }

    @Override
    protected double doDoubleValue() throws IOException {
        // Can be implemented, does not matter now.
        throw new UnsupportedOperationException();
    }

    @Override
    public XContentType contentType() {
        return XContentType.JSON;
    }

    @Override
    public void allowDuplicateKeys(boolean allowDuplicateKeys) {
        throw new UnsupportedOperationException();
    }

    @Override
    public Token nextToken() throws IOException {
        var token = state.nextToken();
        if (token == Token.END_OBJECT || token == Token.END_ARRAY) {
            if (stack.empty() == false) {
                state = stack.pop();
            }
        }
        if (token == Token.START_OBJECT) {
            stack.push(state);
            state = state.stepIntoObject();
        }

        if (token == Token.START_ARRAY) {
            stack.push(state);
            state = state.stepIntoArray();
        }

        return token;
    }

    @Override
    public void skipChildren() throws IOException {

    }

    @Override
    public Token currentToken() {
        return state.currentToken();
    }

    @Override
    public String currentName() throws IOException {
        return "";
    }

    @Override
    public String text() throws IOException {
        return state.text();
    }

    @Override
    public CharBuffer charBuffer() throws IOException {
        return null;
    }

    @Override
    public Object objectText() throws IOException {
        // Can be implemented, does not matter now.
        throw new UnsupportedOperationException();
    }

    @Override
    public Object objectBytes() throws IOException {
        // Can be implemented, does not matter now.
        throw new UnsupportedOperationException();
    }

    @Override
    public boolean hasTextCharacters() {
        return false;
    }

    @Override
    public char[] textCharacters() throws IOException {
        return null;
    }

    @Override
    public int textLength() throws IOException {
        return 0;
    }

    @Override
    public int textOffset() throws IOException {
        return 0;
    }

    @Override
    public Number numberValue() throws IOException {
        return null;
    }

    @Override
    public NumberType numberType() throws IOException {
        return null;
    }

    @Override
    public byte[] binaryValue() throws IOException {
        return null;
    }

    @Override
    public XContentLocation getTokenLocation() {
        return XContentLocation.UNKNOWN;
    }

    @Override
    public boolean isClosed() {
        return false;
    }

    @Override
    public void close() throws IOException {

    }
}
