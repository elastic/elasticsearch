/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.common.xcontent.json;

import com.fasterxml.jackson.core.JsonLocation;
import com.fasterxml.jackson.core.JsonParser;
import com.fasterxml.jackson.core.JsonToken;
import org.elasticsearch.core.RestApiVersion;
import org.elasticsearch.common.xcontent.DeprecationHandler;
import org.elasticsearch.common.xcontent.NamedXContentRegistry;
import org.elasticsearch.common.xcontent.XContentLocation;
import org.elasticsearch.common.xcontent.XContentType;
import org.elasticsearch.common.xcontent.support.AbstractXContentParser;
import org.elasticsearch.core.internal.io.IOUtils;

import java.io.IOException;
import java.nio.CharBuffer;

public class JsonXContentParser extends AbstractXContentParser {

    final JsonParser parser;

    public JsonXContentParser(NamedXContentRegistry xContentRegistry,
            DeprecationHandler deprecationHandler, JsonParser parser) {
        super(xContentRegistry, deprecationHandler, RestApiVersion.current());
        this.parser = parser;
    }

    public JsonXContentParser(NamedXContentRegistry xContentRegistry,
                              DeprecationHandler deprecationHandler, JsonParser parser,
                              RestApiVersion restApiVersion) {
        super(xContentRegistry, deprecationHandler, restApiVersion);
        this.parser = parser;
    }

    @Override
    public XContentType contentType() {
        return XContentType.JSON;
    }

    @Override
    public void allowDuplicateKeys(boolean allowDuplicateKeys) {
        parser.configure(JsonParser.Feature.STRICT_DUPLICATE_DETECTION, allowDuplicateKeys == false);
    }

    @Override
    public Token nextToken() throws IOException {
        return convertToken(parser.nextToken());
    }

    @Override
    public void skipChildren() throws IOException {
        parser.skipChildren();
    }

    @Override
    public Token currentToken() {
        return convertToken(parser.getCurrentToken());
    }

    @Override
    public NumberType numberType() throws IOException {
        return convertNumberType(parser.getNumberType());
    }

    @Override
    public String currentName() throws IOException {
        return parser.getCurrentName();
    }

    @Override
    protected boolean doBooleanValue() throws IOException {
        return parser.getBooleanValue();
    }

    @Override
    public String text() throws IOException {
        if (currentToken().isValue()) {
            return parser.getText();
        }
        throw new IllegalStateException("Can't get text on a " + currentToken() + " at " + getTokenLocation());
    }

    @Override
    public CharBuffer charBuffer() throws IOException {
        return CharBuffer.wrap(parser.getTextCharacters(), parser.getTextOffset(), parser.getTextLength());
    }

    @Override
    public Object objectText() throws IOException {
        JsonToken currentToken = parser.getCurrentToken();
        if (currentToken == JsonToken.VALUE_STRING) {
            return text();
        } else if (currentToken == JsonToken.VALUE_NUMBER_INT || currentToken == JsonToken.VALUE_NUMBER_FLOAT) {
            return parser.getNumberValue();
        } else if (currentToken == JsonToken.VALUE_TRUE) {
            return Boolean.TRUE;
        } else if (currentToken == JsonToken.VALUE_FALSE) {
            return Boolean.FALSE;
        } else if (currentToken == JsonToken.VALUE_NULL) {
            return null;
        } else {
            return text();
        }
    }

    @Override
    public Object objectBytes() throws IOException {
        JsonToken currentToken = parser.getCurrentToken();
        if (currentToken == JsonToken.VALUE_STRING) {
            return charBuffer();
        } else if (currentToken == JsonToken.VALUE_NUMBER_INT || currentToken == JsonToken.VALUE_NUMBER_FLOAT) {
            return parser.getNumberValue();
        } else if (currentToken == JsonToken.VALUE_TRUE) {
            return Boolean.TRUE;
        } else if (currentToken == JsonToken.VALUE_FALSE) {
            return Boolean.FALSE;
        } else if (currentToken == JsonToken.VALUE_NULL) {
            return null;
        } else {
            return charBuffer();
        }
    }

    @Override
    public boolean hasTextCharacters() {
        return parser.hasTextCharacters();
    }

    @Override
    public char[] textCharacters() throws IOException {
        return parser.getTextCharacters();
    }

    @Override
    public int textLength() throws IOException {
        return parser.getTextLength();
    }

    @Override
    public int textOffset() throws IOException {
        return parser.getTextOffset();
    }

    @Override
    public Number numberValue() throws IOException {
        return parser.getNumberValue();
    }

    @Override
    public short doShortValue() throws IOException {
        return parser.getShortValue();
    }

    @Override
    public int doIntValue() throws IOException {
        return parser.getIntValue();
    }

    @Override
    public long doLongValue() throws IOException {
        return parser.getLongValue();
    }

    @Override
    public float doFloatValue() throws IOException {
        return parser.getFloatValue();
    }

    @Override
    public double doDoubleValue() throws IOException {
        return parser.getDoubleValue();
    }

    @Override
    public byte[] binaryValue() throws IOException {
        return parser.getBinaryValue();
    }

    @Override
    public XContentLocation getTokenLocation() {
        JsonLocation loc = parser.getTokenLocation();
        if (loc == null) {
            return null;
        }
        return new XContentLocation(loc.getLineNr(), loc.getColumnNr());
    }

    @Override
    public void close() {
        IOUtils.closeWhileHandlingException(parser);
    }

    private NumberType convertNumberType(JsonParser.NumberType numberType) {
        switch (numberType) {
            case INT:
                return NumberType.INT;
            case BIG_INTEGER:
                return NumberType.BIG_INTEGER;
            case LONG:
                return NumberType.LONG;
            case FLOAT:
                return NumberType.FLOAT;
            case DOUBLE:
                return NumberType.DOUBLE;
            case BIG_DECIMAL:
                return NumberType.BIG_DECIMAL;
        }
        throw new IllegalStateException("No matching token for number_type [" + numberType + "]");
    }

    private Token convertToken(JsonToken token) {
        if (token == null) {
            return null;
        }
        switch (token) {
            case FIELD_NAME:
                return Token.FIELD_NAME;
            case VALUE_FALSE:
            case VALUE_TRUE:
                return Token.VALUE_BOOLEAN;
            case VALUE_STRING:
                return Token.VALUE_STRING;
            case VALUE_NUMBER_INT:
            case VALUE_NUMBER_FLOAT:
                return Token.VALUE_NUMBER;
            case VALUE_NULL:
                return Token.VALUE_NULL;
            case START_OBJECT:
                return Token.START_OBJECT;
            case END_OBJECT:
                return Token.END_OBJECT;
            case START_ARRAY:
                return Token.START_ARRAY;
            case END_ARRAY:
                return Token.END_ARRAY;
            case VALUE_EMBEDDED_OBJECT:
                return Token.VALUE_EMBEDDED_OBJECT;
        }
        throw new IllegalStateException("No matching token for json_token [" + token + "]");
    }

    @Override
    public boolean isClosed() {
        return parser.isClosed();
    }
}
