/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.xcontent.provider.json;

import com.fasterxml.jackson.core.JsonLocation;
import com.fasterxml.jackson.core.JsonParseException;
import com.fasterxml.jackson.core.JsonParser;
import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.core.JsonToken;
import com.fasterxml.jackson.core.exc.InputCoercionException;
import com.fasterxml.jackson.core.io.JsonEOFException;

import org.elasticsearch.core.IOUtils;
import org.elasticsearch.xcontent.XContentEOFException;
import org.elasticsearch.xcontent.XContentLocation;
import org.elasticsearch.xcontent.XContentParseException;
import org.elasticsearch.xcontent.XContentParserConfiguration;
import org.elasticsearch.xcontent.XContentType;
import org.elasticsearch.xcontent.provider.XContentParserConfigurationImpl;
import org.elasticsearch.xcontent.support.AbstractXContentParser;

import java.io.IOException;
import java.nio.CharBuffer;

public class JsonXContentParser extends AbstractXContentParser {

    final JsonParser parser;

    public JsonXContentParser(XContentParserConfiguration config, JsonParser parser) {
        super(config.registry(), config.deprecationHandler(), config.restApiVersion());
        this.parser = ((XContentParserConfigurationImpl) config).filter(parser);
    }

    @Override
    public XContentType contentType() {
        return XContentType.JSON;
    }

    @Override
    public void allowDuplicateKeys(boolean allowDuplicateKeys) {
        parser.configure(JsonParser.Feature.STRICT_DUPLICATE_DETECTION, allowDuplicateKeys == false);
    }

    private static XContentParseException newXContentParseException(JsonProcessingException e) {
        JsonLocation loc = e.getLocation();
        throw new XContentParseException(new XContentLocation(loc.getLineNr(), loc.getColumnNr()), e.getMessage(), e);
    }

    @Override
    public Token nextToken() throws IOException {
        try {
            return convertToken(parser.nextToken());
        } catch (JsonEOFException e) {
            JsonLocation location = e.getLocation();
            throw new XContentEOFException(new XContentLocation(location.getLineNr(), location.getColumnNr()), "Unexpected end of file", e);
        } catch (JsonParseException e) {
            throw newXContentParseException(e);
        }
    }

    @Override
    public String nextFieldName() throws IOException {
        try {
            return parser.nextFieldName();
        } catch (JsonParseException e) {
            throw newXContentParseException(e);
        }
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
        try {
            return parser.getBooleanValue();
        } catch (JsonParseException e) {
            throw newXContentParseException(e);
        }
    }

    @Override
    public String text() throws IOException {
        if (currentToken().isValue() == false) {
            throwOnNoText();
        }
        try {
            return parser.getText();
        } catch (JsonParseException e) {
            throw newXContentParseException(e);
        }
    }

    private void throwOnNoText() {
        throw new IllegalArgumentException("Expected text at " + getTokenLocation() + " but found " + currentToken());
    }

    @Override
    public CharBuffer charBuffer() throws IOException {
        try {
            return CharBuffer.wrap(parser.getTextCharacters(), parser.getTextOffset(), parser.getTextLength());
        } catch (JsonParseException e) {
            throw newXContentParseException(e);
        }
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
        try {
            return parser.getTextCharacters();
        } catch (JsonParseException e) {
            throw newXContentParseException(e);
        }
    }

    @Override
    public int textLength() throws IOException {
        try {
            return parser.getTextLength();
        } catch (JsonParseException e) {
            throw newXContentParseException(e);
        }
    }

    @Override
    public int textOffset() throws IOException {
        try {
            return parser.getTextOffset();
        } catch (JsonParseException e) {
            throw newXContentParseException(e);
        }
    }

    @Override
    public Number numberValue() throws IOException {
        try {
            return parser.getNumberValue();
        } catch (InputCoercionException | JsonParseException e) {
            throw newXContentParseException(e);
        }
    }

    @Override
    public short doShortValue() throws IOException {
        try {
            return parser.getShortValue();
        } catch (InputCoercionException | JsonParseException e) {
            throw newXContentParseException(e);
        }
    }

    @Override
    public int doIntValue() throws IOException {
        try {
            return parser.getIntValue();
        } catch (InputCoercionException | JsonParseException e) {
            throw newXContentParseException(e);
        }
    }

    @Override
    public long doLongValue() throws IOException {
        try {
            return parser.getLongValue();
        } catch (InputCoercionException | JsonParseException e) {
            throw newXContentParseException(e);
        }
    }

    @Override
    public float doFloatValue() throws IOException {
        try {
            return parser.getFloatValue();
        } catch (InputCoercionException | JsonParseException e) {
            throw newXContentParseException(e);
        }
    }

    @Override
    public double doDoubleValue() throws IOException {
        try {
            return parser.getDoubleValue();
        } catch (InputCoercionException | JsonParseException e) {
            throw newXContentParseException(e);
        }
    }

    @Override
    public byte[] binaryValue() throws IOException {
        try {
            return parser.getBinaryValue();
        } catch (JsonParseException e) {
            throw newXContentParseException(e);
        }
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

    private static NumberType convertNumberType(JsonParser.NumberType numberType) {
        return switch (numberType) {
            case INT -> NumberType.INT;
            case BIG_INTEGER -> NumberType.BIG_INTEGER;
            case LONG -> NumberType.LONG;
            case FLOAT -> NumberType.FLOAT;
            case DOUBLE -> NumberType.DOUBLE;
            case BIG_DECIMAL -> NumberType.BIG_DECIMAL;
        };
    }

    private static Token convertToken(JsonToken token) {
        if (token == null) {
            return null;
        }
        return switch (token) {
            case START_OBJECT -> Token.START_OBJECT;
            case END_OBJECT -> Token.END_OBJECT;
            case START_ARRAY -> Token.START_ARRAY;
            case END_ARRAY -> Token.END_ARRAY;
            case FIELD_NAME -> Token.FIELD_NAME;
            case VALUE_EMBEDDED_OBJECT -> Token.VALUE_EMBEDDED_OBJECT;
            case VALUE_STRING -> Token.VALUE_STRING;
            case VALUE_NUMBER_INT, VALUE_NUMBER_FLOAT -> Token.VALUE_NUMBER;
            case VALUE_FALSE, VALUE_TRUE -> Token.VALUE_BOOLEAN;
            case VALUE_NULL -> Token.VALUE_NULL;
            default -> throw unknownTokenException(token);
        };
    }

    private static IllegalStateException unknownTokenException(JsonToken token) {
        return new IllegalStateException("No matching token for json_token [" + token + "]");
    }

    @Override
    public boolean isClosed() {
        return parser.isClosed();
    }
}
