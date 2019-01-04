/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.xpack.security.authc.support.jwt;

import org.elasticsearch.common.ParsingException;
import org.elasticsearch.common.Strings;
import org.elasticsearch.common.xcontent.DeprecationHandler;
import org.elasticsearch.common.xcontent.NamedXContentRegistry;
import org.elasticsearch.common.xcontent.XContentParser;
import org.elasticsearch.common.xcontent.XContentParserUtils;
import org.elasticsearch.common.xcontent.json.JsonXContent;
import org.elasticsearch.xpack.security.authc.oidc.RPConfiguration;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Base64;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Locale;
import java.util.Map;

import static org.elasticsearch.common.xcontent.XContentParserUtils.parseFieldsValue;

/**
 * Contains the necessary functionality for parsing a serialized OpenID Connect ID Token to a {@link JsonWebToken}
 */
public class JsonWebTokenParser {
    private final RPConfiguration rpConfig;

    public JsonWebTokenParser(RPConfiguration rpConfig) {
        this.rpConfig = rpConfig;
    }

    public final JsonWebToken parseJwt(String jwt) throws IOException {
        final JsonWebTokenBuilder builder = new JsonWebTokenBuilder();
        final String[] jwtParts = jwt.split("\\.");
        if (jwtParts.length != 3) {
            throw new IllegalArgumentException("The provided token is not a valid JWT");
        }
        final String deserializedHeader = deserializePart(jwtParts[0]);
        final String deserializedPayload = deserializePart(jwtParts[1]);
        final byte[] deserializedSignature = Strings.hasText(jwtParts[2]) ? Base64.getUrlDecoder().decode(jwtParts[2]) : new byte[0];
        final Map<String, Object> headerMap = parseHeader(deserializedHeader);
        final Map<String, Object> payloadMap = parsePayload(deserializedPayload);
        return new JsonWebToken(headerMap, payloadMap);
    }

    private static final String deserializePart(String encodedString) throws IOException {
        return new String(Base64.getUrlDecoder().decode(encodedString), StandardCharsets.UTF_8.name());
    }

    private final Map<String, Object> parseHeader(String headerJson) throws IOException {
        try (XContentParser parser = JsonXContent.jsonXContent.createParser(NamedXContentRegistry.EMPTY,
            DeprecationHandler.THROW_UNSUPPORTED_OPERATION, headerJson)) {
            final Map<String, Object> headerMap = new HashMap<>();
            XContentParserUtils.ensureExpectedToken(XContentParser.Token.START_OBJECT, parser.nextToken(), parser::getTokenLocation);
            XContentParser.Token token;
            String currentFieldName = null;
            while ((token = parser.nextToken()) != XContentParser.Token.END_OBJECT) {
                if (token == XContentParser.Token.FIELD_NAME) {
                    currentFieldName = parser.currentName();
                } else if (Claims.HeaderClaims.validHeaderClaims().contains(currentFieldName)) {
                    XContentParserUtils.ensureExpectedToken(XContentParser.Token.VALUE_STRING, parser.currentToken(), parser::getTokenLocation);
                    if (Strings.hasText(parser.text())) {
                        headerMap.put(currentFieldName, parser.text());
                    }
                } else {
                    parser.skipChildren();
                }
            }
            return headerMap;
        }
    }

    private final Map<String, Object> parsePayload(String payloadJson) throws IOException {
        try (XContentParser parser = JsonXContent.jsonXContent.createParser(NamedXContentRegistry.EMPTY,
            DeprecationHandler.THROW_UNSUPPORTED_OPERATION, payloadJson)) {
            final Map<String, Object> payloadMap = new HashMap<>();
            XContentParserUtils.ensureExpectedToken(XContentParser.Token.START_OBJECT, parser.nextToken(), parser::getTokenLocation);
            XContentParser.Token token;
            String currentFieldName = null;
            while ((token = parser.nextToken()) != XContentParser.Token.END_OBJECT) {
                if (token == XContentParser.Token.FIELD_NAME) {
                    currentFieldName = parser.currentName();
                } else if (currentFieldName.equals(Claims.StandardClaims.AUDIENCE.getClaimName())) {
                    if (token == XContentParser.Token.START_ARRAY) {
                        while ((token = parser.nextToken()) != XContentParser.Token.END_ARRAY) {
                            XContentParserUtils.
                                ensureExpectedToken(XContentParser.Token.VALUE_STRING, parser.currentToken(), parser::getTokenLocation);
                            payloadMap.put(currentFieldName, parseFieldsValue(parser));
                        }
                    } else {
                        XContentParserUtils.
                            ensureExpectedToken(XContentParser.Token.VALUE_STRING, parser.currentToken(), parser::getTokenLocation);
                        payloadMap.put(currentFieldName, Collections.singletonList(parseFieldsValue(parser)));
                    }

                } else if (Claims.StandardClaims.getKnownClaims().contains(currentFieldName)) {
                    if (parser.currentToken() == XContentParser.Token.VALUE_NULL) {
                        String message = "Failed to parse object: null value for field";
                        throw new ParsingException(parser.getTokenLocation(), String.format(Locale.ROOT, message, currentFieldName));
                    } else if (parser.currentToken() == XContentParser.Token.VALUE_NUMBER) {
                        Number number = (Number) parseFieldsValue(parser);
                        payloadMap.put(currentFieldName, number.longValue());
                    } else {
                        payloadMap.put(currentFieldName, parseFieldsValue(parser));
                    }
                } else if (this.rpConfig.getAllowedScopes().contains(currentFieldName)) {
                    if (Strings.hasText(parser.text())) {
                        payloadMap.put(currentFieldName, parseFieldsValue(parser));
                    }
                } else {
                    parser.skipChildren();
                }
            }
            return payloadMap;
        }
    }
}
