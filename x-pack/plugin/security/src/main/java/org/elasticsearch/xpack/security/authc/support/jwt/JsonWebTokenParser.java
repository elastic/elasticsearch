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
import java.security.Key;
import java.util.Base64;
import java.util.Collections;
import java.util.HashMap;
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

    /**
     * Parses the serialized format of an ID Token into a {@link JsonWebToken}. In doing so it
     * <ul>
     * <li>Validates that the format and structure of the ID Token is correct</li>
     * <li>Validates that the ID Token is signed and that one of the supported algorithms is used</li>
     * <li>Validates the signature using the appropriate</li>
     * </ul>
     *
     * @param jwt Serialized string representation of the ID Token
     * @param key The {@link Key} to be used for verifying the signature
     * @return a {@link JsonWebToken}
     * @throws IOException if the ID Token cannot be deserialized
     */
    public final JsonWebToken parseAndValidateJwt(String jwt, Key key) throws IOException {
        final String[] jwtParts = jwt.split("\\.");
        if (jwtParts.length != 3) {
            throw new IllegalArgumentException("The provided token is not a valid JWT");
        }
        final String serializedHeader = jwtParts[0];
        final String serializedPayload = jwtParts[1];
        final String serializedSignature = jwtParts[2];
        final String deserializedHeader = deserializePart(serializedHeader);
        final String deserializedPayload = deserializePart(serializedPayload);

        final Map<String, Object> headerMap = parseHeader(deserializedHeader);
        final SignatureAlgorithm algorithm = getAlgorithm(headerMap);
        if (algorithm == null || algorithm.equals(SignatureAlgorithm.NONE.name())) {
            //TODO what kind of Exception?
            throw new IllegalStateException("JWT not signed or unrecognised algorithm");
        }
        if (Strings.hasText(serializedSignature) == false) {
            //TODO what kind of Exception?
            throw new IllegalStateException("Unsigned JWT");
        }
        JwtSignatureValidator validator = getValidator(algorithm, key);
        if (null == validator) {
            //TODO what kind of Exception?
            throw new IllegalStateException("Wrong algorithm");
        }
        final byte[] signatureBytes = serializedSignature.getBytes(StandardCharsets.US_ASCII);
        final byte[] data = (serializedHeader + "." + serializedPayload).getBytes(StandardCharsets.UTF_8);
        validator.validateSignature(data, signatureBytes);
        final Map<String, Object> payloadMap = parsePayload(deserializedPayload);
        return new JsonWebToken(headerMap, payloadMap);
    }

    /**
     * Returns the {@link SignatureAlgorithm} that corresponds to the value of the alg claim
     *
     * @param header The {@link Map} containing the parsed header claims
     * @return the SignatureAlgorithm that corresponds to alg
     */
    private SignatureAlgorithm getAlgorithm(Map<String, Object> header) {
        if (header.containsKey("alg")) {
            return SignatureAlgorithm.fromName((String) header.get("alg"));
        } else {
            return null;
        }
    }

    private static String deserializePart(String encodedString) throws IOException {
        return new String(Base64.getUrlDecoder().decode(encodedString), StandardCharsets.UTF_8.name());
    }

    /**
     * Parses a JSON string representing the header of an ID Token into a {@link Map} where the key is the claim name and
     * the value is the claim value.
     *
     * @param headerJson a JSON string representing the payload of a JWT
     * @return a {@link Map} containing the parsed claims
     * @throws IOException if the JSON string is malformed and cannot be parsed
     */
    private Map<String, Object> parseHeader(String headerJson) throws IOException {
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

    /**
     * Parses a JSON string representing the payload of an ID Token into a {@link Map} where the key is the claim name and
     * the value is the claim value. It parses only claims that are either in the set of Standard Claims that the
     * <a href="https://openid.net/specs/openid-connect-core-1_0.html#StandardClaims">specification</a> defines or explicitly defined by the
     * user in the realm settings. For the Standard Claims, the claim is also syntactically checked to conform to the expected types
     * (string, number, boolean, object).
     *
     * @param payloadJson a JSON string representing the payload of a JWT
     * @return a {@link Map} containing the parsed claims
     * @throws IOException if the JSON string is malformed and cannot be parsed
     */
    private Map<String, Object> parsePayload(String payloadJson) throws IOException {
        try (XContentParser parser = JsonXContent.jsonXContent.createParser(NamedXContentRegistry.EMPTY,
            DeprecationHandler.THROW_UNSUPPORTED_OPERATION, payloadJson)) {
            final Map<String, Object> payloadMap = new HashMap<>();
            XContentParserUtils.ensureExpectedToken(XContentParser.Token.START_OBJECT, parser.nextToken(), parser::getTokenLocation);
            XContentParser.Token token;
            String currentFieldName = null;
            while ((token = parser.nextToken()) != XContentParser.Token.END_OBJECT) {
                if (token == XContentParser.Token.FIELD_NAME) {
                    currentFieldName = parser.currentName();
                } else if (Claims.StandardClaims.AUDIENCE.getClaimName().equals(currentFieldName)) {
                    if (token == XContentParser.Token.START_ARRAY) {
                        payloadMap.put(currentFieldName, parser.list());
                    } else {
                        XContentParserUtils.
                            ensureExpectedToken(XContentParser.Token.VALUE_STRING, parser.currentToken(), parser::getTokenLocation);
                        payloadMap.put(currentFieldName, Collections.singletonList(parseFieldsValue(parser)));
                    }

                } else if (Claims.StandardClaims.ADDRESS.getClaimName().equals(currentFieldName)) {
                    XContentParserUtils.
                        ensureExpectedToken(XContentParser.Token.START_OBJECT, parser.currentToken(), parser::getTokenLocation);
                    payloadMap.put(currentFieldName, parser.mapStrings());

                } else if (Claims.StandardClaims.getStandardClaims().contains(currentFieldName)) {
                    if (parser.currentToken() == XContentParser.Token.VALUE_NULL) {
                        String message = "Failed to parse object: null value for field";
                        throw new ParsingException(parser.getTokenLocation(), String.format(Locale.ROOT, message, currentFieldName));
                    } else if (Claims.StandardClaims.getClaimsOfType("string").contains(currentFieldName)) {
                        XContentParserUtils.
                            ensureExpectedToken(XContentParser.Token.VALUE_STRING, parser.currentToken(), parser::getTokenLocation);
                        payloadMap.put(currentFieldName, parser.text());
                    } else if (Claims.StandardClaims.getClaimsOfType("long").contains(currentFieldName)) {
                        XContentParserUtils.
                            ensureExpectedToken(XContentParser.Token.VALUE_NUMBER, parser.currentToken(), parser::getTokenLocation);
                        Number number = (Number) parseFieldsValue(parser);
                        payloadMap.put(currentFieldName, number.longValue());
                    } else if (Claims.StandardClaims.getClaimsOfType("boolean").contains(currentFieldName)) {
                        XContentParserUtils.
                            ensureExpectedToken(XContentParser.Token.VALUE_BOOLEAN, parser.currentToken(), parser::getTokenLocation);
                        payloadMap.put(currentFieldName, parser.booleanValue());
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

    private JwtSignatureValidator getValidator(SignatureAlgorithm algorithm, Key key) {
        if (SignatureAlgorithm.getHmacAlgorithms().contains(algorithm)) {
            return new HmacSignatureValidator(algorithm, key);
        } else if (SignatureAlgorithm.getRsaAlgorithms().contains(algorithm)) {
            return new RsaSignatureValidator(algorithm, key);
        } else if (SignatureAlgorithm.getEcAlgorithms().contains(algorithm)) {
            return new EcSignatureValidator(algorithm, key);
        }
        return null;
    }
}
