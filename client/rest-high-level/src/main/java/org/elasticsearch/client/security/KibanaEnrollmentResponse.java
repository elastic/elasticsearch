/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.client.security;

import org.elasticsearch.common.settings.SecureString;
import org.elasticsearch.common.xcontent.ConstructingObjectParser;
import org.elasticsearch.common.xcontent.ParseField;
import org.elasticsearch.common.xcontent.XContentParser;

import java.io.IOException;
import java.util.Objects;

import static org.elasticsearch.common.xcontent.ConstructingObjectParser.constructorArg;

public final class KibanaEnrollmentResponse {

    private String tokenName;
    private SecureString tokenValue;
    private String httpCa;

    public KibanaEnrollmentResponse(String tokenName, SecureString tokenValue, String httpCa) {
        this.tokenName = tokenName;
        this.tokenValue = tokenValue;
        this.httpCa = httpCa;
    }

    public String getTokenName() { return tokenName; }

    public SecureString getTokenValue() { return tokenValue; }

    public String getHttpCa() {
        return httpCa;
    }

    private static final ParseField TOKEN = new ParseField("token");
    private static final ParseField TOKEN_NAME = new ParseField("name");
    private static final ParseField TOKEN_VALUE = new ParseField("value");
    private static final ParseField HTTP_CA = new ParseField("http_ca");

    static final ConstructingObjectParser<Token, Void> TOKEN_PARSER = new ConstructingObjectParser<>(
        KibanaEnrollmentResponse.class.getName(), true,
        a -> new Token((String) a[0], (String) a[1])
    );

    private static final ConstructingObjectParser<KibanaEnrollmentResponse, Void> PARSER =
        new ConstructingObjectParser<>(
            KibanaEnrollmentResponse.class.getName(), true,
            a -> {
                final Token token = (Token) a[0];
                return new KibanaEnrollmentResponse(token.name, new SecureString(token.value.toCharArray()), (String) a[1]);
            });

    static {
        TOKEN_PARSER.declareString(constructorArg(), TOKEN_NAME);
        TOKEN_PARSER.declareString(constructorArg(), TOKEN_VALUE);
        PARSER.declareObject(constructorArg(), TOKEN_PARSER, TOKEN);
        PARSER.declareString(constructorArg(), HTTP_CA);
    }

    public static KibanaEnrollmentResponse fromXContent(XContentParser parser) throws IOException {
        return PARSER.apply(parser, null);
    }

    @Override public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        KibanaEnrollmentResponse that = (KibanaEnrollmentResponse) o;
        return tokenName.equals(that.tokenName) && tokenValue.equals(that.tokenValue) && httpCa.equals(that.httpCa);
    }

    @Override public int hashCode() {
        return Objects.hash(tokenName, tokenValue, httpCa);
    }

    private static class Token {
        private final String name;
        private final String value;

        Token(String name, String value) {
            this.name = name;
            this.value = value;
        }
    }
}
