/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.client.security;

import org.elasticsearch.common.settings.SecureString;
import org.elasticsearch.xcontent.ConstructingObjectParser;
import org.elasticsearch.xcontent.ParseField;
import org.elasticsearch.xcontent.XContentParser;

import java.io.IOException;
import java.util.Objects;

import static org.elasticsearch.xcontent.ConstructingObjectParser.constructorArg;

/**
 * Response for creating a service account token. Contains the token's name and value for bearer authentication.
 */
public final class CreateServiceAccountTokenResponse {

    private final String name;
    private final SecureString value;

    public CreateServiceAccountTokenResponse(String name, SecureString value) {
        this.name = name;
        this.value = value;
    }

    public String getName() {
        return name;
    }

    public SecureString getValue() {
        return value;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        CreateServiceAccountTokenResponse that = (CreateServiceAccountTokenResponse) o;
        return Objects.equals(name, that.name) && Objects.equals(value, that.value);
    }

    @Override
    public int hashCode() {
        return Objects.hash(name, value);
    }

    static final ConstructingObjectParser<Token, Void> TOKEN_PARSER = new ConstructingObjectParser<>(
        "create_service_token_response_token",
        args -> new Token((String) args[0], (String) args[1])
    );

    static final ConstructingObjectParser<CreateServiceAccountTokenResponse, Void> PARSER = new ConstructingObjectParser<>(
        "create_service_token_response",
        args -> {
            if (false == (Boolean) args[0]) {
                throw new IllegalStateException("The create field should always be true");
            }
            final Token token = (Token) args[1];
            return new CreateServiceAccountTokenResponse(token.name, new SecureString(token.value.toCharArray()));
        }
    );

    static {
        TOKEN_PARSER.declareString(constructorArg(), new ParseField("name"));
        TOKEN_PARSER.declareString(constructorArg(), new ParseField("value"));
        PARSER.declareBoolean(constructorArg(), new ParseField("created"));
        PARSER.declareObject(constructorArg(), TOKEN_PARSER, new ParseField("token"));
    }

    public static CreateServiceAccountTokenResponse fromXContent(XContentParser parser) throws IOException {
        return PARSER.parse(parser, null);
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
