/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */
package org.elasticsearch.xpack.security.action.user;

import org.elasticsearch.exception.ElasticsearchParseException;
import org.elasticsearch.action.ActionRequestBuilder;
import org.elasticsearch.action.ActionResponse;
import org.elasticsearch.action.support.WriteRequestBuilder;
import org.elasticsearch.client.internal.ElasticsearchClient;
import org.elasticsearch.common.ValidationException;
import org.elasticsearch.common.bytes.BytesReference;
import org.elasticsearch.common.settings.SecureString;
import org.elasticsearch.common.xcontent.LoggingDeprecationHandler;
import org.elasticsearch.common.xcontent.XContentHelper;
import org.elasticsearch.xcontent.XContentParser;
import org.elasticsearch.xcontent.XContentType;
import org.elasticsearch.xpack.core.security.action.user.ChangePasswordRequest;
import org.elasticsearch.xpack.core.security.authc.support.Hasher;
import org.elasticsearch.xpack.core.security.support.Validation;
import org.elasticsearch.xpack.core.security.user.User;
import org.elasticsearch.xpack.core.security.xcontent.XContentUtils;

import java.io.IOException;
import java.nio.CharBuffer;
import java.util.Locale;

/**
 * Request to change a user's password.
 */
public class ChangePasswordRequestBuilder extends ActionRequestBuilder<ChangePasswordRequest, ActionResponse.Empty>
    implements
        WriteRequestBuilder<ChangePasswordRequestBuilder> {

    public ChangePasswordRequestBuilder(ElasticsearchClient client) {
        super(client, TransportChangePasswordAction.TYPE, new ChangePasswordRequest());
    }

    public ChangePasswordRequestBuilder username(String username) {
        request.username(username);
        return this;
    }

    public static char[] validateAndHashPassword(SecureString password, Hasher hasher) {
        Validation.Error error = Validation.Users.validatePassword(password);
        if (error != null) {
            throw validationException(error.toString());
        }
        return hasher.hash(password);
    }

    /**
     * Sets the password. Note: the char[] passed to this method will be cleared.
     */
    public ChangePasswordRequestBuilder password(char[] password, Hasher hasher) {
        try (SecureString secureString = new SecureString(password)) {
            char[] hash = validateAndHashPassword(secureString, hasher);
            if (request.passwordHash() != null) {
                throw validationException("password_hash has already been set");
            }
            request.passwordHash(hash);
        }
        return this;
    }

    /**
     * Sets the password hash.
     */
    public ChangePasswordRequestBuilder passwordHash(char[] passwordHashChars, Hasher configuredHasher) {
        final Hasher resolvedHasher = Hasher.resolveFromHash(passwordHashChars);
        if (resolvedHasher.equals(configuredHasher) == false
            && Hasher.getAvailableAlgoStoredPasswordHash().contains(resolvedHasher.name().toLowerCase(Locale.ROOT)) == false) {
            throw new IllegalArgumentException(
                "The provided password hash is not a hash or it could not be resolved to a supported hash algorithm. "
                    + "The supported password hash algorithms are "
                    + Hasher.getAvailableAlgoStoredPasswordHash().toString()
            );
        }
        if (request.passwordHash() != null) {
            throw validationException("password_hash has already been set");
        }
        request.passwordHash(passwordHashChars);
        return this;
    }

    /**
     * Populate the change password request from the source in the provided content type
     */
    public ChangePasswordRequestBuilder source(BytesReference source, XContentType xContentType, Hasher hasher) throws IOException {
        // EMPTY is ok here because we never call namedObject
        try (
            XContentParser parser = XContentHelper.createParserNotCompressed(
                LoggingDeprecationHandler.XCONTENT_PARSER_CONFIG,
                source,
                xContentType
            )
        ) {
            XContentUtils.verifyObject(parser);
            XContentParser.Token token;
            String currentFieldName = null;
            while ((token = parser.nextToken()) != XContentParser.Token.END_OBJECT) {
                if (token == XContentParser.Token.FIELD_NAME) {
                    currentFieldName = parser.currentName();
                } else if (User.Fields.PASSWORD.match(currentFieldName, parser.getDeprecationHandler())) {
                    if (token == XContentParser.Token.VALUE_STRING) {
                        String password = parser.text();
                        final char[] passwordChars = password.toCharArray();
                        password(passwordChars, hasher);
                        assert CharBuffer.wrap(passwordChars).chars().noneMatch((i) -> (char) i != (char) 0)
                            : "expected password to " + "clear the char[] but it did not!";
                    } else {
                        throw new ElasticsearchParseException(
                            "expected field [{}] to be of type string, but found [{}] instead",
                            currentFieldName,
                            token
                        );
                    }
                } else if (User.Fields.PASSWORD_HASH.match(currentFieldName, parser.getDeprecationHandler())) {
                    if (token == XContentParser.Token.VALUE_STRING) {
                        char[] passwordHashChars = parser.text().toCharArray();
                        passwordHash(passwordHashChars, hasher);
                    } else {
                        throw new ElasticsearchParseException(
                            "expected field [{}] to be of type string, but found [{}] instead",
                            currentFieldName,
                            token
                        );
                    }
                } else {
                    throw new ElasticsearchParseException(
                        "failed to parse change password request. unexpected field [{}]",
                        currentFieldName
                    );
                }
            }
        }
        return this;
    }

    private static ValidationException validationException(String message) {
        ValidationException validationException = new ValidationException();
        validationException.addValidationError(message);
        return validationException;
    }
}
