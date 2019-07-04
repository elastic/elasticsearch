/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.xpack.core.security.action.user;

import org.elasticsearch.ElasticsearchParseException;
import org.elasticsearch.action.ActionRequestBuilder;
import org.elasticsearch.action.support.WriteRequestBuilder;
import org.elasticsearch.client.ElasticsearchClient;
import org.elasticsearch.common.ValidationException;
import org.elasticsearch.common.bytes.BytesReference;
import org.elasticsearch.common.settings.SecureString;
import org.elasticsearch.common.xcontent.LoggingDeprecationHandler;
import org.elasticsearch.common.xcontent.NamedXContentRegistry;
import org.elasticsearch.common.xcontent.XContentParser;
import org.elasticsearch.common.xcontent.XContentType;
import org.elasticsearch.xpack.core.security.authc.support.Hasher;
import org.elasticsearch.xpack.core.security.support.Validation;
import org.elasticsearch.xpack.core.security.user.User;
import org.elasticsearch.xpack.core.security.xcontent.XContentUtils;

import java.io.IOException;
import java.io.InputStream;
import java.nio.CharBuffer;

/**
 * Request to change a user's password.
 */
public class ChangePasswordRequestBuilder
        extends ActionRequestBuilder<ChangePasswordRequest, ChangePasswordResponse>
        implements WriteRequestBuilder<ChangePasswordRequestBuilder> {

    public ChangePasswordRequestBuilder(ElasticsearchClient client) {
        super(client, ChangePasswordAction.INSTANCE, new ChangePasswordRequest());
    }

    public ChangePasswordRequestBuilder username(String username) {
        request.username(username);
        return this;
    }

    public static char[] validateAndHashPassword(SecureString password, Hasher hasher) {
        Validation.Error error = Validation.Users.validatePassword(password);
        if (error != null) {
            ValidationException validationException = new ValidationException();
            validationException.addValidationError(error.toString());
            throw validationException;
        }
        return hasher.hash(password);
    }

    /**
     * Sets the password. Note: the char[] passed to this method will be cleared.
     */
    public ChangePasswordRequestBuilder password(char[] password, Hasher hasher) {
        try (SecureString secureString = new SecureString(password)) {
            char[] hash = validateAndHashPassword(secureString, hasher);
            request.passwordHash(hash);
        }
        return this;
    }

    /**
     * Populate the change password request from the source in the provided content type
     */
    public ChangePasswordRequestBuilder source(BytesReference source, XContentType xContentType, Hasher hasher) throws
        IOException {
        // EMPTY is ok here because we never call namedObject
        try (InputStream stream = source.streamInput();
             XContentParser parser = xContentType.xContent()
                .createParser(NamedXContentRegistry.EMPTY, LoggingDeprecationHandler.INSTANCE, stream)) {
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
                        assert CharBuffer.wrap(passwordChars).chars().noneMatch((i) -> (char) i != (char) 0) : "expected password to " +
                                "clear the char[] but it did not!";
                    } else {
                        throw new ElasticsearchParseException(
                                "expected field [{}] to be of type string, but found [{}] instead", currentFieldName, token);
                    }
                } else {
                    throw new ElasticsearchParseException("failed to parse change password request. unexpected field [{}]",
                            currentFieldName);
                }
            }
        }
        return this;
    }
}
