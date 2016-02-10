/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.shield.action.user;

import org.elasticsearch.ElasticsearchParseException;
import org.elasticsearch.action.ActionRequestBuilder;
import org.elasticsearch.client.ElasticsearchClient;
import org.elasticsearch.common.ParseFieldMatcher;
import org.elasticsearch.common.ValidationException;
import org.elasticsearch.common.bytes.BytesReference;
import org.elasticsearch.common.xcontent.XContentHelper;
import org.elasticsearch.common.xcontent.XContentParser;
import org.elasticsearch.shield.user.User;
import org.elasticsearch.shield.authc.support.Hasher;
import org.elasticsearch.shield.authc.support.SecuredString;
import org.elasticsearch.shield.support.Validation;
import org.elasticsearch.xpack.common.xcontent.XContentUtils;

import java.io.IOException;
import java.util.Arrays;

/**
 */
public class ChangePasswordRequestBuilder
        extends ActionRequestBuilder<ChangePasswordRequest, ChangePasswordResponse, ChangePasswordRequestBuilder> {

    public ChangePasswordRequestBuilder(ElasticsearchClient client) {
        this(client, ChangePasswordAction.INSTANCE);
    }

    public ChangePasswordRequestBuilder(ElasticsearchClient client, ChangePasswordAction action) {
        super(client, action, new ChangePasswordRequest());
    }

    public ChangePasswordRequestBuilder username(String username) {
        request.username(username);
        return this;
    }

    public ChangePasswordRequestBuilder password(char[] password) {
        Validation.Error error = Validation.Users.validatePassword(password);
        if (error != null) {
            ValidationException validationException = new ValidationException();
            validationException.addValidationError(error.toString());
            throw validationException;
        }

        try (SecuredString securedString = new SecuredString(password)) {
            request.passwordHash(Hasher.BCRYPT.hash(securedString));
        }
        return this;
    }

    public ChangePasswordRequestBuilder source(BytesReference source) throws IOException {
        try (XContentParser parser = XContentHelper.createParser(source)) {
            XContentUtils.verifyObject(parser);
            XContentParser.Token token;
            String currentFieldName = null;
            while ((token = parser.nextToken()) != XContentParser.Token.END_OBJECT) {
                if (token == XContentParser.Token.FIELD_NAME) {
                    currentFieldName = parser.currentName();
                } else if (ParseFieldMatcher.STRICT.match(currentFieldName, User.Fields.PASSWORD)) {
                    if (token == XContentParser.Token.VALUE_STRING) {
                        String password = parser.text();
                        char[] passwordChars = password.toCharArray();
                        password(passwordChars);
                        password = null;
                        Arrays.fill(passwordChars, (char) 0);
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

    public ChangePasswordRequestBuilder refresh(boolean refresh) {
        request.refresh(refresh);
        return this;
    }
}
