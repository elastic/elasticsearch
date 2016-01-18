/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.shield.action.admin.user;

import org.elasticsearch.ElasticsearchParseException;
import org.elasticsearch.action.ActionRequest;
import org.elasticsearch.action.ActionRequestValidationException;
import org.elasticsearch.common.Strings;
import org.elasticsearch.common.bytes.BytesReference;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.common.xcontent.XContentHelper;
import org.elasticsearch.common.xcontent.XContentParser;
import org.elasticsearch.shield.authc.support.CharArrays;
import org.elasticsearch.shield.authc.support.Hasher;
import org.elasticsearch.shield.authc.support.SecuredString;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

import static org.elasticsearch.action.ValidateActions.addValidationError;

/**
 * Request object to add a {@code User} to the shield administrative index
 */
public class AddUserRequest extends ActionRequest<AddUserRequest> {

    private final Hasher hasher = Hasher.BCRYPT;

    private String username;
    private String roles[];
    private char[] passwordHash;

    public AddUserRequest() {
    }

    @Override
    public ActionRequestValidationException validate() {
        ActionRequestValidationException validationException = null;
        if (username == null) {
            validationException = addValidationError("user is missing", validationException);
        }
        if (roles == null) {
            validationException = addValidationError("roles are missing", validationException);
        }
        if (passwordHash == null) {
            validationException = addValidationError("passwordHash is missing", validationException);
        }
        return validationException;
    }

    public void username(String username) {
        this.username = username;
    }

    public void roles(String... roles) {
        this.roles = roles;
    }

    public void passwordHash(char[] passwordHash) {
        this.passwordHash = passwordHash;
    }

    public String username() {
        return username;
    }

    public String[] roles() {
        return roles;
    }

    public char[] passwordHash() {
        return passwordHash;
    }

    public AddUserRequest source(BytesReference source) throws Exception {
        List<String> parsedRoles = new ArrayList<>();
        try (XContentParser parser = XContentHelper.createParser(source)) {
            XContentParser.Token token;
            String currentFieldName = null;
            while ((token = parser.nextToken()) != XContentParser.Token.END_OBJECT) {
                if (token == XContentParser.Token.FIELD_NAME) {
                    currentFieldName = parser.currentName();
                } else if (token.isValue()) {
                    if ("username".equals(currentFieldName)) {
                        username = parser.text();
                    } else if ("password".equals(currentFieldName)) {
                        // It's assumed the password is plaintext and needs to be hashed
                        passwordHash = hasher.hash(new SecuredString(parser.text().toCharArray()));
                    } else if ("roles".equals(currentFieldName)) {
                        parsedRoles.add(parser.text());
                    } else {
                        throw new ElasticsearchParseException("unexpected field in add user request [{}]", currentFieldName);
                    }
                } else if (token == XContentParser.Token.START_OBJECT) {
                    // expected
                } else if (token == XContentParser.Token.START_ARRAY || token == XContentParser.Token.END_ARRAY) {
                    if ("roles".equals(currentFieldName) == false) {
                        throw new ElasticsearchParseException("unexpected array for field [{}]", currentFieldName);
                    }
                } else {
                    throw new ElasticsearchParseException("failed to parse add user request, got value with wrong type [{}]", currentFieldName);
                }
            }
        }
        roles = parsedRoles.toArray(Strings.EMPTY_ARRAY);
        return this;
    }

    @Override
    public void readFrom(StreamInput in) throws IOException {
        super.readFrom(in);
        username = in.readString();
        passwordHash = CharArrays.utf8BytesToChars(in.readByteArray());
        roles = in.readStringArray();
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        super.writeTo(out);
        out.writeString(username);
        out.writeByteArray(CharArrays.toUtf8Bytes(passwordHash));
        out.writeStringArray(roles);
    }
}
