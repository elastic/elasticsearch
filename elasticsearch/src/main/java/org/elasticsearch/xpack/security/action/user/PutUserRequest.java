/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.xpack.security.action.user;

import org.elasticsearch.action.ActionRequest;
import org.elasticsearch.action.ActionRequestValidationException;
import org.elasticsearch.action.support.WriteRequest;
import org.elasticsearch.common.Nullable;
import org.elasticsearch.common.bytes.BytesArray;
import org.elasticsearch.common.bytes.BytesReference;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.xpack.security.authc.support.CharArrays;
import org.elasticsearch.xpack.security.support.MetadataUtils;
import org.elasticsearch.xpack.security.support.Validation;

import java.io.IOException;
import java.util.Map;

import static org.elasticsearch.action.ValidateActions.addValidationError;

/**
 * Request object to put a native user.
 */
public class PutUserRequest extends ActionRequest implements UserRequest, WriteRequest<PutUserRequest> {

    private String username;
    private String[] roles;
    private String fullName;
    private String email;
    private Map<String, Object> metadata;
    private char[] passwordHash;
    private boolean enabled = true;
    private RefreshPolicy refreshPolicy = RefreshPolicy.IMMEDIATE;

    public PutUserRequest() {
    }

    @Override
    public ActionRequestValidationException validate() {
        ActionRequestValidationException validationException = null;
        if (username == null) {
            validationException = addValidationError("user is missing", validationException);
        } else {
            Validation.Error error = Validation.Users.validateUsername(username, false, Settings.EMPTY);
            if (error != null) {
                validationException = addValidationError(error.toString(), validationException);
            }
        }
        if (roles == null) {
            validationException = addValidationError("roles are missing", validationException);
        }
        if (metadata != null && MetadataUtils.containsReservedMetadata(metadata)) {
            validationException = addValidationError("metadata keys may not start with [" + MetadataUtils.RESERVED_PREFIX + "]",
                    validationException);
        }
        // we do not check for a password hash here since it is possible that the user exists and we don't want to update the password
        return validationException;
    }

    public void username(String username) {
        this.username = username;
    }

    public void roles(String... roles) {
        this.roles = roles;
    }

    public void fullName(String fullName) {
        this.fullName = fullName;
    }

    public void email(String email) {
        this.email = email;
    }

    public void metadata(Map<String, Object> metadata) {
        this.metadata = metadata;
    }

    public void passwordHash(@Nullable char[] passwordHash) {
        this.passwordHash = passwordHash;
    }

    public boolean enabled() {
        return enabled;
    }

    /**
     * Should this request trigger a refresh ({@linkplain RefreshPolicy#IMMEDIATE}, the default), wait for a refresh (
     * {@linkplain RefreshPolicy#WAIT_UNTIL}), or proceed ignore refreshes entirely ({@linkplain RefreshPolicy#NONE}).
     */
    @Override
    public RefreshPolicy getRefreshPolicy() {
        return refreshPolicy;
    }

    @Override
    public PutUserRequest setRefreshPolicy(RefreshPolicy refreshPolicy) {
        this.refreshPolicy = refreshPolicy;
        return this;
    }

    public String username() {
        return username;
    }

    public String[] roles() {
        return roles;
    }

    public String fullName() {
        return fullName;
    }

    public String email() {
        return email;
    }

    public Map<String, Object> metadata() {
        return metadata;
    }

    @Nullable
    public char[] passwordHash() {
        return passwordHash;
    }

    public void enabled(boolean enabled) {
        this.enabled = enabled;
    }

    @Override
    public String[] usernames() {
        return new String[] { username };
    }

    @Override
    public void readFrom(StreamInput in) throws IOException {
        super.readFrom(in);
        username = in.readString();
        BytesReference passwordHashRef = in.readBytesReference();
        if (passwordHashRef == BytesArray.EMPTY) {
            passwordHash = null;
        } else {
            passwordHash = CharArrays.utf8BytesToChars(BytesReference.toBytes(passwordHashRef));
        }
        roles = in.readStringArray();
        fullName = in.readOptionalString();
        email = in.readOptionalString();
        metadata = in.readBoolean() ? in.readMap() : null;
        refreshPolicy = RefreshPolicy.readFrom(in);
        enabled = in.readBoolean();
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        super.writeTo(out);
        out.writeString(username);
        BytesReference passwordHashRef;
        if (passwordHash == null) {
            passwordHashRef = null;
        } else {
            passwordHashRef = new BytesArray(CharArrays.toUtf8Bytes(passwordHash));
        }
        out.writeBytesReference(passwordHashRef);
        out.writeStringArray(roles);
        out.writeOptionalString(fullName);
        out.writeOptionalString(email);
        if (metadata == null) {
            out.writeBoolean(false);
        } else {
            out.writeBoolean(true);
            out.writeMap(metadata);
        }
        refreshPolicy.writeTo(out);
        out.writeBoolean(enabled);
    }
}
