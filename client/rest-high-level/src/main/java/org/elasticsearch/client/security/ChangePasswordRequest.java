/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.client.security;

import org.elasticsearch.client.Validatable;
import org.elasticsearch.core.CharArrays;
import org.elasticsearch.core.Nullable;
import org.elasticsearch.common.xcontent.ToXContentObject;
import org.elasticsearch.common.xcontent.XContentBuilder;

import java.io.IOException;
import java.util.Arrays;
import java.util.Objects;

/**
 * Request object to change the password of a user of a native realm or a built-in user.
 */
public final class ChangePasswordRequest implements Validatable, ToXContentObject {

    private final String username;
    private final char[] password;
    private final RefreshPolicy refreshPolicy;

    /**
     * @param username      The username of the user whose password should be changed or null for the current user.
     * @param password      The new password. The password array is not cleared by the {@link ChangePasswordRequest} object so the
     *                      calling code must clear it after receiving the response.
     * @param refreshPolicy The refresh policy for the request.
     */
    public ChangePasswordRequest(@Nullable String username, char[] password, RefreshPolicy refreshPolicy) {
        this.username = username;
        this.password = Objects.requireNonNull(password, "password is required");
        this.refreshPolicy = refreshPolicy == null ? RefreshPolicy.getDefault() : refreshPolicy;
    }

    public String getUsername() {
        return username;
    }

    public char[] getPassword() {
        return password;
    }

    public RefreshPolicy getRefreshPolicy() {
        return refreshPolicy;
    }

    @Override
    public XContentBuilder toXContent(XContentBuilder builder, Params params) throws IOException {
        byte[] charBytes = CharArrays.toUtf8Bytes(password);
        try {
            return builder.startObject()
                .field("password").utf8Value(charBytes, 0, charBytes.length)
                .endObject();
        } finally {
            Arrays.fill(charBytes, (byte) 0);
        }
    }
}
