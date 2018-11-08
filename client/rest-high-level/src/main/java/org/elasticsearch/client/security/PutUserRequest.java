/*
 * Licensed to Elasticsearch under one or more contributor
 * license agreements. See the NOTICE file distributed with
 * this work for additional information regarding copyright
 * ownership. Elasticsearch licenses this file to you under
 * the Apache License, Version 2.0 (the "License"); you may
 * not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package org.elasticsearch.client.security;

import org.elasticsearch.client.Validatable;
import org.elasticsearch.client.ValidationException;
import org.elasticsearch.client.security.user.User;
import org.elasticsearch.common.CharArrays;
import org.elasticsearch.common.Nullable;
import org.elasticsearch.common.xcontent.ToXContentObject;
import org.elasticsearch.common.xcontent.XContentBuilder;

import java.io.IOException;
import java.util.Arrays;
import java.util.Objects;
import java.util.Optional;

/**
 * Request object to create or update a user in the native realm.
 */
public final class PutUserRequest implements Validatable, ToXContentObject {

    private final User user;
    private final @Nullable char[] password;
    private final boolean enabled;
    private final RefreshPolicy refreshPolicy;

    /**
     * Creates a new request that is used to create or update a user in the native realm.
     *
     * @param user the user to be created or updated
     * @param password the password of the user. The password array is not modified by this class.
     *                 It is the responsibility of the caller to clear the password after receiving
     *                 a response.
     * @param enabled true if the user is enabled and allowed to access elasticsearch
     * @param refreshPolicy the refresh policy for the request.
     */
    public PutUserRequest(User user, @Nullable char[] password, boolean enabled, @Nullable RefreshPolicy refreshPolicy) {
        this.user = Objects.requireNonNull(user, "user is required, cannot be null");
        this.password = password;
        this.enabled = enabled;
        this.refreshPolicy = refreshPolicy == null ? RefreshPolicy.getDefault() : refreshPolicy;
    }

    public User getUser() {
        return user;
    }

    public @Nullable char[] getPassword() {
        return password;
    }

    public boolean isEnabled() {
        return enabled;
    }

    public RefreshPolicy getRefreshPolicy() {
        return refreshPolicy;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        final PutUserRequest that = (PutUserRequest) o;
        return Objects.equals(user, that.user)
                && Arrays.equals(password, that.password)
                && enabled == that.enabled
                && refreshPolicy == that.refreshPolicy;
    }

    @Override
    public int hashCode() {
        int result = Objects.hash(user, enabled, refreshPolicy);
        result = 31 * result + Arrays.hashCode(password);
        return result;
    }

    @Override
    public Optional<ValidationException> validate() {
        if (user.getMetadata() != null && user.getMetadata().keySet().stream().anyMatch(s -> s.startsWith("_"))) {
            ValidationException validationException = new ValidationException();
            validationException.addValidationError("user metadata keys may not start with [_]");
            return Optional.of(validationException);
        }
        return Optional.empty();
    }

    @Override
    public XContentBuilder toXContent(XContentBuilder builder, Params params) throws IOException {
        builder.startObject();
        builder.field("username", user.getUsername());
        if (password != null) {
            byte[] charBytes = CharArrays.toUtf8Bytes(password);
            try {
                builder.field("password").utf8Value(charBytes, 0, charBytes.length);
            } finally {
                Arrays.fill(charBytes, (byte) 0);
            }
        }
        builder.field("roles", user.getRoles());
        if (user.getFullName() != null) {
            builder.field("full_name", user.getFullName());
        }
        if (user.getEmail() != null) {
            builder.field("email", user.getEmail());
        }
        builder.field("metadata", user.getMetadata());
        builder.field("enabled", enabled);
        return builder.endObject();
    }
}
