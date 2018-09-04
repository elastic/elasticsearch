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
import org.elasticsearch.common.CharArrays;
import org.elasticsearch.common.xcontent.ToXContentObject;
import org.elasticsearch.common.xcontent.XContentBuilder;

import java.io.Closeable;
import java.io.IOException;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;

/**
 * Request object to create or update a user in the native realm.
 */
public final class PutUserRequest implements Validatable, Closeable, ToXContentObject {

    private final String username;
    private final List<String> roles;
    private final String fullName;
    private final String email;
    private final Map<String, Object> metadata;
    private final char[] password;
    private final boolean enabled;
    private final RefreshPolicy refreshPolicy;

    public PutUserRequest(String username, char[] password, List<String> roles, String fullName, String email, boolean enabled,
                          Map<String, Object> metadata, RefreshPolicy refreshPolicy) {
        this.username = Objects.requireNonNull(username, "username is required");
        this.password = password;
        this.roles = Collections.unmodifiableList(Objects.requireNonNull(roles, "roles must be specified"));
        this.fullName = fullName;
        this.email = email;
        this.enabled = enabled;
        this.metadata = metadata == null ? Collections.emptyMap() : Collections.unmodifiableMap(metadata);
        this.refreshPolicy = refreshPolicy == null ? RefreshPolicy.getDefault() : refreshPolicy;
    }

    public String getUsername() {
        return username;
    }

    public List<String> getRoles() {
        return roles;
    }

    public String getFullName() {
        return fullName;
    }

    public String getEmail() {
        return email;
    }

    public Map<String, Object> getMetadata() {
        return metadata;
    }

    public char[] getPassword() {
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
        PutUserRequest that = (PutUserRequest) o;
        return enabled == that.enabled &&
            Objects.equals(username, that.username) &&
            Objects.equals(roles, that.roles) &&
            Objects.equals(fullName, that.fullName) &&
            Objects.equals(email, that.email) &&
            Objects.equals(metadata, that.metadata) &&
            Arrays.equals(password, that.password) &&
            refreshPolicy == that.refreshPolicy;
    }

    @Override
    public int hashCode() {
        int result = Objects.hash(username, roles, fullName, email, metadata, enabled, refreshPolicy);
        result = 31 * result + Arrays.hashCode(password);
        return result;
    }

    @Override
    public void close() {
        if (password != null) {
            Arrays.fill(password, (char) 0);
        }
    }

    @Override
    public Optional<ValidationException> validate() {
        if (metadata != null && metadata.keySet().stream().anyMatch(s -> s.startsWith("_"))) {
            ValidationException validationException = new ValidationException();
            validationException.addValidationError("metadata keys may not start with [_]");
            return Optional.of(validationException);
        }
        return Optional.empty();
    }

    @Override
    public XContentBuilder toXContent(XContentBuilder builder, Params params) throws IOException {
        builder.startObject();
        builder.field("username", username);
        if (password != null) {
            byte[] charBytes = CharArrays.toUtf8Bytes(password);
            builder.field("password").utf8Value(charBytes, 0, charBytes.length);
        }
        if (roles != null) {
            builder.field("roles", roles);
        }
        if (fullName != null) {
            builder.field("full_name", fullName);
        }
        if (email != null) {
            builder.field("email", email);
        }
        if (metadata != null) {
            builder.field("metadata", metadata);
        }
        return builder.endObject();
    }
}
