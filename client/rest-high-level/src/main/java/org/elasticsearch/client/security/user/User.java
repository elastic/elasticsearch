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

package org.elasticsearch.client.security.user;

import org.elasticsearch.common.Nullable;
import org.elasticsearch.common.Strings;

import java.util.Collection;
import java.util.Collections;
import java.util.Map;
import java.util.Objects;


/**
 * An authenticated user
 */
public final class User {

    private final String username;
    private final Collection<String> roles;
    private final Map<String, Object> metadata;
    @Nullable private final String fullName;
    @Nullable private final String email;

    public User(String username, Collection<String> roles, Map<String, Object> metadata, @Nullable String fullName,
            @Nullable String email) {
        Objects.requireNonNull(username, "`username` cannot be null");
        Objects.requireNonNull(roles, "`roles` cannot be null. Pass an empty collection instead.");
        Objects.requireNonNull(roles, "`metadata` cannot be null. Pass an empty map instead.");
        this.username = username;
        this.roles = roles;
        this.metadata = Collections.unmodifiableMap(metadata);
        this.fullName = fullName;
        this.email = email;
    }

    /**
     * @return  The principal of this user - effectively serving as the
     *          unique identity of the user. Can never be {@code null}.
     */
    public String username() {
        return this.username;
    }

    /**
     * @return  The roles this user is associated with. The roles are
     *          identified by their unique names and each represents as
     *          set of permissions. Can never be {@code null}.
     */
    public Collection<String> roles() {
        return this.roles;
    }

    /**
     * @return  The metadata that is associated with this user. Can never be {@code null}.
     */
    public Map<String, Object> metadata() {
        return metadata;
    }

    /**
     * @return  The full name of this user. May be {@code null}.
     */
    public @Nullable String fullName() {
        return fullName;
    }

    /**
     * @return  The email of this user. May be {@code null}.
     */
    public @Nullable String email() {
        return email;
    }

    @Override
    public String toString() {
        final StringBuilder sb = new StringBuilder();
        sb.append("User[username=").append(username);
        sb.append(",roles=[").append(Strings.collectionToCommaDelimitedString(roles)).append("]");
        sb.append(",metadata=").append(metadata);
        sb.append(",fullName=").append(fullName);
        sb.append(",email=").append(email);
        sb.append("]");
        return sb.toString();
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }
        if (o instanceof User == false) {
            return false;
        }

        final User user = (User) o;

        if (!username.equals(user.username)) {
            return false;
        }
        if (!roles.equals(user.roles)) {
            return false;
        }
        if (!metadata.equals(user.metadata)) {
            return false;
        }
        if (fullName != null ? !fullName.equals(user.fullName) : user.fullName != null) {
            return false;
        }
        return !(email != null ? !email.equals(user.email) : user.email != null);
    }

    @Override
    public int hashCode() {
        return Objects.hash(username, roles, metadata, fullName, email);
    }

}
