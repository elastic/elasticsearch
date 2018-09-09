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

import org.elasticsearch.common.Nullable;
import org.elasticsearch.common.ParseField;
import org.elasticsearch.common.Strings;
import org.elasticsearch.common.xcontent.ConstructingObjectParser;
import org.elasticsearch.common.xcontent.XContentParser;

import java.io.IOException;
import java.util.Arrays;
import java.util.Collections;
import java.util.Map;
import java.util.List;

import static org.elasticsearch.common.xcontent.ConstructingObjectParser.constructorArg;
import static org.elasticsearch.common.xcontent.ConstructingObjectParser.optionalConstructorArg;

/**
 * An authenticated user
 */
public class User {

    static final ParseField USERNAME = new ParseField("username");
    static final ParseField ROLES = new ParseField("roles");
    static final ParseField FULL_NAME = new ParseField("full_name");
    static final ParseField EMAIL = new ParseField("email");
    static final ParseField METADATA = new ParseField("metadata");
    static final ParseField ENABLED = new ParseField("enabled");

    @SuppressWarnings("unchecked")
    private static final ConstructingObjectParser<User, Void> PARSER = new ConstructingObjectParser<>("client_security_user",
            a -> new User((String) a[0], ((List<String>) a[1]).toArray(new String[0]), (String) a[2], (String) a[3],
                    (Map<String, Object>) a[4], (Boolean) a[5]));
    static {
        PARSER.declareString(constructorArg(), USERNAME);
        PARSER.declareStringArray(optionalConstructorArg(), ROLES);
        PARSER.declareStringOrNull(optionalConstructorArg(), FULL_NAME);
        PARSER.declareStringOrNull(optionalConstructorArg(), EMAIL);
        PARSER.<Map<String, Object>>declareObject(optionalConstructorArg(), (parser, c) -> parser.map(), METADATA);
        PARSER.declareBoolean(constructorArg(), ENABLED);
    }

    private final String username;
    private final String[] roles;
    private final Map<String, Object> metadata;
    private final boolean enabled;

    @Nullable private final String fullName;
    @Nullable private final String email;

    private User(String username, @Nullable String[] roles, @Nullable String fullName, @Nullable String email,
            @Nullable Map<String, Object> metadata, Boolean enabled) {
        this.username = username;
        this.roles = roles == null ? Strings.EMPTY_ARRAY : roles;
        this.fullName = fullName;
        this.email = email;
        this.metadata = metadata != null ? Collections.unmodifiableMap(metadata) : Collections.emptyMap();
        this.enabled = enabled;
    }

    /**
     * @return  The principal of this user - effectively serving as the
     *          unique identity of of the user.
     */
    public String principal() {
        return this.username;
    }

    /**
     * @return  The roles this user is associated with. The roles are
     *          identified by their unique names and each represents as
     *          set of permissions
     */
    public String[] roles() {
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

    /**
     * @return whether the user is enabled or not
     */
    public boolean enabled() {
        return enabled;
    }

    @Override
    public String toString() {
        final StringBuilder sb = new StringBuilder();
        sb.append("User[username=").append(username);
        sb.append(",roles=[").append(Strings.arrayToCommaDelimitedString(roles)).append("]");
        sb.append(",fullName=").append(fullName);
        sb.append(",email=").append(email);
        sb.append(",metadata=");
        sb.append(metadata);
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
        if (!Arrays.equals(roles, user.roles)) {
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
        int result = username.hashCode();
        result = 31 * result + Arrays.hashCode(roles);
        result = 31 * result + metadata.hashCode();
        result = 31 * result + (fullName != null ? fullName.hashCode() : 0);
        result = 31 * result + (email != null ? email.hashCode() : 0);
        return result;
    }

    public static User fromXContent(XContentParser parser) throws IOException {
        return PARSER.parse(parser, null);
    }

}
