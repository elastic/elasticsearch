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

import org.elasticsearch.client.security.user.User;
import org.elasticsearch.common.ParseField;
import org.elasticsearch.common.xcontent.ConstructingObjectParser;
import org.elasticsearch.common.xcontent.XContentParser;

import java.io.IOException;
import java.util.List;
import java.util.Map;
import java.util.Objects;

import static org.elasticsearch.common.xcontent.ConstructingObjectParser.constructorArg;
import static org.elasticsearch.common.xcontent.ConstructingObjectParser.optionalConstructorArg;

/**
 * The response for the authenticate call. The response contains two fields: a
 * user field and a boolean flag signaling if the user is enabled or not. The
 * user object contains all user metadata which Elasticsearch uses to map roles,
 * etc.
 */
public final class AuthenticateResponse {

    static final ParseField USERNAME = new ParseField("username");
    static final ParseField ROLES = new ParseField("roles");
    static final ParseField METADATA = new ParseField("metadata");
    static final ParseField FULL_NAME = new ParseField("full_name");
    static final ParseField EMAIL = new ParseField("email");
    static final ParseField ENABLED = new ParseField("enabled");
    static final ParseField AUTHENTICATION_REALM_NAME = new ParseField("authentication_realm_name");
    static final ParseField AUTHENTICATION_REALM_TYPE = new ParseField("authentication_realm_type");
    static final ParseField LOOKUP_REALM_NAME = new ParseField("lookup_realm_name");
    static final ParseField LOOKUP_REALM_TYPE = new ParseField("lookup_realm_type");

    @SuppressWarnings("unchecked")
    private static final ConstructingObjectParser<AuthenticateResponse, Void> PARSER = new ConstructingObjectParser<>(
            "client_security_authenticate_response",
            a -> new AuthenticateResponse(new User((String) a[0], ((List<String>) a[1]), (Map<String, Object>) a[2],
                (String) a[3], (String) a[4]), (Boolean) a[5], (String) a[6], (String) a[7], (String) a[8], (String) a[9]));
    static {
        PARSER.declareString(constructorArg(), USERNAME);
        PARSER.declareStringArray(constructorArg(), ROLES);
        PARSER.<Map<String, Object>>declareObject(constructorArg(), (parser, c) -> parser.map(), METADATA);
        PARSER.declareStringOrNull(optionalConstructorArg(), FULL_NAME);
        PARSER.declareStringOrNull(optionalConstructorArg(), EMAIL);
        PARSER.declareBoolean(constructorArg(), ENABLED);
        PARSER.declareString(constructorArg(), AUTHENTICATION_REALM_NAME);
        PARSER.declareString(constructorArg(), AUTHENTICATION_REALM_TYPE);
        PARSER.declareString(constructorArg(), LOOKUP_REALM_NAME);
        PARSER.declareString(constructorArg(), LOOKUP_REALM_TYPE);
    }

    private final User user;
    private final boolean enabled;
    private final String authenticationRealmName;
    private final String authenticationRealmType;
    private final String lookupRealmName;
    private final String lookupRealmType;

    public AuthenticateResponse(User user, boolean enabled, String authenticationRealmName, String authenticationRealmType,
                                String lookupRealmName, String lookupRealmType) {
        this.user = user;
        this.enabled = enabled;
        this.authenticationRealmName = authenticationRealmName;
        this.authenticationRealmType = authenticationRealmType;
        this.lookupRealmName = lookupRealmName;
        this.lookupRealmType = lookupRealmType;
    }

    /**
     * @return The effective user. This is the authenticated user, or, when
     *         submitting requests on behalf of other users, it is the
     *         impersonated user.
     */
    public User getUser() {
        return user;
    }

    /**
     * @return whether the user is enabled or not
     */
    public boolean enabled() {
        return enabled;
    }

    /**
     * @return the name of the realm that authenticated the user
     */
    public String getAuthenticationRealmName() {
        return authenticationRealmName;
    }

    /**
     * @return the type of the realm that authenticated the user
     */
    public String getAuthenticationRealmType() {
        return authenticationRealmType;
    }

    /**
     * @return the name of the realm where the user information was looked up
     */
    public String getLookupRealmName() {
        return lookupRealmName;
    }

    /**
     * @return the type of the realm where the user information was looked up
     */
    public String getLookupRealmType() {
        return lookupRealmType;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        AuthenticateResponse that = (AuthenticateResponse) o;
        return enabled == that.enabled &&
            Objects.equals(user, that.user) &&
            Objects.equals(authenticationRealmName, that.authenticationRealmName) &&
            Objects.equals(authenticationRealmType, that.authenticationRealmType) &&
            Objects.equals(lookupRealmName, that.lookupRealmName) &&
            Objects.equals(lookupRealmType, that.lookupRealmType);
    }

    @Override
    public int hashCode() {
        return Objects.hash(user, enabled, authenticationRealmName, authenticationRealmType, lookupRealmName, lookupRealmType);
    }

    public static AuthenticateResponse fromXContent(XContentParser parser) throws IOException {
        return PARSER.parse(parser, null);
    }

}
