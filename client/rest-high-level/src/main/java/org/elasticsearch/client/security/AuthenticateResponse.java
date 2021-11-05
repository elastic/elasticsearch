/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.client.security;

import org.elasticsearch.client.security.user.User;
import org.elasticsearch.core.Nullable;
import org.elasticsearch.xcontent.ConstructingObjectParser;
import org.elasticsearch.xcontent.ParseField;
import org.elasticsearch.xcontent.ToXContentObject;
import org.elasticsearch.xcontent.XContentBuilder;
import org.elasticsearch.xcontent.XContentParser;

import java.io.IOException;
import java.util.List;
import java.util.Map;
import java.util.Objects;

import static org.elasticsearch.xcontent.ConstructingObjectParser.constructorArg;
import static org.elasticsearch.xcontent.ConstructingObjectParser.optionalConstructorArg;

/**
 * The response for the authenticate call. The response contains two fields: a
 * user field and a boolean flag signaling if the user is enabled or not. The
 * user object contains all user metadata which Elasticsearch uses to map roles,
 * etc.
 */
public final class AuthenticateResponse implements ToXContentObject {

    static final ParseField USERNAME = new ParseField("username");
    static final ParseField ROLES = new ParseField("roles");
    static final ParseField METADATA = new ParseField("metadata");
    static final ParseField FULL_NAME = new ParseField("full_name");
    static final ParseField EMAIL = new ParseField("email");
    static final ParseField ENABLED = new ParseField("enabled");
    static final ParseField AUTHENTICATION_REALM = new ParseField("authentication_realm");
    static final ParseField LOOKUP_REALM = new ParseField("lookup_realm");
    static final ParseField REALM_NAME = new ParseField("name");
    static final ParseField REALM_TYPE = new ParseField("type");
    static final ParseField AUTHENTICATION_TYPE = new ParseField("authentication_type");
    static final ParseField TOKEN = new ParseField("token");
    static final ParseField API_KEY_INFO = new ParseField("api_key"); // authentication.api_key={"id":"abc123","name":"my-api-key"}
    static final ParseField API_KEY_INFO_ID = new ParseField("id");
    static final ParseField API_KEY_INFO_NAME = new ParseField("name");

    @SuppressWarnings("unchecked")
    private static final ConstructingObjectParser<AuthenticateResponse, Void> PARSER = new ConstructingObjectParser<>(
        "client_security_authenticate_response",
        true,
        a -> new AuthenticateResponse(
            new User((String) a[0], ((List<String>) a[1]), (Map<String, Object>) a[2], (String) a[3], (String) a[4]),
            (Boolean) a[5],
            (RealmInfo) a[6],
            (RealmInfo) a[7],
            (String) a[8],
            (Map<String, Object>) a[9],
            (ApiKeyInfo) a[10]
        )
    );
    static {
        final ConstructingObjectParser<RealmInfo, Void> realmInfoParser = new ConstructingObjectParser<>(
            "realm_info",
            true,
            a -> new RealmInfo((String) a[0], (String) a[1])
        );
        realmInfoParser.declareString(constructorArg(), REALM_NAME);
        realmInfoParser.declareString(constructorArg(), REALM_TYPE);

        final ConstructingObjectParser<ApiKeyInfo, Void> apiKeyInfoParser = new ConstructingObjectParser<>(
            "api_key",
            true,
            a -> new ApiKeyInfo((String) a[0], (String) a[1])
        );
        apiKeyInfoParser.declareString(constructorArg(), API_KEY_INFO_ID);
        apiKeyInfoParser.declareString(optionalConstructorArg(), API_KEY_INFO_NAME);

        PARSER.declareString(constructorArg(), USERNAME);
        PARSER.declareStringArray(constructorArg(), ROLES);
        PARSER.<Map<String, Object>>declareObject(constructorArg(), (parser, c) -> parser.map(), METADATA);
        PARSER.declareStringOrNull(optionalConstructorArg(), FULL_NAME);
        PARSER.declareStringOrNull(optionalConstructorArg(), EMAIL);
        PARSER.declareBoolean(constructorArg(), ENABLED);
        PARSER.declareObject(constructorArg(), realmInfoParser, AUTHENTICATION_REALM);
        PARSER.declareObject(constructorArg(), realmInfoParser, LOOKUP_REALM);
        PARSER.declareString(constructorArg(), AUTHENTICATION_TYPE);
        PARSER.declareObjectOrNull(optionalConstructorArg(), (p, c) -> p.map(), null, TOKEN);
        PARSER.declareObjectOrNull(optionalConstructorArg(), apiKeyInfoParser, null, API_KEY_INFO);
    }

    private final User user;
    private final boolean enabled;
    private final RealmInfo authenticationRealm;
    private final RealmInfo lookupRealm;
    private final String authenticationType;
    @Nullable
    private final Map<String, Object> token;
    @Nullable
    private final ApiKeyInfo apikeyinfo;

    public AuthenticateResponse(
        User user,
        boolean enabled,
        RealmInfo authenticationRealm,
        RealmInfo lookupRealm,
        String authenticationType
    ) {
        this(user, enabled, authenticationRealm, lookupRealm, authenticationType, null);
    }

    public AuthenticateResponse(
        User user,
        boolean enabled,
        RealmInfo authenticationRealm,
        RealmInfo lookupRealm,
        String authenticationType,
        @Nullable Map<String, Object> token
    ) {
        this(user, enabled, authenticationRealm, lookupRealm, authenticationType, null, null);
    }

    public AuthenticateResponse(
        User user,
        boolean enabled,
        RealmInfo authenticationRealm,
        RealmInfo lookupRealm,
        String authenticationType,
        @Nullable Map<String, Object> token,
        @Nullable ApiKeyInfo apikeyinfo
    ) {
        this.user = user;
        this.enabled = enabled;
        this.authenticationRealm = authenticationRealm;
        this.lookupRealm = lookupRealm;
        this.authenticationType = authenticationType;
        this.token = token;
        this.apikeyinfo = apikeyinfo;
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
     * @return the realm that authenticated the user
     */
    public RealmInfo getAuthenticationRealm() {
        return authenticationRealm;
    }

    /**
     * @return the realm where the user information was looked up
     */
    public RealmInfo getLookupRealm() {
        return lookupRealm;
    }

    public String getAuthenticationType() {
        return authenticationType;
    }

    public Map<String, Object> getToken() {
        return token;
    }

    public ApiKeyInfo getApiKeyInfo() {
        return apikeyinfo;
    }

    @Override
    public XContentBuilder toXContent(XContentBuilder builder, Params params) throws IOException {
        builder.startObject();
        builder.field(AuthenticateResponse.USERNAME.getPreferredName(), user.getUsername());
        builder.field(AuthenticateResponse.ROLES.getPreferredName(), user.getRoles());
        builder.field(AuthenticateResponse.METADATA.getPreferredName(), user.getMetadata());
        if (user.getFullName() != null) {
            builder.field(AuthenticateResponse.FULL_NAME.getPreferredName(), user.getFullName());
        }
        if (user.getEmail() != null) {
            builder.field(AuthenticateResponse.EMAIL.getPreferredName(), user.getEmail());
        }
        builder.field(AuthenticateResponse.ENABLED.getPreferredName(), enabled);
        builder.startObject(AuthenticateResponse.AUTHENTICATION_REALM.getPreferredName());
        builder.field(AuthenticateResponse.REALM_NAME.getPreferredName(), authenticationRealm.getName());
        builder.field(AuthenticateResponse.REALM_TYPE.getPreferredName(), authenticationRealm.getType());
        builder.endObject();
        builder.startObject(AuthenticateResponse.LOOKUP_REALM.getPreferredName());
        builder.field(AuthenticateResponse.REALM_NAME.getPreferredName(), lookupRealm.getName());
        builder.field(AuthenticateResponse.REALM_TYPE.getPreferredName(), lookupRealm.getType());
        builder.endObject();
        builder.field(AuthenticateResponse.AUTHENTICATION_TYPE.getPreferredName(), authenticationType);
        if (token != null) {
            builder.field(AuthenticateResponse.TOKEN.getPreferredName(), token);
        }
        if (apikeyinfo != null) {
            builder.startObject(AuthenticateResponse.API_KEY_INFO.getPreferredName());
            builder.field(AuthenticateResponse.API_KEY_INFO_ID.getPreferredName(), apikeyinfo.getId());
            if (apikeyinfo.getName() != null) {
                builder.field(AuthenticateResponse.API_KEY_INFO_NAME.getPreferredName(), apikeyinfo.getName());
            }
            builder.endObject();
        }
        return builder.endObject();
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        AuthenticateResponse that = (AuthenticateResponse) o;
        return enabled == that.enabled
            && Objects.equals(user, that.user)
            && Objects.equals(authenticationRealm, that.authenticationRealm)
            && Objects.equals(lookupRealm, that.lookupRealm)
            && Objects.equals(authenticationType, that.authenticationType)
            && Objects.equals(token, that.token)
            && Objects.equals(apikeyinfo, that.apikeyinfo);
    }

    @Override
    public int hashCode() {
        return Objects.hash(user, enabled, authenticationRealm, lookupRealm, authenticationType, token, apikeyinfo);
    }

    public static AuthenticateResponse fromXContent(XContentParser parser) throws IOException {
        return PARSER.parse(parser, null);
    }

    public static class RealmInfo {
        private String name;
        private String type;

        RealmInfo(String name, String type) {
            this.name = name;
            this.type = type;
        }

        public String getName() {
            return name;
        }

        public String getType() {
            return type;
        }

        @Override
        public boolean equals(Object o) {
            if (this == o) return true;
            if (o == null || getClass() != o.getClass()) return false;
            RealmInfo realmInfo = (RealmInfo) o;
            return Objects.equals(name, realmInfo.name) && Objects.equals(type, realmInfo.type);
        }

        @Override
        public int hashCode() {
            return Objects.hash(name, type);
        }
    }

    public static class ApiKeyInfo { // authentication.api_key={"id":"abc123","name":"my-api-key"}
        private String id;
        private String name;

        ApiKeyInfo(String id, String name) {
            this.id = id;
            this.name = name;
        }

        public String getId() {
            return id;
        }

        public String getName() {
            return name;
        }

        @Override
        public boolean equals(Object o) {
            if (this == o) return true;
            if (o == null || getClass() != o.getClass()) return false;
            final ApiKeyInfo that = (ApiKeyInfo) o;
            return Objects.equals(this.id, that.id) && Objects.equals(this.name, that.name);
        }

        @Override
        public int hashCode() {
            return Objects.hash(id, name);
        }
    }
}
