/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */
package org.elasticsearch.xpack.core.security.authc;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.elasticsearch.Assertions;
import org.elasticsearch.Version;
import org.elasticsearch.common.bytes.BytesReference;
import org.elasticsearch.common.io.stream.BytesStreamOutput;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.common.io.stream.Writeable;
import org.elasticsearch.common.util.concurrent.ThreadContext;
import org.elasticsearch.common.xcontent.XContentHelper;
import org.elasticsearch.core.Nullable;
import org.elasticsearch.xcontent.ConstructingObjectParser;
import org.elasticsearch.xcontent.ParseField;
import org.elasticsearch.xcontent.ToXContentObject;
import org.elasticsearch.xcontent.XContentBuilder;
import org.elasticsearch.xcontent.XContentType;
import org.elasticsearch.xpack.core.security.authc.esnative.NativeRealmSettings;
import org.elasticsearch.xpack.core.security.authc.file.FileRealmSettings;
import org.elasticsearch.xpack.core.security.authc.service.ServiceAccountSettings;
import org.elasticsearch.xpack.core.security.authc.support.AuthenticationContextSerializer;
import org.elasticsearch.xpack.core.security.user.AnonymousUser;
import org.elasticsearch.xpack.core.security.user.InternalUserSerializationHelper;
import org.elasticsearch.xpack.core.security.user.User;

import java.io.IOException;
import java.io.UncheckedIOException;
import java.util.Base64;
import java.util.Collections;
import java.util.EnumSet;
import java.util.HashMap;
import java.util.Locale;
import java.util.Map;
import java.util.Objects;

import static org.elasticsearch.xcontent.ConstructingObjectParser.constructorArg;
import static org.elasticsearch.xcontent.ConstructingObjectParser.optionalConstructorArg;
import static org.elasticsearch.xpack.core.security.authc.Authentication.RealmRef.newAnonymousRealmRef;
import static org.elasticsearch.xpack.core.security.authc.Authentication.RealmRef.newApiKeyRealmRef;
import static org.elasticsearch.xpack.core.security.authc.Authentication.RealmRef.newInternalAttachRealmRef;
import static org.elasticsearch.xpack.core.security.authc.Authentication.RealmRef.newInternalFallbackRealmRef;
import static org.elasticsearch.xpack.core.security.authc.Authentication.RealmRef.newServiceAccountRealmRef;
import static org.elasticsearch.xpack.core.security.authc.AuthenticationField.ANONYMOUS_REALM_NAME;
import static org.elasticsearch.xpack.core.security.authc.AuthenticationField.ANONYMOUS_REALM_TYPE;
import static org.elasticsearch.xpack.core.security.authc.AuthenticationField.ATTACH_REALM_NAME;
import static org.elasticsearch.xpack.core.security.authc.AuthenticationField.ATTACH_REALM_TYPE;
import static org.elasticsearch.xpack.core.security.authc.AuthenticationField.FALLBACK_REALM_NAME;
import static org.elasticsearch.xpack.core.security.authc.AuthenticationField.FALLBACK_REALM_TYPE;
import static org.elasticsearch.xpack.core.security.authc.RealmDomain.REALM_DOMAIN_PARSER;

// TODO(hub-cap) Clean this up after moving User over - This class can re-inherit its field AUTHENTICATION_KEY in AuthenticationField.
// That interface can be removed
public class Authentication implements ToXContentObject {

    private static final Logger logger = LogManager.getLogger(Authentication.class);

    public static final Version VERSION_API_KEY_ROLES_AS_BYTES = Version.V_7_9_0;
    public static final Version VERSION_REALM_DOMAINS = Version.V_8_2_0;

    private final User user;
    private final RealmRef authenticatedBy;
    private final RealmRef lookedUpBy;
    private final Version version;
    private final AuthenticationType type;
    private final Map<String, Object> metadata; // authentication contains metadata, includes api_key details (including api_key metadata)

    public Authentication(User user, RealmRef authenticatedBy, RealmRef lookedUpBy) {
        this(user, authenticatedBy, lookedUpBy, Version.CURRENT, AuthenticationType.REALM, Collections.emptyMap());
    }

    public Authentication(
        User user,
        RealmRef authenticatedBy,
        RealmRef lookedUpBy,
        Version version,
        AuthenticationType type,
        Map<String, Object> metadata
    ) {
        this.user = Objects.requireNonNull(user);
        this.authenticatedBy = Objects.requireNonNull(authenticatedBy);
        this.lookedUpBy = lookedUpBy;
        this.version = version;
        this.type = type;
        this.metadata = metadata;
        this.assertApiKeyMetadata();
        this.assertDomainAssignment();
    }

    public Authentication(StreamInput in) throws IOException {
        this.user = InternalUserSerializationHelper.readFrom(in);
        this.authenticatedBy = new RealmRef(in);
        if (in.readBoolean()) {
            this.lookedUpBy = new RealmRef(in);
        } else {
            this.lookedUpBy = null;
        }
        this.version = in.getVersion();
        type = AuthenticationType.values()[in.readVInt()];
        metadata = in.readMap();
        this.assertApiKeyMetadata();
        this.assertDomainAssignment();
    }

    public User getUser() {
        return user;
    }

    public RealmRef getAuthenticatedBy() {
        return authenticatedBy;
    }

    public RealmRef getLookedUpBy() {
        return lookedUpBy;
    }

    /**
     * Get the realm where the effective user comes from.
     * The effective user is the es-security-runas-user if present or the authenticated user.
     */
    public RealmRef getSourceRealm() {
        return lookedUpBy == null ? authenticatedBy : lookedUpBy;
    }

    /**
     * Returns the authentication version.
     * Nodes can only interpret authentications from current or older versions as the node's.
     *
     * Authentication is serialized and travels across the cluster nodes as the sub-requests are handled,
     * and can also be cached by long-running jobs that continue to act on behalf of the user, beyond
     * the lifetime of the original request.
     */
    public Version getVersion() {
        return version;
    }

    public AuthenticationType getAuthenticationType() {
        return type;
    }

    public Map<String, Object> getMetadata() {
        return metadata;
    }

    /**
     * Returns a new {@code Authentication}, like this one, but which is compatible with older version nodes.
     * This is commonly employed when the {@code Authentication} is serialized across cluster nodes with mixed versions.
     */
    public Authentication maybeRewriteForOlderVersion(Version olderVersion) {
        // TODO how can this not be true
        // assert olderVersion.onOrBefore(getVersion());
        Authentication newAuthentication = new Authentication(
            getUser(),
            maybeRewriteRealmRef(olderVersion, getAuthenticatedBy()),
            maybeRewriteRealmRef(olderVersion, getLookedUpBy()),
            olderVersion,
            getAuthenticationType(),
            maybeRewriteMetadataForApiKeyRoleDescriptors(olderVersion, this)
        );
        if (isAssignedToDomain() && false == newAuthentication.isAssignedToDomain()) {
            logger.info("Rewriting authentication [" + this + "] without domain");
        }
        return newAuthentication;
    }

    /**
     * Returns a new {@code Authentication} that reflects a "run as another user" action under the current {@code Authentication}.
     * The security {@code RealmRef#Domain} of the resulting {@code Authentication} is that of the run-as user's realm.
     */
    public Authentication runAs(User runAs, @Nullable RealmRef lookupRealmRef) {
        Objects.requireNonNull(runAs);
        assert false == runAs.isRunAs();
        assert false == getUser().isRunAs();
        assert AuthenticationType.REALM == getAuthenticationType() || AuthenticationType.API_KEY == getAuthenticationType();
        return new Authentication(
            new User(runAs, getUser()),
            getAuthenticatedBy(),
            lookupRealmRef,
            getVersion(),
            getAuthenticationType(),
            getMetadata()
        );
    }

    /** Returns a new {@code Authentication} for tokens created by the current {@code Authentication}, which is used when
     * authenticating using the token credential.
     */
    public Authentication token() {
        assert false == isServiceAccount();
        final Authentication newTokenAuthentication = new Authentication(
            getUser(),
            getAuthenticatedBy(),
            getLookedUpBy(),
            getVersion(),
            AuthenticationType.TOKEN,
            getMetadata()
        );
        assert Objects.equals(getDomain(), newTokenAuthentication.getDomain());
        return newTokenAuthentication;
    }

    /**
     * Returns {@code true} if the effective user belongs to a realm under a domain.
     * See also {@link #getDomain()} and {@link #getSourceRealm()}.
     */
    public boolean isAssignedToDomain() {
        return getDomain() != null;
    }

    /**
     * Returns the {@link RealmDomain} that the effective user belongs to.
     * A user belongs to a realm which in turn belongs to a domain.
     *
     * The same username can be authenticated by different realms (e.g. with different credential types),
     * but resources created across realms cannot be accessed unless the realms are also part of the same domain.
     */
    public @Nullable RealmDomain getDomain() {
        return getSourceRealm().getDomain();
    }

    public boolean isAuthenticatedWithServiceAccount() {
        return ServiceAccountSettings.REALM_TYPE.equals(getAuthenticatedBy().getType());
    }

    /**
     * Whether the authenticating user is an API key, including a simple API key or a token created by an API key.
     * @return
     */
    public boolean isAuthenticatedAsApiKey() {
        final boolean result = AuthenticationField.API_KEY_REALM_TYPE.equals(getAuthenticatedBy().getType());
        assert false == result || AuthenticationField.API_KEY_REALM_NAME.equals(getAuthenticatedBy().getName());
        return result;
    }

    public boolean isAuthenticatedAnonymously() {
        return AuthenticationType.ANONYMOUS.equals(getAuthenticationType());
    }

    public boolean isAuthenticatedInternally() {
        return AuthenticationType.INTERNAL.equals(getAuthenticationType());
    }

    /**
     * Authenticate with a service account and no run-as
     */
    public boolean isServiceAccount() {
        final boolean result = ServiceAccountSettings.REALM_TYPE.equals(getSourceRealm().getType());
        assert false == result || ServiceAccountSettings.REALM_NAME.equals(getSourceRealm().getName())
            : "service account realm name mismatch";
        return result;
    }

    /**
     * Whether the effective user is an API key, this including a simple API key authentication
     * or a token created by the API key.
     */
    public boolean isApiKey() {
        final boolean result = AuthenticationField.API_KEY_REALM_TYPE.equals(getSourceRealm().getType());
        assert false == result || AuthenticationField.API_KEY_REALM_NAME.equals(getSourceRealm().getName()) : "api key realm name mismatch";
        return result;
    }

    /**
     * Writes the authentication to the context. There must not be an existing authentication in the context and if there is an
     * {@link IllegalStateException} will be thrown
     */
    public void writeToContext(ThreadContext ctx) throws IOException, IllegalArgumentException {
        new AuthenticationContextSerializer().writeToContext(this, ctx);
    }

    public String encode() throws IOException {
        BytesStreamOutput output = new BytesStreamOutput();
        output.setVersion(version);
        Version.writeVersion(version, output);
        writeTo(output);
        return Base64.getEncoder().encodeToString(BytesReference.toBytes(output.bytes()));
    }

    public void writeTo(StreamOutput out) throws IOException {
        InternalUserSerializationHelper.writeTo(user, out);
        authenticatedBy.writeTo(out);
        if (lookedUpBy != null) {
            out.writeBoolean(true);
            lookedUpBy.writeTo(out);
        } else {
            out.writeBoolean(false);
        }
        out.writeVInt(type.ordinal());
        out.writeGenericMap(metadata);
    }

    /**
     * Checks whether the current authentication, which can be for a user or for an API Key, can access the resources
     * (e.g. search scrolls and async search results) created (owned) by the passed in authentication.
     *
     * The rules are as follows:
     *   * a resource created by an API Key can only be accessed by the exact same key; the creator user, its tokens,
     *   or any of its other keys cannot access it.
     *   * a resource created by a user authenticated by a realm, or any of its tokens, can be accessed by the same
     *   username authenticated by the same realm or by other realms from the same security domain (at the time of the
     *   access), or any of its tokens; realms are considered the same if they have the same type and name (except for
     *   file and native realms, for which only the type is considered, the name is irrelevant), see also
     *      <a href="https://www.elastic.co/guide/en/elasticsearch/reference/master/security-limitations.html">
     *      security limitations</a>
     */
    public boolean canAccessResourcesOf(Authentication resourceCreatorAuthentication) {
        // if we introduce new authentication types in the future, it is likely that we'll need to revisit this method
        assert EnumSet.of(
            Authentication.AuthenticationType.REALM,
            Authentication.AuthenticationType.API_KEY,
            Authentication.AuthenticationType.TOKEN,
            Authentication.AuthenticationType.ANONYMOUS,
            Authentication.AuthenticationType.INTERNAL
        ).containsAll(EnumSet.of(getAuthenticationType(), resourceCreatorAuthentication.getAuthenticationType()))
            : "cross AuthenticationType comparison for canAccessResourcesOf is not applicable for: "
                + EnumSet.of(getAuthenticationType(), resourceCreatorAuthentication.getAuthenticationType());
        final AuthenticationContext myAuthContext = AuthenticationContext.fromAuthentication(this);
        final AuthenticationContext creatorAuthContext = AuthenticationContext.fromAuthentication(resourceCreatorAuthentication);
        final Subject mySubject = myAuthContext.getEffectiveSubject();
        final Subject creatorSubject = creatorAuthContext.getEffectiveSubject();
        return mySubject.canAccessResourcesOf(creatorSubject);
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        Authentication that = (Authentication) o;
        return user.equals(that.user)
            && authenticatedBy.equals(that.authenticatedBy)
            && Objects.equals(lookedUpBy, that.lookedUpBy)
            && version.equals(that.version)
            && type == that.type
            && metadata.equals(that.metadata);
    }

    @Override
    public int hashCode() {
        return Objects.hash(user, authenticatedBy, lookedUpBy, version, type, metadata);
    }

    @Override
    public XContentBuilder toXContent(XContentBuilder builder, Params params) throws IOException {
        builder.startObject();
        toXContentFragment(builder);
        return builder.endObject();
    }

    /**
     * Generates XContent without the start/end object.
     */
    public void toXContentFragment(XContentBuilder builder) throws IOException {
        builder.field(User.Fields.USERNAME.getPreferredName(), user.principal());
        builder.array(User.Fields.ROLES.getPreferredName(), user.roles());
        builder.field(User.Fields.FULL_NAME.getPreferredName(), user.fullName());
        builder.field(User.Fields.EMAIL.getPreferredName(), user.email());
        if (isServiceAccount()) {
            final String tokenName = (String) getMetadata().get(ServiceAccountSettings.TOKEN_NAME_FIELD);
            assert tokenName != null : "token name cannot be null";
            final String tokenSource = (String) getMetadata().get(ServiceAccountSettings.TOKEN_SOURCE_FIELD);
            assert tokenSource != null : "token source cannot be null";
            builder.field(
                User.Fields.TOKEN.getPreferredName(),
                Map.of("name", tokenName, "type", ServiceAccountSettings.REALM_TYPE + "_" + tokenSource)
            );
        }
        builder.field(User.Fields.METADATA.getPreferredName(), user.metadata());
        builder.field(User.Fields.ENABLED.getPreferredName(), user.enabled());
        builder.startObject(User.Fields.AUTHENTICATION_REALM.getPreferredName());
        builder.field(User.Fields.REALM_NAME.getPreferredName(), getAuthenticatedBy().getName());
        builder.field(User.Fields.REALM_TYPE.getPreferredName(), getAuthenticatedBy().getType());
        // domain name is generally ambiguous, because it can change during the lifetime of the authentication,
        // but it is good enough for display purposes (including auditing)
        if (getAuthenticatedBy().getDomain() != null) {
            builder.field(User.Fields.REALM_DOMAIN.getPreferredName(), getAuthenticatedBy().getDomain().name());
        }
        builder.endObject();
        builder.startObject(User.Fields.LOOKUP_REALM.getPreferredName());
        if (getLookedUpBy() != null) {
            builder.field(User.Fields.REALM_NAME.getPreferredName(), getLookedUpBy().getName());
            builder.field(User.Fields.REALM_TYPE.getPreferredName(), getLookedUpBy().getType());
            if (getLookedUpBy().getDomain() != null) {
                builder.field(User.Fields.REALM_DOMAIN.getPreferredName(), getLookedUpBy().getDomain().name());
            }
        } else {
            builder.field(User.Fields.REALM_NAME.getPreferredName(), getAuthenticatedBy().getName());
            builder.field(User.Fields.REALM_TYPE.getPreferredName(), getAuthenticatedBy().getType());
            if (getAuthenticatedBy().getDomain() != null) {
                builder.field(User.Fields.REALM_DOMAIN.getPreferredName(), getAuthenticatedBy().getDomain().name());
            }
        }
        builder.endObject();
        builder.field(User.Fields.AUTHENTICATION_TYPE.getPreferredName(), getAuthenticationType().name().toLowerCase(Locale.ROOT));
        if (isApiKey()) {
            this.assertApiKeyMetadata();
            final String apiKeyId = (String) this.metadata.get(AuthenticationField.API_KEY_ID_KEY);
            final String apiKeyName = (String) this.metadata.get(AuthenticationField.API_KEY_NAME_KEY);
            if (apiKeyName == null) {
                builder.field("api_key", Map.of("id", apiKeyId));
            } else {
                builder.field("api_key", Map.of("id", apiKeyId, "name", apiKeyName));
            }
        }
    }

    private void assertApiKeyMetadata() {
        assert (false == isAuthenticatedAsApiKey()) || (this.metadata.get(AuthenticationField.API_KEY_ID_KEY) != null)
            : "API KEY authentication requires metadata to contain API KEY id, and the value must be non-null.";
    }

    private void assertDomainAssignment() {
        if (Assertions.ENABLED) {
            if (isAssignedToDomain()) {
                assert false == isApiKey();
                assert false == isServiceAccount();
                assert false == isAuthenticatedAnonymously();
                assert false == isAuthenticatedInternally();
            }
        }
    }

    @Override
    public String toString() {
        StringBuilder builder = new StringBuilder("Authentication[").append(user)
            .append(",type=")
            .append(type)
            .append(",by=")
            .append(authenticatedBy);
        if (lookedUpBy != null) {
            builder.append(",lookup=").append(lookedUpBy);
        }
        builder.append("]");
        return builder.toString();
    }

    public static class RealmRef implements Writeable, ToXContentObject {

        private final String nodeName;
        private final String name;
        private final String type;
        private final @Nullable RealmDomain domain;

        public RealmRef(String name, String type, String nodeName) {
            this(name, type, nodeName, null);
        }

        public RealmRef(String name, String type, String nodeName, @Nullable RealmDomain domain) {
            this.nodeName = Objects.requireNonNull(nodeName, "node name cannot be null");
            this.name = Objects.requireNonNull(name, "realm name cannot be null");
            this.type = Objects.requireNonNull(type, "realm type cannot be null");
            this.domain = domain;
        }

        public RealmRef(StreamInput in) throws IOException {
            this.nodeName = in.readString();
            this.name = in.readString();
            this.type = in.readString();
            if (in.getVersion().onOrAfter(VERSION_REALM_DOMAINS)) {
                this.domain = in.readOptionalWriteable(RealmDomain::readFrom);
            } else {
                this.domain = null;
            }
        }

        @Override
        public void writeTo(StreamOutput out) throws IOException {
            out.writeString(nodeName);
            out.writeString(name);
            out.writeString(type);
            if (out.getVersion().onOrAfter(VERSION_REALM_DOMAINS)) {
                out.writeOptionalWriteable(domain);
            }
        }

        @Override
        public XContentBuilder toXContent(XContentBuilder builder, Params params) throws IOException {
            builder.startObject();
            {
                builder.field("name", name);
                builder.field("type", type);
                builder.field("node_name", nodeName);
                if (domain != null) {
                    builder.field("domain", domain);
                }
            }
            builder.endObject();
            return builder;
        }

        public String getNodeName() {
            return nodeName;
        }

        public String getName() {
            return name;
        }

        public String getType() {
            return type;
        }

        /**
         * Returns the domain assignment for the realm, if one assigned, or {@code null} otherwise, as per the
         * {@code RealmSettings#DOMAIN_TO_REALM_ASSOC_SETTING} setting.
         */
        public @Nullable RealmDomain getDomain() {
            return domain;
        }

        @Override
        public boolean equals(Object o) {
            if (this == o) return true;
            if (o == null || getClass() != o.getClass()) return false;

            RealmRef realmRef = (RealmRef) o;

            if (nodeName.equals(realmRef.nodeName) == false) return false;
            if (type.equals(realmRef.type) == false) return false;
            return Objects.equals(domain, realmRef.domain);
        }

        @Override
        public int hashCode() {
            int result = nodeName.hashCode();
            result = 31 * result + name.hashCode();
            result = 31 * result + type.hashCode();
            if (domain != null) {
                result = 31 * result + domain.hashCode();
            }
            return result;
        }

        @Override
        public String toString() {
            if (domain != null) {
                return "{Realm[" + type + "." + name + "] under Domain[" + domain.name() + "] on Node[" + nodeName + "]}";
            } else {
                return "{Realm[" + type + "." + name + "] on Node[" + nodeName + "]}";
            }
        }

        static RealmRef newInternalAttachRealmRef(String nodeName) {
            // the "attach" internal realm is not part of any realm domain
            return new Authentication.RealmRef(ATTACH_REALM_NAME, ATTACH_REALM_TYPE, nodeName, null);
        }

        static RealmRef newInternalFallbackRealmRef(String nodeName) {
            // the "fallback" internal realm is not part of any realm domain
            RealmRef realmRef = new RealmRef(FALLBACK_REALM_NAME, FALLBACK_REALM_TYPE, nodeName, null);
            return realmRef;
        }

        static RealmRef newAnonymousRealmRef(String nodeName) {
            // the "anonymous" internal realm is not part of any realm domain
            return new Authentication.RealmRef(ANONYMOUS_REALM_NAME, ANONYMOUS_REALM_TYPE, nodeName, null);
        }

        static RealmRef newServiceAccountRealmRef(String nodeName) {
            // no domain for service account tokens
            return new Authentication.RealmRef(ServiceAccountSettings.REALM_NAME, ServiceAccountSettings.REALM_TYPE, nodeName, null);
        }

        static RealmRef newApiKeyRealmRef(String nodeName) {
            // no domain for API Key tokens
            return new RealmRef(AuthenticationField.API_KEY_REALM_NAME, AuthenticationField.API_KEY_REALM_TYPE, nodeName, null);
        }
    }

    public static boolean isFileOrNativeRealm(String realmType) {
        return FileRealmSettings.TYPE.equals(realmType) || NativeRealmSettings.TYPE.equals(realmType);
    }

    public static ConstructingObjectParser<RealmRef, Void> REALM_REF_PARSER = new ConstructingObjectParser<>(
        "realm_ref",
        false,
        (args, v) -> new RealmRef((String) args[0], (String) args[1], (String) args[2], (RealmDomain) args[3])
    );

    static {
        REALM_REF_PARSER.declareString(constructorArg(), new ParseField("name"));
        REALM_REF_PARSER.declareString(constructorArg(), new ParseField("type"));
        REALM_REF_PARSER.declareString(constructorArg(), new ParseField("node_name"));
        REALM_REF_PARSER.declareObject(optionalConstructorArg(), (p, c) -> REALM_DOMAIN_PARSER.parse(p, c), new ParseField("domain"));
    }

    // TODO is a newer version than the node's a valid value?
    public static Authentication newInternalAuthentication(User internalUser, Version version, String nodeName) {
        // TODO create a system user class, so that the type system guarantees that this is only invoked for internal users
        assert User.isInternal(internalUser);
        final Authentication.RealmRef authenticatedBy = newInternalAttachRealmRef(nodeName);
        Authentication authentication = new Authentication(
            internalUser,
            authenticatedBy,
            null,
            version,
            AuthenticationType.INTERNAL,
            Collections.emptyMap()
        );
        assert false == authentication.isAssignedToDomain();
        return authentication;
    }

    public static Authentication newInternalFallbackAuthentication(User fallbackUser, String nodeName) {
        // TODO assert SystemUser.is(fallbackUser);
        final Authentication.RealmRef authenticatedBy = newInternalFallbackRealmRef(nodeName);
        Authentication authentication = new Authentication(
            fallbackUser,
            authenticatedBy,
            null,
            Version.CURRENT,
            Authentication.AuthenticationType.INTERNAL,
            Collections.emptyMap()
        );
        assert false == authentication.isAssignedToDomain();
        return authentication;
    }

    public static Authentication newAnonymousAuthentication(AnonymousUser anonymousUser, String nodeName) {
        final Authentication.RealmRef authenticatedBy = newAnonymousRealmRef(nodeName);
        Authentication authentication = new Authentication(
            anonymousUser,
            authenticatedBy,
            null,
            Version.CURRENT,
            Authentication.AuthenticationType.ANONYMOUS,
            Collections.emptyMap()
        );
        assert false == authentication.isAssignedToDomain();
        return authentication;
    }

    public static Authentication newServiceAccountAuthentication(User serviceAccountUser, String nodeName, Map<String, Object> metadata) {
        // TODO make the service account user a separate class/interface
        assert false == serviceAccountUser.isRunAs();
        final Authentication.RealmRef authenticatedBy = newServiceAccountRealmRef(nodeName);
        Authentication authentication = new Authentication(
            serviceAccountUser,
            authenticatedBy,
            null,
            Version.CURRENT,
            AuthenticationType.TOKEN,
            metadata
        );
        assert false == authentication.isAssignedToDomain();
        return authentication;
    }

    public static Authentication newRealmAuthentication(User user, RealmRef realmRef) {
        // TODO make the type system ensure that this is not a run-as user
        assert false == user.isRunAs();
        Authentication authentication = new Authentication(user, realmRef, null, Version.CURRENT, AuthenticationType.REALM, Map.of());
        assert false == authentication.isServiceAccount();
        assert false == authentication.isApiKey();
        assert false == authentication.isAuthenticatedInternally();
        assert false == authentication.isAuthenticatedAnonymously();
        return authentication;
    }

    public static Authentication newApiKeyAuthentication(AuthenticationResult<User> authResult, String nodeName) {
        assert authResult.isAuthenticated() : "API Key authn result must be successful";
        final User apiKeyUser = authResult.getValue();
        assert false == apiKeyUser.isRunAs();
        final Authentication.RealmRef authenticatedBy = newApiKeyRealmRef(nodeName);
        Authentication authentication = new Authentication(
            apiKeyUser,
            authenticatedBy,
            null,
            Version.CURRENT,
            AuthenticationType.API_KEY,
            authResult.getMetadata()
        );
        assert false == authentication.isAssignedToDomain();
        return authentication;
    }

    private static RealmRef maybeRewriteRealmRef(Version streamVersion, RealmRef realmRef) {
        if (realmRef != null && realmRef.getDomain() != null && streamVersion.before(VERSION_REALM_DOMAINS)) {
            // security domain erasure
            new RealmRef(realmRef.getName(), realmRef.getType(), realmRef.getNodeName(), null);
        }
        return realmRef;
    }

    @SuppressWarnings("unchecked")
    private static Map<String, Object> maybeRewriteMetadataForApiKeyRoleDescriptors(Version streamVersion, Authentication authentication) {
        Map<String, Object> metadata = authentication.getMetadata();
        // If authentication user is an API key or a token created by an API key,
        // regardless whether it has run-as, the metadata must contain API key role descriptors
        if (authentication.isAuthenticatedAsApiKey()) {
            assert metadata.containsKey(AuthenticationField.API_KEY_ROLE_DESCRIPTORS_KEY)
                : "metadata must contain role descriptor for API key authentication";
            assert metadata.containsKey(AuthenticationField.API_KEY_LIMITED_ROLE_DESCRIPTORS_KEY)
                : "metadata must contain limited role descriptor for API key authentication";
            if (authentication.getVersion().onOrAfter(VERSION_API_KEY_ROLES_AS_BYTES)
                && streamVersion.before(VERSION_API_KEY_ROLES_AS_BYTES)) {
                metadata = new HashMap<>(metadata);
                metadata.put(
                    AuthenticationField.API_KEY_ROLE_DESCRIPTORS_KEY,
                    convertRoleDescriptorsBytesToMap((BytesReference) metadata.get(AuthenticationField.API_KEY_ROLE_DESCRIPTORS_KEY))
                );
                metadata.put(
                    AuthenticationField.API_KEY_LIMITED_ROLE_DESCRIPTORS_KEY,
                    convertRoleDescriptorsBytesToMap(
                        (BytesReference) metadata.get(AuthenticationField.API_KEY_LIMITED_ROLE_DESCRIPTORS_KEY)
                    )
                );
            } else if (authentication.getVersion().before(VERSION_API_KEY_ROLES_AS_BYTES)
                && streamVersion.onOrAfter(VERSION_API_KEY_ROLES_AS_BYTES)) {
                    metadata = new HashMap<>(metadata);
                    metadata.put(
                        AuthenticationField.API_KEY_ROLE_DESCRIPTORS_KEY,
                        convertRoleDescriptorsMapToBytes(
                            (Map<String, Object>) metadata.get(AuthenticationField.API_KEY_ROLE_DESCRIPTORS_KEY)
                        )
                    );
                    metadata.put(
                        AuthenticationField.API_KEY_LIMITED_ROLE_DESCRIPTORS_KEY,
                        convertRoleDescriptorsMapToBytes(
                            (Map<String, Object>) metadata.get(AuthenticationField.API_KEY_LIMITED_ROLE_DESCRIPTORS_KEY)
                        )
                    );
                }
        }
        return metadata;
    }

    private static Map<String, Object> convertRoleDescriptorsBytesToMap(BytesReference roleDescriptorsBytes) {
        return XContentHelper.convertToMap(roleDescriptorsBytes, false, XContentType.JSON).v2();
    }

    private static BytesReference convertRoleDescriptorsMapToBytes(Map<String, Object> roleDescriptorsMap) {
        try (XContentBuilder builder = XContentBuilder.builder(XContentType.JSON.xContent())) {
            builder.map(roleDescriptorsMap);
            return BytesReference.bytes(builder);
        } catch (IOException e) {
            throw new UncheckedIOException(e);
        }
    }

    static boolean equivalentRealms(String name1, String type1, String name2, String type2) {
        if (false == type1.equals(type2)) {
            return false;
        }
        if (isFileOrNativeRealm(type1)) {
            // file and native realms can be renamed, but they always point to the same set of users
            return true;
        } else {
            // if other realms are renamed, it is an indication that they point to a different user set
            return name1.equals(name2);
        }
    }

    public enum AuthenticationType {
        REALM,
        API_KEY,
        TOKEN,
        ANONYMOUS,
        INTERNAL
    }
}
