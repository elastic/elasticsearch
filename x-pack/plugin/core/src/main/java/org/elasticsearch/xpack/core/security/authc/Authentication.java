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
import org.elasticsearch.common.util.ArrayUtils;
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
import org.elasticsearch.xpack.core.security.user.AsyncSearchUser;
import org.elasticsearch.xpack.core.security.user.SecurityProfileUser;
import org.elasticsearch.xpack.core.security.user.SystemUser;
import org.elasticsearch.xpack.core.security.user.User;
import org.elasticsearch.xpack.core.security.user.XPackSecurityUser;
import org.elasticsearch.xpack.core.security.user.XPackUser;

import java.io.IOException;
import java.io.UncheckedIOException;
import java.util.Base64;
import java.util.EnumSet;
import java.util.HashMap;
import java.util.List;
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
import static org.elasticsearch.xpack.core.security.authc.AuthenticationField.API_KEY_REALM_NAME;
import static org.elasticsearch.xpack.core.security.authc.AuthenticationField.API_KEY_REALM_TYPE;
import static org.elasticsearch.xpack.core.security.authc.AuthenticationField.ATTACH_REALM_NAME;
import static org.elasticsearch.xpack.core.security.authc.AuthenticationField.ATTACH_REALM_TYPE;
import static org.elasticsearch.xpack.core.security.authc.AuthenticationField.FALLBACK_REALM_NAME;
import static org.elasticsearch.xpack.core.security.authc.AuthenticationField.FALLBACK_REALM_TYPE;
import static org.elasticsearch.xpack.core.security.authc.RealmDomain.REALM_DOMAIN_PARSER;

public final class Authentication implements ToXContentObject {

    private static final Logger logger = LogManager.getLogger(Authentication.class);

    public static final Version VERSION_API_KEY_ROLES_AS_BYTES = Version.V_7_9_0;
    public static final Version VERSION_REALM_DOMAINS = Version.V_8_2_0;
    private final AuthenticationType type;
    private final Subject authenticatingSubject;
    private final Subject effectiveSubject;

    private Authentication(Subject subject, AuthenticationType type) {
        this(subject, subject, type);
    }

    private Authentication(Subject effectiveSubject, Subject authenticatingSubject, AuthenticationType type) {
        this.effectiveSubject = effectiveSubject;
        this.authenticatingSubject = authenticatingSubject;
        this.type = type;
        assertInternalConsistency();
    }

    public Authentication(StreamInput in) throws IOException {
        // Read the user(s)
        final User outerUser = AuthenticationSerializationHelper.readUserWithoutTrailingBoolean(in);
        final boolean hasInnerUser;
        if (User.isInternal(outerUser)) {
            hasInnerUser = false;
        } else {
            hasInnerUser = in.readBoolean();
        }
        final User innerUser;
        if (hasInnerUser) {
            innerUser = AuthenticationSerializationHelper.readUserFrom(in);
            assert false == User.isInternal(innerUser) : "internal users cannot participate in run-as";
        } else {
            innerUser = null;
        }

        final RealmRef authenticatedBy = new RealmRef(in);
        final RealmRef lookedUpBy;
        if (in.readBoolean()) {
            lookedUpBy = new RealmRef(in);
        } else {
            lookedUpBy = null;
        }

        // The valid combinations for innerUser and lookedUpBy are:
        // 1. InnerUser == null -> no run-as -> lookedUpBy must be null as well
        // 2. innerUser != null -> lookedUp by can be either null (failed run-as lookup) or non-null (successful lookup)
        // 3. lookedUpBy == null -> innerUser can be either null (no run-as) or non-null (failed run-as lookup)
        // 4. lookedUpBy != null -> successful run-as -> innerUser must be NOT null
        assert innerUser != null || lookedUpBy == null : "Authentication has no inner-user, but looked-up-by is [" + lookedUpBy + "]";

        final Version version = in.getVersion();
        type = AuthenticationType.values()[in.readVInt()];
        final Map<String, Object> metadata = in.readMap();
        if (innerUser != null) {
            authenticatingSubject = new Subject(innerUser, authenticatedBy, version, metadata);
            // The lookup user for run-as currently doesn't have authentication metadata associated with them because
            // lookupUser only returns the User object. The lookup user for authorization delegation does have
            // authentication metadata, but the realm does not expose this difference between authenticatingUser and
            // delegateUser so effectively this is handled together with the authenticatingSubject not effectiveSubject.
            effectiveSubject = new Subject(outerUser, lookedUpBy, version, Map.of());
        } else {
            authenticatingSubject = effectiveSubject = new Subject(outerUser, authenticatedBy, version, metadata);
        }
        assertInternalConsistency();
    }

    /**
     * Get the {@link Subject} that performs the actual authentication. This normally means it provides a credentials.
     */
    public Subject getAuthenticatingSubject() {
        return authenticatingSubject;
    }

    /**
     * Get the {@link Subject} that the authentication effectively represents. It may not be the authenticating subject
     * because the authentication subject can run-as another subject.
     */
    public Subject getEffectiveSubject() {
        return effectiveSubject;
    }

    public AuthenticationType getAuthenticationType() {
        return type;
    }

    /**
     * Whether the authentication contains a subject run-as another subject. That is, the authentication subject
     * is different from the effective subject.
     */
    public boolean isRunAs() {
        return authenticatingSubject != effectiveSubject;
    }

    /**
     * Use {@code getEffectiveSubject().getUser()} instead.
     */
    @Deprecated
    public User getUser() {
        return effectiveSubject.getUser();
    }

    /**
     * Use {@code getAuthenticatingSubject().getRealm()} instead.
     */
    @Deprecated
    public RealmRef getAuthenticatedBy() {
        return authenticatingSubject.getRealm();
    }

    /**
     * The use case for this method is largely trying to tell whether there is a run-as user
     * and can be replaced by {@code isRunAs}
     */
    @Deprecated
    public RealmRef getLookedUpBy() {
        if (isRunAs()) {
            return effectiveSubject.getRealm();
        } else {
            // retain the behaviour of returning null for lookup realm for if the authentication is not run-as
            return null;
        }
    }

    /**
     * Get the realm where the effective user comes from.
     * The effective user is the es-security-runas-user if present or the authenticated user.
     *
     * Use {@code getEffectiveSubject().getRealm()} instead.
     */
    @Deprecated
    public RealmRef getSourceRealm() {
        // TODO: This code retains the existing behaviour which is slightly wrong because
        // when run-as lookup fails, the effectiveSubject will have a null realm. In this
        // case, the code returns the authenticatingSubject's realm. This is wrong in theory
        // because it is not the intention of this method. In practice, it does not matter
        // because failed lookup will be rejected at authZ time. But fixing it causes test
        // failures. So leave it for now.
        final RealmRef sourceRealm = effectiveSubject.getRealm();
        return sourceRealm == null ? authenticatingSubject.getRealm() : sourceRealm;
    }

    /**
     * Returns the authentication version.
     * Nodes can only interpret authentications from current or older versions as the node's.
     *
     * Authentication is serialized and travels across the cluster nodes as the sub-requests are handled,
     * and can also be cached by long-running jobs that continue to act on behalf of the user, beyond
     * the lifetime of the original request.
     *
     * Use {@code getEffectiveSubject().getVersion()} instead.
     */
    @Deprecated
    public Version getVersion() {
        return effectiveSubject.getVersion();
    }

    /**
     * Use {@code getAuthenticatingSubject().getMetadata()} instead.
     */
    @Deprecated
    public Map<String, Object> getMetadata() {
        return authenticatingSubject.getMetadata();
    }

    /**
     * Returns a new {@code Authentication}, like this one, but which is compatible with older version nodes.
     * This is commonly employed when the {@code Authentication} is serialized across cluster nodes with mixed versions.
     */
    public Authentication maybeRewriteForOlderVersion(Version olderVersion) {
        // TODO how can this not be true
        // assert olderVersion.onOrBefore(getVersion());

        final Map<String, Object> newMetadata = maybeRewriteMetadataForApiKeyRoleDescriptors(olderVersion, this);

        final Authentication newAuthentication;
        if (isRunAs()) {
            // The lookup user for run-as currently doesn't have authentication metadata associated with them because
            // lookupUser only returns the User object. The lookup user for authorization delegation does have
            // authentication metadata, but the realm does not expose this difference between authenticatingUser and
            // delegateUser so effectively this is handled together with the authenticatingSubject not effectiveSubject.
            newAuthentication = new Authentication(
                new Subject(
                    effectiveSubject.getUser(),
                    maybeRewriteRealmRef(olderVersion, effectiveSubject.getRealm()),
                    olderVersion,
                    effectiveSubject.getMetadata()
                ),
                new Subject(
                    authenticatingSubject.getUser(),
                    maybeRewriteRealmRef(olderVersion, authenticatingSubject.getRealm()),
                    olderVersion,
                    newMetadata
                ),
                type
            );
        } else {
            newAuthentication = new Authentication(
                new Subject(
                    authenticatingSubject.getUser(),
                    maybeRewriteRealmRef(olderVersion, authenticatingSubject.getRealm()),
                    olderVersion,
                    newMetadata
                ),
                type
            );

        }
        if (isAssignedToDomain() && false == newAuthentication.isAssignedToDomain()) {
            logger.info("Rewriting authentication [" + this + "] without domain");
        }
        return newAuthentication;
    }

    /**
     * Returns a new {@code Authentication} that reflects a "run as another user" action under the current {@code Authentication}.
     * The security {@code RealmRef#Domain} of the resulting {@code Authentication} is that of the run-as user's realm.
     *
     * @param runAs The user to be impersonated
     * @param lookupRealmRef The realm where the impersonated user is looked up from. It can be null if the user does
     *                       not exist. The null lookup realm is used to indicate the lookup failure which will be rejected
     *                       at authorization time.
     */
    public Authentication runAs(User runAs, @Nullable RealmRef lookupRealmRef) {
        assert supportsRunAs(null);
        assert false == runAs instanceof AnonymousUser;
        assert false == hasSyntheticRealmNameOrType(lookupRealmRef) : "should not use synthetic realm name/type for lookup realms";

        Objects.requireNonNull(runAs);
        return new Authentication(new Subject(runAs, lookupRealmRef, getVersion(), Map.of()), authenticatingSubject, type);
    }

    /** Returns a new {@code Authentication} for tokens created by the current {@code Authentication}, which is used when
     * authenticating using the token credential.
     */
    public Authentication token() {
        assert false == isServiceAccount();
        final Authentication newTokenAuthentication = new Authentication(effectiveSubject, authenticatingSubject, AuthenticationType.TOKEN);
        assert Objects.equals(getDomain(), newTokenAuthentication.getDomain());
        return newTokenAuthentication;
    }

    /**
      * The final list of roles a user has should include all roles granted to the anonymous user when
      *  1. Anonymous access is enable
      *  2. The user itself is not the anonymous user
      *  3. The authentication is not an API key or service account
      *
      *  Depending on whether the above criteria is satisfied, the method may either return a new
      *  authentication object incorporating anonymous roles or the same authentication object (if anonymous
      *  roles are not applicable)
      *
      *  NOTE this method is an artifact of how anonymous roles are resolved today on each node as opposed to
      *  just on the coordinating node. Whether this behaviour should be changed is an ongoing discussion.
      *  Therefore, using this method in more places other than its current usage requires careful consideration.
      */
    public Authentication maybeAddAnonymousRoles(@Nullable AnonymousUser anonymousUser) {
        final boolean shouldAddAnonymousRoleNames = anonymousUser != null
            && anonymousUser.enabled()
            && false == anonymousUser.equals(getUser())
            && false == User.isInternal(getUser())
            && false == isApiKey()
            && false == isServiceAccount();

        if (false == shouldAddAnonymousRoleNames) {
            return this;
        }

        // TODO: should we validate enable status and length of role names on instantiation time of anonymousUser?
        if (anonymousUser.roles().length == 0) {
            throw new IllegalStateException("anonymous is only enabled when the anonymous user has roles");
        }
        final String[] allRoleNames = ArrayUtils.concat(getUser().roles(), anonymousUser.roles());

        if (isRunAs()) {
            final User user = effectiveSubject.getUser();
            return new Authentication(
                new Subject(
                    new User(user.principal(), allRoleNames, user.fullName(), user.email(), user.metadata(), user.enabled()),
                    effectiveSubject.getRealm(),
                    effectiveSubject.getVersion(),
                    effectiveSubject.getMetadata()
                ),
                authenticatingSubject,
                type
            );

        } else {
            final User user = authenticatingSubject.getUser();
            return new Authentication(
                new Subject(
                    new User(user.principal(), allRoleNames, user.fullName(), user.email(), user.metadata(), user.enabled()),
                    authenticatingSubject.getRealm(),
                    authenticatingSubject.getVersion(),
                    authenticatingSubject.getMetadata()
                ),
                type
            );
        }
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
     */
    public boolean isAuthenticatedAsApiKey() {
        return authenticatingSubject.getType() == Subject.Type.API_KEY;
    }

    // TODO: this is not entirely accurate if anonymous user can create a token
    private boolean isAuthenticatedAnonymously() {
        return AuthenticationType.ANONYMOUS.equals(getAuthenticationType());
    }

    private boolean isAuthenticatedInternally() {
        return AuthenticationType.INTERNAL.equals(getAuthenticationType());
    }

    /**
     * Authenticate with a service account and no run-as
     */
    public boolean isServiceAccount() {
        return effectiveSubject.getType() == Subject.Type.SERVICE_ACCOUNT;
    }

    /**
     * Whether the effective user is an API key, this including a simple API key authentication
     * or a token created by the API key.
     */
    public boolean isApiKey() {
        return effectiveSubject.getType() == Subject.Type.API_KEY;
    }

    /**
     * Whether the authentication can run-as another user
     */
    public boolean supportsRunAs(@Nullable AnonymousUser anonymousUser) {
        // Chained run-as not allowed
        if (isRunAs()) {
            return false;
        }

        // We may allow service account to run-as in the future, but for now no service account requires it
        if (isServiceAccount()) {
            return false;
        }

        // There is no reason for internal users to run-as. This check prevents either internal user itself
        // or a token created for it (though no such thing in current code) to run-as.
        if (User.isInternal(getUser())) {
            return false;
        }

        // Anonymous user or its token cannot run-as
        // There is no perfect way to determine an anonymous user if we take custom realms into consideration
        // 1. A custom realm can return a user object that can pass `equals(anonymousUser)` check
        // (this is the existing check used elsewhere)
        // 2. A custom realm can declare its type and name to be __anonymous
        //
        // This problem is at least partly due to we don't have special serialisation for the AnonymousUser class.
        // As a result, it is serialised just as a normal user. At deserializing time, it is impossible to reliably
        // tell the difference. This is what happens when AnonymousUser creates a token.
        // Also, if anonymous access is disabled or anonymous username, roles are changed after the token is created.
        // Should we still consider the token being created by an anonymous user which is now different from the new
        // anonymous user?
        if (getUser().equals(anonymousUser)) {
            assert ANONYMOUS_REALM_TYPE.equals(getAuthenticatingSubject().getRealm().getType())
                && ANONYMOUS_REALM_NAME.equals(getAuthenticatingSubject().getRealm().getName());
            return false;
        }

        // Run-as is supported for authentication with realm, api_key or token.
        if (AuthenticationType.REALM == getAuthenticationType()
            || AuthenticationType.API_KEY == getAuthenticationType()
            || AuthenticationType.TOKEN == getAuthenticationType()) {
            return true;
        }

        return false;
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
        output.setVersion(getVersion());
        Version.writeVersion(getVersion(), output);
        writeTo(output);
        return Base64.getEncoder().encodeToString(BytesReference.toBytes(output.bytes()));
    }

    public void writeTo(StreamOutput out) throws IOException {
        if (isRunAs()) {
            final User outerUser = effectiveSubject.getUser();
            final User innerUser = authenticatingSubject.getUser();
            assert false == User.isInternal(outerUser) && false == User.isInternal(innerUser)
                : "internal users cannot participate in run-as";
            User.writeUser(outerUser, out);
            out.writeBoolean(true);
            User.writeUser(innerUser, out);
            out.writeBoolean(false);
        } else {
            final User user = effectiveSubject.getUser();
            AuthenticationSerializationHelper.writeUserTo(user, out);
        }
        authenticatingSubject.getRealm().writeTo(out);
        final RealmRef lookedUpBy = getLookedUpBy();
        // See detailed comment on the same assertion in the Constructor with StreamInput
        assert isRunAs() || lookedUpBy == null : "Authentication has no inner-user, but looked-up-by is [" + lookedUpBy + "]";

        if (lookedUpBy != null) {
            out.writeBoolean(true);
            lookedUpBy.writeTo(out);
        } else {
            out.writeBoolean(false);
        }
        out.writeVInt(type.ordinal());
        out.writeGenericMap(getMetadata());
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
        final Subject mySubject = getEffectiveSubject();
        final Subject creatorSubject = resourceCreatorAuthentication.getEffectiveSubject();
        return mySubject.canAccessResourcesOf(creatorSubject);
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        Authentication that = (Authentication) o;
        return type == that.type
            && authenticatingSubject.equals(that.authenticatingSubject)
            && effectiveSubject.equals(that.effectiveSubject);
    }

    @Override
    public int hashCode() {
        return Objects.hash(type, authenticatingSubject, effectiveSubject);
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
        final User user = effectiveSubject.getUser();
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
            final String apiKeyId = (String) getMetadata().get(AuthenticationField.API_KEY_ID_KEY);
            final String apiKeyName = (String) getMetadata().get(AuthenticationField.API_KEY_NAME_KEY);
            if (apiKeyName == null) {
                builder.field("api_key", Map.of("id", apiKeyId));
            } else {
                builder.field("api_key", Map.of("id", apiKeyId, "name", apiKeyName));
            }
        }
    }

    private void assertInternalConsistency() {
        if (false == Assertions.ENABLED) {
            return;
        }

        assert effectiveSubject != null;
        assert authenticatingSubject != null;
        assert type != null;
        assert effectiveSubject.getVersion().equals(authenticatingSubject.getVersion());

        if (isRunAs()) {
            assert authenticatingSubject != effectiveSubject : "isRunAs logic does not hold";
            assert false == User.isInternal(effectiveSubject.getUser()) && false == User.isInternal(authenticatingSubject.getUser())
                : "internal users cannot participate in run-as";
        } else {
            assert authenticatingSubject == effectiveSubject : "isRunAs logic does not hold";
        }

        // Assert API key metadata
        assert (false == isAuthenticatedAsApiKey()) || (this.getMetadata().get(AuthenticationField.API_KEY_ID_KEY) != null)
            : "API KEY authentication requires metadata to contain API KEY id, and the value must be non-null.";

        // Assert domain assignment
        if (isAssignedToDomain()) {
            assert false == isApiKey();
            assert false == isServiceAccount();
            assert false == isAuthenticatedAnonymously();
            assert false == isAuthenticatedInternally();
        }
    }

    private boolean hasSyntheticRealmNameOrType(@Nullable RealmRef realmRef) {
        if (realmRef == null) {
            return false;
        }
        if (List.of(API_KEY_REALM_NAME, ServiceAccountSettings.REALM_NAME, ANONYMOUS_REALM_NAME, FALLBACK_REALM_NAME, ATTACH_REALM_NAME)
            .contains(realmRef.getName())) {
            return true;
        }
        if (List.of(API_KEY_REALM_TYPE, ServiceAccountSettings.REALM_TYPE, ANONYMOUS_REALM_TYPE, FALLBACK_REALM_TYPE, ATTACH_REALM_TYPE)
            .contains(realmRef.getType())) {
            return true;
        }
        return false;
    }

    @Override
    public String toString() {
        StringBuilder builder = new StringBuilder("Authentication[effectiveSubject=").append(effectiveSubject);
        if (isRunAs()) {
            builder.append(",authenticatingSubject=").append(authenticatingSubject);
        }
        builder.append(",type=").append(type);
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

        public static RealmRef newAnonymousRealmRef(String nodeName) {
            // the "anonymous" internal realm is not part of any realm domain
            return new Authentication.RealmRef(ANONYMOUS_REALM_NAME, ANONYMOUS_REALM_TYPE, nodeName, null);
        }

        static RealmRef newServiceAccountRealmRef(String nodeName) {
            // no domain for service account tokens
            return new Authentication.RealmRef(ServiceAccountSettings.REALM_NAME, ServiceAccountSettings.REALM_TYPE, nodeName, null);
        }

        static RealmRef newApiKeyRealmRef(String nodeName) {
            // no domain for API Key tokens
            return new RealmRef(API_KEY_REALM_NAME, API_KEY_REALM_TYPE, nodeName, null);
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
            new Subject(internalUser, authenticatedBy, version, Map.of()),
            AuthenticationType.INTERNAL
        );
        assert false == authentication.isAssignedToDomain();
        return authentication;
    }

    public static Authentication newInternalFallbackAuthentication(User fallbackUser, String nodeName) {
        // TODO assert SystemUser.is(fallbackUser);
        final Authentication.RealmRef authenticatedBy = newInternalFallbackRealmRef(nodeName);
        Authentication authentication = new Authentication(
            new Subject(fallbackUser, authenticatedBy, Version.CURRENT, Map.of()),
            Authentication.AuthenticationType.INTERNAL
        );
        assert false == authentication.isAssignedToDomain();
        return authentication;
    }

    public static Authentication newAnonymousAuthentication(AnonymousUser anonymousUser, String nodeName) {
        final Authentication.RealmRef authenticatedBy = newAnonymousRealmRef(nodeName);
        Authentication authentication = new Authentication(
            new Subject(anonymousUser, authenticatedBy, Version.CURRENT, Map.of()),
            Authentication.AuthenticationType.ANONYMOUS
        );
        assert false == authentication.isAssignedToDomain();
        return authentication;
    }

    public static Authentication newServiceAccountAuthentication(User serviceAccountUser, String nodeName, Map<String, Object> metadata) {
        // TODO make the service account user a separate class/interface
        final Authentication.RealmRef authenticatedBy = newServiceAccountRealmRef(nodeName);
        Authentication authentication = new Authentication(
            new Subject(serviceAccountUser, authenticatedBy, Version.CURRENT, metadata),
            AuthenticationType.TOKEN
        );
        assert false == authentication.isAssignedToDomain();
        return authentication;
    }

    public static Authentication newRealmAuthentication(User user, RealmRef realmRef) {
        // TODO make the type system ensure that this is not a run-as user
        Authentication authentication = new Authentication(
            new Subject(user, realmRef, Version.CURRENT, Map.of()),
            AuthenticationType.REALM
        );
        assert false == authentication.isServiceAccount();
        assert false == authentication.isApiKey();
        assert false == authentication.isAuthenticatedInternally();
        assert false == authentication.isAuthenticatedAnonymously();
        return authentication;
    }

    public static Authentication newApiKeyAuthentication(AuthenticationResult<User> authResult, String nodeName) {
        assert authResult.isAuthenticated() : "API Key authn result must be successful";
        final User apiKeyUser = authResult.getValue();
        assert apiKeyUser.roles().length == 0 : "The user associated to an API key authentication must have no role";
        final Authentication.RealmRef authenticatedBy = newApiKeyRealmRef(nodeName);
        Authentication authentication = new Authentication(
            new Subject(apiKeyUser, authenticatedBy, Version.CURRENT, authResult.getMetadata()),
            AuthenticationType.API_KEY
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

    // TODO: Rename to AuthenticationMethod
    public enum AuthenticationType {
        REALM,
        API_KEY,
        TOKEN,
        ANONYMOUS,
        INTERNAL
    }

    public static class AuthenticationSerializationHelper {

        private AuthenticationSerializationHelper() {}

        /**
         * Read the User object as well as the trailing boolean flag if the user is *not* an internal user.
         * The trailing boolean, if exits, must be false (indicating no following inner-user).
         */
        public static User readUserFrom(StreamInput input) throws IOException {
            final User user = readUserWithoutTrailingBoolean(input);
            if (false == User.isInternal(user)) {
                boolean hasInnerUser = input.readBoolean();
                assert false == hasInnerUser : "no inner user is possible, otherwise use UserTuple.readFrom";
            }
            return user;
        }

        public static void writeUserTo(User user, StreamOutput output) throws IOException {
            if (User.isInternal(user)) {
                writeInternalUser(user, output);
            } else {
                User.writeUser(user, output);
                output.writeBoolean(false); // simple user, no inner user possible
            }
        }

        private static User readUserWithoutTrailingBoolean(StreamInput input) throws IOException {
            final boolean isInternalUser = input.readBoolean();
            final String username = input.readString();
            if (isInternalUser) {
                if (SystemUser.NAME.equals(username)) {
                    return SystemUser.INSTANCE;
                } else if (XPackUser.NAME.equals(username)) {
                    return XPackUser.INSTANCE;
                } else if (XPackSecurityUser.NAME.equals(username)) {
                    return XPackSecurityUser.INSTANCE;
                } else if (SecurityProfileUser.NAME.equals(username)) {
                    return SecurityProfileUser.INSTANCE;
                } else if (AsyncSearchUser.NAME.equals(username)) {
                    return AsyncSearchUser.INSTANCE;
                }
                throw new IllegalStateException("username [" + username + "] does not match any internal user");
            }
            String[] roles = input.readStringArray();
            Map<String, Object> metadata = input.readMap();
            String fullName = input.readOptionalString();
            String email = input.readOptionalString();
            boolean enabled = input.readBoolean();
            return new User(username, roles, fullName, email, metadata, enabled);
        }

        private static void writeInternalUser(User user, StreamOutput output) throws IOException {
            assert User.isInternal(user);
            output.writeBoolean(true);
            if (SystemUser.is(user)) {
                output.writeString(SystemUser.NAME);
            } else if (XPackUser.is(user)) {
                output.writeString(XPackUser.NAME);
            } else if (XPackSecurityUser.is(user)) {
                output.writeString(XPackSecurityUser.NAME);
            } else if (SecurityProfileUser.is(user)) {
                output.writeString(SecurityProfileUser.NAME);
            } else if (AsyncSearchUser.is(user)) {
                output.writeString(AsyncSearchUser.NAME);
            } else {
                assert false;
                throw new IllegalStateException("user [" + user + "] is not internal");
            }
        }
    }
}
