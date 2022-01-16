/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */
package org.elasticsearch.xpack.core.security.authc;

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
import java.util.Set;

import static org.elasticsearch.xcontent.ConstructingObjectParser.constructorArg;
import static org.elasticsearch.xcontent.ConstructingObjectParser.optionalConstructorArg;
import static org.elasticsearch.xpack.core.security.authc.AuthenticationField.ATTACH_REALM_NAME;
import static org.elasticsearch.xpack.core.security.authc.AuthenticationField.ATTACH_REALM_TYPE;

// TODO(hub-cap) Clean this up after moving User over - This class can re-inherit its field AUTHENTICATION_KEY in AuthenticationField.
// That interface can be removed
public class Authentication implements ToXContentObject {

    public static final Version VERSION_API_KEY_ROLES_AS_BYTES = Version.V_7_9_0;
    public static final Version VERSION_REALM_DOMAINS = Version.V_8_1_0;

    private final User user;
    private final RealmRef authenticatedBy;
    private final RealmRef lookedUpBy;
    private final Version version;
    private final AuthenticationType type;
    private final Map<String, Object> metadata; // authentication contains metadata, includes api_key details (including api_key metadata)
    private final Set<RealmRef> domainRealms;

    public Authentication(User user, RealmRef authenticatedBy, RealmRef lookedUpBy) {
        this(user, authenticatedBy, lookedUpBy, Version.CURRENT, AuthenticationType.REALM, Collections.emptyMap(), Collections.emptySet());
    }

    public Authentication(User user, RealmRef authenticatedBy, RealmRef lookedUpBy, Version version) {
        this(user, authenticatedBy, lookedUpBy, version, AuthenticationType.REALM, Collections.emptyMap(), Collections.emptySet());
    }

    public Authentication(
        User user,
        RealmRef authenticatedBy,
        RealmRef lookedUpBy,
        Version version,
        AuthenticationType authenticationType,
        Map<String, Object> metadata
    ) {
        this(user, authenticatedBy, lookedUpBy, version, authenticationType, metadata, Collections.emptySet());
    }

    public Authentication(User user, RealmRef authenticatedBy, RealmRef lookedUpBy, Version version, Map<String, Object> metadata) {
        this(user, authenticatedBy, lookedUpBy, version, AuthenticationType.REALM, metadata, Collections.emptySet());
    }

    public Authentication(Authentication copy, Version version) {
        this(
            copy.getUser(),
            rewriteRealmRef(version, copy.getAuthenticatedBy()),
            rewriteRealmRef(version, copy.getLookedUpBy()),
            version,
            copy.getAuthenticationType(),
            rewriteMetadataForApiKeyRoleDescriptors(version, copy),
            version.before(VERSION_REALM_DOMAINS) ? Set.of() : copy.getDomainRealms() // security domain erasure
        );
    }

    public Authentication(
        User user,
        RealmRef authenticatedBy,
        RealmRef lookedUpBy,
        Version version,
        AuthenticationType type,
        Map<String, Object> metadata,
        Set<RealmRef> domainRealms
    ) {
        this.user = Objects.requireNonNull(user);
        this.authenticatedBy = Objects.requireNonNull(authenticatedBy);
        this.lookedUpBy = lookedUpBy;
        this.version = version;
        this.type = type;
        this.metadata = metadata;
        this.domainRealms = domainRealms;
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
        if (in.getVersion().onOrAfter(VERSION_REALM_DOMAINS)) {
            this.domainRealms = in.readSet(RealmRef::new);
        } else {
            this.domainRealms = Set.of();
        }
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
     * Returns the version of the node that performed the authentication.
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
     * Returns {@code true} if the effective user comes from a realm under a domain.
     * The same username can be authenticated by different realms (eg with different credential types),
     * but resources created across realms cannot be accessed unless the realms are also part of the same domain.
     */
    public boolean isAssignedToDomain() {
        return getSourceRealm().getDomain() != null;
    }

    /**
     * Returns the set of realm identifiers under the domain of the effective user's own realm.
     * The same resources are accessible, when the same username is authenticated by any of these realms.
     * The returned set does NOT include the current effective user's realm.
     */
    public Set<RealmRef> getDomainRealms() {
        return domainRealms;
    }

    public boolean isAuthenticatedWithServiceAccount() {
        return ServiceAccountSettings.REALM_TYPE.equals(getAuthenticatedBy().getType());
    }

    public boolean isAuthenticatedWithApiKey() {
        return AuthenticationType.API_KEY.equals(getAuthenticationType());
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
        out.writeMap(metadata);
        if (out.getVersion().onOrAfter(VERSION_REALM_DOMAINS)) {
            out.writeCollection(domainRealms);
        }
    }

    /**
     * Checks whether the user or API key of the passed in authentication can access the resources owned by the user
     * or API key of this authentication. The rules are as follows:
     *   * True if the authentications are for the same API key (same API key ID)
     *   * True if they are the same username from the same realm
     *      - For file and native realm, same realm means the same realm type
     *      - For all other realms, same realm means same realm type plus same realm name
     *   * An user and its API key cannot access each other's resources
     *   * An user and its token can access each other's resources
     *   * Two API keys are never able to access each other's resources regardless of their ownership.
     *
     *  This check is a best effort and it does not account for certain static and external changes.
     *  See also <a href="https://www.elastic.co/guide/en/elasticsearch/reference/master/security-limitations.html">
     *      security limitations</a>
     */
    public boolean canAccessResourcesOf(Authentication other) {
        if (AuthenticationType.API_KEY == getAuthenticationType() && AuthenticationType.API_KEY == other.getAuthenticationType()) {
            final boolean sameKeyId = getMetadata().get(AuthenticationField.API_KEY_ID_KEY)
                .equals(other.getMetadata().get(AuthenticationField.API_KEY_ID_KEY));
            if (sameKeyId) {
                assert getUser().principal().equals(other.getUser().principal())
                    : "The same API key ID cannot be attributed to two different usernames";
            }
            return sameKeyId;
        }

        if (getAuthenticationType().equals(other.getAuthenticationType())
            || (AuthenticationType.REALM == getAuthenticationType() && AuthenticationType.TOKEN == other.getAuthenticationType())
            || (AuthenticationType.TOKEN == getAuthenticationType() && AuthenticationType.REALM == other.getAuthenticationType())) {
            if (false == getUser().principal().equals(other.getUser().principal())) {
                return false;
            }
            final RealmRef thisRealm = getSourceRealm();
            final RealmRef otherRealm = other.getSourceRealm();
            if (FileRealmSettings.TYPE.equals(thisRealm.getType()) || NativeRealmSettings.TYPE.equals(thisRealm.getType())) {
                return thisRealm.getType().equals(otherRealm.getType());
            }
            return thisRealm.getName().equals(otherRealm.getName()) && thisRealm.getType().equals(otherRealm.getType());
        } else {
            assert EnumSet.of(
                AuthenticationType.REALM,
                AuthenticationType.API_KEY,
                AuthenticationType.TOKEN,
                AuthenticationType.ANONYMOUS,
                AuthenticationType.INTERNAL
            ).containsAll(EnumSet.of(getAuthenticationType(), other.getAuthenticationType()))
                : "cross AuthenticationType comparison for canAccessResourcesOf is not applicable for: "
                    + EnumSet.of(getAuthenticationType(), other.getAuthenticationType());
            return false;
        }
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
            && metadata.equals(that.metadata)
            && Objects.equals(domainRealms, that.domainRealms);
    }

    @Override
    public int hashCode() {
        return Objects.hash(user, authenticatedBy, lookedUpBy, version, type, metadata, domainRealms);
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
        if (isAuthenticatedWithServiceAccount()) {
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
        builder.field(User.Fields.REALM_DOMAIN.getPreferredName(), getAuthenticatedBy().getDomain());
        builder.endObject();
        builder.startObject(User.Fields.LOOKUP_REALM.getPreferredName());
        if (getLookedUpBy() != null) {
            builder.field(User.Fields.REALM_NAME.getPreferredName(), getLookedUpBy().getName());
            builder.field(User.Fields.REALM_TYPE.getPreferredName(), getLookedUpBy().getType());
            builder.field(User.Fields.REALM_DOMAIN.getPreferredName(), getLookedUpBy().getDomain());
        } else {
            builder.field(User.Fields.REALM_NAME.getPreferredName(), getAuthenticatedBy().getName());
            builder.field(User.Fields.REALM_TYPE.getPreferredName(), getAuthenticatedBy().getType());
            builder.field(User.Fields.REALM_DOMAIN.getPreferredName(), getAuthenticatedBy().getDomain());
        }
        builder.endObject();
        builder.field(User.Fields.AUTHENTICATION_TYPE.getPreferredName(), getAuthenticationType().name().toLowerCase(Locale.ROOT));
        if (isAuthenticatedWithApiKey()) {
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
        assert (AuthenticationType.API_KEY.equals(this.type) == false) || (this.metadata.get(AuthenticationField.API_KEY_ID_KEY) != null)
            : "API KEY authentication requires metadata to contain API KEY id, and the value must be non-null.";
    }

    private void assertDomainAssignment() {
        assert isAssignedToDomain() || domainRealms.isEmpty()
            : "the set of realms under the same domain must be empty if realm is not assigned to any domain";
        final RealmRef sourceRealmRef = getSourceRealm();
        for (RealmRef domainRealmRef : domainRealms) {
            assert Objects.equals(domainRealmRef.nodeName, sourceRealmRef.nodeName);
            assert Objects.equals(domainRealmRef.domain, sourceRealmRef.domain);
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

    public static class RealmRef implements Writeable {

        private final String nodeName;
        private final String name;
        private final String type;
        private final @Nullable String domain;

        public RealmRef(String name, String type, String nodeName) {
            this(name, type, nodeName, null);
        }

        public RealmRef(String name, String type, String nodeName, @Nullable String domain) {
            this.nodeName = nodeName;
            this.name = name;
            this.type = type;
            this.domain = domain;
        }

        public RealmRef(StreamInput in) throws IOException {
            this.nodeName = in.readString();
            this.name = in.readString();
            this.type = in.readString();
            if (in.getVersion().onOrAfter(VERSION_REALM_DOMAINS)) {
                this.domain = in.readOptionalString();
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
                out.writeOptionalString(domain);
            }
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
        public @Nullable String getDomain() {
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
                return "{Realm[" + type + "." + name + "] under Domain[" + domain + "] on Node[" + nodeName + "]}";
            } else {
                return "{Realm[" + type + "." + name + "] on Node[" + nodeName + "]}";
            }
        }

        public static RealmRef newInternalRealmRef(String nodeName) {
            // the "attach" internal realm is not part of any realm domain
            return new Authentication.RealmRef(ATTACH_REALM_NAME, ATTACH_REALM_TYPE, nodeName, null);
        }
    }

    public static Authentication newInternalAuthentication(User user, Version version, String nodeName) {
        if (false == User.isInternal(user)) {
            throw new IllegalArgumentException("Expected internal user, but provided [" + user + "]");
        }
        final Authentication.RealmRef authenticatedBy = Authentication.RealmRef.newInternalRealmRef(nodeName);
        return new Authentication(
            user,
            authenticatedBy,
            null,
            version,
            AuthenticationType.INTERNAL,
            Collections.emptyMap(),
            Collections.emptySet()
        );
    }

    public static Authentication newRealmAuthentication(User user, Realm realm) {
        if (user.isRunAs()) {
            throw new IllegalStateException("Realm authentication must not be run-as");
        }
        return new Authentication(
            user,
            realm.getRealmRef(),
            null,
            Version.CURRENT,
            AuthenticationType.REALM,
            Map.of(),
            realm.getDomainRealmRef()
        );
    }

    private static RealmRef rewriteRealmRef(Version streamVersion, RealmRef realmRef) {
        if (realmRef != null && realmRef.getDomain() != null && streamVersion.before(VERSION_REALM_DOMAINS)) {
            // security domain erasure
            new RealmRef(realmRef.getName(), realmRef.getType(), realmRef.getNodeName(), null);
        }
        return realmRef;
    }

    @SuppressWarnings("unchecked")
    private static Map<String, Object> rewriteMetadataForApiKeyRoleDescriptors(Version streamVersion, Authentication authentication) {
        Map<String, Object> metadata = authentication.getMetadata();
        if (authentication.getAuthenticationType() == AuthenticationType.API_KEY) {
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

    public static ConstructingObjectParser<RealmRef, Void> REALM_REF_PARSER = new ConstructingObjectParser<>(
        "realm_ref",
        false,
        (args, v) -> new RealmRef((String) args[0], (String) args[1], (String) args[2], (String) args[3])
    );

    static {
        REALM_REF_PARSER.declareString(constructorArg(), new ParseField("name"));
        REALM_REF_PARSER.declareString(constructorArg(), new ParseField("type"));
        REALM_REF_PARSER.declareString(constructorArg(), new ParseField("node_name"));
        REALM_REF_PARSER.declareString(optionalConstructorArg(), new ParseField("domain"));
    }

    public enum AuthenticationType {
        REALM,
        API_KEY,
        TOKEN,
        ANONYMOUS,
        INTERNAL
    }
}
