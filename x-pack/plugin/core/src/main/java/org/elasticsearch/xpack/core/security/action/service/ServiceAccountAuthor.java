/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.core.security.action.service;

import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.common.io.stream.Writeable;
import org.elasticsearch.core.Nullable;
import org.elasticsearch.xcontent.ToXContentObject;
import org.elasticsearch.xcontent.XContentBuilder;
import org.elasticsearch.xpack.core.security.authc.Authentication;
import org.elasticsearch.xpack.core.security.authc.AuthenticationField;
import org.elasticsearch.xpack.core.security.authc.RealmDomain;
import org.elasticsearch.xpack.core.security.authc.Subject;
import org.elasticsearch.xpack.core.security.user.User;

import java.io.IOException;
import java.util.Map;
import java.util.Objects;

/**
 * Who performed a write to a user-managed service account, recorded so that an administrator can tell later who
 * created or last changed it. It is the effective subject of the request, as an API key's creator is, so a request
 * run as another user attributes the write to that user, and a request made with an API key attributes it to the
 * key's owner. Unlike an API key's creator it leaves out the user's metadata, which can be large and is of no use for
 * attribution.
 * <p>
 * For a caller using an API key, the realm recorded is the owner's own realm rather than the synthetic API key
 * realm, which is how audit logging names such a caller and what lets the author be matched to a user profile. The
 * key itself is recorded alongside, by id and name, so that a write made directly and one made through a key can
 * still be told apart.
 * <p>
 * The realm domain is carried whole so that the stored form matches the {@code creator} mapping of the security
 * index, which API keys already use. Responses render only its name, as an authentication response does. It is
 * absent for an author who acted through an API key, since the key's metadata does not carry the owner's domain.
 */
public record ServiceAccountAuthor(
    String principal,
    @Nullable String fullName,
    @Nullable String email,
    String realm,
    String realmType,
    @Nullable RealmDomain realmDomain,
    @Nullable ApiKey apiKey
) implements Writeable, ToXContentObject {

    public static final String PRINCIPAL_FIELD = "principal";
    public static final String FULL_NAME_FIELD = "full_name";
    public static final String EMAIL_FIELD = "email";
    public static final String REALM_FIELD = "realm";
    public static final String REALM_TYPE_FIELD = "realm_type";
    public static final String REALM_DOMAIN_FIELD = "realm_domain";
    public static final String API_KEY_FIELD = "api_key";

    public ServiceAccountAuthor {
        Objects.requireNonNull(principal, "principal cannot be null");
        Objects.requireNonNull(realm, "realm cannot be null");
        Objects.requireNonNull(realmType, "realm type cannot be null");
    }

    /**
     * An author who acted directly rather than through an API key.
     */
    public ServiceAccountAuthor(
        String principal,
        @Nullable String fullName,
        @Nullable String email,
        String realm,
        String realmType,
        @Nullable RealmDomain realmDomain
    ) {
        this(principal, fullName, email, realm, realmType, realmDomain, null);
    }

    /**
     * The API key a write was made with, as the audit log names it. The name is what the key had at the time and
     * may since have changed or, for a key created without one, be absent; the id is stable.
     */
    public record ApiKey(String id, @Nullable String name) implements Writeable, ToXContentObject {

        public static final String ID_FIELD = "id";
        public static final String NAME_FIELD = "name";

        public ApiKey {
            Objects.requireNonNull(id, "api key id cannot be null");
        }

        public static ApiKey readFrom(StreamInput in) throws IOException {
            return new ApiKey(in.readString(), in.readOptionalString());
        }

        @Override
        public void writeTo(StreamOutput out) throws IOException {
            out.writeString(id);
            out.writeOptionalString(name);
        }

        @Override
        public XContentBuilder toXContent(XContentBuilder builder, Params params) throws IOException {
            builder.startObject();
            builder.field(ID_FIELD, id);
            if (name != null) {
                builder.field(NAME_FIELD, name);
            }
            return builder.endObject();
        }
    }

    /**
     * Records the effective user, resolving an API key owner's realm from the authentication metadata so that the
     * author can be matched to a user profile, and recording the key itself. {@link Authentication#isApiKey()} tests
     * the effective subject, so a key running as another user records that user, from their own realm, and no key.
     */
    public static ServiceAccountAuthor fromAuthentication(Authentication authentication) {
        final Subject subject = authentication.getEffectiveSubject();
        final User user = subject.getUser();
        final Authentication.RealmRef realmRef = subject.getRealm();
        if (authentication.isApiKey()) {
            final Map<String, Object> metadata = subject.getMetadata();
            final ApiKey apiKey = new ApiKey(
                (String) metadata.get(AuthenticationField.API_KEY_ID_KEY),
                (String) metadata.get(AuthenticationField.API_KEY_NAME_KEY)
            );
            final String ownerRealm = (String) metadata.get(AuthenticationField.API_KEY_CREATOR_REALM_NAME);
            final String ownerRealmType = (String) metadata.get(AuthenticationField.API_KEY_CREATOR_REALM_TYPE);
            if (ownerRealm != null && ownerRealmType != null) {
                return new ServiceAccountAuthor(user.principal(), user.fullName(), user.email(), ownerRealm, ownerRealmType, null, apiKey);
            }
            // Older API keys may lack owner realm metadata. Keep their synthetic realm rather than inventing an owner realm.
            return new ServiceAccountAuthor(
                user.principal(),
                user.fullName(),
                user.email(),
                realmRef.getName(),
                realmRef.getType(),
                realmRef.getDomain(),
                apiKey
            );
        }
        return new ServiceAccountAuthor(
            user.principal(),
            user.fullName(),
            user.email(),
            realmRef.getName(),
            realmRef.getType(),
            realmRef.getDomain()
        );
    }

    public static ServiceAccountAuthor readFrom(StreamInput in) throws IOException {
        return new ServiceAccountAuthor(
            in.readString(),
            in.readOptionalString(),
            in.readOptionalString(),
            in.readString(),
            in.readString(),
            in.readOptionalWriteable(RealmDomain::readFrom),
            in.readOptionalWriteable(ApiKey::readFrom)
        );
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        out.writeString(principal);
        out.writeOptionalString(fullName);
        out.writeOptionalString(email);
        out.writeString(realm);
        out.writeString(realmType);
        out.writeOptionalWriteable(realmDomain);
        out.writeOptionalWriteable(apiKey);
    }

    /**
     * Renders the author for a response. Absent values are left out rather than written as {@code null}, and the
     * realm domain is reduced to its name: which realms make up the domain is cluster configuration, not something
     * about the caller.
     */
    @Override
    public XContentBuilder toXContent(XContentBuilder builder, Params params) throws IOException {
        builder.startObject();
        builder.field(PRINCIPAL_FIELD, principal);
        if (fullName != null) {
            builder.field(FULL_NAME_FIELD, fullName);
        }
        if (email != null) {
            builder.field(EMAIL_FIELD, email);
        }
        builder.field(REALM_FIELD, realm);
        builder.field(REALM_TYPE_FIELD, realmType);
        if (realmDomain != null) {
            builder.field(REALM_DOMAIN_FIELD, realmDomain.name());
        }
        if (apiKey != null) {
            builder.field(API_KEY_FIELD, apiKey);
        }
        return builder.endObject();
    }
}
