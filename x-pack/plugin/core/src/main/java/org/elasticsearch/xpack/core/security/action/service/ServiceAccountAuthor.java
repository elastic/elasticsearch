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
import org.elasticsearch.xpack.core.security.authc.RealmDomain;
import org.elasticsearch.xpack.core.security.authc.Subject;
import org.elasticsearch.xpack.core.security.user.User;

import java.io.IOException;
import java.util.Objects;

/**
 * Who performed a write to a user-managed service account, recorded so that an administrator can tell later who
 * created or last changed it. It is the same view of the caller that an API key records as its creator: the
 * effective subject of the request, so a request run as another user attributes the write to that user, and a
 * request made with an API key attributes it to the key's owner. Unlike an API key's creator it leaves out the
 * user's metadata, which can be large and is of no use for attribution.
 * <p>
 * The realm domain is carried whole so that the stored form matches the {@code creator} mapping of the security
 * index, which API keys already use. Responses render only its name, as an authentication response does.
 */
public record ServiceAccountAuthor(
    String principal,
    @Nullable String fullName,
    @Nullable String email,
    String realm,
    String realmType,
    @Nullable RealmDomain realmDomain
) implements Writeable, ToXContentObject {

    public static final String PRINCIPAL_FIELD = "principal";
    public static final String FULL_NAME_FIELD = "full_name";
    public static final String EMAIL_FIELD = "email";
    public static final String REALM_FIELD = "realm";
    public static final String REALM_TYPE_FIELD = "realm_type";
    public static final String REALM_DOMAIN_FIELD = "realm_domain";

    public ServiceAccountAuthor {
        Objects.requireNonNull(principal, "principal cannot be null");
        Objects.requireNonNull(realm, "realm cannot be null");
        Objects.requireNonNull(realmType, "realm type cannot be null");
    }

    public static ServiceAccountAuthor fromAuthentication(Authentication authentication) {
        final Subject subject = authentication.getEffectiveSubject();
        final User user = subject.getUser();
        final Authentication.RealmRef realmRef = subject.getRealm();
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
            in.readOptionalWriteable(RealmDomain::readFrom)
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
        return builder.endObject();
    }
}
