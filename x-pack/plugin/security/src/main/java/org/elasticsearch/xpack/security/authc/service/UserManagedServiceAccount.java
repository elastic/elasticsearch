/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.security.authc.service;

import org.elasticsearch.common.VersionId;
import org.elasticsearch.core.Nullable;
import org.elasticsearch.xpack.core.security.action.service.ServiceAccountAuthor;
import org.elasticsearch.xpack.core.security.authc.service.ServiceAccount;
import org.elasticsearch.xpack.core.security.authc.service.ServiceAccountSettings;
import org.elasticsearch.xpack.core.security.user.User;

import java.time.Instant;
import java.util.List;
import java.util.Map;
import java.util.Objects;

/**
 * A service account created through the API and stored in the security index, as opposed to one of the built-in
 * accounts declared in {@link ElasticServiceAccounts}.
 * <p>
 * Its privileges are the named roles in {@link #roles()}, which reach authorization as the roles of the {@link User}
 * built here. That routing is selected by {@link ServiceAccountSettings#USER_MANAGED_SERVICE_ACCOUNT_FIELD} in the
 * user's metadata: without the marker the authorization layer would look for a built-in account of the same name
 * instead, so every instance must set it.
 * <p>
 * The full name goes on the {@link User} as its full name, the same as for a user, so it is what {@code _authenticate},
 * audit records and the credentials the account creates report as the actor's full name. The principal stays the
 * identity. An account without a full name keeps the fixed name it has always had, so that nothing that already reads
 * the value sees it change.
 * <p>
 * The description and the initials are carried for the clients that read the account. Neither means anything to
 * Elasticsearch, and a user has no field for either, so both are deliberately kept out of the {@link User}, which is
 * what authorization and audit see. The same goes for the
 * attribution: who created the account and who last replaced it, and when. Each of those is {@code null} when the
 * document does not record it, which is the case for the updating author until the account is first replaced and
 * for the creating author of an account written before attribution was recorded.
 */
final class UserManagedServiceAccount implements ServiceAccount {

    /**
     * Schema version of the stored {@code service_account} document. Increment when the document
     * format changes so readers can branch on how old a document is, independently of the
     * Elasticsearch release that wrote it.
     * <p>
     * Version 2 added the {@code creator}, {@code creation_time}, {@code updated_by} and {@code update_time} fields.
     * Version 3 added the optional {@code full_name} and {@code initials} fields.
     */
    record Version(int version) implements VersionId<Version> {
        static final Version CURRENT = new Version(3);

        @Override
        public int id() {
            return version;
        }
    }

    private final ServiceAccountId id;
    private final List<String> roles;
    private final boolean enabled;
    private final UserManagedServiceAccountStore.Details details;
    @Nullable
    private final ServiceAccountAuthor createdBy;
    @Nullable
    private final Instant createdAt;
    @Nullable
    private final ServiceAccountAuthor updatedBy;
    @Nullable
    private final Instant updatedAt;
    private final User user;

    UserManagedServiceAccount(ServiceAccountId id, List<String> roles, boolean enabled, UserManagedServiceAccountStore.Details details) {
        this(id, roles, enabled, details, null, null, null, null);
    }

    UserManagedServiceAccount(
        ServiceAccountId id,
        List<String> roles,
        boolean enabled,
        UserManagedServiceAccountStore.Details details,
        @Nullable ServiceAccountAuthor createdBy,
        @Nullable Instant createdAt,
        @Nullable ServiceAccountAuthor updatedBy,
        @Nullable Instant updatedAt
    ) {
        this.id = Objects.requireNonNull(id, "service account id cannot be null");
        this.roles = List.copyOf(Objects.requireNonNull(roles, "roles cannot be null"));
        this.enabled = enabled;
        this.details = Objects.requireNonNull(details, "details cannot be null");
        this.createdBy = createdBy;
        this.createdAt = createdAt;
        this.updatedBy = updatedBy;
        this.updatedAt = updatedAt;
        this.user = new User(
            id.asPrincipal(),
            this.roles.toArray(String[]::new),
            details.fullName() != null ? details.fullName() : "User-managed service account - " + id,
            null,
            Map.of(ServiceAccountSettings.USER_MANAGED_SERVICE_ACCOUNT_FIELD, true),
            enabled
        );
    }

    @Override
    public ServiceAccountId id() {
        return id;
    }

    @Override
    public User asUser() {
        return user;
    }

    List<String> roles() {
        return roles;
    }

    boolean enabled() {
        return enabled;
    }

    UserManagedServiceAccountStore.Details details() {
        return details;
    }

    @Nullable
    String description() {
        return details.description();
    }

    @Nullable
    String fullName() {
        return details.fullName();
    }

    @Nullable
    String initials() {
        return details.initials();
    }

    @Nullable
    ServiceAccountAuthor createdBy() {
        return createdBy;
    }

    @Nullable
    Instant createdAt() {
        return createdAt;
    }

    @Nullable
    ServiceAccountAuthor updatedBy() {
        return updatedBy;
    }

    @Nullable
    Instant updatedAt() {
        return updatedAt;
    }

    @Override
    public String toString() {
        return "UserManagedServiceAccount{id="
            + id
            + ", roles="
            + roles
            + ", enabled="
            + enabled
            + ", details="
            + details
            + ", createdBy="
            + createdBy
            + ", createdAt="
            + createdAt
            + ", updatedBy="
            + updatedBy
            + ", updatedAt="
            + updatedAt
            + '}';
    }
}
