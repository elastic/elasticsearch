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
 * The description is free text carried for whoever administers the account. It means nothing to Elasticsearch and
 * so is deliberately kept out of the {@link User}, which is what authorization and audit see. The same goes for the
 * attribution: who created the account and who last replaced it, and when. Each of those is {@code null} when the
 * document does not record it, which is the case for the editor until the account is first replaced and for the
 * creator of an account written before attribution was recorded.
 */
final class UserManagedServiceAccount implements ServiceAccount {

    /**
     * Schema version of the stored {@code service_account} document. Increment when the document
     * format changes so readers can branch on how old a document is, independently of the
     * Elasticsearch release that wrote it.
     * <p>
     * Version 2 added the {@code creator}, {@code created_at}, {@code editor} and {@code edited_at} fields.
     */
    record Version(int version) implements VersionId<Version> {
        static final Version CURRENT = new Version(2);

        @Override
        public int id() {
            return version;
        }
    }

    private final ServiceAccountId id;
    private final List<String> roles;
    private final boolean enabled;
    @Nullable
    private final String description;
    @Nullable
    private final ServiceAccountAuthor creator;
    @Nullable
    private final Instant createdAt;
    @Nullable
    private final ServiceAccountAuthor editor;
    @Nullable
    private final Instant editedAt;
    private final User user;

    UserManagedServiceAccount(ServiceAccountId id, List<String> roles, boolean enabled, @Nullable String description) {
        this(id, roles, enabled, description, null, null, null, null);
    }

    UserManagedServiceAccount(
        ServiceAccountId id,
        List<String> roles,
        boolean enabled,
        @Nullable String description,
        @Nullable ServiceAccountAuthor creator,
        @Nullable Instant createdAt,
        @Nullable ServiceAccountAuthor editor,
        @Nullable Instant editedAt
    ) {
        this.id = Objects.requireNonNull(id, "service account id cannot be null");
        this.roles = List.copyOf(Objects.requireNonNull(roles, "roles cannot be null"));
        this.enabled = enabled;
        this.description = description;
        this.creator = creator;
        this.createdAt = createdAt;
        this.editor = editor;
        this.editedAt = editedAt;
        this.user = new User(
            id.asPrincipal(),
            this.roles.toArray(String[]::new),
            "User-managed service account - " + id,
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

    @Nullable
    String description() {
        return description;
    }

    @Nullable
    ServiceAccountAuthor creator() {
        return creator;
    }

    @Nullable
    Instant createdAt() {
        return createdAt;
    }

    @Nullable
    ServiceAccountAuthor editor() {
        return editor;
    }

    @Nullable
    Instant editedAt() {
        return editedAt;
    }

    @Override
    public String toString() {
        return "UserManagedServiceAccount{id="
            + id
            + ", roles="
            + roles
            + ", enabled="
            + enabled
            + ", description="
            + description
            + ", creator="
            + creator
            + ", createdAt="
            + createdAt
            + ", editor="
            + editor
            + ", editedAt="
            + editedAt
            + '}';
    }
}
