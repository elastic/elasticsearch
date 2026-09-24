/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.core.security.action.service;

import org.elasticsearch.TransportVersion;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.common.io.stream.Writeable;
import org.elasticsearch.core.Nullable;
import org.elasticsearch.xcontent.ToXContent;
import org.elasticsearch.xcontent.XContentBuilder;
import org.elasticsearch.xpack.core.security.authz.RoleDescriptor;

import java.io.IOException;
import java.time.Instant;
import java.util.List;
import java.util.Objects;

/**
 * What the service account API reports about one account. The two kinds of account describe their privileges
 * differently. A built-in account carries the fixed {@link RoleDescriptor} it was declared with, a user-managed one
 * carries the names of the roles it was created with plus whether it is enabled. So this is a union tagged by
 * {@link #type()}.
 */
public sealed interface ServiceAccountInfo extends Writeable, ToXContent {

    /**
     * Gates the tag and everything that follows it. Before this version the wire form was a principal followed
     * directly by a role descriptor, which is what {@link BuiltIn} still writes to such a node.
     */
    TransportVersion USER_MANAGED_SERVICE_ACCOUNT_INFO = TransportVersion.fromName("user_managed_service_account_info");

    /**
     * Gates the optional description of a user-managed account, which is written after the older fields both in this
     * wire form and in that of the request that writes an account. A node before this version is sent the account
     * without its description rather than being refused it: the description carries no meaning, so the account is
     * still whole without it.
     */
    TransportVersion USER_MANAGED_SERVICE_ACCOUNT_DESCRIPTION = TransportVersion.fromName("user_managed_service_account_description");

    /**
     * Gates who created and last changed a user-managed account and when, written after the description. As with the
     * description, a node before this version is sent the account without them: they describe the account's history,
     * not what it may do.
     */
    TransportVersion USER_MANAGED_SERVICE_ACCOUNT_ATTRIBUTION = TransportVersion.fromName("user_managed_service_account_attribution");

    String principal();

    ServiceAccountType type();

    /**
     * A built-in account, whose privileges are the role descriptor declared for it in the Elasticsearch distribution.
     */
    record BuiltIn(String principal, RoleDescriptor roleDescriptor) implements ServiceAccountInfo {

        public BuiltIn {
            Objects.requireNonNull(principal, "service account principal cannot be null");
            Objects.requireNonNull(roleDescriptor, "service account role descriptor cannot be null");
        }

        @Override
        public ServiceAccountType type() {
            return ServiceAccountType.BUILT_IN;
        }
    }

    /**
     * An account created through the API, whose privileges are the named roles resolved when it authenticates. The
     * roles are reported as the caller gave them. The description is free text that means nothing to Elasticsearch,
     * carried for the caller's benefit, and is {@code null} when the account has none.
     * <p>
     * The creator and editor record who created the account and who last replaced it, and the two timestamps when.
     * Each is {@code null} when unknown: the editor and its timestamp until the account is first replaced, and the
     * creator and its timestamp for an account written before they were recorded.
     * <p>
     * The two profile uids are not stored with the account. They are looked up when a caller asks for them with
     * {@code with_profile_uid}, and are {@code null} otherwise, or when the author has no profile.
     */
    record UserManaged(
        String principal,
        List<String> roles,
        boolean enabled,
        @Nullable String description,
        @Nullable ServiceAccountAuthor creator,
        @Nullable Instant createdAt,
        @Nullable ServiceAccountAuthor editor,
        @Nullable Instant editedAt,
        @Nullable String creatorProfileUid,
        @Nullable String editorProfileUid
    ) implements ServiceAccountInfo {

        public UserManaged {
            Objects.requireNonNull(principal, "service account principal cannot be null");
            roles = List.copyOf(Objects.requireNonNull(roles, "roles cannot be null"));
        }

        /**
         * An account with no attribution, as one written before attribution was recorded reads back.
         */
        public UserManaged(String principal, List<String> roles, boolean enabled, @Nullable String description) {
            this(principal, roles, enabled, description, null, null, null, null);
        }

        /**
         * An account as it is read from the store, with no profile uids resolved.
         */
        public UserManaged(
            String principal,
            List<String> roles,
            boolean enabled,
            @Nullable String description,
            @Nullable ServiceAccountAuthor creator,
            @Nullable Instant createdAt,
            @Nullable ServiceAccountAuthor editor,
            @Nullable Instant editedAt
        ) {
            this(principal, roles, enabled, description, creator, createdAt, editor, editedAt, null, null);
        }

        /**
         * The same account with the profile uids of its creator and editor filled in. A uid given for an author the
         * account does not have is dropped, since there is no one it could belong to.
         */
        public UserManaged withProfileUids(@Nullable String creatorProfileUid, @Nullable String editorProfileUid) {
            return new UserManaged(
                principal,
                roles,
                enabled,
                description,
                creator,
                createdAt,
                editor,
                editedAt,
                creator == null ? null : creatorProfileUid,
                editor == null ? null : editorProfileUid
            );
        }

        @Override
        public ServiceAccountType type() {
            return ServiceAccountType.USER_MANAGED;
        }
    }

    static ServiceAccountInfo readFrom(StreamInput in) throws IOException {
        final String principal = in.readString();
        if (in.getTransportVersion().supports(USER_MANAGED_SERVICE_ACCOUNT_INFO) == false) {
            return new BuiltIn(principal, new RoleDescriptor(in));
        }
        return switch (in.readEnum(ServiceAccountType.class)) {
            case BUILT_IN -> new BuiltIn(principal, new RoleDescriptor(in));
            case USER_MANAGED -> readUserManaged(principal, in);
        };
    }

    private static UserManaged readUserManaged(String principal, StreamInput in) throws IOException {
        final List<String> roles = in.readStringCollectionAsImmutableList();
        final boolean enabled = in.readBoolean();
        final String description = in.getTransportVersion().supports(USER_MANAGED_SERVICE_ACCOUNT_DESCRIPTION)
            ? in.readOptionalString()
            : null;
        if (in.getTransportVersion().supports(USER_MANAGED_SERVICE_ACCOUNT_ATTRIBUTION) == false) {
            return new UserManaged(principal, roles, enabled, description);
        }
        return new UserManaged(
            principal,
            roles,
            enabled,
            description,
            in.readOptionalWriteable(ServiceAccountAuthor::readFrom),
            in.readOptionalInstant(),
            in.readOptionalWriteable(ServiceAccountAuthor::readFrom),
            in.readOptionalInstant(),
            in.readOptionalString(),
            in.readOptionalString()
        );
    }

    @Override
    default void writeTo(StreamOutput out) throws IOException {
        final boolean tagged = out.getTransportVersion().supports(USER_MANAGED_SERVICE_ACCOUNT_INFO);
        // Unreachable: a node that cannot read the tag also cannot ask for user-managed accounts, since its request
        // arrives with a type of just [built_in]. Stated as a failure rather than an assertion so that a future
        // caller that gets this wrong is told, instead of writing a truncated account into the stream.
        if (tagged == false && this instanceof BuiltIn == false) {
            throw new IllegalStateException(
                "cannot send information about the user-managed service account ["
                    + principal()
                    + "] to a node that does not support user-managed service accounts"
            );
        }
        out.writeString(principal());
        if (tagged) {
            out.writeEnum(type());
        }
        switch (this) {
            case BuiltIn builtIn -> builtIn.roleDescriptor().writeTo(out);
            case UserManaged userManaged -> {
                out.writeStringCollection(userManaged.roles());
                out.writeBoolean(userManaged.enabled());
                if (out.getTransportVersion().supports(USER_MANAGED_SERVICE_ACCOUNT_DESCRIPTION)) {
                    out.writeOptionalString(userManaged.description());
                }
                if (out.getTransportVersion().supports(USER_MANAGED_SERVICE_ACCOUNT_ATTRIBUTION)) {
                    out.writeOptionalWriteable(userManaged.creator());
                    out.writeOptionalInstant(userManaged.createdAt());
                    out.writeOptionalWriteable(userManaged.editor());
                    out.writeOptionalInstant(userManaged.editedAt());
                    out.writeOptionalString(userManaged.creatorProfileUid());
                    out.writeOptionalString(userManaged.editorProfileUid());
                }
            }
        }
    }

    /**
     * Renders the account as a field named for its principal, so that a response can hold many of them.
     */
    @Override
    default XContentBuilder toXContent(XContentBuilder builder, Params params) throws IOException {
        builder.startObject(principal());
        innerToXContent(builder, params);
        return builder.endObject();
    }

    /**
     * Renders the account's type and privileges, but not its principal, into an object the caller has already
     * opened. For a response that keys accounts by principal, or one that carries the principal as a field alongside
     * these and so must not repeat it in the key.
     */
    default XContentBuilder innerToXContent(XContentBuilder builder, Params params) throws IOException {
        builder.field("type", type().value());
        switch (this) {
            case BuiltIn builtIn -> {
                builder.field("role_descriptor");
                builtIn.roleDescriptor().toXContent(builder, params);
            }
            case UserManaged userManaged -> {
                builder.stringListField("roles", userManaged.roles());
                builder.field("enabled", userManaged.enabled());
                if (userManaged.description() != null) {
                    builder.field("description", userManaged.description());
                }
                if (userManaged.creator() != null) {
                    builder.field("creator", userManaged.creator());
                }
                if (userManaged.createdAt() != null) {
                    builder.field("created_at", userManaged.createdAt().toEpochMilli());
                }
                if (userManaged.creatorProfileUid() != null) {
                    builder.field("creator_profile_uid", userManaged.creatorProfileUid());
                }
                if (userManaged.editor() != null) {
                    builder.field("editor", userManaged.editor());
                }
                if (userManaged.editedAt() != null) {
                    builder.field("edited_at", userManaged.editedAt().toEpochMilli());
                }
                if (userManaged.editorProfileUid() != null) {
                    builder.field("editor_profile_uid", userManaged.editorProfileUid());
                }
            }
        }
        return builder;
    }
}
