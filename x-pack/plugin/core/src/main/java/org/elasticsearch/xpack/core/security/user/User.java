/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.xpack.core.security.user;

import org.elasticsearch.common.Nullable;
import org.elasticsearch.common.ParseField;
import org.elasticsearch.common.Strings;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.common.xcontent.ToXContentObject;
import org.elasticsearch.common.xcontent.XContentBuilder;

import java.io.IOException;
import java.util.Arrays;
import java.util.Map;
import java.util.Objects;

/**
 * An authenticated user
 */
public class User implements ToXContentObject {

    private final String username;
    private final String[] roles;
    private final User authenticatedUser;
    private final Map<String, Object> metadata;
    private final boolean enabled;

    @Nullable private final String fullName;
    @Nullable private final String email;

    public User(String username, String... roles) {
        this(username, roles, null, null, Map.of(), true);
    }

    public User(String username, String[] roles, User authenticatedUser) {
        this(username, roles, null, null, Map.of(), true, authenticatedUser);
    }

    public User(User user, User authenticatedUser) {
        this(user.principal(), user.roles(), user.fullName(), user.email(), user.metadata(), user.enabled(), authenticatedUser);
    }

    public User(String username, String[] roles, String fullName, String email, Map<String, Object> metadata, boolean enabled) {
        this(username, roles, fullName, email, metadata, enabled, null);
    }

    private User(String username, String[] roles, String fullName, String email, Map<String, Object> metadata, boolean enabled,
                User authenticatedUser) {
        this.username = Objects.requireNonNull(username);
        this.roles = roles == null ? Strings.EMPTY_ARRAY : roles;
        this.metadata = metadata == null ? Map.of() : metadata;
        this.fullName = fullName;
        this.email = email;
        this.enabled = enabled;
        assert (authenticatedUser == null || authenticatedUser.isRunAs() == false) : "the authenticated user should not be a run_as user";
        this.authenticatedUser = authenticatedUser;
    }

    /**
     * @return  The principal of this user - effectively serving as the
     *          unique identity of of the user.
     */
    public String principal() {
        return this.username;
    }

    /**
     * @return  The roles this user is associated with. The roles are
     *          identified by their unique names and each represents as
     *          set of permissions
     */
    public String[] roles() {
        return this.roles;
    }

    /**
     * @return  The metadata that is associated with this user. Can never be {@code null}.
     */
    public Map<String, Object> metadata() {
        return metadata;
    }

    /**
     * @return  The full name of this user. May be {@code null}.
     */
    public String fullName() {
        return fullName;
    }

    /**
     * @return  The email of this user. May be {@code null}.
     */
    public String email() {
        return email;
    }

    /**
     * @return whether the user is enabled or not
     */
    public boolean enabled() {
        return enabled;
    }

    /**
     * @return The user that was originally authenticated.
     * This may be the user itself, or a different user which used runAs.
     */
    public User authenticatedUser() {
        return authenticatedUser == null ? this : authenticatedUser;
    }

    /** Return true if this user was not the originally authenticated user, false otherwise. */
    public boolean isRunAs() {
        return authenticatedUser != null;
    }

    @Override
    public String toString() {
        StringBuilder sb = new StringBuilder();
        sb.append("User[username=").append(username);
        sb.append(",roles=[").append(Strings.arrayToCommaDelimitedString(roles)).append("]");
        sb.append(",fullName=").append(fullName);
        sb.append(",email=").append(email);
        sb.append(",metadata=");
        sb.append(metadata);
        if (authenticatedUser != null) {
            sb.append(",authenticatedUser=[").append(authenticatedUser.toString()).append("]");
        }
        sb.append("]");
        return sb.toString();
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o instanceof User == false) return false;

        User user = (User) o;

        if (!username.equals(user.username)) return false;
        // Probably incorrect - comparing Object[] arrays with Arrays.equals
        if (!Arrays.equals(roles, user.roles)) return false;
        if (authenticatedUser != null ? !authenticatedUser.equals(user.authenticatedUser) : user.authenticatedUser != null) return false;
        if (!metadata.equals(user.metadata)) return false;
        if (fullName != null ? !fullName.equals(user.fullName) : user.fullName != null) return false;
        return !(email != null ? !email.equals(user.email) : user.email != null);

    }

    @Override
    public int hashCode() {
        int result = username.hashCode();
        result = 31 * result + Arrays.hashCode(roles);
        result = 31 * result + (authenticatedUser != null ? authenticatedUser.hashCode() : 0);
        result = 31 * result + metadata.hashCode();
        result = 31 * result + (fullName != null ? fullName.hashCode() : 0);
        result = 31 * result + (email != null ? email.hashCode() : 0);
        return result;
    }

    @Override
    public final XContentBuilder toXContent(XContentBuilder builder, Params params) throws IOException {
        builder.startObject();
        builder.field(Fields.USERNAME.getPreferredName(), principal());
        builder.array(Fields.ROLES.getPreferredName(), roles());
        builder.field(Fields.FULL_NAME.getPreferredName(), fullName());
        builder.field(Fields.EMAIL.getPreferredName(), email());
        builder.field(Fields.METADATA.getPreferredName(), metadata());
        builder.field(Fields.ENABLED.getPreferredName(), enabled());
        return builder.endObject();
    }

    public static User partialReadFrom(String username, StreamInput input) throws IOException {
        String[] roles = input.readStringArray();
        Map<String, Object> metadata = input.readMap();
        String fullName = input.readOptionalString();
        String email = input.readOptionalString();
        boolean enabled = input.readBoolean();
        User outerUser = new User(username, roles, fullName, email, metadata, enabled, null);
        boolean hasInnerUser = input.readBoolean();
        if (hasInnerUser) {
            User innerUser = readFrom(input);
            return new User(outerUser, innerUser);
        } else {
            return outerUser;
        }
    }

    public static User readFrom(StreamInput input) throws IOException {
        final boolean isInternalUser = input.readBoolean();
        assert isInternalUser == false: "should always return false. Internal users should use the InternalUserSerializationHelper";
        final String username = input.readString();
        return partialReadFrom(username, input);
    }

    public static void writeTo(User user, StreamOutput output) throws IOException {
        if (user.authenticatedUser == null) {
            // no backcompat necessary, since there is no inner user
            writeUser(user, output);
        } else {
            writeUser(user, output);
            output.writeBoolean(true);
            writeUser(user.authenticatedUser, output);
        }
        output.writeBoolean(false); // last user written, regardless of bwc, does not have an inner user
    }

    public static boolean isInternal(User user) {
        return SystemUser.is(user) || XPackUser.is(user) || XPackSecurityUser.is(user) || AsyncSearchUser.is(user);
    }

    /** Write just the given {@link User}, but not the inner {@link #authenticatedUser}. */
    private static void writeUser(User user, StreamOutput output) throws IOException {
        output.writeBoolean(false); // not a system user
        output.writeString(user.username);
        output.writeStringArray(user.roles);
        output.writeMap(user.metadata);
        output.writeOptionalString(user.fullName);
        output.writeOptionalString(user.email);
        output.writeBoolean(user.enabled);
    }

    public interface Fields {
        ParseField USERNAME = new ParseField("username");
        ParseField PASSWORD = new ParseField("password");
        ParseField PASSWORD_HASH = new ParseField("password_hash");
        ParseField ROLES = new ParseField("roles");
        ParseField FULL_NAME = new ParseField("full_name");
        ParseField EMAIL = new ParseField("email");
        ParseField METADATA = new ParseField("metadata");
        ParseField ENABLED = new ParseField("enabled");
        ParseField TYPE = new ParseField("type");
        ParseField AUTHENTICATION_REALM = new ParseField("authentication_realm");
        ParseField LOOKUP_REALM = new ParseField("lookup_realm");
        ParseField REALM_TYPE = new ParseField("type");
        ParseField REALM_NAME = new ParseField("name");
    }
}

