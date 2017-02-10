/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.xpack.security.user;

import org.elasticsearch.ElasticsearchSecurityException;
import org.elasticsearch.common.ParseField;
import org.elasticsearch.common.Strings;
import org.elasticsearch.common.inject.internal.Nullable;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.common.xcontent.ToXContentObject;
import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.xpack.security.support.MetadataUtils;

import java.io.IOException;
import java.util.Arrays;
import java.util.Collections;
import java.util.Map;

/**
 * An authenticated user
 */
public class User implements ToXContentObject {

    private final String username;
    private final String[] roles;
    private final User runAs;
    private final Map<String, Object> metadata;
    private final boolean enabled;

    @Nullable private final String fullName;
    @Nullable private final String email;

    public User(String username, String... roles) {
        this(username, roles, null, null, null, true);
    }

    public User(String username, String[] roles, User runAs) {
        this(username, roles, null, null, null, true, runAs);
    }

    public User(User user, User runAs) {
        this(user.principal(), user.roles(), user.fullName(), user.email(), user.metadata(), user.enabled(), runAs);
    }

    public User(String username, String[] roles, String fullName, String email, Map<String, Object> metadata, boolean enabled) {
        this.username = username;
        this.roles = roles == null ? Strings.EMPTY_ARRAY : roles;
        this.metadata = metadata != null ? Collections.unmodifiableMap(metadata) : Collections.emptyMap();
        this.fullName = fullName;
        this.email = email;
        this.enabled = enabled;
        this.runAs = null;
    }

    public User(String username, String[] roles, String fullName, String email, Map<String, Object> metadata, boolean enabled, User runAs) {
        this.username = username;
        this.roles = roles == null ? Strings.EMPTY_ARRAY : roles;
        this.metadata = metadata != null ? Collections.unmodifiableMap(metadata) : Collections.emptyMap();
        this.fullName = fullName;
        this.email = email;
        this.enabled = enabled;
        assert (runAs == null || runAs.runAs() == null) : "the run_as user should not be a user that can run as";
        if (runAs == SystemUser.INSTANCE) {
            throw new ElasticsearchSecurityException("invalid run_as user");
        }
        this.runAs = runAs;
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
     * @return The user that will be used for run as functionality. If run as
     *         functionality is not being used, then <code>null</code> will be
     *         returned
     */
    public User runAs() {
        return runAs;
    }

    @Override
    public String toString() {
        StringBuilder sb = new StringBuilder();
        sb.append("User[username=").append(username);
        sb.append(",roles=[").append(Strings.arrayToCommaDelimitedString(roles)).append("]");
        sb.append(",fullName=").append(fullName);
        sb.append(",email=").append(email);
        sb.append(",metadata=");
        MetadataUtils.writeValue(sb, metadata);
        if (runAs != null) {
            sb.append(",runAs=[").append(runAs.toString()).append("]");
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
        if (runAs != null ? !runAs.equals(user.runAs) : user.runAs != null) return false;
        if (!metadata.equals(user.metadata)) return false;
        if (fullName != null ? !fullName.equals(user.fullName) : user.fullName != null) return false;
        return !(email != null ? !email.equals(user.email) : user.email != null);

    }

    @Override
    public int hashCode() {
        int result = username.hashCode();
        result = 31 * result + Arrays.hashCode(roles);
        result = 31 * result + (runAs != null ? runAs.hashCode() : 0);
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

    public static User readFrom(StreamInput input) throws IOException {
        final boolean isInternalUser = input.readBoolean();
        final String username = input.readString();
        if (isInternalUser) {
            if (SystemUser.is(username)) {
                return SystemUser.INSTANCE;
            } else if (XPackUser.is(username)) {
                return XPackUser.INSTANCE;
            }
            throw new IllegalStateException("user [" + username + "] is not an internal user");
        }
        String[] roles = input.readStringArray();
        Map<String, Object> metadata = input.readMap();
        String fullName = input.readOptionalString();
        String email = input.readOptionalString();
        boolean enabled = input.readBoolean();
        User runAs = input.readBoolean() ? readFrom(input) : null;
        return new User(username, roles, fullName, email, metadata, enabled, runAs);
    }

    public static void writeTo(User user, StreamOutput output) throws IOException {
        if (SystemUser.is(user)) {
            output.writeBoolean(true);
            output.writeString(SystemUser.NAME);
        } else if (XPackUser.is(user)) {
            output.writeBoolean(true);
            output.writeString(XPackUser.NAME);
        } else {
            output.writeBoolean(false);
            output.writeString(user.username);
            output.writeStringArray(user.roles);
            output.writeMap(user.metadata);
            output.writeOptionalString(user.fullName);
            output.writeOptionalString(user.email);
            output.writeBoolean(user.enabled);
            if (user.runAs == null) {
                output.writeBoolean(false);
            } else {
                output.writeBoolean(true);
                writeTo(user.runAs, output);
            }
        }
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
    }
}
