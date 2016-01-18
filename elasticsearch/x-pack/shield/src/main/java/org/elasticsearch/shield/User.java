/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.shield;

import org.elasticsearch.ElasticsearchSecurityException;
import org.elasticsearch.common.Strings;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.common.xcontent.ToXContent;
import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.shield.authz.SystemRole;

import java.io.IOException;
import java.util.Arrays;

/**
 * An authenticated user
 */
public class User implements ToXContent {

    public static final User SYSTEM = new System();

    private final String username;
    private final String[] roles;
    private final User runAs;

    public User(String username, String... roles) {
        this.username = username;
        this.roles = roles == null ? Strings.EMPTY_ARRAY : roles;
        this.runAs = null;
    }

    public User(String username, String[] roles, User runAs) {
        this.username = username;
        this.roles = roles == null ? Strings.EMPTY_ARRAY : roles;
        assert (runAs == null || runAs.runAs() == null) : "the runAs user should not be a user that can run as";
        if (runAs == SYSTEM) {
            throw new ElasticsearchSecurityException("the runAs user cannot be the internal system user");
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
     * @return The user that will be used for run as functionality. If run as
     *         functionality is not being used, then <code>null</code> will be
     *         returned
     */
    public User runAs() {
        return runAs;
    }

    public final boolean isSystem() {
        return this == SYSTEM;
    }

    @Override
    public String toString() {
        StringBuilder sb = new StringBuilder();
        sb.append("User[username=").append(username);
        sb.append(",roles=[");
        if (roles != null) {
            for (String role : roles) {
                sb.append(role).append(",");
            }
        }
        sb.append("]");
        if (runAs != null) {
            sb.append(",runAs=[").append(runAs.toString()).append("]");
        }
        sb.append("]");
        return sb.toString();
    }

    public static User readFrom(StreamInput input) throws IOException {
        if (input.readBoolean()) {
            String name = input.readString();
            if (System.NAME.equals(name)) {
                return SYSTEM;
            } else {
                throw new IllegalStateException("invalid system user");
            }
        }
        String username = input.readString();
        String[] roles = input.readStringArray();
        if (input.readBoolean()) {
            String runAsUsername = input.readString();
            String[] runAsRoles = input.readStringArray();
            return new User(username, roles, new User(runAsUsername, runAsRoles));
        }
        return new User(username, roles);
    }

    public static void writeTo(User user, StreamOutput output) throws IOException {
        if (user.isSystem()) {
            output.writeBoolean(true);
            output.writeString(System.NAME);
        } else {
            output.writeBoolean(false);
            output.writeString(user.principal());
            output.writeStringArray(user.roles());
            if (user.runAs == null) {
                output.writeBoolean(false);
            } else {
                output.writeBoolean(true);
                output.writeString(user.runAs.principal());
                output.writeStringArray(user.runAs.roles());
            }
        }
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null) return false;
        if (!(o instanceof User)) return false;

        User user = (User) o;
        if (!principal().equals(user.principal())) return false;
        if (!Arrays.equals(roles(), user.roles())) return false;
        if (runAs != null ? !runAs.equals(user.runAs) : user.runAs != null) {
            return false;
        }

        return true;
    }

    @Override
    public int hashCode() {
        int result = principal().hashCode();
        result = 31 * result + Arrays.hashCode(roles());
        result = 31 * result + (runAs != null ? runAs.hashCode() : 0);
        return result;
    }

    @Override
    public XContentBuilder toXContent(XContentBuilder builder, Params params) throws IOException {
        builder.startObject();
        builder.field("username", principal());
        builder.array("roles", roles());
        builder.endObject();
        return builder;
    }

    private static class System extends User {
        private static final String NAME = "__es_system_user";
        private static final String[] ROLES = new String[] { SystemRole.NAME };

        private System() {
            super(NAME, ROLES);
        }
    }
}
