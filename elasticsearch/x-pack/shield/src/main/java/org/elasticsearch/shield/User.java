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
public abstract class User implements ToXContent {

    public static final User SYSTEM = new System();

    /**
     * @return  The principal of this user - effectively serving as the unique identity of of the user.
     */
    public abstract String principal();

    /**
     * @return  The roles this user is associated with. The roles are identified by their unique names
     *          and each represents as set of permissions
     */
    public abstract String[] roles();

    /**
     * @return The user that will be used for run as functionality. If run as functionality is not being
     *         used, then <code>null</code> will be returned
     */
    public abstract User runAs();

    public final boolean isSystem() {
        return this == SYSTEM;
    }

    public static User readFrom(StreamInput input) throws IOException {
        if (input.readBoolean()) {
            String name = input.readString();
            if (!System.NAME.equals(name)) {
                throw new IllegalStateException("invalid system user");
            }
            return SYSTEM;
        }
        String username = input.readString();
        String[] roles = input.readStringArray();
        if (input.readBoolean()) {
            String runAsUsername = input.readString();
            String[] runAsRoles = input.readStringArray();
            return new Simple(username, roles, new Simple(runAsUsername, runAsRoles));
        }
        return new Simple(username, roles);
    }

    public static void writeTo(User user, StreamOutput output) throws IOException {
        if (user.isSystem()) {
            output.writeBoolean(true);
            output.writeString(System.NAME);
            return;
        }
        output.writeBoolean(false);
        Simple simple = (Simple) user;
        output.writeString(simple.username);
        output.writeStringArray(simple.roles);
        if (simple.runAs == null) {
            output.writeBoolean(false);
        } else {
            output.writeBoolean(true);
            output.writeString(simple.runAs.principal());
            output.writeStringArray(simple.runAs.roles());
        }
    }

    public static class Simple extends User {

        private final String username;
        private final String[] roles;
        private final User runAs;

        public Simple(String username, String[] roles) {
            this(username, roles, null);
        }

        public Simple(String username, String[] roles, User runAs) {
            this.username = username;
            this.roles = roles == null ? Strings.EMPTY_ARRAY : roles;
            assert (runAs == null || runAs.runAs() == null) : "the runAs user should not be a user that can run as";
            if (runAs == SYSTEM) {
                throw new ElasticsearchSecurityException("the runAs user cannot be the internal system user");
            }
            this.runAs = runAs;
        }

        @Override
        public String principal() {
            return username;
        }

        @Override
        public String[] roles() {
            return roles;
        }

        @Override
        public User runAs() {
            return runAs;
        }

        @Override
        public boolean equals(Object o) {
            if (this == o) return true;
            if (o == null || getClass() != o.getClass()) return false;

            Simple simple = (Simple) o;

            if (username != null ? !username.equals(simple.username) : simple.username != null) {
                return false;
            }
            if (!Arrays.equals(roles, simple.roles)) {
                return false;
            }
            if (runAs != null ? !runAs.equals(simple.runAs) : simple.runAs != null) {
                return false;
            }

            return true;
        }

        @Override
        public int hashCode() {
            int result = username != null ? username.hashCode() : 0;
            result = 31 * result + Arrays.hashCode(roles);
            result = 31 * result + (runAs != null ? runAs.hashCode() : 0);
            return result;
        }
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
        }

        @Override
        public String principal() {
            return NAME;
        }

        @Override
        public String[] roles() {
            return ROLES;
        }

        @Override
        public User runAs() {
            return null;
        }
    }
}
