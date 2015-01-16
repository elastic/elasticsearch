/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.shield;

import org.elasticsearch.common.Strings;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.shield.authz.SystemRole;

import java.io.IOException;
import java.util.Arrays;

/**
 * An authenticated user
 */
public abstract class User {

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

    public final boolean isSystem() {
        return this == SYSTEM;
    }

    public static User readFrom(StreamInput input) throws IOException {
        if (input.readBoolean()) {
            String name = input.readString();
            if (!System.NAME.equals(name)) {
                throw new ShieldException("invalid system user");
            }
            return SYSTEM;
        }
        return new Simple(input.readString(), input.readStringArray());
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
    }

    public static class Simple extends User {

        private final String username;
        private final String[] roles;

        public Simple(String username, String... roles) {
            this.username = username;
            this.roles = roles == null ? Strings.EMPTY_ARRAY : roles;
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
        public boolean equals(Object o) {
            if (this == o) return true;
            if (o == null || getClass() != o.getClass()) return false;

            Simple simple = (Simple) o;

            if (!Arrays.equals(roles, simple.roles)) return false;
            if (!username.equals(simple.username)) return false;

            return true;
        }

        @Override
        public int hashCode() {
            int result = username.hashCode();
            result = 31 * result + Arrays.hashCode(roles);
            return result;
        }
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

    }
}
