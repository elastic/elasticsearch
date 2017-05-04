/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.xpack.security.authc.support;

import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;

import org.elasticsearch.action.ActionListener;
import org.elasticsearch.common.Nullable;
import org.elasticsearch.xpack.security.authc.RealmConfig;

/**
 * Where a realm users an authentication method that does not have in-built support for X-Pack
 * {@link org.elasticsearch.xpack.security.authz.permission.Role roles}, it may delegate to an implementation of this class the
 * responsibility for determining the set roles that an authenticated user should have.
 */
public interface UserRoleMapper {
    /**
     * Determines the set of roles that should be applied to <code>user</code>.
     */
    void resolveRoles(UserData user, ActionListener<Set<String>> listener);

    /**
     * Informs the mapper that the provided <code>realm</code> should be refreshed when
     * the set of role-mappings change. The realm may be updated for the local node only, or across
     * the whole cluster depending on whether this role-mapper has node-local data or cluster-wide
     * data.
     */
    void refreshRealmOnChange(CachingUsernamePasswordRealm realm);

    /**
     * A representation of a user for whom roles should be mapped.
     * The user has been authenticated, but does not yet have any roles.
     */
    class UserData {
        private final String username;
        @Nullable
        private final String dn;
        private final Set<String> groups;
        private final Map<String, Object> metadata;
        private final RealmConfig realm;

        public UserData(String username, @Nullable String dn, Collection<String> groups,
                        Map<String, Object> metadata, RealmConfig realm) {
            this.username = username;
            this.dn = dn;
            this.groups = groups == null || groups.isEmpty()
                    ? Collections.emptySet() : Collections.unmodifiableSet(new HashSet<>(groups));
            this.metadata = metadata == null || metadata.isEmpty()
                    ? Collections.emptyMap() : Collections.unmodifiableMap(metadata);
            this.realm = realm;
        }

        /**
         * Formats the user data as a <code>Map</code>.
         * The map is <em>not</em> nested - all values are simple Java values, but keys may
         * contain <code>.</code>.
         * For example, the {@link #metadata} values will be stored in the map with a key of
         * <code>"metadata.KEY"</code> where <code>KEY</code> is the key from the metadata object.
         */
        public Map<String, Object> asMap() {
            final Map<String, Object> map = new HashMap<>();
            map.put("username", username);
            map.put("dn", dn);
            map.put("groups", groups);
            metadata.keySet().forEach(k -> map.put("metadata." + k, metadata.get(k)));
            map.put("realm.name", realm.name());
            return map;
        }

        @Override
        public String toString() {
            return "UserData{" +
                    "username:" + username +
                    "; dn:" + dn +
                    "; groups:" + groups +
                    "; metadata:" + metadata +
                    "; realm=" + realm.name() +
                    '}';
        }

        /**
         * The username for the authenticated user.
         */
        public String getUsername() {
            return username;
        }

        /**
         * The <em>distinguished name</em> of the authenticated user, if applicable to the
         * authentication method used. Otherwise, <code>null</code>.
         */
        @Nullable
        public String getDn() {
            return dn;
        }

        /**
         * The groups to which the user belongs in the originating user store. Should be empty
         * if the user store or authentication method does not support groups.
         */
        public Set<String> getGroups() {
            return groups;
        }

        /**
         * Any additional metadata that was provided at authentication time. The set of keys will
         * vary according to the authenticating realm.
         */
        public Map<String, Object> getMetadata() {
            return metadata;
        }

        /**
         * The realm that authenticated the user.
         */
        public RealmConfig getRealm() {
            return realm;
        }
    }
}
