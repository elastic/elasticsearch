/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.cluster.node;

import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.util.set.Sets;

import java.lang.reflect.Field;
import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.SortedSet;
import java.util.function.Function;
import java.util.stream.Collectors;

/**
 * Represents a node role.
 */
public class DiscoveryNodeRole implements Comparable<DiscoveryNodeRole> {

    private final String roleName;

    /**
     * The name of the role.
     *
     * @return the role name
     */
    public final String roleName() {
        return roleName;
    }

    private final String roleNameAbbreviation;

    /**
     * The abbreviation of the name of the role. This is used in the cat nodes API to display an abbreviated version of the name of the
     * role.
     *
     * @return the role name abbreviation
     */
    public final String roleNameAbbreviation() {
        return roleNameAbbreviation;
    }

    private final boolean canContainData;

    /**
     * Indicates whether a node with this role can contain data.
     *
     * @return true if a node with this role can contain data, otherwise false
     */
    public final boolean canContainData() {
        return canContainData;
    }

    private final boolean isKnownRole;

    /**
     * Whether or not the role is enabled by default given the specified settings
     *
     * @param settings the settings instance
     * @return true if the role is enabled by default given the specified settings, otherwise false
     */
    public boolean isEnabledByDefault(final Settings settings) {
        return true;
    }

    /**
     * Validate this role against all configured roles. Implementors are expected to throw an {@link IllegalArgumentException} when the
     * combination of configured roles is invalid with this role.
     *
     * @param roles the complete set of configured roles
     */
    public void validateRoles(final List<DiscoveryNodeRole> roles) {

    }

    protected DiscoveryNodeRole(final String roleName, final String roleNameAbbreviation) {
        this(roleName, roleNameAbbreviation, false);
    }

    protected DiscoveryNodeRole(final String roleName, final String roleNameAbbreviation, final boolean canContainData) {
        this(true, roleName, roleNameAbbreviation, canContainData);
    }

    private DiscoveryNodeRole(
        final boolean isKnownRole,
        final String roleName,
        final String roleNameAbbreviation,
        final boolean canContainData
    ) {
        this.isKnownRole = isKnownRole;
        this.roleName = Objects.requireNonNull(roleName);
        this.roleNameAbbreviation = Objects.requireNonNull(roleNameAbbreviation);
        this.canContainData = canContainData;
    }

    @Override
    public final boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        DiscoveryNodeRole that = (DiscoveryNodeRole) o;
        return roleName.equals(that.roleName)
            && roleNameAbbreviation.equals(that.roleNameAbbreviation)
            && canContainData == that.canContainData
            && isKnownRole == that.isKnownRole;
    }

    @Override
    public final int hashCode() {
        return Objects.hash(isKnownRole, roleName(), roleNameAbbreviation(), canContainData());
    }

    @Override
    public final int compareTo(final DiscoveryNodeRole o) {
        return roleName.compareTo(o.roleName);
    }

    @Override
    public final String toString() {
        return "DiscoveryNodeRole{"
            + "roleName='"
            + roleName
            + '\''
            + ", roleNameAbbreviation='"
            + roleNameAbbreviation
            + '\''
            + ", canContainData="
            + canContainData
            + (isKnownRole ? "" : ", isKnownRole=false")
            + '}';
    }

    /**
     * Represents the role for a data node.
     */
    public static final DiscoveryNodeRole DATA_ROLE = new DiscoveryNodeRole("data", "d", true) {

        @Override
        public boolean isEnabledByDefault(Settings settings) {
            return DiscoveryNode.isStateless(settings) == false;
        }
    };

    /**
     * Represents the role for a content node.
     */
    public static final DiscoveryNodeRole DATA_CONTENT_NODE_ROLE = new DiscoveryNodeRole("data_content", "s", true) {

        @Override
        public boolean isEnabledByDefault(final Settings settings) {
            return DiscoveryNode.hasRole(settings, DiscoveryNodeRole.DATA_ROLE);
        }
    };

    /**
     * Represents the role for a hot node.
     */
    public static final DiscoveryNodeRole DATA_HOT_NODE_ROLE = new DiscoveryNodeRole("data_hot", "h", true) {

        @Override
        public boolean isEnabledByDefault(final Settings settings) {
            return DiscoveryNode.hasRole(settings, DiscoveryNodeRole.DATA_ROLE);
        }
    };

    /**
     * Represents the role for a warm node.
     */
    public static final DiscoveryNodeRole DATA_WARM_NODE_ROLE = new DiscoveryNodeRole("data_warm", "w", true) {

        @Override
        public boolean isEnabledByDefault(final Settings settings) {
            return DiscoveryNode.hasRole(settings, DiscoveryNodeRole.DATA_ROLE);
        }
    };

    /**
     * Represents the role for a cold node.
     */
    public static final DiscoveryNodeRole DATA_COLD_NODE_ROLE = new DiscoveryNodeRole("data_cold", "c", true) {

        @Override
        public boolean isEnabledByDefault(final Settings settings) {
            return DiscoveryNode.hasRole(settings, DiscoveryNodeRole.DATA_ROLE);
        }
    };

    /**
     * Represents the role for a frozen node.
     */
    public static final DiscoveryNodeRole DATA_FROZEN_NODE_ROLE = new DiscoveryNodeRole("data_frozen", "f", true) {

        @Override
        public boolean isEnabledByDefault(final Settings settings) {
            return DiscoveryNode.hasRole(settings, DiscoveryNodeRole.DATA_ROLE);
        }
    };

    /**
     * Represents the role for an ingest node.
     */
    public static final DiscoveryNodeRole INGEST_ROLE = new DiscoveryNodeRole("ingest", "i");

    /**
     * Represents the role for a master-eligible node.
     */
    public static final DiscoveryNodeRole MASTER_ROLE = new DiscoveryNodeRole("master", "m");

    /**
     * Represents the role for a voting-only node.
     */
    public static final DiscoveryNodeRole VOTING_ONLY_NODE_ROLE = new DiscoveryNodeRole("voting_only", "v") {

        @Override
        public boolean isEnabledByDefault(final Settings settings) {
            return false;
        }

        @Override
        public void validateRoles(final List<DiscoveryNodeRole> roles) {
            if (roles.contains(MASTER_ROLE) == false) {
                throw new IllegalArgumentException("voting-only node must be master-eligible");
            }
        }

    };

    /**
     * Represents the role for a node that can be a remote cluster client.
     */
    public static final DiscoveryNodeRole REMOTE_CLUSTER_CLIENT_ROLE = new DiscoveryNodeRole("remote_cluster_client", "r");

    /**
     * Represents the role for a machine learning node.
     */
    public static final DiscoveryNodeRole ML_ROLE = new DiscoveryNodeRole("ml", "l");

    /**
     * Represents the role for a transform node.
     */
    public static final DiscoveryNodeRole TRANSFORM_ROLE = new DiscoveryNodeRole("transform", "t");

    /**
     * Represents the role for an index node.
     */
    public static final DiscoveryNodeRole INDEX_ROLE = new DiscoveryNodeRole("index", "I", true) {

        @Override
        public boolean isEnabledByDefault(Settings settings) {
            return DiscoveryNode.isStateless(settings);
        }
    };

    /**
     * Represents the role for a search node.
     */
    public static final DiscoveryNodeRole SEARCH_ROLE = new DiscoveryNodeRole("search", "S", true) {

        public boolean isEnabledByDefault(Settings settings) {
            return false;
        }
    };

    /**
     * Represents an unknown role. This can occur if a newer version adds a role that an older version does not know about, or a newer
     * version removes a role that an older version knows about.
     */
    static class UnknownRole extends DiscoveryNodeRole {

        /**
         * Construct an unknown role with the specified role name and role name abbreviation.
         *
         * @param roleName             the role name
         * @param roleNameAbbreviation the role name abbreviation
         * @param canContainData       whether or not nodes with the role can contain data
         */
        UnknownRole(final String roleName, final String roleNameAbbreviation, final boolean canContainData) {
            super(false, roleName, roleNameAbbreviation, canContainData);
        }

    }

    // the set of possible roles
    private static final SortedSet<DiscoveryNodeRole> ROLES;

    // a map from role names to their role representations
    private static final Map<String, DiscoveryNodeRole> ROLE_MAP;

    static {
        final List<Field> roleFields = Arrays.stream(DiscoveryNodeRole.class.getFields())
            .filter(f -> f.getType().equals(DiscoveryNodeRole.class))
            .toList();
        // this will detect duplicate role names
        final Map<String, DiscoveryNodeRole> roleMap = roleFields.stream().map(f -> {
            try {
                return (DiscoveryNodeRole) f.get(null);
            } catch (final IllegalAccessException e) {
                throw new AssertionError(e);
            }
        }).collect(Collectors.toUnmodifiableMap(DiscoveryNodeRole::roleName, Function.identity()));
        assert roleMap.size() == roleFields.size() : "roles by name [" + roleMap + "], role fields [" + roleFields + "]";
        // now we can collect the roles, don't do this first and then collect the role map because the set collector will allow duplicates
        final SortedSet<DiscoveryNodeRole> roles = roleMap.values().stream().collect(Sets.toUnmodifiableSortedSet());
        // this will detect duplicate role abbreviations
        final Map<String, DiscoveryNodeRole> abbreviations = roles.stream()
            .collect(Collectors.toUnmodifiableMap(DiscoveryNodeRole::roleNameAbbreviation, Function.identity()));
        assert abbreviations.size() == roleFields.size() : "role abbreviations [" + abbreviations + "], role fields [" + roleFields + "]";
        ROLES = roles;
        ROLE_MAP = roleMap;
    }

    /**
     * The possible node roles.
     *
     * @return an ordered, immutable set of possible roles
     */
    public static SortedSet<DiscoveryNodeRole> roles() {
        return ROLES;
    }

    /**
     * The set of possible role names.
     *
     * @return an ordered, immutable set of possible role names
     */
    public static SortedSet<String> roleNames() {
        return ROLE_MAP.keySet().stream().collect(Sets.toUnmodifiableSortedSet());
    }

    /**
     * Get an optional representing the role with the given role name, if such a role exists.
     *
     * @param roleName the role name to get the associated role representation for
     * @return an optional node role
     */
    public static Optional<DiscoveryNodeRole> maybeGetRoleFromRoleName(final String roleName) {
        return Optional.ofNullable(ROLE_MAP.get(roleName));
    }

    /**
     * Get a representation of the role with the given role name, if such a role exists, otherwise an exception is thrown.
     *
     * @param roleName the role name to get the associated role representation for
     * @return a node role
     *
     * @throws IllegalArgumentException if no node role with the given role name exists
     */
    public static DiscoveryNodeRole getRoleFromRoleName(final String roleName) {
        return maybeGetRoleFromRoleName(roleName).orElseThrow(() -> new IllegalArgumentException("unknown role [" + roleName + "]"));
    }
}
