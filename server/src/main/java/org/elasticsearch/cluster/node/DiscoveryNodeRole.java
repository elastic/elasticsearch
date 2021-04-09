/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.cluster.node;

import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.util.set.Sets;

import java.lang.reflect.Field;
import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.Objects;
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
        return roleName.equals(that.roleName) &&
            roleNameAbbreviation.equals(that.roleNameAbbreviation) &&
            canContainData == that.canContainData &&
            isKnownRole == that.isKnownRole;
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
        return "DiscoveryNodeRole{" +
                "roleName='" + roleName + '\'' +
                ", roleNameAbbreviation='" + roleNameAbbreviation + '\'' +
                ", canContainData=" + canContainData +
                (isKnownRole ? "" : ", isKnownRole=false") +
                '}';
    }

    /**
     * Represents the role for a data node.
     */
    public static final DiscoveryNodeRole DATA_ROLE = new DiscoveryNodeRole("data", "d", true);

    public static DiscoveryNodeRole DATA_CONTENT_NODE_ROLE = new DiscoveryNodeRole("data_content", "s", true) {

        @Override
        public boolean isEnabledByDefault(final Settings settings) {
            return DiscoveryNode.hasRole(settings, DiscoveryNodeRole.DATA_ROLE);
        }

    };

    public static DiscoveryNodeRole DATA_HOT_NODE_ROLE = new DiscoveryNodeRole("data_hot", "h", true) {

        @Override
        public boolean isEnabledByDefault(final Settings settings) {
            return DiscoveryNode.hasRole(settings, DiscoveryNodeRole.DATA_ROLE);
        }

    };

    public static DiscoveryNodeRole DATA_WARM_NODE_ROLE = new DiscoveryNodeRole("data_warm", "w", true) {

        @Override
        public boolean isEnabledByDefault(final Settings settings) {
            return DiscoveryNode.hasRole(settings, DiscoveryNodeRole.DATA_ROLE);
        }

    };

    public static DiscoveryNodeRole DATA_COLD_NODE_ROLE = new DiscoveryNodeRole("data_cold", "c", true) {

        @Override
        public boolean isEnabledByDefault(final Settings settings) {
            return DiscoveryNode.hasRole(settings, DiscoveryNodeRole.DATA_ROLE);
        }

    };

    public static DiscoveryNodeRole DATA_FROZEN_NODE_ROLE = new DiscoveryNodeRole("data_frozen", "f", true) {

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

    public static final DiscoveryNodeRole REMOTE_CLUSTER_CLIENT_ROLE = new DiscoveryNodeRole("remote_cluster_client", "r");

    public static final DiscoveryNodeRole ML_ROLE = new DiscoveryNodeRole("ml", "l");

    public static final DiscoveryNodeRole TRANSFORM_ROLE = new DiscoveryNodeRole("transform", "t");

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

    /**
     * The possible node roles.
     */
    public static final SortedSet<DiscoveryNodeRole> ROLES;

    /**
     * A map from role names to their role representations.
     */
    public static final Map<String, DiscoveryNodeRole> ROLE_MAP;

    static {
        final List<Field> roleFields = Arrays.stream(DiscoveryNodeRole.class.getFields())
            .filter(f -> f.getType().equals(DiscoveryNodeRole.class))
            .collect(Collectors.toUnmodifiableList());
        // this will detect duplicate role names
        final Map<String, DiscoveryNodeRole> roleMap = roleFields.stream()
            .map(f -> {
                try {
                    return (DiscoveryNodeRole) f.get(null);
                } catch (final IllegalAccessException e) {
                    throw new AssertionError(e);
                }
            })
            .collect(Collectors.toMap(DiscoveryNodeRole::roleName, Function.identity()));
        assert roleMap.size() == roleFields.size() :
            "roles by name [" + roleMap + "], role fields [" + roleFields + "]";
        final SortedSet<DiscoveryNodeRole> builtInRoles = roleMap.values().stream().collect(Sets.toUnmodifiableSortedSet());
        // collect the abbreviation names into a map to ensure that there are not any duplicate abbreviations
        final Map<String, DiscoveryNodeRole> abbreviations = builtInRoles
            .stream()
            .collect(Collectors.toUnmodifiableMap(DiscoveryNodeRole::roleNameAbbreviation, Function.identity()));
        assert roleMap.size() == abbreviations.size() :
            "roles by name [" + roleMap + "], roles by name abbreviation [" + abbreviations + "]";
        ROLES = builtInRoles;
        ROLE_MAP = Map.copyOf(roleMap); // this ensures ROLE_MAP is immutable
    }

    public static DiscoveryNodeRole getRoleFromRoleName(final String roleName) {
        if (ROLE_MAP.containsKey(roleName) == false) {
            throw new IllegalArgumentException("unknown role [" + roleName + "]");
        }
        return ROLE_MAP.get(roleName);
    }

}
