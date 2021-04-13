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

import java.util.List;
import java.util.Objects;
import java.util.Set;
import java.util.SortedSet;

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

    public static DiscoveryNodeRole VOTING_ONLY_NODE_ROLE = new DiscoveryNodeRole("voting_only", "v") {

        @Override
        public boolean isEnabledByDefault(final Settings settings) {
            return false;
        }

        @Override
        public void validateRoles(final List<DiscoveryNodeRole> roles) {
            if (roles.contains(DiscoveryNodeRole.MASTER_ROLE) == false) {
                throw new IllegalArgumentException("voting-only node must be master-eligible");
            }
        }

    };

    /**
     * The built-in node roles.
     */
    public static final SortedSet<DiscoveryNodeRole> BUILT_IN_ROLES =
        Set.of(
            DATA_ROLE,
            INGEST_ROLE,
            MASTER_ROLE,
            REMOTE_CLUSTER_CLIENT_ROLE,
            DATA_CONTENT_NODE_ROLE,
            DATA_HOT_NODE_ROLE,
            DATA_WARM_NODE_ROLE,
            DATA_COLD_NODE_ROLE,
            DATA_FROZEN_NODE_ROLE,
            ML_ROLE,
            TRANSFORM_ROLE,
            VOTING_ONLY_NODE_ROLE
        ).stream().collect(Sets.toUnmodifiableSortedSet());

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

}
