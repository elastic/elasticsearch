/*
 * Licensed to Elasticsearch under one or more contributor
 * license agreements. See the NOTICE file distributed with
 * this work for additional information regarding copyright
 * ownership. Elasticsearch licenses this file to you under
 * the Apache License, Version 2.0 (the "License"); you may
 * not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package org.elasticsearch.cluster.node;

import org.elasticsearch.common.settings.Setting;
import org.elasticsearch.common.util.set.Sets;
import org.elasticsearch.node.Node;

import java.util.Objects;
import java.util.Set;
import java.util.SortedSet;

/**
 * Represents a node role.
 */
public abstract class DiscoveryNodeRole implements Comparable<DiscoveryNodeRole> {

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

    private final boolean isKnownRole;

    protected DiscoveryNodeRole(final String roleName, final String roleNameAbbreviation) {
        this(true, roleName, roleNameAbbreviation);
    }

    private DiscoveryNodeRole(final boolean isKnownRole, final String roleName, final String roleNameAbbreviation) {
        this.isKnownRole = isKnownRole;
        this.roleName = Objects.requireNonNull(roleName);
        this.roleNameAbbreviation = Objects.requireNonNull(roleNameAbbreviation);
    }

    protected abstract Setting<Boolean> roleSetting();

    @Override
    public final boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        DiscoveryNodeRole that = (DiscoveryNodeRole) o;
        return roleName.equals(that.roleName) &&
            roleNameAbbreviation.equals(that.roleNameAbbreviation) &&
            isKnownRole == that.isKnownRole;
    }

    @Override
    public final int hashCode() {
        return Objects.hash(isKnownRole, roleName(), roleNameAbbreviation());
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
                (isKnownRole ? "" : ", isKnownRole=false") +
                '}';
    }

    /**
     * Represents the role for a data node.
     */
    public static final DiscoveryNodeRole DATA_ROLE = new DiscoveryNodeRole("data", "d") {

        @Override
        protected Setting<Boolean> roleSetting() {
            return Node.NODE_DATA_SETTING;
        }

    };

    /**
     * Represents the role for an ingest node.
     */
    public static final DiscoveryNodeRole INGEST_ROLE = new DiscoveryNodeRole("ingest", "i") {

        @Override
        protected Setting<Boolean> roleSetting() {
            return Node.NODE_INGEST_SETTING;
        }

    };

    /**
     * Represents the role for a master-eligible node.
     */
    public static final DiscoveryNodeRole MASTER_ROLE = new DiscoveryNodeRole("master", "m") {

        @Override
        protected Setting<Boolean> roleSetting() {
            return Node.NODE_MASTER_SETTING;
        }

    };

    public static final DiscoveryNodeRole REMOTE_CLUSTER_CLIENT_ROLE = new DiscoveryNodeRole("remote_cluster_client", "r") {

        @Override
        protected Setting<Boolean> roleSetting() {
            return Node.NODE_REMOTE_CLUSTER_CLIENT;
        }

    };

    /**
     * The built-in node roles.
     */
    public static final SortedSet<DiscoveryNodeRole> BUILT_IN_ROLES =
        Set.of(DATA_ROLE, INGEST_ROLE, MASTER_ROLE, REMOTE_CLUSTER_CLIENT_ROLE).stream().collect(Sets.toUnmodifiableSortedSet());

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
         */
        UnknownRole(final String roleName, final String roleNameAbbreviation) {
            super(false, roleName, roleNameAbbreviation);
        }

        @Override
        protected Setting<Boolean> roleSetting() {
            // since this setting is not registered, it will always return false when testing if the local node has the role
            assert false;
            return Setting.boolSetting("node. " + roleName(), false, Setting.Property.NodeScope);
        }

    }

}
