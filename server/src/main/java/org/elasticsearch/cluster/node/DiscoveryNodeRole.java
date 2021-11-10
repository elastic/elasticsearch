/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.cluster.node;

import org.elasticsearch.Version;
import org.elasticsearch.common.settings.Setting;
import org.elasticsearch.common.settings.Setting.Property;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.util.set.Sets;
import org.elasticsearch.core.Set;
import org.elasticsearch.transport.RemoteClusterService;

import java.util.Arrays;
import java.util.Collections;
import java.util.Objects;
import java.util.SortedSet;
import java.util.TreeSet;

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
     * Whether this role is known by this node, or is an {@link DiscoveryNodeRole.UnknownRole}.
     */
    public final boolean isKnownRole() {
        return isKnownRole;
    }

    public boolean isEnabledByDefault(final Settings settings) {
        return legacySetting() != null && legacySetting().get(settings);
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

    public abstract Setting<Boolean> legacySetting();

    /**
     * When serializing a {@link DiscoveryNodeRole}, the role may not be available to nodes of
     * previous versions, where the role had not yet been added. This method allows overriding
     * the role that should be serialized when communicating to versions prior to the introduction
     * of the discovery node role.
     */
    public DiscoveryNodeRole getCompatibilityRole(Version nodeVersion) {
        return this;
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
        public Setting<Boolean> legacySetting() {
            // copy the setting here so we can mark it private in org.elasticsearch.node.Node
            return Setting.boolSetting("node.data", true, Property.Deprecated, Property.NodeScope);
        }

    };

    public static DiscoveryNodeRole DATA_CONTENT_NODE_ROLE = new DiscoveryNodeRole("data_content", "s", true) {
        @Override
        public boolean isEnabledByDefault(final Settings settings) {
            return DiscoveryNode.hasRole(settings, DiscoveryNodeRole.DATA_ROLE);
        }

        @Override
        public Setting<Boolean> legacySetting() {
            // we do not register these settings, they're not intended to be used externally, only for proper defaults
            return Setting.boolSetting("node.data_content", settings ->
            // Don't use DiscoveryNode#isDataNode(Settings) here, as it is called before all plugins are initialized
            Boolean.toString(DiscoveryNode.hasRole(settings, DiscoveryNodeRole.DATA_ROLE)),
                Setting.Property.Deprecated,
                Setting.Property.NodeScope
            );
        }

        @Override
        public DiscoveryNodeRole getCompatibilityRole(Version nodeVersion) {
            return nodeVersion.before(Version.V_7_10_0) ? DiscoveryNodeRole.DATA_ROLE : this;
        }
    };

    public static DiscoveryNodeRole DATA_HOT_NODE_ROLE = new DiscoveryNodeRole("data_hot", "h", true) {
        @Override
        public boolean isEnabledByDefault(final Settings settings) {
            return DiscoveryNode.hasRole(settings, DiscoveryNodeRole.DATA_ROLE);
        }

        @Override
        public Setting<Boolean> legacySetting() {
            // we do not register these settings, they're not intended to be used externally, only for proper defaults
            return Setting.boolSetting("node.data_hot", settings ->
            // Don't use DiscoveryNode#isDataNode(Settings) here, as it is called before all plugins are initialized
            Boolean.toString(DiscoveryNode.hasRole(settings, DiscoveryNodeRole.DATA_ROLE)),
                Setting.Property.Deprecated,
                Setting.Property.NodeScope
            );
        }

        @Override
        public DiscoveryNodeRole getCompatibilityRole(Version nodeVersion) {
            return nodeVersion.before(Version.V_7_10_0) ? DiscoveryNodeRole.DATA_ROLE : this;
        }
    };

    public static DiscoveryNodeRole DATA_WARM_NODE_ROLE = new DiscoveryNodeRole("data_warm", "w", true) {
        @Override
        public boolean isEnabledByDefault(final Settings settings) {
            return DiscoveryNode.hasRole(settings, DiscoveryNodeRole.DATA_ROLE);
        }

        @Override
        public Setting<Boolean> legacySetting() {
            // we do not register these settings, they're not intended to be used externally, only for proper defaults
            return Setting.boolSetting("node.data_warm", settings ->
            // Don't use DiscoveryNode#isDataNode(Settings) here, as it is called before all plugins are initialized
            Boolean.toString(DiscoveryNode.hasRole(settings, DiscoveryNodeRole.DATA_ROLE)),
                Setting.Property.Deprecated,
                Setting.Property.NodeScope
            );
        }

        @Override
        public DiscoveryNodeRole getCompatibilityRole(Version nodeVersion) {
            return nodeVersion.before(Version.V_7_10_0) ? DiscoveryNodeRole.DATA_ROLE : this;
        }
    };

    public static DiscoveryNodeRole DATA_COLD_NODE_ROLE = new DiscoveryNodeRole("data_cold", "c", true) {
        @Override
        public boolean isEnabledByDefault(final Settings settings) {
            return DiscoveryNode.hasRole(settings, DiscoveryNodeRole.DATA_ROLE);
        }

        @Override
        public Setting<Boolean> legacySetting() {
            // we do not register these settings, they're not intended to be used externally, only for proper defaults
            return Setting.boolSetting("node.data_cold", settings ->
            // Don't use DiscoveryNode#isDataNode(Settings) here, as it is called before all plugins are initialized
            Boolean.toString(DiscoveryNode.hasRole(settings, DiscoveryNodeRole.DATA_ROLE)),
                Setting.Property.Deprecated,
                Setting.Property.NodeScope
            );
        }

        @Override
        public DiscoveryNodeRole getCompatibilityRole(Version nodeVersion) {
            return nodeVersion.before(Version.V_7_10_0) ? DiscoveryNodeRole.DATA_ROLE : this;
        }
    };

    public static DiscoveryNodeRole DATA_FROZEN_NODE_ROLE = new DiscoveryNodeRole("data_frozen", "f", true) {
        @Override
        public boolean isEnabledByDefault(final Settings settings) {
            return DiscoveryNode.hasRole(settings, DiscoveryNodeRole.DATA_ROLE);
        }

        @Override
        public Setting<Boolean> legacySetting() {
            // we do not register these settings, they're not intended to be used externally, only for proper defaults
            return Setting.boolSetting("node.data_frozen", settings ->
            // Don't use DiscoveryNode#isDataNode(Settings) here, as it is called before all plugins are initialized
            Boolean.toString(DiscoveryNode.hasRole(settings, DiscoveryNodeRole.DATA_ROLE)),
                Setting.Property.Deprecated,
                Setting.Property.NodeScope
            );
        }

    };

    /**
     * Represents the role for an ingest node.
     */
    public static final DiscoveryNodeRole INGEST_ROLE = new DiscoveryNodeRole("ingest", "i") {

        @Override
        public Setting<Boolean> legacySetting() {
            // copy the setting here so we can mark it private in org.elasticsearch.node.Node
            return Setting.boolSetting("node.ingest", true, Property.Deprecated, Property.NodeScope);
        }

    };

    /**
     * Represents the role for a master-eligible node.
     */
    public static final DiscoveryNodeRole MASTER_ROLE = new DiscoveryNodeRole("master", "m") {

        @Override
        public Setting<Boolean> legacySetting() {
            // copy the setting here so we can mark it private in org.elasticsearch.node.Node
            return Setting.boolSetting("node.master", true, Property.Deprecated, Property.NodeScope);
        }

    };

    public static final DiscoveryNodeRole REMOTE_CLUSTER_CLIENT_ROLE = new DiscoveryNodeRole("remote_cluster_client", "r") {

        @Override
        public Setting<Boolean> legacySetting() {
            // copy the setting here so we can mark it private in org.elasticsearch.node.Node
            return Setting.boolSetting(
                "node.remote_cluster_client",
                RemoteClusterService.ENABLE_REMOTE_CLUSTERS,
                Property.Deprecated,
                Property.NodeScope
            );
        }

    };

    /**
     * The built-in node roles.
     */
    public static final SortedSet<DiscoveryNodeRole> BUILT_IN_ROLES = Set.of(
        DATA_ROLE,
        INGEST_ROLE,
        MASTER_ROLE,
        REMOTE_CLUSTER_CLIENT_ROLE,
        DATA_CONTENT_NODE_ROLE,
        DATA_HOT_NODE_ROLE,
        DATA_WARM_NODE_ROLE,
        DATA_COLD_NODE_ROLE,
        DATA_FROZEN_NODE_ROLE
    ).stream().collect(Sets.toUnmodifiableSortedSet());

    /**
     * The version that {@link #REMOTE_CLUSTER_CLIENT_ROLE} is introduced. Nodes before this version do not have that role even
     * they can connect to remote clusters.
     */
    public static final Version REMOTE_CLUSTER_CLIENT_ROLE_VERSION = Version.V_7_8_0;

    static SortedSet<DiscoveryNodeRole> LEGACY_ROLES = Collections.unmodifiableSortedSet(
        new TreeSet<>(Arrays.asList(DATA_ROLE, INGEST_ROLE, MASTER_ROLE))
    );

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

        @Override
        public Setting<Boolean> legacySetting() {
            // since this setting is not registered, it will always return false when testing if the local node has the role
            assert false;
            return Setting.boolSetting("node. " + roleName(), false, Setting.Property.NodeScope);
        }

    }

}
