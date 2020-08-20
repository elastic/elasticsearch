/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */

package org.elasticsearch.xpack.core;

import org.elasticsearch.cluster.node.DiscoveryNode;
import org.elasticsearch.cluster.node.DiscoveryNodeRole;
import org.elasticsearch.common.settings.Setting;
import org.elasticsearch.common.settings.Settings;

/**
 * The {@code DataTier} class encapsulates the formalization of the "hot",
 * "warm", "cold", and "frozen" tiers as node roles. In contains the roles
 * themselves as well as helpers for validation and determining if a node has
 * a tier configured.
 *
 * Related:
 * {@link org.elasticsearch.xpack.cluster.routing.allocation.DataTierAllocationDecider}
 */
public class DataTier {

    public static final String DATA_HOT = "data_hot";
    public static final String DATA_WARM = "data_warm";
    public static final String DATA_COLD = "data_cold";
    public static final String DATA_FROZEN = "data_frozen";

    /**
     * Returns true if the given tier name is a valid tier
     */
    public static boolean validTierName(String tierName) {
        return DATA_HOT.equals(tierName) ||
            DATA_WARM.equals(tierName) ||
            DATA_COLD.equals(tierName) ||
            DATA_FROZEN.equals(tierName);
    }

    /**
     * Returns true iff the given settings have a data tier setting configured
     */
    public static boolean isExplicitDataTier(Settings settings) {
        /*
         * This method can be called before the o.e.n.NodeRoleSettings.NODE_ROLES_SETTING is
         * initialized. We do not want to trigger initialization prematurely because that will bake
         *  the default roles before plugins have had a chance to register them. Therefore,
         * to avoid initializing this setting prematurely, we avoid using the actual node roles
         * setting instance here in favor of the string.
         */
        if (settings.hasValue("node.roles")) {
            return settings.getAsList("node.roles").stream().anyMatch(DataTier::validTierName);
        }
        return false;
    }

    public static DiscoveryNodeRole DATA_HOT_NODE_ROLE = new DiscoveryNodeRole("data_hot", "h") {
        @Override
        public boolean isEnabledByDefault(final Settings settings) {
            return false;
        }

        @Override
        public Setting<Boolean> legacySetting() {
            return null;
        }

        @Override
        public boolean canContainData() {
            return true;
        }
    };

    public static DiscoveryNodeRole DATA_WARM_NODE_ROLE = new DiscoveryNodeRole("data_warm", "w") {
        @Override
        public boolean isEnabledByDefault(final Settings settings) {
            return false;
        }

        @Override
        public Setting<Boolean> legacySetting() {
            return null;
        }

        @Override
        public boolean canContainData() {
            return true;
        }
    };

    public static DiscoveryNodeRole DATA_COLD_NODE_ROLE = new DiscoveryNodeRole("data_cold", "c") {
        @Override
        public boolean isEnabledByDefault(final Settings settings) {
            return false;
        }

        @Override
        public Setting<Boolean> legacySetting() {
            return null;
        }

        @Override
        public boolean canContainData() {
            return true;
        }
    };

    public static DiscoveryNodeRole DATA_FROZEN_NODE_ROLE = new DiscoveryNodeRole("data_frozen", "f") {
        @Override
        public boolean isEnabledByDefault(final Settings settings) {
            return false;
        }

        @Override
        public Setting<Boolean> legacySetting() {
            return null;
        }

        @Override
        public boolean canContainData() {
            return true;
        }
    };

    public static boolean isHotNode(DiscoveryNode discoveryNode) {
        return discoveryNode.getRoles().contains(DATA_HOT_NODE_ROLE) || discoveryNode.getRoles().contains(DiscoveryNodeRole.DATA_ROLE);
    }

    public static boolean isWarmNode(DiscoveryNode discoveryNode) {
        return discoveryNode.getRoles().contains(DATA_WARM_NODE_ROLE) || discoveryNode.getRoles().contains(DiscoveryNodeRole.DATA_ROLE);
    }

    public static boolean isColdNode(DiscoveryNode discoveryNode) {
        return discoveryNode.getRoles().contains(DATA_COLD_NODE_ROLE) || discoveryNode.getRoles().contains(DiscoveryNodeRole.DATA_ROLE);
    }

    public static boolean isFrozenNode(DiscoveryNode discoveryNode) {
        return discoveryNode.getRoles().contains(DATA_FROZEN_NODE_ROLE) || discoveryNode.getRoles().contains(DiscoveryNodeRole.DATA_ROLE);
    }
}
