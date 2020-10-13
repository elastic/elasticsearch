/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */

package org.elasticsearch.xpack.core;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.elasticsearch.Version;
import org.elasticsearch.cluster.metadata.IndexMetadata;
import org.elasticsearch.cluster.node.DiscoveryNode;
import org.elasticsearch.cluster.node.DiscoveryNodeRole;
import org.elasticsearch.common.settings.Setting;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.index.shard.IndexSettingProvider;
import org.elasticsearch.xpack.cluster.routing.allocation.DataTierAllocationDecider;

import java.util.Arrays;
import java.util.HashSet;
import java.util.Set;

/**
 * The {@code DataTier} class encapsulates the formalization of the "content",
 * "hot", "warm", and "cold" tiers as node roles. In contains the
 * roles themselves as well as helpers for validation and determining if a node
 * has a tier configured.
 *
 * Related:
 * {@link org.elasticsearch.xpack.cluster.routing.allocation.DataTierAllocationDecider}
 */
public class DataTier {

    public static final String DATA_CONTENT = "data_content";
    public static final String DATA_HOT = "data_hot";
    public static final String DATA_WARM = "data_warm";
    public static final String DATA_COLD = "data_cold";

    public static final Set<String> ALL_DATA_TIERS = new HashSet<>(Arrays.asList(DATA_CONTENT, DATA_HOT, DATA_WARM, DATA_COLD));

    /**
     * Returns true if the given tier name is a valid tier
     */
    public static boolean validTierName(String tierName) {
        return DATA_CONTENT.equals(tierName) ||
            DATA_HOT.equals(tierName) ||
            DATA_WARM.equals(tierName) ||
            DATA_COLD.equals(tierName);
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

    public static DiscoveryNodeRole DATA_CONTENT_NODE_ROLE = new DiscoveryNodeRole("data_content", "s") {
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

        @Override
        public DiscoveryNodeRole getCompatibilityRole(Version nodeVersion) {
            return nodeVersion.before(Version.V_7_10_0) ? DiscoveryNodeRole.DATA_ROLE : this;
        }
    };

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

        @Override
        public DiscoveryNodeRole getCompatibilityRole(Version nodeVersion) {
            return nodeVersion.before(Version.V_7_10_0) ? DiscoveryNodeRole.DATA_ROLE : this;
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

        @Override
        public DiscoveryNodeRole getCompatibilityRole(Version nodeVersion) {
            return nodeVersion.before(Version.V_7_10_0) ? DiscoveryNodeRole.DATA_ROLE : this;
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

        @Override
        public DiscoveryNodeRole getCompatibilityRole(Version nodeVersion) {
            return nodeVersion.before(Version.V_7_10_0) ? DiscoveryNodeRole.DATA_ROLE : this;
        }
    };

    public static boolean isContentNode(DiscoveryNode discoveryNode) {
        return discoveryNode.getRoles().contains(DATA_CONTENT_NODE_ROLE) || discoveryNode.getRoles().contains(DiscoveryNodeRole.DATA_ROLE);
    }

    public static boolean isHotNode(DiscoveryNode discoveryNode) {
        return discoveryNode.getRoles().contains(DATA_HOT_NODE_ROLE) || discoveryNode.getRoles().contains(DiscoveryNodeRole.DATA_ROLE);
    }

    public static boolean isWarmNode(DiscoveryNode discoveryNode) {
        return discoveryNode.getRoles().contains(DATA_WARM_NODE_ROLE) || discoveryNode.getRoles().contains(DiscoveryNodeRole.DATA_ROLE);
    }

    public static boolean isColdNode(DiscoveryNode discoveryNode) {
        return discoveryNode.getRoles().contains(DATA_COLD_NODE_ROLE) || discoveryNode.getRoles().contains(DiscoveryNodeRole.DATA_ROLE);
    }

    /**
     * This setting provider injects the setting allocating all newly created indices with
     * {@code index.routing.allocation.include._tier: "data_hot"} unless the user overrides the
     * setting while the index is being created (in a create index request for instance)
     */
    public static class DefaultHotAllocationSettingProvider implements IndexSettingProvider {
        private static final Logger logger = LogManager.getLogger(DefaultHotAllocationSettingProvider.class);

        @Override
        public Settings getAdditionalIndexSettings(String indexName, boolean isDataStreamIndex, Settings indexSettings) {
            Set<String> settings = indexSettings.keySet();
            if (settings.contains(DataTierAllocationDecider.INDEX_ROUTING_PREFER)) {
                // It's okay to put it, it will be removed or overridden by the template/request settings
                return Settings.builder().put(DataTierAllocationDecider.INDEX_ROUTING_PREFER, DATA_HOT).build();
            } else if (settings.stream().anyMatch(s -> s.startsWith(IndexMetadata.INDEX_ROUTING_REQUIRE_GROUP_PREFIX + ".")) ||
                settings.stream().anyMatch(s -> s.startsWith(IndexMetadata.INDEX_ROUTING_EXCLUDE_GROUP_PREFIX + ".")) ||
                settings.stream().anyMatch(s -> s.startsWith(IndexMetadata.INDEX_ROUTING_INCLUDE_GROUP_PREFIX + "."))) {
                // A different index level require, include, or exclude has been specified, so don't put the setting
                logger.debug("index [{}] specifies custom index level routing filtering, skipping tier allocation", indexName);
                return Settings.EMPTY;
            } else {
                // Otherwise, put the setting in place by default, the "hot"
                // tier if the index is part of a data stream, the "content"
                // tier if it is not.
                if (isDataStreamIndex) {
                    return Settings.builder().put(DataTierAllocationDecider.INDEX_ROUTING_PREFER, DATA_HOT).build();
                } else {
                    return Settings.builder().put(DataTierAllocationDecider.INDEX_ROUTING_PREFER, DATA_CONTENT).build();
                }
            }
        }
    }
}
