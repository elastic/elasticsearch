/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.cluster.routing.allocation;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.elasticsearch.cluster.metadata.IndexMetadata;
import org.elasticsearch.cluster.metadata.ProjectMetadata;
import org.elasticsearch.cluster.node.DiscoveryNode;
import org.elasticsearch.cluster.node.DiscoveryNodeRole;
import org.elasticsearch.common.Strings;
import org.elasticsearch.common.compress.CompressedXContent;
import org.elasticsearch.common.settings.Setting;
import org.elasticsearch.common.settings.Setting.Property;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.core.Nullable;
import org.elasticsearch.index.IndexMode;
import org.elasticsearch.index.IndexModule;
import org.elasticsearch.index.IndexSettingProvider;
import org.elasticsearch.snapshots.SearchableSnapshotsSettings;

import java.time.Instant;
import java.util.Collection;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.function.Function;

/**
 * The {@code DataTier} class encapsulates the formalization of the "content",
 * "hot", "warm", and "cold" tiers as node roles. In contains the
 * roles themselves as well as helpers for validation and determining if a node
 * has a tier configured.
 */
public class DataTier {

    public static final String DATA_CONTENT = "data_content";
    public static final String DATA_HOT = "data_hot";
    public static final String DATA_WARM = "data_warm";
    public static final String DATA_COLD = "data_cold";
    public static final String DATA_FROZEN = "data_frozen";

    public static final Set<String> ALL_DATA_TIERS = Set.of(DATA_CONTENT, DATA_HOT, DATA_WARM, DATA_COLD, DATA_FROZEN);

    // deprecated setting for migrating from 7.x (where a tier preference was not required, and did not necessarily
    // have a default value), to 8.x (where a tier preference will be required, and a default value will be injected).
    // in version 8.0 and onward, this setting doesn't control any logic anymore, and it will be removed as a breaking change in
    // some future version, likely 9.0.
    public static final String ENFORCE_DEFAULT_TIER_PREFERENCE = "cluster.routing.allocation.enforce_default_tier_preference";
    public static final Setting<Boolean> ENFORCE_DEFAULT_TIER_PREFERENCE_SETTING = Setting.boolSetting(
        ENFORCE_DEFAULT_TIER_PREFERENCE,
        true,
        Property.Dynamic,
        Property.NodeScope,
        Property.DeprecatedWarning
    );

    public static final String TIER_PREFERENCE = "index.routing.allocation.include._tier_preference";

    private static final Settings DATA_CONTENT_TIER_PREFERENCE_SETTINGS = Settings.builder().put(TIER_PREFERENCE, DATA_CONTENT).build();

    private static final Settings DATA_HOT_TIER_PREFERENCE_SETTINGS = Settings.builder().put(TIER_PREFERENCE, DATA_HOT).build();

    private static final Settings NULL_TIER_PREFERENCE_SETTINGS = Settings.builder().putNull(TIER_PREFERENCE).build();

    public static final Setting<String> TIER_PREFERENCE_SETTING = new Setting<>(
        TIER_PREFERENCE,
        DataTierSettingValidator::getDefaultTierPreference,
        Function.identity(),
        new DataTierSettingValidator(),
        Property.Dynamic,
        Property.IndexScope
    );

    static {
        for (String tier : ALL_DATA_TIERS) {
            assert tier.equals(DATA_FROZEN) || tier.contains(DATA_FROZEN) == false
                : "can't have two tier names containing ["
                    + DATA_FROZEN
                    + "] because it would break setting validation optimizations"
                    + " in the data tier allocation decider";
        }
    }

    // Represents an ordered list of data tiers from frozen to hot (or slow to fast)
    private static final List<String> ORDERED_FROZEN_TO_HOT_TIERS = List.of(DATA_FROZEN, DATA_COLD, DATA_WARM, DATA_HOT);

    private static final Map<String, String> PREFERENCE_TIER_CONFIGURATIONS;

    private static final Map<String, Settings> PREFERENCE_TIER_CONFIGURATION_SETTINGS;

    static {
        final Map<String, String> tmp = new HashMap<>();
        final Map<String, Settings> tmpSettings = new HashMap<>();
        for (int i = 0, ordered_frozen_to_hot_tiersSize = ORDERED_FROZEN_TO_HOT_TIERS.size(); i < ordered_frozen_to_hot_tiersSize; i++) {
            String tier = ORDERED_FROZEN_TO_HOT_TIERS.get(i);
            if (tier.equals(DATA_FROZEN)) {
                tmp.put(tier, DATA_FROZEN);
                tmpSettings.put(DATA_FROZEN, Settings.builder().put(DataTier.TIER_PREFERENCE, DATA_FROZEN).build());
            } else {
                final String prefTierString = String.join(",", ORDERED_FROZEN_TO_HOT_TIERS.subList(i, ORDERED_FROZEN_TO_HOT_TIERS.size()))
                    .intern();
                tmp.put(tier, prefTierString);
                tmpSettings.put(tier, Settings.builder().put(DataTier.TIER_PREFERENCE, prefTierString).build());
            }
        }
        PREFERENCE_TIER_CONFIGURATIONS = Map.copyOf(tmp);
        PREFERENCE_TIER_CONFIGURATION_SETTINGS = Map.copyOf(tmpSettings);
    }

    /**
     * Returns true if the given tier name is a valid tier
     */
    public static boolean validTierName(String tierName) {
        return ALL_DATA_TIERS.contains(tierName);
    }

    /**
     * Based on the provided target tier it will return a comma separated list of preferred tiers.
     * ie. if `data_cold` is the target tier, it will return `data_cold,data_warm,data_hot`.
     * This is usually used in conjunction with {@link #TIER_PREFERENCE_SETTING}.
     */
    public static String getPreferredTiersConfiguration(String targetTier) {
        final String res = PREFERENCE_TIER_CONFIGURATIONS.get(targetTier);
        if (res == null) {
            throw new IllegalArgumentException("invalid data tier [" + targetTier + "]");
        }
        return res;
    }

    public static Settings getPreferredTiersConfigurationSettings(String targetTier) {
        final Settings res = PREFERENCE_TIER_CONFIGURATION_SETTINGS.get(targetTier);
        if (res == null) {
            throw new IllegalArgumentException("invalid data tier [" + targetTier + "]");
        }
        return res;
    }

    public static boolean isContentNode(DiscoveryNode discoveryNode) {
        return discoveryNode.getRoles().contains(DiscoveryNodeRole.DATA_CONTENT_NODE_ROLE)
            || discoveryNode.getRoles().contains(DiscoveryNodeRole.DATA_ROLE);
    }

    public static boolean isHotNode(DiscoveryNode discoveryNode) {
        return discoveryNode.getRoles().contains(DiscoveryNodeRole.DATA_HOT_NODE_ROLE)
            || discoveryNode.getRoles().contains(DiscoveryNodeRole.DATA_ROLE);
    }

    public static boolean isWarmNode(DiscoveryNode discoveryNode) {
        return discoveryNode.getRoles().contains(DiscoveryNodeRole.DATA_WARM_NODE_ROLE)
            || discoveryNode.getRoles().contains(DiscoveryNodeRole.DATA_ROLE);
    }

    public static boolean isColdNode(DiscoveryNode discoveryNode) {
        return discoveryNode.getRoles().contains(DiscoveryNodeRole.DATA_COLD_NODE_ROLE)
            || discoveryNode.getRoles().contains(DiscoveryNodeRole.DATA_ROLE);
    }

    public static boolean isFrozenNode(DiscoveryNode discoveryNode) {
        return isFrozenNode(discoveryNode.getRoles());
    }

    public static boolean isFrozenNode(final Set<DiscoveryNodeRole> roles) {
        return roles.contains(DiscoveryNodeRole.DATA_FROZEN_NODE_ROLE) || roles.contains(DiscoveryNodeRole.DATA_ROLE);
    }

    public static List<String> parseTierList(String tiers) {
        if (Strings.hasText(tiers) == false) {
            // avoid parsing overhead in the null/empty string case
            return List.of();
        } else {
            return List.of(tiers.split(","));
        }
    }

    /**
     * Compares the provided tiers for coldness order (eg. warm is colder than hot).
     *
     * Similar to {@link java.util.Comparator#compare(Object, Object)} returns
     *   -1 if tier1 is colder than tier2 (ie. compare("data_cold", "data_hot"))
     *   0 if tier1 is as cold as tier2 (ie. tier1.equals(tier2) )
     *   1 if tier1 is warmer than tier2 (ie. compare("data_hot", "data_cold"))
     *
     * The provided tiers parameters must be valid data tiers values (ie. {@link #ALL_DATA_TIERS}.
     * NOTE: `data_content` is treated as "equal to data_hot" in the tiers hierarchy.
     * If invalid tier names are passed the result is non-deterministic.
     */
    public static int compare(String tier1, String tier2) {
        if (tier1.equals(DATA_CONTENT)) {
            tier1 = DATA_HOT;
        }
        if (tier2.equals(DATA_CONTENT)) {
            tier2 = DATA_HOT;
        }
        int indexOfTier1 = ORDERED_FROZEN_TO_HOT_TIERS.indexOf(tier1);
        assert indexOfTier1 >= 0 : "expecting a valid tier to compare but got:" + tier1;
        int indexOfTier2 = ORDERED_FROZEN_TO_HOT_TIERS.indexOf(tier2);
        assert indexOfTier2 >= 0 : "expecting a valid tier to compare but got:" + tier2;

        if (indexOfTier1 == indexOfTier2) {
            return 0;
        } else {
            return indexOfTier1 < indexOfTier2 ? -1 : 1;
        }
    }

    /**
     * This setting provider injects the setting allocating all newly created indices with
     * {@code index.routing.allocation.include._tier_preference: "data_hot"} for a data stream index
     * or {@code index.routing.allocation.include._tier_preference: "data_content"} for an index not part of
     * a data stream unless the user overrides the setting while the index is being created
     * (in a create index request for instance)
     */
    public static class DefaultHotAllocationSettingProvider implements IndexSettingProvider {
        private static final Logger logger = LogManager.getLogger(DefaultHotAllocationSettingProvider.class);

        @Override
        public Settings getAdditionalIndexSettings(
            String indexName,
            @Nullable String dataStreamName,
            IndexMode templateIndexMode,
            ProjectMetadata projectMetadata,
            Instant resolvedAt,
            Settings indexTemplateAndCreateRequestSettings,
            List<CompressedXContent> combinedTemplateMappings
        ) {
            Set<String> settings = indexTemplateAndCreateRequestSettings.keySet();
            if (settings.contains(TIER_PREFERENCE)) {
                // just a marker -- this null value will be removed or overridden by the template/request settings
                return NULL_TIER_PREFERENCE_SETTINGS;
            } else if (settings.stream().anyMatch(s -> s.startsWith(IndexMetadata.INDEX_ROUTING_REQUIRE_GROUP_PREFIX + "."))
                || settings.stream().anyMatch(s -> s.startsWith(IndexMetadata.INDEX_ROUTING_EXCLUDE_GROUP_PREFIX + "."))
                || settings.stream().anyMatch(s -> s.startsWith(IndexMetadata.INDEX_ROUTING_INCLUDE_GROUP_PREFIX + "."))) {
                    // A different index level require, include, or exclude has been specified, so don't put the setting
                    logger.debug("index [{}] specifies custom index level routing filtering, skipping tier allocation", indexName);
                    return Settings.EMPTY;
                } else {
                    // Otherwise, put the setting in place by default, the "hot"
                    // tier if the index is part of a data stream, the "content"
                    // tier if it is not.
                    if (dataStreamName != null) {
                        return DATA_HOT_TIER_PREFERENCE_SETTINGS;
                    } else {
                        return DATA_CONTENT_TIER_PREFERENCE_SETTINGS;
                    }
                }
        }
    }

    // visible for testing
    static final class DataTierSettingValidator implements Setting.Validator<String> {

        private static final Collection<Setting<?>> dependencies = List.of(
            IndexModule.INDEX_STORE_TYPE_SETTING,
            SearchableSnapshotsSettings.SNAPSHOT_PARTIAL_SETTING
        );

        public static String getDefaultTierPreference(Settings settings) {
            if (SearchableSnapshotsSettings.isPartialSearchableSnapshotIndex(settings)) {
                return DATA_FROZEN;
            } else {
                return "";
            }
        }

        @Override
        public void validate(String value) {
            if (Strings.hasText(value)) {
                for (String s : parseTierList(value)) {
                    if (validTierName(s) == false) {
                        throw new IllegalArgumentException(
                            "invalid tier names found in [" + value + "] allowed values are " + ALL_DATA_TIERS
                        );
                    }
                }
            }
        }

        @Override
        public void validate(String value, Map<Setting<?>, Object> settings, boolean exists) {
            if (exists && value != null) {
                if (SearchableSnapshotsSettings.isPartialSearchableSnapshotIndex(settings)) {
                    if (value.equals(DATA_FROZEN) == false) {
                        throw new IllegalArgumentException(
                            "only the ["
                                + DATA_FROZEN
                                + "] tier preference may be used for partial searchable snapshots (got: ["
                                + value
                                + "])"
                        );
                    }
                } else {
                    if (value.contains(DATA_FROZEN)) {
                        throw new IllegalArgumentException("[" + DATA_FROZEN + "] tier can only be used for partial searchable snapshots");
                    }
                }
            }
        }

        @Override
        public Iterator<Setting<?>> settings() {
            return dependencies.iterator();
        }
    }
}
