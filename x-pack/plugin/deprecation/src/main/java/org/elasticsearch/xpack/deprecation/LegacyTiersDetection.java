/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.deprecation;

import org.elasticsearch.common.settings.Settings;

import java.util.List;
import java.util.Map;
import java.util.Set;

import static org.elasticsearch.cluster.metadata.IndexMetadata.INDEX_ROUTING_EXCLUDE_GROUP_SETTING;
import static org.elasticsearch.cluster.metadata.IndexMetadata.INDEX_ROUTING_INCLUDE_GROUP_SETTING;
import static org.elasticsearch.cluster.metadata.IndexMetadata.INDEX_ROUTING_REQUIRE_GROUP_SETTING;

/**
 * Contains conctants and helper methods that help detect when the settings or the ILM policy are
 * implementing tiers via filtered allocation.
 */
public class LegacyTiersDetection {

    public static final String DEPRECATION_MESSAGE = "Configuring tiers via filtered allocation is not recommended.";
    public static final String DEPRECATION_COMMON_DETAIL =
        "This is typically used to create a hot/warm or tiered architecture, based on legacy guidelines."
            + " Data tiers are a recommended replacement for tiered architecture clusters.";
    public static final String DEPRECATION_HELP_URL = "https://ela.st/migrate-to-tiers";

    public static final String NODE_ATTRIBUTE = "data";
    private static final Set<String> DEPRECATED_SETTINGS = Set.of(
        INDEX_ROUTING_REQUIRE_GROUP_SETTING.getKey() + NODE_ATTRIBUTE,
        INDEX_ROUTING_INCLUDE_GROUP_SETTING.getKey() + NODE_ATTRIBUTE,
        INDEX_ROUTING_EXCLUDE_GROUP_SETTING.getKey() + NODE_ATTRIBUTE
    );

    private static final Set<String> TIER_VALUES = Set.of("hot", "warm", "cold", "frozen");

    /**
     * @return the deprecated filtered allocation settings that are used as tier implementation
     */
    public static List<String> getDeprecatedFilteredAllocationSettings(Settings settings) {
        if (settings == null || settings.isEmpty()) {
            return List.of();
        }

        return DEPRECATED_SETTINGS.stream().filter(setting -> {
            String value = settings.get(setting);
            return value != null && TIER_VALUES.contains(value);
        }).toList();
    }

    /**
     * @param nodeAttributeConfiguration a map from node attribute to value
     * @return
     */
    public static boolean containsDeprecatedFilteredAllocationConfig(Map<String, String> nodeAttributeConfiguration) {
        return nodeAttributeConfiguration != null
            && nodeAttributeConfiguration.containsKey(NODE_ATTRIBUTE)
            && TIER_VALUES.contains(nodeAttributeConfiguration.get(NODE_ATTRIBUTE));
    }
}
