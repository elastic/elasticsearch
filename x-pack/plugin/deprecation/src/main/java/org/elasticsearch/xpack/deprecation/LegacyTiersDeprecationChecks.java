/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */
package org.elasticsearch.xpack.deprecation;

import org.elasticsearch.action.admin.cluster.node.info.PluginsAndModules;
import org.elasticsearch.cluster.ClusterState;
import org.elasticsearch.cluster.metadata.ComponentTemplate;
import org.elasticsearch.cluster.metadata.ComposableIndexTemplate;
import org.elasticsearch.cluster.metadata.IndexMetadata;
import org.elasticsearch.cluster.metadata.IndexTemplateMetadata;
import org.elasticsearch.cluster.metadata.Template;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.license.XPackLicenseState;
import org.elasticsearch.xpack.core.deprecation.DeprecationIssue;
import org.elasticsearch.xpack.core.ilm.AllocateAction;
import org.elasticsearch.xpack.core.ilm.IndexLifecycleMetadata;
import org.elasticsearch.xpack.core.ilm.LifecyclePolicy;
import org.elasticsearch.xpack.core.ilm.LifecyclePolicyMetadata;
import org.elasticsearch.xpack.core.ilm.Phase;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

import static org.elasticsearch.cluster.metadata.IndexMetadata.INDEX_ROUTING_REQUIRE_GROUP_PREFIX;

/**
 * Adds deprecation warnings for any traces of the legacy tier implementation via filtered allocation. The checks include:
 * - looking for nodes with the `node.attr.data`
 * - looking for component templates, composable templates, legacy templates and all indices that contain
 *   the `index.routing.allocation.require.data` setting
 * - looking for the allocate action in ILM policies that set the `require.data` setting.
 * We only check for the data node attribute, not all usages of the filtered allocation.
 */
public class LegacyTiersDeprecationChecks {

    private static final String NODE_ATTRIBUTE = "data";
    private static final String LEGACY_TIER_INDEX_SETTING = INDEX_ROUTING_REQUIRE_GROUP_PREFIX + "." + NODE_ATTRIBUTE;

    static DeprecationIssue checkNodeAttrData(
        final Settings settings,
        final PluginsAndModules pluginsAndModules,
        final ClusterState clusterState,
        final XPackLicenseState licenseState
    ) {
        String nodeAttrDataValue = settings.get("node.attr." + NODE_ATTRIBUTE);
        if (nodeAttrDataValue == null) {
            return null;
        }
        return createDeprecationIssue(
            "Setting node.attributes.data is not recommended",
            "One or more of your nodes is configured with node.attributes.data settings. This is typically used to create a "
                + "hot/warm or tiered architecture, based on legacy guidelines. Data tiers are a recommended replacement for tiered "
                + "architecture clusters."
        );
    }

    static DeprecationIssue checkTemplatesV1(ClusterState clusterState) {
        if (clusterState.metadata().templates().isEmpty()) {
            return null;
        }
        // Check legacy
        List<String> templateNames = new ArrayList<>(clusterState.metadata().templates().size());
        for (IndexTemplateMetadata templateMetadata : clusterState.metadata().templates().values()) {
            String nodeAttrDataValue = templateMetadata.settings().get(LEGACY_TIER_INDEX_SETTING);
            if (nodeAttrDataValue != null) {
                templateNames.add(templateMetadata.name());
            }
        }
        return createTierLegacyRoutingDeprecationIssue(templateNames, "legacy index templates ");
    }

    static DeprecationIssue checkTemplatesV2(ClusterState clusterState) {
        if (clusterState.metadata().templatesV2().isEmpty()) {
            return null;
        }
        // Check legacy
        List<String> templateNames = new ArrayList<>(clusterState.metadata().templatesV2().size());
        for (Map.Entry<String, ComposableIndexTemplate> entry : clusterState.metadata().templatesV2().entrySet()) {
            String templateName = entry.getKey();
            ComposableIndexTemplate template = entry.getValue();
            if (template.template() != null
                && template.template().settings() != null
                && template.template().settings().get(LEGACY_TIER_INDEX_SETTING) != null) {
                templateNames.add(templateName);
            }
        }
        return createTierLegacyRoutingDeprecationIssue(templateNames, "index templates ");
    }

    static DeprecationIssue checkComponentTemplates(ClusterState clusterState) {
        if (clusterState.metadata().componentTemplates().isEmpty()) {
            return null;
        }
        // Check legacy
        List<String> templateNames = new ArrayList<>(clusterState.metadata().componentTemplates().size());
        for (Map.Entry<String, ComponentTemplate> entry : clusterState.metadata().componentTemplates().entrySet()) {
            String templateName = entry.getKey();
            Template template = entry.getValue().template();
            if (template != null && template.settings() != null && template.settings().get(LEGACY_TIER_INDEX_SETTING) != null) {
                templateNames.add(templateName);
            }
        }
        return createTierLegacyRoutingDeprecationIssue(templateNames, "component templates ");
    }

    static DeprecationIssue checkIlmPolicies(ClusterState clusterState) {
        IndexLifecycleMetadata metadata = clusterState.metadata().custom(IndexLifecycleMetadata.TYPE);
        if (metadata == null || metadata.getPolicyMetadatas().isEmpty()) {
            return null;
        }

        List<String> policyNames = new ArrayList<>(metadata.getPolicyMetadatas().size());
        for (Map.Entry<String, LifecyclePolicyMetadata> entry : metadata.getPolicyMetadatas().entrySet()) {
            String policyName = entry.getKey();
            LifecyclePolicy policy = entry.getValue().getPolicy();
            for (Map.Entry<String, Phase> phaseEntry : policy.getPhases().entrySet()) {
                AllocateAction allocateAction = (AllocateAction) phaseEntry.getValue().getActions().get(AllocateAction.NAME);
                if (allocateAction != null && allocateAction.getRequire().get("data") != null) {
                    policyNames.add(policyName);
                }
            }
        }
        if (policyNames.isEmpty()) {
            return null;
        }
        return createDeprecationIssue(
            "ILM policies "
                + truncatedListOfNames(policyNames)
                + " have an Allocate action configured that sets 'require.data'. This setting is not recommended to"
                + " be used for setting tiers.",
            "One or more of your  ILM policies has an Allocate action configured that sets 'require.data'."
                + " This is typically used to create a hot/warm or tiered architecture, based on legacy guidelines."
                + " Data tiers are a recommended replacement for tiered architecture clusters."
        );
    }

    private static DeprecationIssue createTierLegacyRoutingDeprecationIssue(List<String> names, String messagePrefix) {
        if (names.isEmpty()) {
            return null;
        }
        return createDeprecationIssue(
            messagePrefix
                + truncatedListOfNames(names)
                + " have configured 'index.routing.allocation.require.data'. This setting is not recommended to"
                + " be used for setting tiers.",
            "One or more of your "
                + messagePrefix
                + "is configured with 'index.routing.allocation.require.data' settings."
                + " This is typically used to create a hot/warm or tiered architecture, based on legacy guidelines."
                + " Data tiers are a recommended replacement for tiered architecture clusters."
        );
    }

    static DeprecationIssue checkIndexSettings(IndexMetadata indexMetadata, ClusterState clusterState) {
        String nodeAttrDataValue = indexMetadata.getSettings().get(INDEX_ROUTING_REQUIRE_GROUP_PREFIX + ".data");
        if (nodeAttrDataValue == null) {
            return null;
        }
        String indexName = indexMetadata.getIndex().getName();
        return createDeprecationIssue(
            "index ["
                + indexName
                + "] has configured 'index.routing.allocation.require.data: "
                + nodeAttrDataValue
                + "'. This setting is not recommended to be used for setting tiers.",
            "One or more of your indices is configured with 'index.routing.allocation.require.data' setting."
                + " This is typically used to create a hot/warm or tiered architecture, based on legacy guidelines."
                + " Data tiers are a recommended replacement for tiered architecture clusters."
        );
    }

    // Visible for testing
    static String truncatedListOfNames(List<String> names) {
        names.sort(String.CASE_INSENSITIVE_ORDER);
        return names.stream().limit(10).collect(Collectors.joining(",", "[", names.size() > 10 ? ",...]" : "]"));
    }

    private static DeprecationIssue createDeprecationIssue(String message, String details) {
        return new DeprecationIssue(
            DeprecationIssue.Level.WARNING,
            message,
            "https://ela.st/es-deprecation-7-node-attr-data-setting",
            details,
            false,
            null
        );
    }
}
