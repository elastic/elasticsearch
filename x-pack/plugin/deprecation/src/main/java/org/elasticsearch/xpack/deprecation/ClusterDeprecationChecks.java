/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */
package org.elasticsearch.xpack.deprecation;

import org.elasticsearch.cluster.ClusterState;
import org.elasticsearch.cluster.metadata.ComposableIndexTemplate;
import org.elasticsearch.cluster.metadata.IndexTemplateMetadata;
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

/**
 * Cluster-specific deprecation checks
 */
public class ClusterDeprecationChecks {

    static DeprecationIssue legacyRoutingInLegacyTemplatesCheck(ClusterState clusterState) {
        if (clusterState.metadata().templates().isEmpty()) {
            return null;
        }
        // Check legacy
        List<String> templateNames = new ArrayList<>(clusterState.metadata().templates().size());
        for (IndexTemplateMetadata templateMetadata : clusterState.metadata().templates().values()) {
            String nodeAttrDataValue = templateMetadata.settings().get("index.routing.allocation.require.data");
            if (nodeAttrDataValue != null) {
                templateNames.add(templateMetadata.name());
            }
        }
        if (templateNames.isEmpty() == false) {
            templateNames.sort(String.CASE_INSENSITIVE_ORDER);
            return new DeprecationIssue(
                DeprecationIssue.Level.WARNING,
                "Legacy index templates ["
                    + templateNames.stream().limit(10).collect(Collectors.joining(","))
                    + "] have configured 'index.routing.allocation.require.data'. This setting is not recommended to"
                    + " be used for setting tiers.",
                "https://ela.st/es-deprecation-7-node-attr-data-setting",
                "One or more of your legacy templates is configured with index.routing.allocation.require.data settings."
                    + " This is typically used to create a hot/warm or tiered architecture, based on legacy guidelines."
                    + " Data tiers are a recommended replacement for tiered architecture clusters.",
                true,
                null
            );
        }
        return null;
    }

    static DeprecationIssue legacyRoutingInTemplatesCheck(ClusterState clusterState) {
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
                && template.template().settings().get("index.routing.allocation.require.data") != null) {
                templateNames.add(templateName);
            }
        }
        return createLegacyRoutingDeprecationIssue(templateNames, "index templates ");
    }

    static DeprecationIssue legacyRoutingInIlmPoliciesCheck(ClusterState clusterState) {
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
                if (allocateAction != null && allocateAction.getInclude().get("data") != null) {
                    policyNames.add(policyName);
                }
            }
        }
        return createLegacyRoutingDeprecationIssue(policyNames, "ILM policies ");
    }

    private static DeprecationIssue createLegacyRoutingDeprecationIssue(List<String> names, String messagePrefix) {
        if (names.isEmpty()) {
            return null;
        }
        names.sort(String.CASE_INSENSITIVE_ORDER);
        return new DeprecationIssue(
            DeprecationIssue.Level.WARNING,
            messagePrefix
                + names.stream().limit(10).collect(Collectors.joining(",", "[", "]"))
                + " have configured 'index.routing.allocation.require.data'. This setting is not recommended to"
                + " be used for setting tiers.",
            "https://ela.st/es-deprecation-7-node-attr-data-setting",
            "One or more of your "
                + messagePrefix
                + "is configured with index.routing.allocation.require.data settings."
                + " This is typically used to create a hot/warm or tiered architecture, based on legacy guidelines."
                + " Data tiers are a recommended replacement for tiered architecture clusters.",
            false,
            null
        );
    }
}
