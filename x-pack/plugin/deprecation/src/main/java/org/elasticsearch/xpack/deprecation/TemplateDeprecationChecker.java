/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.deprecation;

import org.elasticsearch.cluster.ClusterState;
import org.elasticsearch.cluster.metadata.ComponentTemplate;
import org.elasticsearch.cluster.metadata.ComposableIndexTemplate;
import org.elasticsearch.cluster.metadata.Template;
import org.elasticsearch.common.xcontent.XContentHelper;
import org.elasticsearch.index.mapper.SourceFieldMapper;
import org.elasticsearch.xpack.core.deprecation.DeprecationIssue;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.function.Function;

import static org.elasticsearch.xpack.deprecation.LegacyTiersDetection.DEPRECATION_COMMON_DETAIL;
import static org.elasticsearch.xpack.deprecation.LegacyTiersDetection.DEPRECATION_HELP_URL;
import static org.elasticsearch.xpack.deprecation.LegacyTiersDetection.DEPRECATION_MESSAGE;

/**
 * Checks the index and component templates for deprecation warnings.
 */
public class TemplateDeprecationChecker implements ResourceDeprecationChecker {

    public static final String NAME = "templates";
    private final List<Function<ComposableIndexTemplate, DeprecationIssue>> indexTemplateChecks = List.of(
        this::checkLegacyTiersInIndexTemplate
    );
    private final List<Function<ComponentTemplate, DeprecationIssue>> componentTemplateChecks = List.of(
        this::checkSourceModeInComponentTemplates,
        this::checkLegacyTiersInComponentTemplates
    );

    /**
     * @param clusterState The cluster state provided for the checker
     * @param request not used yet in these checks
     * @param precomputedData not used yet in these checks
     * @return the name of the data streams that have violated the checks with their respective warnings.
     */
    @Override
    public Map<String, List<DeprecationIssue>> check(
        ClusterState clusterState,
        DeprecationInfoAction.Request request,
        TransportDeprecationInfoAction.PrecomputedData precomputedData
    ) {
        return check(clusterState);
    }

    /**
     * @param clusterState The cluster state provided for the checker
     * @return the name of the data streams that have violated the checks with their respective warnings.
     */
    Map<String, List<DeprecationIssue>> check(ClusterState clusterState) {
        var indexTemplates = clusterState.metadata().getProject().templatesV2().entrySet();
        var componentTemplates = clusterState.metadata().getProject().componentTemplates().entrySet();
        if (indexTemplates.isEmpty() && componentTemplates.isEmpty()) {
            return Map.of();
        }
        Map<String, List<DeprecationIssue>> issues = new HashMap<>();
        for (Map.Entry<String, ComposableIndexTemplate> entry : indexTemplates) {
            String name = entry.getKey();
            ComposableIndexTemplate template = entry.getValue();

            List<DeprecationIssue> issuesForSingleIndexTemplate = indexTemplateChecks.stream()
                .map(c -> c.apply(template))
                .filter(Objects::nonNull)
                .toList();
            if (issuesForSingleIndexTemplate.isEmpty() == false) {
                issues.computeIfAbsent(name, ignored -> new ArrayList<>()).addAll(issuesForSingleIndexTemplate);
            }
        }
        for (Map.Entry<String, ComponentTemplate> entry : componentTemplates) {
            String name = entry.getKey();
            ComponentTemplate template = entry.getValue();

            List<DeprecationIssue> issuesForSingleIndexTemplate = componentTemplateChecks.stream()
                .map(c -> c.apply(template))
                .filter(Objects::nonNull)
                .toList();
            if (issuesForSingleIndexTemplate.isEmpty() == false) {
                issues.computeIfAbsent(name, ignored -> new ArrayList<>()).addAll(issuesForSingleIndexTemplate);
            }
        }
        return issues.isEmpty() ? Map.of() : issues;
    }

    private DeprecationIssue checkLegacyTiersInIndexTemplate(ComposableIndexTemplate composableIndexTemplate) {
        Template template = composableIndexTemplate.template();
        if (template != null) {
            List<String> deprecatedSettings = LegacyTiersDetection.getDeprecatedFilteredAllocationSettings(template.settings());
            if (deprecatedSettings.isEmpty()) {
                return null;
            }
            return new DeprecationIssue(
                DeprecationIssue.Level.WARNING,
                DEPRECATION_MESSAGE,
                DEPRECATION_HELP_URL,
                "One or more of your index templates is configured with 'index.routing.allocation.*.data' settings. "
                    + DEPRECATION_COMMON_DETAIL,
                false,
                DeprecationIssue.createMetaMapForRemovableSettings(deprecatedSettings)
            );
        }
        return null;
    }

    private DeprecationIssue checkSourceModeInComponentTemplates(ComponentTemplate template) {
        if (template.template().mappings() != null) {
            var sourceAsMap = (Map<?, ?>) XContentHelper.convertToMap(template.template().mappings().uncompressed(), true).v2().get("_doc");
            if (sourceAsMap != null) {
                Object source = sourceAsMap.get("_source");
                if (source instanceof Map<?, ?> sourceMap) {
                    if (sourceMap.containsKey("mode")) {
                        return new DeprecationIssue(
                            DeprecationIssue.Level.CRITICAL,
                            SourceFieldMapper.DEPRECATION_WARNING_TITLE,
                            "https://ela.st/migrate-source-mode",
                            SourceFieldMapper.DEPRECATION_WARNING,
                            false,
                            null
                        );
                    }
                }
            }
        }
        return null;
    }

    private DeprecationIssue checkLegacyTiersInComponentTemplates(ComponentTemplate componentTemplate) {
        Template template = componentTemplate.template();
        List<String> deprecatedSettings = LegacyTiersDetection.getDeprecatedFilteredAllocationSettings(template.settings());
        if (deprecatedSettings.isEmpty()) {
            return null;
        }
        return new DeprecationIssue(
            DeprecationIssue.Level.WARNING,
            DEPRECATION_MESSAGE,
            DEPRECATION_HELP_URL,
            "One or more of your component templates is configured with 'index.routing.allocation.*.data' settings. "
                + DEPRECATION_COMMON_DETAIL,
            false,
            DeprecationIssue.createMetaMapForRemovableSettings(deprecatedSettings)
        );
    }

    @Override
    public String getName() {
        return NAME;
    }
}
