/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.deprecation;

import org.elasticsearch.cluster.ClusterState;
import org.elasticsearch.cluster.metadata.ComponentTemplate;
import org.elasticsearch.cluster.metadata.Template;
import org.elasticsearch.common.xcontent.XContentHelper;
import org.elasticsearch.index.mapper.SourceFieldMapper;
import org.elasticsearch.xpack.core.deprecation.DeprecationIssue;

import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.function.Function;

import static org.elasticsearch.cluster.metadata.IndexMetadata.INDEX_ROUTING_REQUIRE_GROUP_PREFIX;
import static org.elasticsearch.xpack.deprecation.DeprecationInfoAction.filterChecks;

/**
 * Checks the component templates for deprecation warnings.
 */
public class ComponentTemplateDeprecationChecks implements ResourceDeprecationChecker {

    public static final String NAME = "component_templates";
    private static final List<Function<ComponentTemplate, DeprecationIssue>> CHECKS = List.of(
        ComponentTemplateDeprecationChecks::checkSourceModeInComponentTemplates,
        ComponentTemplateDeprecationChecks::checkComponentTemplates
    );

    /**
     * @param clusterState The cluster state provided for the checker
     * @return the name of the data streams that have violated the checks with their respective warnings.
     */
    @Override
    public Map<String, List<DeprecationIssue>> check(ClusterState clusterState, DeprecationInfoAction.Request request) {
        var templates = clusterState.metadata().componentTemplates().entrySet();
        if (templates.isEmpty()) {
            return Map.of();
        }
        Map<String, List<DeprecationIssue>> issues = new HashMap<>();
        for (Map.Entry<String, ComponentTemplate> entry : templates) {
            String name = entry.getKey();
            ComponentTemplate template = entry.getValue();

            List<DeprecationIssue> issuesForSingleComponentTemplate = filterChecks(CHECKS, c -> c.apply(template));
            if (issuesForSingleComponentTemplate.isEmpty() == false) {
                issues.put(name, issuesForSingleComponentTemplate);
            }
        }
        return issues.isEmpty() ? Map.of() : issues;
    }

    static DeprecationIssue checkSourceModeInComponentTemplates(ComponentTemplate template) {
        if (template.template().mappings() != null) {
            var sourceAsMap = (Map<?, ?>) XContentHelper.convertToMap(template.template().mappings().uncompressed(), true).v2().get("_doc");
            if (sourceAsMap != null) {
                Object source = sourceAsMap.get("_source");
                if (source instanceof Map<?, ?> sourceMap) {
                    if (sourceMap.containsKey("mode")) {
                        return new DeprecationIssue(
                            DeprecationIssue.Level.CRITICAL,
                            SourceFieldMapper.DEPRECATION_WARNING,
                            "https://github.com/elastic/elasticsearch/pull/117172",
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

    static DeprecationIssue checkComponentTemplates(ComponentTemplate componentTemplate) {

        Template template = componentTemplate.template();
        if (template != null
            && template.settings() != null
            && template.settings().get(INDEX_ROUTING_REQUIRE_GROUP_PREFIX + ".data") != null) {
            return new DeprecationIssue(
                DeprecationIssue.Level.WARNING,
                "Setting 'index.routing.allocation.require.data' is not recommended",
                "https://ela.st/migrate-to-tiers",
                "One or more of your component templates is configured with 'index.routing.allocation.require.data' settings."
                    + " This is typically used to create a hot/warm or tiered architecture, based on legacy guidelines."
                    + " Data tiers are a recommended replacement for tiered architecture clusters.",
                false,
                null
            );
        }
        return null;
    }

    @Override
    public String getName() {
        return NAME;
    }
}
