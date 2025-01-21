/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.deprecation;

import org.elasticsearch.cluster.ClusterState;
import org.elasticsearch.cluster.metadata.IndexTemplateMetadata;
import org.elasticsearch.xpack.core.deprecation.DeprecationIssue;

import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.function.Function;

import static org.elasticsearch.xpack.deprecation.DeprecationInfoAction.filterChecks;
import static org.elasticsearch.xpack.deprecation.LegacyTiersDetection.DEPRECATION_COMMON_DETAIL;
import static org.elasticsearch.xpack.deprecation.LegacyTiersDetection.DEPRECATION_HELP_URL;
import static org.elasticsearch.xpack.deprecation.LegacyTiersDetection.DEPRECATION_MESSAGE;

/**
 * Checks the legacy index templates for deprecation warnings.
 */
public class LegacyIndexTemplateDeprecationChecker implements ResourceDeprecationChecker {

    public static final String NAME = "legacy_templates";
    private static final List<Function<IndexTemplateMetadata, DeprecationIssue>> CHECKS = List.of(
        LegacyIndexTemplateDeprecationChecker::checkIndexTemplates
    );

    /**
     * @param clusterState The cluster state provided for the checker
     * @return the name of the data streams that have violated the checks with their respective warnings.
     */
    @Override
    public Map<String, List<DeprecationIssue>> check(ClusterState clusterState, DeprecationInfoAction.Request request) {
        var templates = clusterState.metadata().templates().entrySet();
        if (templates.isEmpty()) {
            return Map.of();
        }
        Map<String, List<DeprecationIssue>> issues = new HashMap<>();
        for (Map.Entry<String, IndexTemplateMetadata> entry : templates) {
            String name = entry.getKey();
            IndexTemplateMetadata template = entry.getValue();

            List<DeprecationIssue> issuesForSingleIndexTemplate = filterChecks(CHECKS, c -> c.apply(template));
            if (issuesForSingleIndexTemplate.isEmpty() == false) {
                issues.put(name, issuesForSingleIndexTemplate);
            }
        }
        return issues.isEmpty() ? Map.of() : issues;
    }

    static DeprecationIssue checkIndexTemplates(IndexTemplateMetadata indexTemplateMetadata) {
        List<String> deprecatedSettings = LegacyTiersDetection.getDeprecatedFilteredAllocationSettings(indexTemplateMetadata.settings());
        if (deprecatedSettings.isEmpty()) {
            return null;
        }
        return new DeprecationIssue(
            DeprecationIssue.Level.WARNING,
            DEPRECATION_MESSAGE,
            DEPRECATION_HELP_URL,
            "One or more of your legacy index templates is configured with 'index.routing.allocation.*.data' settings. "
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
