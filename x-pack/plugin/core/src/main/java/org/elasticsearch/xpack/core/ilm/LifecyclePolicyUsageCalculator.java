/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.core.ilm;

import org.apache.lucene.util.CollectionUtil;
import org.elasticsearch.action.support.IndicesOptions;
import org.elasticsearch.cluster.metadata.ComposableIndexTemplate;
import org.elasticsearch.cluster.metadata.IndexMetadata;
import org.elasticsearch.cluster.metadata.IndexNameExpressionResolver;
import org.elasticsearch.cluster.metadata.ItemUsage;
import org.elasticsearch.cluster.metadata.MetadataIndexTemplateService;
import org.elasticsearch.cluster.metadata.ProjectMetadata;
import org.elasticsearch.common.regex.Regex;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.util.Maps;

import java.util.ArrayList;
import java.util.Comparator;
import java.util.List;
import java.util.Map;

/**
 * A class that can be used to calculate the usages of ILM policies across the cluster. The class makes a tradeoff by using some more memory
 * (but not a lot) to significantly improve the processing time.
 */
public class LifecyclePolicyUsageCalculator {

    /** A map from policy name to list of composable templates that use that policy. */
    private final Map<String, List<String>> policyToTemplates;
    /** A map from policy name to list of data streams that use that policy. */
    private final Map<String, List<String>> policyToDataStreams;
    /** A map from policy name to list of indices that use that policy. */
    private final Map<String, List<String>> policyToIndices;

    public LifecyclePolicyUsageCalculator(
        final IndexNameExpressionResolver indexNameExpressionResolver,
        ProjectMetadata project,
        List<String> requestedPolicyNames
    ) {
        final List<String> allDataStreams = indexNameExpressionResolver.dataStreamNames(
            project,
            IndicesOptions.LENIENT_EXPAND_OPEN_CLOSED_HIDDEN_NO_SELECTOR
        );
        // Sort all templates by descending priority. That way, findV2Template can exit on the first found template.
        final var indexTemplates = new ArrayList<>(project.templatesV2().entrySet());
        CollectionUtil.timSort(indexTemplates, Comparator.comparing(entry -> entry.getValue().priorityOrZero(), Comparator.reverseOrder()));

        final IndexLifecycleMetadata metadata = project.custom(IndexLifecycleMetadata.TYPE);
        // We're making a bet here that if the `name` contains a wildcard, there's a large chance it'll simply match all policies.
        final var expectedSize = Regex.isSimpleMatchPattern(requestedPolicyNames.get(0))
            ? metadata.getPolicyMetadatas().size()
            : requestedPolicyNames.size();

        // We keep a map from composable template name to policy name to avoid having to resolve the template settings to determine
        // the template's policy twice.
        final Map<String, String> templateToPolicy = Maps.newHashMapWithExpectedSize(indexTemplates.size());

        // Build the maps that will be used for the usage calculation later on.
        policyToTemplates = Maps.newHashMapWithExpectedSize(expectedSize);
        for (Map.Entry<String, ComposableIndexTemplate> entry : project.templatesV2().entrySet()) {
            Settings settings = MetadataIndexTemplateService.resolveSettings(entry.getValue(), project.componentTemplates());
            final var policyName = LifecycleSettings.LIFECYCLE_NAME_SETTING.get(settings);
            // We only store the template if its policy matched any of the requested names.
            if (doesPolicyMatchAnyName(policyName, requestedPolicyNames) == false) {
                continue;
            }
            policyToTemplates.computeIfAbsent(policyName, k -> new ArrayList<>()).add(entry.getKey());
            templateToPolicy.put(entry.getKey(), policyName);
        }

        policyToDataStreams = Maps.newHashMapWithExpectedSize(expectedSize);
        for (String dataStream : allDataStreams) {
            String indexTemplate = MetadataIndexTemplateService.findV2TemplateFromSortedList(project, indexTemplates, dataStream, false);
            if (indexTemplate == null) {
                // Every data stream should ordinarily have an index template, so this branch should not fire under normal circumstances.
                continue;
            }
            final var policyName = templateToPolicy.get(indexTemplate);
            // If there was no entry, either the template didn't specify an ILM policy or the policy didn't match any of the requested names
            if (policyName == null) {
                continue;
            }
            policyToDataStreams.computeIfAbsent(policyName, k -> new ArrayList<>()).add(dataStream);
        }

        policyToIndices = Maps.newHashMapWithExpectedSize(expectedSize);
        for (IndexMetadata indexMetadata : project.indices().values()) {
            final var policyName = indexMetadata.getLifecyclePolicyName();
            // We only store the index if its policy matched any of the specified names.
            if (doesPolicyMatchAnyName(policyName, requestedPolicyNames) == false) {
                continue;
            }
            policyToIndices.computeIfAbsent(policyName, k -> new ArrayList<>()).add(indexMetadata.getIndex().getName());
        }
    }

    /**
     * Retrieves the pre-calculated indices, data streams, and composable templates that use the given policy.
     */
    public ItemUsage retrieveCalculatedUsage(String policyName) {
        return new ItemUsage(
            policyToIndices.getOrDefault(policyName, List.of()),
            policyToDataStreams.getOrDefault(policyName, List.of()),
            policyToTemplates.getOrDefault(policyName, List.of())
        );
    }

    private boolean doesPolicyMatchAnyName(String policyName, List<String> names) {
        for (var name : names) {
            if (Regex.simpleMatch(name, policyName)) {
                return true;
            }
        }
        return false;
    }
}
