/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.core.ilm;

import org.apache.lucene.util.CollectionUtil;
import org.elasticsearch.action.support.IndicesOptions;
import org.elasticsearch.cluster.ClusterState;
import org.elasticsearch.cluster.metadata.ComposableIndexTemplate;
import org.elasticsearch.cluster.metadata.IndexMetadata;
import org.elasticsearch.cluster.metadata.IndexNameExpressionResolver;
import org.elasticsearch.cluster.metadata.ItemUsage;
import org.elasticsearch.cluster.metadata.MetadataIndexTemplateService;
import org.elasticsearch.common.regex.Regex;
import org.elasticsearch.common.settings.Settings;

import java.util.ArrayList;
import java.util.Comparator;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * A class that can be used to calculate the usages of ILM policies. This class computes some information on initialization, which will
 * use a bit more memory but speeds up the usage calculation significantly.
 */
public class LifecyclePolicyUsageCalculator {

    private final ClusterState state;
    /** Whether {@link #calculateUsage} will be called multiple times or not. */
    private final boolean willIterate;
    /** A map from policy name to list of data streams that use that policy. */
    private final Map<String, List<String>> policyToDataStream;
    /** A map from composable template name to the policy name it uses (or null) */
    private final Map<String, String> templateToPolicy;

    public LifecyclePolicyUsageCalculator(
        final IndexNameExpressionResolver indexNameExpressionResolver,
        final ClusterState state,
        List<String> names
    ) {
        this.state = state;
        this.willIterate = names.size() > 1 || Regex.isSimpleMatchPattern(names.get(0));

        var allDataStreams = indexNameExpressionResolver.dataStreamNames(state, IndicesOptions.LENIENT_EXPAND_OPEN_CLOSED_HIDDEN);
        // Sort all templates by descending priority. That way, findV2Template can exit on the first found template.
        var indexTemplates = new ArrayList<>(state.metadata().templatesV2().entrySet());
        CollectionUtil.timSort(indexTemplates, Comparator.comparing(entry -> entry.getValue().priorityOrZero(), Comparator.reverseOrder()));

        // Build the maps that will be used for the usage calculation later on.
        IndexLifecycleMetadata metadata = state.metadata().custom(IndexLifecycleMetadata.TYPE);
        policyToDataStream = new HashMap<>(Regex.isSimpleMatchPattern(names.get(0)) ? metadata.getPolicyMetadatas().size() : names.size());
        templateToPolicy = new HashMap<>(indexTemplates.size());
        for (String dataStream : allDataStreams) {
            String indexTemplate = MetadataIndexTemplateService.findV2Template(state.metadata(), indexTemplates, dataStream, false, true);
            if (indexTemplate == null) {
                continue;
            }
            Settings settings = MetadataIndexTemplateService.resolveSettings(state.metadata(), indexTemplate);
            var policyName = LifecycleSettings.LIFECYCLE_NAME_SETTING.get(settings);
            if (names.stream().noneMatch(name -> Regex.simpleMatch(name, policyName))) {
                // If a template's policy doesn't match any of the supplied names, we can skip it later on.
                templateToPolicy.put(indexTemplate, null);
                continue;
            }
            templateToPolicy.put(indexTemplate, policyName);
            policyToDataStream.computeIfAbsent(policyName, k -> new ArrayList<>()).add(dataStream);
        }
    }

    /**
     * Calculate the indices, data streams, and composable templates that use the given policy.
     */
    public ItemUsage calculateUsage(String policyName) {
        List<String> indices = new ArrayList<>();
        for (IndexMetadata indexMetadata : state.metadata().indices().values()) {
            if (policyName.equals(indexMetadata.getLifecyclePolicyName())) {
                indices.add(indexMetadata.getIndex().getName());
            }
        }

        List<String> composableTemplates = new ArrayList<>();
        for (Map.Entry<String, ComposableIndexTemplate> entry : state.metadata().templatesV2().entrySet()) {
            var foundPolicy = templateToPolicy.get(entry.getKey());
            // Extra `containsKey` check to account for templates not using any policy.
            if (foundPolicy == null && templateToPolicy.containsKey(entry.getKey()) == false) {
                Settings settings = MetadataIndexTemplateService.resolveSettings(entry.getValue(), state.metadata().componentTemplates());
                foundPolicy = LifecycleSettings.LIFECYCLE_NAME_SETTING.get(settings);
                // If this method will only be called once, we don't need to keep building the map.
                if (willIterate) {
                    templateToPolicy.put(entry.getKey(), foundPolicy);
                }
            }
            if (policyName.equals(foundPolicy)) {
                composableTemplates.add(entry.getKey());
            }
        }

        return new ItemUsage(indices, policyToDataStream.getOrDefault(policyName, List.of()), composableTemplates);
    }
}
