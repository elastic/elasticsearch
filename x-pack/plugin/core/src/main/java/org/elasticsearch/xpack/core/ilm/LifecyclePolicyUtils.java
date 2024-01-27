/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.core.ilm;

import org.elasticsearch.ElasticsearchParseException;
import org.elasticsearch.action.support.IndicesOptions;
import org.elasticsearch.cluster.ClusterState;
import org.elasticsearch.cluster.metadata.IndexNameExpressionResolver;
import org.elasticsearch.cluster.metadata.ItemUsage;
import org.elasticsearch.cluster.metadata.MetadataIndexTemplateService;
import org.elasticsearch.common.bytes.BytesArray;
import org.elasticsearch.common.compress.NotXContentException;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.xcontent.XContentHelper;
import org.elasticsearch.xcontent.NamedXContentRegistry;
import org.elasticsearch.xcontent.XContentParser;
import org.elasticsearch.xcontent.XContentParserConfiguration;
import org.elasticsearch.xcontent.XContentType;
import org.elasticsearch.xpack.core.template.resources.TemplateResources;

import java.util.List;
import java.util.Map;
import java.util.regex.Pattern;
import java.util.stream.Collectors;

/**
 * A utility class used for index lifecycle policies
 */
public class LifecyclePolicyUtils {

    private LifecyclePolicyUtils() {};

    /**
     * Loads a built-in index lifecycle policy and returns its source.
     */
    public static LifecyclePolicy loadPolicy(
        String name,
        String resource,
        Map<String, String> variables,
        NamedXContentRegistry xContentRegistry
    ) {
        try {
            String source = TemplateResources.load(resource);
            source = replaceVariables(source, variables);
            validate(source);

            try (
                XContentParser parser = XContentType.JSON.xContent()
                    .createParser(XContentParserConfiguration.EMPTY.withRegistry(xContentRegistry), source)
            ) {
                LifecyclePolicy policy = LifecyclePolicy.parse(parser, name);
                policy.validate();
                return policy;
            }
        } catch (Exception e) {
            throw new IllegalArgumentException("unable to load policy [" + name + "] from [" + resource + "]", e);
        }
    }

    private static String replaceVariables(String template, Map<String, String> variables) {
        for (Map.Entry<String, String> variable : variables.entrySet()) {
            template = replaceVariable(template, variable.getKey(), variable.getValue());
        }
        return template;
    }

    /**
     * Replaces all occurrences of given variable with the value
     */
    public static String replaceVariable(String input, String variable, String value) {
        return Pattern.compile("${" + variable + "}", Pattern.LITERAL).matcher(input).replaceAll(value);
    }

    /**
     * Parses and validates that the source is not empty.
     */
    private static void validate(String source) {
        if (source == null) {
            throw new ElasticsearchParseException("policy must not be null");
        }

        try {
            XContentHelper.convertToMap(new BytesArray(source), false, XContentType.JSON).v2();
        } catch (NotXContentException e) {
            throw new ElasticsearchParseException("policy must not be empty");
        } catch (Exception e) {
            throw new ElasticsearchParseException("invalid policy", e);
        }
    }

    /**
     * Given a cluster state and ILM policy, calculate the {@link ItemUsage} of
     * the policy (what indices, data streams, and templates use the policy)
     */
    public static ItemUsage calculateUsage(
        final IndexNameExpressionResolver indexNameExpressionResolver,
        final ClusterState state,
        final String policyName
    ) {
        final List<String> indices = state.metadata()
            .indices()
            .values()
            .stream()
            .filter(indexMetadata -> policyName.equals(indexMetadata.getLifecyclePolicyName()))
            .map(indexMetadata -> indexMetadata.getIndex().getName())
            .collect(Collectors.toList());

        final List<String> allDataStreams = indexNameExpressionResolver.dataStreamNames(
            state,
            IndicesOptions.LENIENT_EXPAND_OPEN_CLOSED_HIDDEN
        );

        final List<String> dataStreams = allDataStreams.stream().filter(dsName -> {
            String indexTemplate = MetadataIndexTemplateService.findV2Template(state.metadata(), dsName, false);
            if (indexTemplate != null) {
                Settings settings = MetadataIndexTemplateService.resolveSettings(state.metadata(), indexTemplate);
                return policyName.equals(LifecycleSettings.LIFECYCLE_NAME_SETTING.get(settings));
            } else {
                return false;
            }
        }).collect(Collectors.toList());

        final List<String> composableTemplates = state.metadata().templatesV2().keySet().stream().filter(templateName -> {
            Settings settings = MetadataIndexTemplateService.resolveSettings(state.metadata(), templateName);
            return policyName.equals(LifecycleSettings.LIFECYCLE_NAME_SETTING.get(settings));
        }).collect(Collectors.toList());

        return new ItemUsage(indices, dataStreams, composableTemplates);
    }
}
