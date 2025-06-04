/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */
package org.elasticsearch.xpack.core.template;

import org.elasticsearch.cluster.ClusterState;
import org.elasticsearch.cluster.metadata.ComposableIndexTemplate;
import org.elasticsearch.common.Strings;
import org.elasticsearch.common.xcontent.XContentHelper;
import org.elasticsearch.exception.ElasticsearchParseException;
import org.elasticsearch.xcontent.json.JsonXContent;
import org.elasticsearch.xpack.core.template.resources.TemplateResources;

import java.util.Collections;
import java.util.Map;

/**
 * Handling versioned templates for time-based indices in x-pack
 */
public class TemplateUtils {

    private TemplateUtils() {}

    /**
     * Loads a built-in template and returns its source.
     */
    public static String loadTemplate(String resource, String version, String versionProperty) {
        return loadTemplate(resource, version, versionProperty, Collections.emptyMap());
    }

    /**
     * Loads a built-in template and returns its source after replacing given variables.
     */
    public static String loadTemplate(String resource, String version, String versionProperty, Map<String, String> variables) {
        try {
            String source = TemplateResources.load(resource);
            source = replaceVariables(source, version, versionProperty, variables);
            validate(source);
            return source;
        } catch (Exception e) {
            throw new IllegalArgumentException("Unable to load template [" + resource + "]", e);
        }
    }

    /**
     * Parses and validates that the source is not empty.
     */
    public static void validate(String source) {
        if (source == null) {
            throw new ElasticsearchParseException("Template must not be null");
        }
        if (Strings.isEmpty(source)) {
            throw new ElasticsearchParseException("Template must not be empty");
        }

        try {
            XContentHelper.convertToMap(JsonXContent.jsonXContent, source, false);
        } catch (Exception e) {
            throw new ElasticsearchParseException("Invalid template", e);
        }
    }

    public static String replaceVariables(String input, String version, String versionProperty, Map<String, String> variables) {
        String template = replaceVariable(input, versionProperty, version);
        for (Map.Entry<String, String> variable : variables.entrySet()) {
            template = replaceVariable(template, variable.getKey(), variable.getValue());
        }
        return template;
    }

    /**
     * Replaces all occurrences of given variable with the value
     */
    public static String replaceVariable(String input, String variable, String value) {
        return input.replace("${" + variable + "}", value);
    }

    /**
     * Checks if a versioned template exists, and if it exists checks if the version is greater than or equal to the current version.
     * @param templateName Name of the index template
     * @param state Cluster state
     * @param currentVersion The current version to check against
     */
    public static boolean checkTemplateExistsAndVersionIsGTECurrentVersion(String templateName, ClusterState state, long currentVersion) {
        ComposableIndexTemplate templateMetadata = state.metadata().getProject().templatesV2().get(templateName);
        if (templateMetadata == null) {
            return false;
        }

        return templateMetadata.version() != null && templateMetadata.version() >= currentVersion;
    }
}
