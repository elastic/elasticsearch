/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */
package org.elasticsearch.xpack.core.template;

import org.elasticsearch.ElasticsearchParseException;
import org.elasticsearch.cluster.ClusterState;
import org.elasticsearch.cluster.metadata.ComposableIndexTemplate;
import org.elasticsearch.cluster.metadata.Template;
import org.elasticsearch.cluster.metadata.TemplateDecoratorProvider;
import org.elasticsearch.common.Strings;
import org.elasticsearch.common.xcontent.XContentHelper;
import org.elasticsearch.xcontent.XContentParser;
import org.elasticsearch.xcontent.XContentParserConfiguration;
import org.elasticsearch.xcontent.json.JsonXContent;
import org.elasticsearch.xpack.core.template.resources.TemplateResources;

import java.io.IOException;
import java.util.Collections;
import java.util.Map;
import java.util.regex.Pattern;

import static java.nio.charset.StandardCharsets.UTF_8;

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

    public static String loadTemplate(String resource, String version, String versionProperty, Map<String, String> variables) {
        return loadTemplate(resource, version, versionProperty, variables, false);
    }

    /**
     * Loads a built-in template and returns its source after replacing given variables.
     */
    public static String loadTemplate(
        String resource,
        String version,
        String versionProperty,
        Map<String, String> variables,
        boolean validateVersion
    ) {
        try {
            String source = TemplateResources.load(resource);
            source = replaceVariables(source, version, versionProperty, variables);
            validate(source, version, validateVersion);
            return source;
        } catch (Exception | AssertionError e) {
            throw new IllegalArgumentException("Unable to load template [" + resource + "]", e);
        }
    }

    public interface TemplateParser<T> {
        T apply(XContentParser parser, String resource, Template.TemplateDecorator decorator) throws IOException;
    }

    public static <T> T loadTemplate(
        String resource,
        String version,
        String versionProperty,
        Map<String, String> variables,
        boolean validateVersion,
        TemplateParser<T> templateParser
    ) throws IOException {
        String source = loadTemplate(resource, version, versionProperty, variables, validateVersion);
        try (var parser = JsonXContent.jsonXContent.createParser(XContentParserConfiguration.EMPTY, source.getBytes(UTF_8))) {
            return templateParser.apply(parser, resource, TemplateDecoratorProvider.getInstance());
        }
    }

    /**
     * Parses and validates that the source is not empty.
     */
    public static void validate(String source, String version, boolean validateVersion) {
        if (source == null) {
            throw new ElasticsearchParseException("Template must not be null");
        }
        if (Strings.isEmpty(source)) {
            throw new ElasticsearchParseException("Template must not be empty");
        }

        assert XContentHelper.convertToMap(JsonXContent.jsonXContent, source, false) != null : "Invalid json template";
        if (validateVersion) {
            assert Pattern.compile("\"version\"\\s*:\\s*" + version).matcher(source).find()
                : "Template must have a version property set to the given version property";
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
