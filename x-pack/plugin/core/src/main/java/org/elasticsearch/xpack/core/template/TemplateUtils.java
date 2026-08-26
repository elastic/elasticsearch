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
import java.util.Map;

import static java.nio.charset.StandardCharsets.UTF_8;

/**
 * Handling versioned templates for time-based indices in x-pack
 */
public class TemplateUtils {

    private TemplateUtils() {}

    /**
     * Loads a built-in template and returns its source after replacing given variables.
     */
    public static String loadTemplate(String resource, String version, String versionProperty, Map<String, String> variables) {
        return loadTemplate(resource, version, versionProperty, variables, false);
    }

    /**
     * Loads a built-in template and returns its source after replacing given variables.
     *
     * If {@code validateVersion} is true and assertions are enabled, a root version field with the provided version
     * is expected to exist after applying substitutions.
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

    /**
     * A template parser leveraging a {@link Template.TemplateDecorator} to modify templates during parsing.
     */
    public interface TemplateParser<T> {
        T apply(XContentParser parser, String resource, Template.TemplateDecorator decorator) throws IOException;
    }

    /**
     * Loads a built-in template, replaces given variables and parses it using the provided {@link TemplateParser}.
     *
     * If {@code validateVersion} is true and assertions are enabled, a root version field with the provided version
     * is expected to exist after applying substitutions.
     */
    public static <T> T loadTemplate(
        String resource,
        String version,
        String versionProperty,
        Map<String, String> variables,
        boolean validateVersion,
        TemplateParser<T> templateParser
    ) throws IOException {
        Template.TemplateDecorator decorator = TemplateDecoratorProvider.getInstance();
        return loadTemplate(resource, version, versionProperty, variables, validateVersion, templateParser, decorator);
    }

    static <T> T loadTemplate(
        String resource,
        String version,
        String versionProperty,
        Map<String, String> variables,
        boolean validateVersion,
        TemplateParser<T> templateParser,
        Template.TemplateDecorator decorator
    ) throws IOException {
        String source = loadTemplate(resource, version, versionProperty, variables, validateVersion);
        try (var parser = JsonXContent.jsonXContent.createParser(XContentParserConfiguration.EMPTY, source.getBytes(UTF_8))) {
            return templateParser.apply(parser, resource, decorator);
        }
    }

    /**
     * Validates that the source is not empty and parses it to check its correctness if assertions are enabled.
     *
     * If {@code validateVersion} is true and assertions are enabled, a root version field with the provided version
     * is expected to exist in the substituted {@source}.
     */
    static void validate(String source, String version, boolean validateVersion) {
        if (source == null) {
            throw new ElasticsearchParseException("Template must not be null");
        }
        if (Strings.isEmpty(source)) {
            throw new ElasticsearchParseException("Template must not be empty");
        }
        assert validateJson(source, version, validateVersion);
    }

    private static boolean validateJson(String source, String version, boolean validateVersion) {
        Map<String, Object> map;
        try {
            map = XContentHelper.convertToMap(JsonXContent.jsonXContent, source, false);
        } catch (Exception e) {
            throw new ElasticsearchParseException("Invalid template", e);
        }
        if (validateVersion && Integer.valueOf(version).equals(map.get("version")) == false) {
            throw new IllegalArgumentException("Template must have a version property set to the given version property");
        }
        return true;
    }

    static String replaceVariables(String input, String version, String versionProperty, Map<String, String> variables) {
        String template = replaceVariable(input, versionProperty, version);
        for (Map.Entry<String, String> variable : variables.entrySet()) {
            template = replaceVariable(template, variable.getKey(), variable.getValue());
        }
        return template;
    }

    /**
     * Replaces all occurrences of given variable with the value
     */
    static String replaceVariable(String input, String variable, String value) {
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
