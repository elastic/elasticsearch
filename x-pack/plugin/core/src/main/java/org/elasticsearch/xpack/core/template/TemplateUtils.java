/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */
package org.elasticsearch.xpack.core.template;

import org.apache.logging.log4j.Logger;
import org.elasticsearch.ElasticsearchParseException;
import org.elasticsearch.Version;
import org.elasticsearch.cluster.ClusterState;
import org.elasticsearch.cluster.metadata.ComposableIndexTemplate;
import org.elasticsearch.cluster.metadata.IndexTemplateMetadata;
import org.elasticsearch.common.Strings;
import org.elasticsearch.common.compress.CompressedXContent;
import org.elasticsearch.common.xcontent.XContentHelper;
import org.elasticsearch.xcontent.XContentFactory;
import org.elasticsearch.xcontent.XContentParser;
import org.elasticsearch.xcontent.XContentParserConfiguration;
import org.elasticsearch.xcontent.XContentType;
import org.elasticsearch.xcontent.json.JsonXContent;
import org.elasticsearch.xpack.core.template.resources.TemplateResources;

import java.io.IOException;
import java.util.Collections;
import java.util.Map;
import java.util.function.Predicate;

import static org.elasticsearch.common.xcontent.XContentHelper.convertToMap;

/**
 * Handling versioned templates for time-based indices in x-pack
 */
public class TemplateUtils {

    private TemplateUtils() {}

    /**
     * Loads a JSON template as a resource and puts it into the provided map
     */
    public static void loadLegacyTemplateIntoMap(
        String resource,
        Map<String, IndexTemplateMetadata> map,
        String templateName,
        String version,
        String versionProperty,
        Logger logger
    ) {
        final String template = loadTemplate(resource, version, versionProperty);
        try (
            XContentParser parser = XContentFactory.xContent(XContentType.JSON).createParser(XContentParserConfiguration.EMPTY, template)
        ) {
            map.put(templateName, IndexTemplateMetadata.Builder.fromXContent(parser, templateName));
        } catch (IOException e) {
            // TODO: should we handle this with a thrown exception?
            logger.error("Error loading template [{}] as part of metadata upgrading", templateName);
        }
    }

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
        ComposableIndexTemplate templateMetadata = state.metadata().templatesV2().get(templateName);
        if (templateMetadata == null) {
            return false;
        }

        return templateMetadata.version() != null && templateMetadata.version() >= currentVersion;
    }

    /**
     * Checks if a versioned template exists, and if it exists checks if it is up-to-date with current version.
     * @param versionKey The property in the mapping's _meta field which stores the version info
     * @param templateName Name of the index template
     * @param state Cluster state
     * @param logger Logger
     */
    public static boolean checkTemplateExistsAndIsUpToDate(String templateName, String versionKey, ClusterState state, Logger logger) {

        return checkTemplateExistsAndVersionMatches(templateName, versionKey, state, logger, Version.CURRENT::equals);
    }

    /**
     * Checks if template with given name exists and if it matches the version predicate given
     * @param versionKey The property in the mapping's _meta field which stores the version info
     * @param templateName Name of the index template
     * @param state Cluster state
     * @param logger Logger
     * @param predicate Predicate to execute on version check
     */
    public static boolean checkTemplateExistsAndVersionMatches(
        String templateName,
        String versionKey,
        ClusterState state,
        Logger logger,
        Predicate<Version> predicate
    ) {

        IndexTemplateMetadata templateMeta = state.metadata().templates().get(templateName);
        if (templateMeta == null) {
            return false;
        }
        CompressedXContent mappings = templateMeta.getMappings();

        // check all mappings contain correct version in _meta
        // we have to parse the source here which is annoying
        if (mappings != null) {
            try {
                Map<String, Object> typeMappingMap = convertToMap(mappings.uncompressed(), false, XContentType.JSON).v2();
                // should always contain one entry with key = typename
                assert (typeMappingMap.size() == 1);
                String key = typeMappingMap.keySet().iterator().next();
                // get the actual mapping entries
                @SuppressWarnings("unchecked")
                Map<String, Object> mappingMap = (Map<String, Object>) typeMappingMap.get(key);
                if (containsCorrectVersion(versionKey, mappingMap, predicate) == false) {
                    return false;
                }
            } catch (ElasticsearchParseException e) {
                logger.error(() -> "Cannot parse the template [" + templateName + "]", e);
                throw new IllegalStateException("Cannot parse the template " + templateName, e);
            }
        }
        return true;
    }

    private static boolean containsCorrectVersion(String versionKey, Map<String, Object> typeMappingMap, Predicate<Version> predicate) {
        @SuppressWarnings("unchecked")
        Map<String, Object> meta = (Map<String, Object>) typeMappingMap.get("_meta");
        if (meta == null) {
            // pre 5.0, cannot be up to date
            return false;
        }
        return predicate.test(Version.fromString((String) meta.get(versionKey)));
    }
}
