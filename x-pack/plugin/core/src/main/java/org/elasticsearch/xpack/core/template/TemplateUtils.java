/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.xpack.core.template;

import org.apache.logging.log4j.Logger;
import org.apache.logging.log4j.message.ParameterizedMessage;
import org.elasticsearch.ElasticsearchParseException;
import org.elasticsearch.Version;
import org.elasticsearch.cluster.ClusterState;
import org.elasticsearch.cluster.metadata.IndexTemplateMetaData;
import org.elasticsearch.common.bytes.BytesArray;
import org.elasticsearch.common.bytes.BytesReference;
import org.elasticsearch.common.collect.ImmutableOpenMap;
import org.elasticsearch.common.compress.CompressedXContent;
import org.elasticsearch.common.compress.NotXContentException;
import org.elasticsearch.common.io.Streams;
import org.elasticsearch.common.xcontent.LoggingDeprecationHandler;
import org.elasticsearch.common.xcontent.NamedXContentRegistry;
import org.elasticsearch.common.xcontent.XContentFactory;
import org.elasticsearch.common.xcontent.XContentHelper;
import org.elasticsearch.common.xcontent.XContentParser;
import org.elasticsearch.common.xcontent.XContentType;

import java.io.IOException;
import java.util.Map;
import java.util.function.Predicate;
import java.util.regex.Pattern;

import static org.elasticsearch.common.xcontent.XContentHelper.convertToMap;

/**
 * Handling versioned templates for time-based indices in x-pack
 */
public class TemplateUtils {

    private TemplateUtils() {}

    /**
     * Loads a JSON template as a resource and puts it into the provided map
     */
    public static void loadTemplateIntoMap(String resource, Map<String, IndexTemplateMetaData> map, String templateName, String version,
                                           String versionProperty, Logger logger) {
        final String template = loadTemplate(resource, version, versionProperty);
        try (XContentParser parser = XContentFactory.xContent(XContentType.JSON)
                .createParser(NamedXContentRegistry.EMPTY, LoggingDeprecationHandler.INSTANCE, template)) {
            map.put(templateName, IndexTemplateMetaData.Builder.fromXContent(parser, templateName));
        } catch (IOException e) {
            // TODO: should we handle this with a thrown exception?
            logger.error("Error loading template [{}] as part of metadata upgrading", templateName);
        }
    }

    /**
     * Loads a built-in template and returns its source.
     */
    public static String loadTemplate(String resource, String version, String versionProperty) {
        try {
            BytesReference source = load(resource);
            validate(source);

            return filter(source, version, versionProperty);
        } catch (Exception e) {
            throw new IllegalArgumentException("Unable to load template [" + resource + "]", e);
        }
    }

    /**
     * Loads a resource from the classpath and returns it as a {@link BytesReference}
     */
    public static BytesReference load(String name) throws IOException {
        return Streams.readFully(TemplateUtils.class.getResourceAsStream(name));
    }

    /**
     * Parses and validates that the source is not empty.
     */
    public static void validate(BytesReference source) {
        if (source == null) {
            throw new ElasticsearchParseException("Template must not be null");
        }

        try {
            XContentHelper.convertToMap(source, false, XContentType.JSON).v2();
        } catch (NotXContentException e) {
            throw new ElasticsearchParseException("Template must not be empty");
        } catch (Exception e) {
            throw new ElasticsearchParseException("Invalid template", e);
        }
    }

    /**
     * Filters the source: replaces any template version property with the version number
     */
    public static String filter(BytesReference source, String version, String versionProperty) {
        return Pattern.compile(versionProperty)
                .matcher(source.utf8ToString())
                .replaceAll(version);
    }

    /**
     * Checks if a versioned template exists, and if it exists checks if the version is greater than or equal to the current version.
     * @param templateName Name of the index template
     * @param state Cluster state
     */
    public static boolean checkTemplateExistsAndVersionIsGTECurrentVersion(String templateName, ClusterState state) {
        IndexTemplateMetaData templateMetaData = state.metaData().templates().get(templateName);
        if (templateMetaData == null) {
            return false;
        }

        return templateMetaData.version() != null && templateMetaData.version() >= Version.CURRENT.id;
    }

    /**
     * Checks if a versioned template exists, and if it exists checks if it is up-to-date with current version.
     * @param versionKey The property in the mapping's _meta field which stores the version info
     * @param templateName Name of the index template
     * @param state Cluster state
     * @param logger Logger
     */
    public static boolean checkTemplateExistsAndIsUpToDate(
        String templateName, String versionKey, ClusterState state, Logger logger) {

        return checkTemplateExistsAndVersionMatches(templateName, versionKey, state, logger,
            Version.CURRENT::equals);
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
        String templateName, String versionKey, ClusterState state, Logger logger, Predicate<Version> predicate) {

        IndexTemplateMetaData templateMeta = state.metaData().templates().get(templateName);
        if (templateMeta == null) {
            return false;
        }
        ImmutableOpenMap<String, CompressedXContent> mappings = templateMeta.getMappings();
        // check all mappings contain correct version in _meta
        // we have to parse the source here which is annoying
        for (Object typeMapping : mappings.values().toArray()) {
            CompressedXContent typeMappingXContent = (CompressedXContent) typeMapping;
            try {
                Map<String, Object> typeMappingMap = convertToMap(
                    new BytesArray(typeMappingXContent.uncompressed()), false,
                    XContentType.JSON).v2();
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
                logger.error(new ParameterizedMessage(
                    "Cannot parse the template [{}]", templateName), e);
                throw new IllegalStateException("Cannot parse the template " + templateName, e);
            }
        }
        return true;
    }

    private static boolean containsCorrectVersion(String versionKey, Map<String, Object> typeMappingMap,
                                                  Predicate<Version> predicate) {
        @SuppressWarnings("unchecked")
        Map<String, Object> meta = (Map<String, Object>) typeMappingMap.get("_meta");
        if (meta == null) {
            // pre 5.0, cannot be up to date
            return false;
        }
        return predicate.test(Version.fromString((String) meta.get(versionKey)));
    }

}
