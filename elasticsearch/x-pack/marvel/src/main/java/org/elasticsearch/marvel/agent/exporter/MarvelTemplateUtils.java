/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.marvel.agent.exporter;

import org.elasticsearch.common.Strings;
import org.elasticsearch.common.io.Streams;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.util.Properties;

public final class MarvelTemplateUtils {

    static final String INDEX_TEMPLATE_FILE         = "/marvel-es.json";
    static final String INDEX_TEMPLATE_NAME_PREFIX  = ".marvel-es-";

    static final String DATA_TEMPLATE_FILE          = "/marvel-es-data.json";
    static final String DATA_TEMPLATE_NAME_PREFIX   = ".marvel-es-data-";

    static final String PROPERTIES_FILE             = "/marvel.properties";
    static final String VERSION_FIELD               = "marvel.template.version";

    public static final Integer TEMPLATE_VERSION    = loadTemplateVersion();

    private MarvelTemplateUtils() {
    }

    /**
     * Loads the default template for the timestamped indices
     */
    public static byte[] loadTimestampedIndexTemplate() {
        try {
            return load(INDEX_TEMPLATE_FILE);
        } catch (IOException e) {
            throw new IllegalStateException("unable to load marvel template", e);
        }
    }

    /**
     * Loads the default template for the data index
     */
    public static byte[] loadDataIndexTemplate() {
        try {
            return load(DATA_TEMPLATE_FILE);
        } catch (IOException e) {
            throw new IllegalStateException("unable to load marvel data template", e);
        }
    }

    /**
     * Loads a resource with a given name and returns it as a byte array.
     */
    static byte[] load(String name) throws IOException {
        try (InputStream is = MarvelTemplateUtils.class.getResourceAsStream(name)) {
            ByteArrayOutputStream out = new ByteArrayOutputStream();
            Streams.copy(is, out);
            return out.toByteArray();
        }
    }

    /**
     * Loads the current version of templates
     *
     * When executing tests in Intellij, the properties file might not be
     * resolved: try running 'gradle processResources' first.
     */
    static Integer loadTemplateVersion() {
        try (InputStream is = MarvelTemplateUtils.class.getResourceAsStream(PROPERTIES_FILE)) {
            Properties properties = new Properties();
            properties.load(is);
            String version = properties.getProperty(VERSION_FIELD);
            if (Strings.hasLength(version)) {
                return Integer.parseInt(version);
            }
            throw new IllegalArgumentException("no marvel template version found");
        } catch (NumberFormatException e) {
            throw new IllegalArgumentException("failed to parse marvel template version");
        } catch (IOException e) {
            throw new IllegalArgumentException("failed to load marvel template version");
        }
    }

    public static String indexTemplateName() {
        return indexTemplateName(TEMPLATE_VERSION);
    }

    public static String indexTemplateName(Integer version) {
        return templateName(INDEX_TEMPLATE_NAME_PREFIX, version);
    }

    public static String dataTemplateName() {
        return dataTemplateName(TEMPLATE_VERSION);
    }

    public static String dataTemplateName(Integer version) {
        return templateName(DATA_TEMPLATE_NAME_PREFIX, version);
    }

    static String templateName(String prefix, Integer version) {
        assert version != null && version >= 0 : "version must be not null and greater or equal to zero";
        return prefix + String.valueOf(version);
    }
}
