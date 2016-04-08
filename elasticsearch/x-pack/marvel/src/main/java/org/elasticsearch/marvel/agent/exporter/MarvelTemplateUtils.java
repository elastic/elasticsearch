/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.marvel.agent.exporter;

import org.elasticsearch.ElasticsearchParseException;
import org.elasticsearch.common.bytes.BytesArray;
import org.elasticsearch.common.bytes.BytesReference;
import org.elasticsearch.common.compress.NotXContentException;
import org.elasticsearch.common.io.Streams;
import org.elasticsearch.common.xcontent.XContentHelper;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.util.Locale;
import java.util.regex.Pattern;

public final class MarvelTemplateUtils {

    private static final String TEMPLATE_FILE = "/monitoring-%s.json";
    private static final String TEMPLATE_VERSION_PROPERTY = Pattern.quote("${monitoring.template.version}");

    /** Current version of es and data templates **/
    public static final Integer TEMPLATE_VERSION = 2;

    private MarvelTemplateUtils() {
    }

    /**
     * Loads a built-in template and returns its source.
     */
    public static String loadTemplate(String id, Integer version) {
        String resource = String.format(Locale.ROOT, TEMPLATE_FILE, id);
        try {
            BytesReference source = load(resource);
            validate(source);

            return filter(source, version);
        } catch (Exception e) {
            throw new IllegalArgumentException("Unable to load monitoring template [" + resource + "]", e);
        }
    }

    /**
     * Loads a resource from the classpath and returns it as a {@link BytesReference}
     */
    static BytesReference load(String name) throws IOException {
        try (InputStream is = MarvelTemplateUtils.class.getResourceAsStream(name)) {
            try (ByteArrayOutputStream out = new ByteArrayOutputStream()) {
                Streams.copy(is, out);
                return new BytesArray(out.toByteArray());
            }
        }
    }

    /**
     * Parses and validates that the source is not empty.
     */
    static void validate(BytesReference source) {
        if (source == null) {
            throw new ElasticsearchParseException("Monitoring template must not be null");
        }

        try {
            XContentHelper.convertToMap(source, false).v2();
        } catch (NotXContentException e) {
            throw new ElasticsearchParseException("Monitoring template must not be empty");
        } catch (Exception e) {
            throw new ElasticsearchParseException("Invalid monitoring template", e);
        }
    }

    /**
     * Filters the source: replaces any template version property with the version number
     */
    static String filter(BytesReference source, Integer version) {
        return Pattern.compile(TEMPLATE_VERSION_PROPERTY)
                .matcher(source.toUtf8())
                .replaceAll(String.valueOf(version));
    }
}
