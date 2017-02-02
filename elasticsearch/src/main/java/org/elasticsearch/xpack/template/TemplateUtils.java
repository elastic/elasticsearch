/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.xpack.template;

import org.elasticsearch.ElasticsearchParseException;
import org.elasticsearch.common.bytes.BytesArray;
import org.elasticsearch.common.bytes.BytesReference;
import org.elasticsearch.common.compress.NotXContentException;
import org.elasticsearch.common.io.Streams;
import org.elasticsearch.common.xcontent.XContentHelper;
import org.elasticsearch.common.xcontent.XContentType;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.nio.charset.StandardCharsets;
import java.util.regex.Pattern;

/**
 * Handling versioned templates for time-based indices in x-pack
 */
public class TemplateUtils {

    private TemplateUtils() {}

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
        try (InputStream is = TemplateUtils.class.getResourceAsStream(name)) {
            try (ByteArrayOutputStream out = new ByteArrayOutputStream()) {
                Streams.copy(is, out);
                return new BytesArray(out.toByteArray());
            }
        }
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

}
