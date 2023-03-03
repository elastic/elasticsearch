/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.core.ingest;

import org.elasticsearch.ElasticsearchParseException;
import org.elasticsearch.common.bytes.BytesArray;
import org.elasticsearch.common.bytes.BytesReference;
import org.elasticsearch.common.compress.NotXContentException;
import org.elasticsearch.common.xcontent.XContentHelper;
import org.elasticsearch.core.Streams;
import org.elasticsearch.ingest.PipelineConfiguration;
import org.elasticsearch.xcontent.XContentType;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.util.Map;
import java.util.regex.Pattern;

/**
 * A utility class used for ingest pipeline management.
 */
public class PipelineConfigurationTemplate {

    private final String id;

    private final String resource;

    private final Map<String, String> variables;

    private final XContentType xContentType;

    public PipelineConfigurationTemplate(String id, String resource, Map<String, String> variables, XContentType xContentType) {
        this.id = id;
        this.resource = resource;
        this.variables = variables;
        this.xContentType = xContentType;
    }

    public PipelineConfigurationTemplate(String id, String resource, Map<String, String> variables) {
        this(id, resource, variables, XContentType.JSON);
    }

    public String id() {
        return id;
    }

    public BytesReference source() throws IOException {
        return load(resource);
    }

    public XContentType xContentType() {
        return xContentType;
    }

    public BytesReference parsedSource() throws IOException {
        BytesReference parsedTemplate = replaceVariables(source(), variables);
        validate(parsedTemplate);

        return parsedTemplate;
    }

    public PipelineConfiguration loadPipelineConfiguration() throws IOException {
        return new PipelineConfiguration(id(), parsedSource(), xContentType());
    }

    /**
     * Loads a resource from the classpath and returns it as a {@link BytesReference}
     */
    private static BytesReference load(String name) throws IOException {
        try (InputStream is = PipelineConfigurationTemplate.class.getResourceAsStream(name)) {
            try (ByteArrayOutputStream out = new ByteArrayOutputStream()) {
                Streams.copy(is, out);
                return new BytesArray(out.toByteArray());
            }
        }
    }

    private static BytesReference replaceVariables(BytesReference input, Map<String, String> variables) {
        String template = input.utf8ToString();
        for (Map.Entry<String, String> variable : variables.entrySet()) {
            template = replaceVariable(template, variable.getKey(), variable.getValue());
        }
        return new BytesArray(template);
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
    private static void validate(BytesReference source) {
        if (source == null) {
            throw new ElasticsearchParseException("pipeline configuration must not be null");
        }
        try {
            XContentHelper.convertToMap(source, false, XContentType.JSON).v2();
        } catch (NotXContentException e) {
            throw new ElasticsearchParseException("pipeline configuration must not be empty");
        } catch (Exception e) {
            throw new ElasticsearchParseException("invalid pipeline configuration", e);
        }
    }
}
