/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.core.ilm;

import org.elasticsearch.common.bytes.BytesArray;
import org.elasticsearch.common.compress.NotXContentException;
import org.elasticsearch.common.xcontent.XContentHelper;
import org.elasticsearch.exception.ElasticsearchParseException;
import org.elasticsearch.xcontent.NamedXContentRegistry;
import org.elasticsearch.xcontent.XContentParser;
import org.elasticsearch.xcontent.XContentParserConfiguration;
import org.elasticsearch.xcontent.XContentType;
import org.elasticsearch.xpack.core.template.resources.TemplateResources;

import java.io.IOException;
import java.util.Map;

/**
 * A utility class used for index lifecycle policies
 */
public class LifecyclePolicyUtils {

    private LifecyclePolicyUtils() {}

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

            return parsePolicy(source, name, xContentRegistry, XContentType.JSON);
        } catch (Exception e) {
            throw new IllegalArgumentException("unable to load policy [" + name + "] from [" + resource + "]", e);
        }
    }

    /**
     * Parses lifecycle policy based on the provided content type without doing any variable substitution.
     * It is caller's responsibility to do any variable substitution if required.
     */
    public static LifecyclePolicy parsePolicy(
        String rawPolicy,
        String name,
        NamedXContentRegistry xContentRegistry,
        XContentType contentType
    ) throws IOException {
        try (
            XContentParser parser = contentType.xContent()
                .createParser(XContentParserConfiguration.EMPTY.withRegistry(xContentRegistry), rawPolicy)
        ) {
            LifecyclePolicy policy = LifecyclePolicy.parse(parser, name);
            policy.validate();
            return policy;
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
        return input.replace("${" + variable + "}", value);
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
}
