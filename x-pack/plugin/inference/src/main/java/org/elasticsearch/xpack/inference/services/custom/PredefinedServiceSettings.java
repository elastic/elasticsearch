/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.inference.services.custom;

import org.elasticsearch.common.ValidationException;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.inference.ModelConfigurations;
import org.elasticsearch.inference.TaskType;
import org.elasticsearch.xcontent.XContentBuilder;
import org.elasticsearch.xcontent.XContentParser;
import org.elasticsearch.xcontent.XContentParserConfiguration;
import org.elasticsearch.xcontent.json.JsonXContent;
import org.elasticsearch.xpack.inference.services.ConfigurationParseContext;

import java.io.IOException;
import java.util.HashMap;
import java.util.Map;

import static org.elasticsearch.xpack.inference.services.ServiceUtils.extractOptionalMap;

// This class is a wrapper for CustomServiceSettings with the purpose of providing serialization that is backwards compatible with the
// existing format of serialized inference endpoints (for non-custom service endpoints)
public class PredefinedServiceSettings extends CustomServiceSettings {
    public static final String PARAMETERS = "parameters";

    private final Map<String, Object> parameters;

    public static PredefinedServiceSettings fromMap(
        Map<String, Object> parsedMap,
        ConfigurationParseContext context,
        TaskType taskType,
        String inferenceId
    ) {
        ValidationException validationException = new ValidationException();

        if (validationException.validationErrors().isEmpty() == false) {
            throw validationException;
        }

        Map<String, Object> parameters = extractOptionalMap(
            parsedMap,
            PARAMETERS,
            ModelConfigurations.SERVICE_SETTINGS,
            validationException
        );

        return new PredefinedServiceSettings(CustomServiceSettings.fromMap(parsedMap, context, taskType), parameters);
    }

    public static PredefinedServiceSettings fromMap(
        Map<String, Object> map,
        ConfigurationParseContext context,
        TaskType taskType,
        PredefinedCustomServiceSchema schema
    ) {
        var parsedServiceSettings = parseServiceSettingsMap(schema, map);

        ValidationException validationException = new ValidationException();

        if (validationException.validationErrors().isEmpty() == false) {
            throw validationException;
        }

        Map<String, Object> parameters = extractOptionalMap(
            parsedServiceSettings,
            PARAMETERS,
            ModelConfigurations.SERVICE_SETTINGS,
            validationException
        );

        return new PredefinedServiceSettings(CustomServiceSettings.fromMap(parsedServiceSettings, context, taskType), parameters);
    }

    public PredefinedServiceSettings(CustomServiceSettings customServiceSettings, Map<String, Object> parameters) {
        super(
            customServiceSettings.getTextEmbeddingSettings(),
            customServiceSettings.getUrl(),
            customServiceSettings.getHeaders(),
            customServiceSettings.getQueryParameters(),
            customServiceSettings.getRequestContentString(),
            customServiceSettings.getResponseJsonParser(),
            customServiceSettings.rateLimitSettings()
        );

        this.parameters = parameters;
    }

    public PredefinedServiceSettings(StreamInput in) throws IOException {
        super(in);
        this.parameters = in.readGenericMap();
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        super.writeTo(out);
        out.writeGenericMap(parameters);
    }

    // TODO: Add equals & hashcode methods if needed.

    // TODO: Check if there are any other values that need to be serialized here.
    @Override
    public XContentBuilder toXContent(XContentBuilder builder, Params params) throws IOException {
        var parameters = getParameters();
        var similarity = similarity();
        if (similarity != null) {
            parameters.put("similarity", similarity);
        }
        builder.value(getParameters());
        return builder;
    }

    public Map<String, Object> getParameters() {
        return parameters;
    }

    private static Map<String, Object> parseServiceSettingsMap(
        PredefinedCustomServiceSchema schema,
        Map<String, Object> unparsedServiceSettingsMap
    ) {
        var customServiceSettings = schema.generateServiceSettings(unparsedServiceSettingsMap);
        var parsedServiceSettings = parseCustomServiceSettings(customServiceSettings);

        Map<String, Object> parameters = new HashMap<>();
        for (String parameter : schema.getServiceSettingsParameters()) {
            if (unparsedServiceSettingsMap.containsKey(parameter)) {
                parameters.put(parameter, unparsedServiceSettingsMap.remove(parameter));
            }
        }
        parsedServiceSettings.put("parameters", parameters);

        for (String setting : schema.getNonParameterServiceSettings()) {
            if (unparsedServiceSettingsMap.containsKey(setting)) {
                parsedServiceSettings.put(setting, unparsedServiceSettingsMap.remove(setting));
            }
        }

        return parsedServiceSettings;
    }

    private static Map<String, Object> parseCustomServiceSettings(String customServiceSettings) {
        try (XContentParser parser = JsonXContent.jsonXContent.createParser(XContentParserConfiguration.EMPTY, customServiceSettings)) {
            return parser.map();
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }
}
