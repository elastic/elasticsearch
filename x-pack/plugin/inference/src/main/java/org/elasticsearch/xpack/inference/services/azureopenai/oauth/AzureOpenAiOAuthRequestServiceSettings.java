/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.inference.services.azureopenai.oauth;

import org.elasticsearch.common.Strings;
import org.elasticsearch.xcontent.ConstructingObjectParser;
import org.elasticsearch.xcontent.XContentBuilder;
import org.elasticsearch.xcontent.XContentParserConfiguration;
import org.elasticsearch.xcontent.json.JsonXContent;

import java.io.IOException;
import java.util.Map;
import java.util.Objects;

public record AzureOpenAiOAuthRequestServiceSettings(AzureOpenAiOAuthSettings settings, AzureOpenAiOAuthSecrets secrets) {

    interface Offset {
        int offset();
    }

    private static final ConstructingObjectParser<AzureOpenAiOAuthRequestServiceSettings, Void> PARSER = new ConstructingObjectParser<>(
        "azure_openai_oauth_request_service_settings",
        false,
        args -> {

            var settingsWithOffset = AzureOpenAiOAuthSettings.SettingsWithCounter.create(args, 0);
            var settings = new AzureOpenAiOAuthSettings(settingsWithOffset.settings());

            var secrets = AzureOpenAiOAuthSecrets.Settings.create(args, settingsWithOffset.offset());
            return new AzureOpenAiOAuthRequestServiceSettings(settings, new AzureOpenAiOAuthSecrets(secrets));
        }
    );

    static {
        AzureOpenAiOAuthSettings.initParserFieldsAsRequired(PARSER);
        AzureOpenAiOAuthSecrets.initParserFieldsAsRequired(PARSER);
    }

    public AzureOpenAiOAuthRequestServiceSettings {
        Objects.requireNonNull(settings);
        Objects.requireNonNull(secrets);
    }

    public static AzureOpenAiOAuthRequestServiceSettings fromMap(Map<String, Object> map) {
        try {
            try (
                var xContent = XContentBuilder.builder(JsonXContent.jsonXContent).map(map);
                var parser = JsonXContent.jsonXContent.createParser(XContentParserConfiguration.EMPTY, Strings.toString(xContent))
            ) {
                return PARSER.parse(parser, null);
            }
        } catch (IOException e) {
            throw new IllegalArgumentException("Failed to parse Azure OpenAI OAuth settings and secrets", e);
        }
    }
}
