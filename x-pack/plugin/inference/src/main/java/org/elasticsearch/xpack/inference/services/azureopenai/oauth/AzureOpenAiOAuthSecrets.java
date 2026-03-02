/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.inference.services.azureopenai.oauth;

import org.elasticsearch.common.Strings;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.common.io.stream.Writeable;
import org.elasticsearch.core.Nullable;
import org.elasticsearch.xcontent.ConstructingObjectParser;
import org.elasticsearch.xcontent.ParseField;
import org.elasticsearch.xcontent.ToXContentObject;
import org.elasticsearch.xcontent.XContentBuilder;
import org.elasticsearch.xcontent.XContentParserConfiguration;
import org.elasticsearch.xcontent.json.JsonXContent;

import java.io.IOException;
import java.util.Map;
import java.util.Objects;

public class AzureOpenAiOAuthSecrets implements ToXContentObject, Writeable {

    public static final String CLIENT_SECRET_FIELD = "client_secret";

    private record UpdateRequestSettings(@Nullable String clientSecret) {}

    private static final ConstructingObjectParser<UpdateRequestSettings, Void> UPDATE_PARSER = new ConstructingObjectParser<>(
        "azure_openai_oauth_secrets_update",
        true,
        args -> new UpdateRequestSettings((String) args[0])
    );

    public record Settings(String clientSecret) {
        public Settings {
            Objects.requireNonNull(clientSecret);
        }

        @SuppressWarnings("unchecked")
        public static Settings create(Object clientSecret) {
            return new Settings((String) clientSecret);
        }

        public static Settings create(Object[] args, int offset) {
            return create(args[offset]);
        }
    }

    private static final ConstructingObjectParser<Settings, Void> STORAGE_PARSER = new ConstructingObjectParser<>(
        "azure_openai_oauth_secrets",
        true,
        args -> Settings.create(args[0])
    );

    static {
        initParserFieldsAsRequired(STORAGE_PARSER);
        initParser(UPDATE_PARSER, true);
    }

    public static <Value, Context> void initParserFieldsAsRequired(ConstructingObjectParser<Value, Context> parser) {
        initParser(parser, false);
    }

    private static <Value, Context> void initParser(ConstructingObjectParser<Value, Context> parser, boolean isForUpdate) {
        parser.declareString(
            isForUpdate ? ConstructingObjectParser.optionalConstructorArg() : ConstructingObjectParser.constructorArg(),
            new ParseField(CLIENT_SECRET_FIELD)
        );
    }

    public static AzureOpenAiOAuthSecrets fromMap(Map<String, Object> map) {
        try {
            try (
                var xContent = XContentBuilder.builder(JsonXContent.jsonXContent).map(map);
                var parser = JsonXContent.jsonXContent.createParser(
                    XContentParserConfiguration.EMPTY,
                    Strings.toString(xContent)
                )
            ) {
                return new AzureOpenAiOAuthSecrets(STORAGE_PARSER.parse(parser, null));
            }
        } catch (IOException e) {
            throw new IllegalArgumentException("Failed to parse Azure OpenAI OAuth secrets", e);
        }
    }

    private final Settings settings;

    AzureOpenAiOAuthSecrets(Settings settings) {
        this.settings = Objects.requireNonNull(settings);
    }

    public AzureOpenAiOAuthSecrets(StreamInput in) throws IOException {
        this(new Settings(in.readString()));
    }

    public String getClientSecret() {
        return settings.clientSecret();
    }

    public AzureOpenAiOAuthSecrets updateServiceSettings(Map<String, Object> serviceSettings) {
        var updated = fromMapForUpdate(serviceSettings);
        return new AzureOpenAiOAuthSecrets(
            new Settings(Objects.requireNonNullElse(updated.clientSecret(), settings.clientSecret()))
        );
    }

    private static UpdateRequestSettings fromMapForUpdate(Map<String, Object> map) {
        try {
            try (
                var xContent = XContentBuilder.builder(JsonXContent.jsonXContent).map(map);
                var parser = JsonXContent.jsonXContent.createParser(
                    XContentParserConfiguration.EMPTY,
                    Strings.toString(xContent)
                )
            ) {
                return UPDATE_PARSER.parse(parser, null);
            }
        } catch (IOException e) {
            throw new IllegalArgumentException("Failed to parse Azure OpenAI OAuth secrets during update", e);
        }
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        out.writeString(settings.clientSecret());
    }

    @Override
    public XContentBuilder toXContent(XContentBuilder builder, Params params) throws IOException {
        builder.startObject();
        builder.field(CLIENT_SECRET_FIELD, settings.clientSecret());
        builder.endObject();
        return builder;
    }
}
