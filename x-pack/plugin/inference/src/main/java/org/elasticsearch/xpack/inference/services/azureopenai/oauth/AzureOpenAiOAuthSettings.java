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
import java.util.List;
import java.util.Map;
import java.util.Objects;

public class AzureOpenAiOAuthSettings implements ToXContentObject, Writeable {
    public static final String OAUTH_FIELD = "oauth";
    public static final String CLIENT_ID_FIELD = "client_id";
    public static final String TENANT_ID_FIELD = "tenant_id";
    public static final String SCOPES_FIELD = "scopes";

    private record UpdateRequestSettings(@Nullable String clientId, @Nullable String tenantId, @Nullable List<String> scopes) {}

    // TODO let's not have an object, instead we'll flatten this so it isn't within oauth. When parsing from storage we'll need to do it
    // in AzureOpenAiEmbeddingsServiceSettings

    @SuppressWarnings("unchecked")
    private static final ConstructingObjectParser<UpdateRequestSettings, Void> UPDATE_PARSER = new ConstructingObjectParser<>(
        "azure_openai_oauth_settings_update",
        true,
        args -> new UpdateRequestSettings((String) args[0], (String) args[1], (List<String>) args[2])
    );

    public record SettingsWithCounter(Settings settings, int offest) implements AzureOpenAiOAuthRequestServiceSettings.Offset {
        public SettingsWithCounter {
            Objects.requireNonNull(settings);
        }

        public static SettingsWithCounter create(Object[] args, int offset) {
            var index = offset;
            return new SettingsWithCounter(Settings.create(args[index++], args[index++], args[index++]), index);
        }

        @Override
        public int offset() {
            return offest;
        }
    }

    // TODO see if this ends up adding any value, if now just inline the fields in this class instead
    public record Settings(String clientId, String tenantId, List<String> scopes) {
        public Settings {
            Objects.requireNonNull(clientId);
            Objects.requireNonNull(tenantId);
            Objects.requireNonNull(scopes);
        }

        @SuppressWarnings("unchecked")
        public static Settings create(Object clientId, Object tenantId, Object scope) {
            return new Settings((String) clientId, (String) tenantId, (List<String>) scope);
        }

        public static Settings create(Object[] args, int offset) {
            return create(args[offset], args[offset + 1], args[offset + 2]);
        }
    }

    private static final ConstructingObjectParser<Settings, Void> STORAGE_PARSER = new ConstructingObjectParser<>(
        "azure_openai_oauth_settings",
        true,
        args -> Settings.create(args[0], args[1], args[2])
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
            new ParseField(CLIENT_ID_FIELD)
        );
        parser.declareString(
            isForUpdate ? ConstructingObjectParser.optionalConstructorArg() : ConstructingObjectParser.constructorArg(),
            new ParseField(TENANT_ID_FIELD)
        );
        parser.declareStringArray(
            isForUpdate ? ConstructingObjectParser.optionalConstructorArg() : ConstructingObjectParser.constructorArg(),
            new ParseField(SCOPES_FIELD)
        );
    }

    public static AzureOpenAiOAuthSettings fromMap(Map<String, Object> map) {
        try {
            try (
                var xContent = XContentBuilder.builder(JsonXContent.jsonXContent).map(map);
                var parser = JsonXContent.jsonXContent.createParser(XContentParserConfiguration.EMPTY, Strings.toString(xContent))
            ) {
                return new AzureOpenAiOAuthSettings(STORAGE_PARSER.parse(parser, null));
            }
        } catch (IOException e) {
            throw new IllegalArgumentException("Failed to parse Azure OpenAI OAuth settings", e);
        }
    }

    private final Settings settings;

    AzureOpenAiOAuthSettings(Settings settings) {
        this.settings = Objects.requireNonNull(settings);
    }

    public AzureOpenAiOAuthSettings(StreamInput in) throws IOException {
        this(new Settings(in.readString(), in.readString(), in.readStringCollectionAsImmutableList()));
    }

    public String getClientId() {
        return settings.clientId();
    }

    public String getTenantId() {
        return settings.tenantId();
    }

    public List<String> getScopes() {
        return settings.scopes();
    }

    public AzureOpenAiOAuthSettings updateServiceSettings(Map<String, Object> serviceSettings) {
        var updated = fromMapForUpdate(serviceSettings);

        // It is not possible to remove a field because all fields are required, so a user can only update existing fields
        return new AzureOpenAiOAuthSettings(
            new Settings(
                Objects.requireNonNullElse(updated.clientId(), settings.clientId()),
                Objects.requireNonNullElse(updated.tenantId(), settings.tenantId()),
                Objects.requireNonNullElse(updated.scopes(), settings.scopes())
            )
        );
    }

    private static UpdateRequestSettings fromMapForUpdate(Map<String, Object> map) {
        try {
            try (
                var xContent = XContentBuilder.builder(JsonXContent.jsonXContent).map(map);
                var parser = JsonXContent.jsonXContent.createParser(XContentParserConfiguration.EMPTY, Strings.toString(xContent))
            ) {
                return UPDATE_PARSER.parse(parser, null);
            }
        } catch (IOException e) {
            throw new IllegalArgumentException("Failed to parse Azure OpenAI OAuth settings during update", e);
        }
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        out.writeString(settings.clientId());
        out.writeString(settings.tenantId());
        out.writeStringCollection(settings.scopes());
    }

    @Override
    public XContentBuilder toXContent(XContentBuilder builder, Params params) throws IOException {
        builder.startObject();
        builder.field(CLIENT_ID_FIELD, settings.clientId());
        builder.field(TENANT_ID_FIELD, settings.tenantId());
        builder.field(SCOPES_FIELD, settings.scopes());
        builder.endObject();
        return builder;
    }
}
