/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.inference.common.configuration;

import org.elasticsearch.common.Strings;

import org.elasticsearch.common.ValidationException;

import software.amazon.awssdk.utils.ImmutableMap;

import org.elasticsearch.common.util.CachedSupplier;
import org.elasticsearch.inference.ServiceSettings;
import org.elasticsearch.inference.TaskType;
import org.elasticsearch.xcontent.ConstructingObjectParser;
import org.elasticsearch.xcontent.ObjectParser;
import org.elasticsearch.xcontent.ParseField;
import org.elasticsearch.xcontent.ToXContentObject;
import org.elasticsearch.xcontent.XContentBuilder;
import org.elasticsearch.xcontent.XContentParser;
import org.elasticsearch.xcontent.XContentParserConfiguration;
import org.elasticsearch.xcontent.json.JsonXContent;
import org.elasticsearch.xpack.inference.services.ConfigurationParseContext;
import org.elasticsearch.xpack.inference.services.elastic.ElasticInferenceService;
import org.elasticsearch.xpack.inference.services.elastic.sparseembeddings.ElasticInferenceServiceSparseEmbeddingsServiceSettings;

import java.io.IOException;
import java.util.Map;
import java.util.Objects;
import java.util.function.Function;

public record InferenceConfigParser(String id, TaskType taskType, String service, Map<String, Object> serviceSettings)
    implements
        ToXContentObject {

    private static final ParseField TASK_TYPE_FIELD = new ParseField("task_type");
    private static final ParseField SERVICE_FIELD = new ParseField("service");
    private static final ParseField SERVICE_SETTINGS_FIELD = new ParseField("service_settings");

    private record ParserRegistryKey(String name, TaskType taskType, ConfigurationParseContext context) {};

    private static final Map<
        ParserRegistryKey,
        CachedSupplier<ConstructingObjectParser<? extends ServiceSettings, Void>>> serviceSettingsParserRegistry;

    static {
        ImmutableMap.Builder<
            ParserRegistryKey,
            CachedSupplier<ConstructingObjectParser<? extends ServiceSettings, Void>>> serviceSettingsParserRegistryBuilder = ImmutableMap
                .builder();
        registerServiceSettingsParser(
            serviceSettingsParserRegistryBuilder,
            ElasticInferenceService.NAME,
            TaskType.SPARSE_EMBEDDING,
            ElasticInferenceServiceSparseEmbeddingsServiceSettings::createParser
        );
        serviceSettingsParserRegistry = serviceSettingsParserRegistryBuilder.build();
    }

    private static <T extends ServiceSettings> void registerServiceSettingsParser(
        ImmutableMap.Builder<ParserRegistryKey, CachedSupplier<ConstructingObjectParser<? extends ServiceSettings, Void>>> map,
        String name,
        TaskType taskType,
        Function<ConfigurationParseContext, ConstructingObjectParser<T, Void>> parserSupplier
    ) {
        map.put(
            new ParserRegistryKey(name, taskType, ConfigurationParseContext.REQUEST),
            CachedSupplier.wrap(() -> parserSupplier.apply(ConfigurationParseContext.REQUEST))
        );
        map.put(
            new ParserRegistryKey(name, taskType, ConfigurationParseContext.PERSISTENT),
            CachedSupplier.wrap(() -> parserSupplier.apply(ConfigurationParseContext.PERSISTENT))
        );
    }

    public static InferenceConfigParser parse(String id, XContentParser parser) {
        return createParser(id).apply(parser, null).build();
    }

    public static ObjectParser<InferenceConfigParser.Builder, Void> createParser(String id) {
        ObjectParser<InferenceConfigParser.Builder, Void> parser = new ObjectParser<>(
            "inference_endpoint_config",
            false,
            () -> new Builder(id)
        );
        parser.declareField(
            InferenceConfigParser.Builder::setTaskType,
            p -> TaskType.fromString(p.text()),
            TASK_TYPE_FIELD,
            ObjectParser.ValueType.STRING
        );
        parser.declareString(InferenceConfigParser.Builder::setService, SERVICE_FIELD);
        parser.declareObject(InferenceConfigParser.Builder::setServiceSettings, (p, c) -> p.map(), SERVICE_SETTINGS_FIELD);
        return parser;
    }

    @Override
    public XContentBuilder toXContent(XContentBuilder builder, Params params) throws IOException {
        builder.startObject();
        builder.field(TASK_TYPE_FIELD.getPreferredName(), taskType.toString());
        builder.field(SERVICE_FIELD.getPreferredName(), service);
        builder.field(SERVICE_SETTINGS_FIELD.getPreferredName(), serviceSettings);
        builder.endObject();
        return builder;
    }

    public <T extends ServiceSettings> T parseServiceSettings(Class<T> expectedClass, ConfigurationParseContext context) {
        CachedSupplier<ConstructingObjectParser<? extends ServiceSettings, Void>> parserSupplier = serviceSettingsParserRegistry.get(
            new ParserRegistryKey(service, taskType, context)
        );
        if (parserSupplier == null) {
            throw new IllegalStateException("No parser registered for service [" + service + "] and task type [" + taskType + "]");
        }

        ConstructingObjectParser<? extends ServiceSettings, Void> parser = parserSupplier.get();

        try (XContentBuilder xContent = XContentBuilder.builder(JsonXContent.jsonXContent).map(serviceSettings)) {
            try (
                XContentParser xContentParser = JsonXContent.jsonXContent.createParser(
                    XContentParserConfiguration.EMPTY,
                    Strings.toString(xContent)
                )
            ) {
                ServiceSettings parsed = parser.apply(xContentParser, null);
                if (expectedClass.isInstance(parsed) == false) {
                    throw new IllegalStateException(
                        "Parsed service settings type ["
                            + parsed.getClass().getName()
                            + "] did not match expected type ["
                            + expectedClass.getName()
                            + "]"
                    );
                }
                if (context == ConfigurationParseContext.REQUEST) {
                    ValidationException validationException = parsed.validate();
                    if (validationException.validationErrors().isEmpty() == false) {
                        throw validationException;
                    }
                }
                return expectedClass.cast(parsed);
            }
        } catch (IOException e) {
            throw new IllegalStateException("Failed to parse service settings", e);
        }
    }

    private static class Builder {
        private String id;
        private TaskType taskType;
        private String service;
        private Map<String, Object> serviceSettings;

        Builder(String id) {
            this.id = Objects.requireNonNull(id);
        }

        Builder setTaskType(TaskType taskType) {
            this.taskType = taskType;
            return this;
        }

        Builder setService(String service) {
            this.service = service;
            return this;
        }

        Builder setServiceSettings(Map<String, Object> serviceSettings) {
            this.serviceSettings = serviceSettings;
            return this;
        }

        InferenceConfigParser build() {
            return new InferenceConfigParser(id, taskType, service, serviceSettings);
        }
    }
}
