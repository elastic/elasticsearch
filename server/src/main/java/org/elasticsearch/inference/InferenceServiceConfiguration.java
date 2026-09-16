/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.inference;

import org.elasticsearch.ElasticsearchParseException;
import org.elasticsearch.common.bytes.BytesReference;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.common.io.stream.Writeable;
import org.elasticsearch.common.xcontent.XContentHelper;
import org.elasticsearch.core.Nullable;
import org.elasticsearch.inference.configuration.InferenceServiceFeatures;
import org.elasticsearch.xcontent.ConstructingObjectParser;
import org.elasticsearch.xcontent.ParseField;
import org.elasticsearch.xcontent.ToXContentObject;
import org.elasticsearch.xcontent.XContentBuilder;
import org.elasticsearch.xcontent.XContentParser;
import org.elasticsearch.xcontent.XContentParserConfiguration;
import org.elasticsearch.xcontent.XContentType;

import java.io.IOException;
import java.util.EnumSet;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;

import static org.elasticsearch.xcontent.ConstructingObjectParser.constructorArg;
import static org.elasticsearch.xcontent.ConstructingObjectParser.optionalConstructorArg;

/**
 * Represents the configuration field settings for an inference provider.
 */
public class InferenceServiceConfiguration implements Writeable, ToXContentObject {

    private final String service;
    private final String name;
    private final EnumSet<TaskType> taskTypes;
    private final Map<String, SettingsConfiguration> configurations;
    private final InferenceServiceFeatures features;

    /**
     * Constructs a new {@link InferenceServiceConfiguration} instance with specified properties.
     *
     * @param service        The name of the service provider.
     * @param name           The user-friendly name of the service provider.
     * @param taskTypes      A list of {@link TaskType} supported by the service provider.
     * @param configurations The configuration of the service provider, defined by {@link SettingsConfiguration}.
     * @param features       The {@link InferenceServiceFeatures} the {@link InferenceService} supports
     */
    private InferenceServiceConfiguration(
        String service,
        String name,
        EnumSet<TaskType> taskTypes,
        Map<String, SettingsConfiguration> configurations,
        @Nullable InferenceServiceFeatures features
    ) {
        this.service = Objects.requireNonNull(service);
        this.name = Objects.requireNonNull(name);
        this.taskTypes = Objects.requireNonNull(taskTypes);
        this.configurations = Objects.requireNonNull(configurations);
        this.features = features;
    }

    public InferenceServiceConfiguration(StreamInput in) throws IOException {
        this(
            in.readString(),
            in.readString(),
            in.readEnumSet(TaskType.class),
            in.readMap(SettingsConfiguration::new),
            in.readOptionalWriteable(InferenceServiceFeatures::new)
        );
    }

    static final ParseField SERVICE_FIELD = new ParseField("service");
    static final ParseField NAME_FIELD = new ParseField("name");
    static final ParseField TASK_TYPES_FIELD = new ParseField("task_types");
    static final ParseField CONFIGURATIONS_FIELD = new ParseField("configurations");
    static final ParseField FEATURES_FIELD = new ParseField("features");

    @SuppressWarnings("unchecked")
    private static final ConstructingObjectParser<InferenceServiceConfiguration, Void> PARSER = new ConstructingObjectParser<>(
        "inference_service_configuration",
        true,
        args -> new InferenceServiceConfiguration.Builder().setService((String) args[0])
            .setName((String) args[1])
            .setTaskTypes((List<String>) args[2])
            .setConfigurations((Map<String, SettingsConfiguration>) args[3])
            .setFeatures((InferenceServiceFeatures) args[4])
            .build()
    );

    static {
        PARSER.declareString(constructorArg(), SERVICE_FIELD);
        PARSER.declareString(constructorArg(), NAME_FIELD);
        PARSER.declareStringArray(constructorArg(), TASK_TYPES_FIELD);
        PARSER.declareObject(constructorArg(), (p, c) -> p.map(HashMap::new, SettingsConfiguration::fromXContent), CONFIGURATIONS_FIELD);
        PARSER.declareObject(optionalConstructorArg(), (p, c) -> InferenceServiceFeatures.fromXContent(p), FEATURES_FIELD);
    }

    public String getService() {
        return service;
    }

    public String getName() {
        return name;
    }

    public EnumSet<TaskType> getTaskTypes() {
        return taskTypes;
    }

    public Map<String, SettingsConfiguration> getConfigurations() {
        return new HashMap<>(configurations);
    }

    public InferenceServiceFeatures getFeatures() {
        return features;
    }

    @Override
    public XContentBuilder toXContent(XContentBuilder builder, Params params) throws IOException {
        builder.startObject();
        {
            builder.field(SERVICE_FIELD.getPreferredName(), service);
            builder.field(NAME_FIELD.getPreferredName(), name);
            builder.field(TASK_TYPES_FIELD.getPreferredName(), taskTypes);
            builder.field(CONFIGURATIONS_FIELD.getPreferredName(), configurations);
            if (features != null) {
                builder.field(FEATURES_FIELD.getPreferredName(), features);
            }
        }
        builder.endObject();
        return builder;
    }

    public static InferenceServiceConfiguration fromXContent(XContentParser parser) throws IOException {
        return PARSER.parse(parser, null);
    }

    public static InferenceServiceConfiguration fromXContentBytes(BytesReference source, XContentType xContentType) {
        var parserConfig = XContentParserConfiguration.EMPTY.withRegistry(InferenceServiceFeatures.NAMED_X_CONTENT_REGISTRY);
        try (XContentParser parser = XContentHelper.createParser(parserConfig, source, xContentType)) {
            return InferenceServiceConfiguration.fromXContent(parser);
        } catch (IOException e) {
            throw new ElasticsearchParseException("failed to parse inference service configuration", e);
        }
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        out.writeString(service);
        out.writeString(name);
        out.writeCollection(taskTypes);
        out.writeMap(configurations, StreamOutput::writeWriteable);
        out.writeOptionalWriteable(features);
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        InferenceServiceConfiguration that = (InferenceServiceConfiguration) o;
        return service.equals(that.service)
            && name.equals(that.name)
            && Objects.equals(taskTypes, that.taskTypes)
            && Objects.equals(configurations, that.configurations)
            && Objects.equals(features, that.features);
    }

    @Override
    public int hashCode() {
        return Objects.hash(service, name, taskTypes, configurations, features);
    }

    public static class Builder {

        private String service;
        private String name;
        private EnumSet<TaskType> taskTypes = EnumSet.noneOf(TaskType.class);
        private Map<String, SettingsConfiguration> configurations = Map.of();
        private InferenceServiceFeatures features;

        public Builder setService(String service) {
            this.service = service;
            return this;
        }

        public Builder setName(String name) {
            this.name = name;
            return this;
        }

        public Builder setTaskTypes(EnumSet<TaskType> taskTypes) {
            this.taskTypes = TaskType.copyOf(taskTypes);
            return this;
        }

        public Builder setTaskTypes(List<String> taskTypes) {
            var enumTaskTypes = EnumSet.noneOf(TaskType.class);

            for (var supportedTaskTypeString : taskTypes) {
                enumTaskTypes.add(TaskType.fromStringOrStatusException(supportedTaskTypeString));
            }
            this.taskTypes = enumTaskTypes;
            return this;
        }

        public Builder setConfigurations(Map<String, SettingsConfiguration> configurations) {
            this.configurations = configurations;
            return this;
        }

        public Builder setFeatures(InferenceServiceFeatures features) {
            this.features = features;
            return this;
        }

        public InferenceServiceConfiguration build() {
            return new InferenceServiceConfiguration(service, name, taskTypes, configurations, features);
        }
    }
}
