/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.inference;

import org.elasticsearch.common.bytes.BytesReference;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.common.io.stream.Writeable;
import org.elasticsearch.common.xcontent.XContentHelper;
import org.elasticsearch.exception.ElasticsearchParseException;
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

/**
 * Represents the configuration field settings for an inference provider.
 */
public class InferenceServiceConfiguration implements Writeable, ToXContentObject {

    private final String service;
    private final String name;
    private final EnumSet<TaskType> taskTypes;
    private final Map<String, SettingsConfiguration> configurations;

    /**
     * Constructs a new {@link InferenceServiceConfiguration} instance with specified properties.
     *
     * @param service        The name of the service provider.
     * @param name           The user-friendly name of the service provider.
     * @param taskTypes      A list of {@link TaskType} supported by the service provider.
     * @param configurations  The configuration of the service provider, defined by {@link SettingsConfiguration}.
     */
    private InferenceServiceConfiguration(
        String service,
        String name,
        EnumSet<TaskType> taskTypes,
        Map<String, SettingsConfiguration> configurations
    ) {
        this.service = service;
        this.name = name;
        this.taskTypes = taskTypes;
        this.configurations = configurations;
    }

    public InferenceServiceConfiguration(StreamInput in) throws IOException {
        this.service = in.readString();
        this.name = in.readString();
        this.taskTypes = in.readEnumSet(TaskType.class);
        this.configurations = in.readMap(SettingsConfiguration::new);
    }

    static final ParseField SERVICE_FIELD = new ParseField("service");
    static final ParseField NAME_FIELD = new ParseField("name");
    static final ParseField TASK_TYPES_FIELD = new ParseField("task_types");
    static final ParseField CONFIGURATIONS_FIELD = new ParseField("configurations");

    @SuppressWarnings("unchecked")
    private static final ConstructingObjectParser<InferenceServiceConfiguration, Void> PARSER = new ConstructingObjectParser<>(
        "inference_service_configuration",
        true,
        args -> new InferenceServiceConfiguration.Builder().setService((String) args[0])
            .setName((String) args[1])
            .setTaskTypes((List<String>) args[2])
            .setConfigurations((Map<String, SettingsConfiguration>) args[3])
            .build()
    );

    static {
        PARSER.declareString(constructorArg(), SERVICE_FIELD);
        PARSER.declareString(constructorArg(), NAME_FIELD);
        PARSER.declareStringArray(constructorArg(), TASK_TYPES_FIELD);
        PARSER.declareObject(constructorArg(), (p, c) -> p.map(), CONFIGURATIONS_FIELD);
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
        return configurations;
    }

    @Override
    public XContentBuilder toXContent(XContentBuilder builder, Params params) throws IOException {
        builder.startObject();
        {
            builder.field(SERVICE_FIELD.getPreferredName(), service);
            builder.field(NAME_FIELD.getPreferredName(), name);
            builder.field(TASK_TYPES_FIELD.getPreferredName(), taskTypes);
            builder.field(CONFIGURATIONS_FIELD.getPreferredName(), configurations);
        }
        builder.endObject();
        return builder;
    }

    public static InferenceServiceConfiguration fromXContent(XContentParser parser) throws IOException {
        return PARSER.parse(parser, null);
    }

    public static InferenceServiceConfiguration fromXContentBytes(BytesReference source, XContentType xContentType) {
        try (XContentParser parser = XContentHelper.createParser(XContentParserConfiguration.EMPTY, source, xContentType)) {
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
        out.writeMapValues(configurations);
    }

    public Map<String, Object> toMap() {
        Map<String, Object> map = new HashMap<>();

        map.put(SERVICE_FIELD.getPreferredName(), service);
        map.put(NAME_FIELD.getPreferredName(), name);
        map.put(TASK_TYPES_FIELD.getPreferredName(), taskTypes);
        map.put(CONFIGURATIONS_FIELD.getPreferredName(), configurations);

        return map;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        InferenceServiceConfiguration that = (InferenceServiceConfiguration) o;
        return service.equals(that.service)
            && name.equals(that.name)
            && Objects.equals(taskTypes, that.taskTypes)
            && Objects.equals(configurations, that.configurations);
    }

    @Override
    public int hashCode() {
        return Objects.hash(service, name, taskTypes, configurations);
    }

    public static class Builder {

        private String service;
        private String name;
        private EnumSet<TaskType> taskTypes;
        private Map<String, SettingsConfiguration> configurations;

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

        public InferenceServiceConfiguration build() {
            return new InferenceServiceConfiguration(service, name, taskTypes, configurations);
        }
    }
}
