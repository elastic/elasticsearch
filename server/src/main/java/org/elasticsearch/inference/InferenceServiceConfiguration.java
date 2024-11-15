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
import org.elasticsearch.TransportVersions;
import org.elasticsearch.common.bytes.BytesReference;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.common.io.stream.Writeable;
import org.elasticsearch.common.xcontent.XContentHelper;
import org.elasticsearch.core.Nullable;
import org.elasticsearch.xcontent.ConstructingObjectParser;
import org.elasticsearch.xcontent.ParseField;
import org.elasticsearch.xcontent.ToXContentObject;
import org.elasticsearch.xcontent.XContentBuilder;
import org.elasticsearch.xcontent.XContentParser;
import org.elasticsearch.xcontent.XContentParserConfiguration;
import org.elasticsearch.xcontent.XContentType;

import java.io.IOException;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;

import static org.elasticsearch.xcontent.ConstructingObjectParser.constructorArg;
import static org.elasticsearch.xcontent.ConstructingObjectParser.optionalConstructorArg;

/**
 * Represents the configuration field settings for an inference provider.
 */
public class InferenceServiceConfiguration implements Writeable, ToXContentObject {

    private final String provider;
    @Nullable
    private final String name;
    @Nullable
    private final String icon;
    private final List<TaskSettingsConfiguration> taskTypes;
    private final Map<String, SettingsConfiguration> configuration;

    /**
     * Constructs a new {@link InferenceServiceConfiguration} instance with specified properties.
     *
     * @param provider       The name of the service provider.
     * @param name           The user friendly name of the service provider
     * @param icon           The icon of the service provider
     * @param taskTypes      A list of {@link TaskSettingsConfiguration} supported by the service provider.
     * @param configuration  The configuration of the service provider, defined by {@link SettingsConfiguration}.
     */
    public InferenceServiceConfiguration(
        String provider,
        String name,
        String icon,
        List<TaskSettingsConfiguration> taskTypes,
        Map<String, SettingsConfiguration> configuration
    ) {
        this.provider = provider;
        this.name = name;
        this.icon = icon;
        this.taskTypes = taskTypes;
        this.configuration = configuration;
    }

    public InferenceServiceConfiguration(StreamInput in) throws IOException {
        this.provider = in.readString();
        this.name = (in.getTransportVersion().onOrAfter(TransportVersions.INFERENCE_SERVICES_NAME_ICON_ADDED))
            ? in.readOptionalString()
            : null;
        this.icon = (in.getTransportVersion().onOrAfter(TransportVersions.INFERENCE_SERVICES_NAME_ICON_ADDED))
            ? in.readOptionalString()
            : null;
        this.taskTypes = in.readCollectionAsList(TaskSettingsConfiguration::new);
        this.configuration = in.readMap(SettingsConfiguration::new);
    }

    static final ParseField PROVIDER_FIELD = new ParseField("provider");
    static final ParseField NAME_FIELD = new ParseField("name");
    static final ParseField ICON_FIELD = new ParseField("icon");
    static final ParseField TASK_TYPES_FIELD = new ParseField("task_types");
    static final ParseField CONFIGURATION_FIELD = new ParseField("configuration");

    @SuppressWarnings("unchecked")
    private static final ConstructingObjectParser<InferenceServiceConfiguration, Void> PARSER = new ConstructingObjectParser<>(
        "inference_service_configuration",
        true,
        args -> new Builder().setProvider((String) args[0])
            .setName((String) args[1])
            .setIcon((String) args[2])
            .setTaskTypes((List<TaskSettingsConfiguration>) args[3])
            .setConfiguration((Map<String, SettingsConfiguration>) args[4])
            .build()
    );

    static {
        PARSER.declareString(constructorArg(), PROVIDER_FIELD);
        PARSER.declareStringOrNull(optionalConstructorArg(), NAME_FIELD);
        PARSER.declareStringOrNull(optionalConstructorArg(), ICON_FIELD);
        PARSER.declareObjectArray(constructorArg(), (p, c) -> TaskSettingsConfiguration.fromXContent(p), TASK_TYPES_FIELD);
        PARSER.declareObject(constructorArg(), (p, c) -> p.map(), CONFIGURATION_FIELD);
    }

    public String getProvider() {
        return provider;
    }

    public String getName() {
        return name;
    }

    public String getIcon() {
        return icon;
    }

    public List<TaskSettingsConfiguration> getTaskTypes() {
        return taskTypes;
    }

    public Map<String, SettingsConfiguration> getConfiguration() {
        return configuration;
    }

    @Override
    public XContentBuilder toXContent(XContentBuilder builder, Params params) throws IOException {
        builder.startObject();
        {
            builder.field(PROVIDER_FIELD.getPreferredName(), provider);
            if (name != null) {
                builder.field(NAME_FIELD.getPreferredName(), name);
            }
            if (icon != null) {
                builder.field(ICON_FIELD.getPreferredName(), icon);
            }
            builder.field(TASK_TYPES_FIELD.getPreferredName(), taskTypes);
            builder.field(CONFIGURATION_FIELD.getPreferredName(), configuration);
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
        out.writeString(provider);
        if (out.getTransportVersion().onOrAfter(TransportVersions.INFERENCE_SERVICES_NAME_ICON_ADDED)) {
            out.writeOptionalString(name);
            out.writeOptionalString(icon);
        }
        out.writeCollection(taskTypes);
        out.writeMap(configuration, StreamOutput::writeWriteable);
    }

    public Map<String, Object> toMap() {
        Map<String, Object> map = new HashMap<>();

        map.put(PROVIDER_FIELD.getPreferredName(), provider);
        Optional.ofNullable(name).ifPresent(n -> map.put(NAME_FIELD.getPreferredName(), n));
        Optional.ofNullable(icon).ifPresent(i -> map.put(ICON_FIELD.getPreferredName(), i));
        map.put(TASK_TYPES_FIELD.getPreferredName(), taskTypes);
        map.put(CONFIGURATION_FIELD.getPreferredName(), configuration);

        return map;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        InferenceServiceConfiguration that = (InferenceServiceConfiguration) o;
        return provider.equals(that.provider)
            && Objects.equals(name, that.name)
            && Objects.equals(icon, that.icon)
            && Objects.equals(taskTypes, that.taskTypes)
            && Objects.equals(configuration, that.configuration);
    }

    @Override
    public int hashCode() {
        return Objects.hash(provider, name, icon, taskTypes, configuration);
    }

    public static class Builder {

        private String provider;
        private String name;
        private String icon;
        private List<TaskSettingsConfiguration> taskTypes;
        private Map<String, SettingsConfiguration> configuration;

        public Builder setProvider(String provider) {
            this.provider = provider;
            return this;
        }

        public Builder setName(@Nullable String name) {
            this.name = name;
            return this;
        }

        public Builder setIcon(@Nullable String icon) {
            this.icon = icon;
            return this;
        }

        public Builder setTaskTypes(List<TaskSettingsConfiguration> taskTypes) {
            this.taskTypes = taskTypes;
            return this;
        }

        public Builder setConfiguration(Map<String, SettingsConfiguration> configuration) {
            this.configuration = configuration;
            return this;
        }

        public InferenceServiceConfiguration build() {
            return new InferenceServiceConfiguration(provider, name, icon, taskTypes, configuration);
        }
    }
}
