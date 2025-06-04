/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.inference;

import org.elasticsearch.exception.ElasticsearchParseException;
import org.elasticsearch.common.bytes.BytesReference;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.common.io.stream.Writeable;
import org.elasticsearch.common.xcontent.XContentHelper;
import org.elasticsearch.core.Nullable;
import org.elasticsearch.inference.configuration.SettingsConfigurationFieldType;
import org.elasticsearch.xcontent.ConstructingObjectParser;
import org.elasticsearch.xcontent.ObjectParser;
import org.elasticsearch.xcontent.ParseField;
import org.elasticsearch.xcontent.ToXContentObject;
import org.elasticsearch.xcontent.XContentBuilder;
import org.elasticsearch.xcontent.XContentParseException;
import org.elasticsearch.xcontent.XContentParser;
import org.elasticsearch.xcontent.XContentParserConfiguration;
import org.elasticsearch.xcontent.XContentType;

import java.io.IOException;
import java.util.EnumSet;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.Set;

import static org.elasticsearch.xcontent.ConstructingObjectParser.constructorArg;
import static org.elasticsearch.xcontent.ConstructingObjectParser.optionalConstructorArg;

/**
 * Represents the configuration field settings for an inference provider.
 */
public class SettingsConfiguration implements Writeable, ToXContentObject {

    @Nullable
    private final Object defaultValue;
    @Nullable
    private final String description;
    private final String label;
    private final boolean required;
    private final boolean sensitive;
    private final boolean updatable;
    private final SettingsConfigurationFieldType type;
    private final EnumSet<TaskType> supportedTaskTypes;

    /**
     * Constructs a new {@link SettingsConfiguration} instance with specified properties.
     *
     * @param defaultValue   The default value for the configuration.
     * @param description    A description of the configuration.
     * @param label          The display label associated with the config field.
     * @param required       A boolean indicating whether the configuration is required.
     * @param sensitive      A boolean indicating whether the configuration contains sensitive information.
     * @param updatable      A boolean indicating whether the configuration can be updated.
     * @param type           The type of the configuration field, defined by {@link SettingsConfigurationFieldType}.
     * @param supportedTaskTypes The task types that support this field.
     */
    private SettingsConfiguration(
        Object defaultValue,
        String description,
        String label,
        boolean required,
        boolean sensitive,
        boolean updatable,
        SettingsConfigurationFieldType type,
        EnumSet<TaskType> supportedTaskTypes
    ) {
        this.defaultValue = defaultValue;
        this.description = description;
        this.label = label;
        this.required = required;
        this.sensitive = sensitive;
        this.updatable = updatable;
        this.type = type;
        this.supportedTaskTypes = supportedTaskTypes;
    }

    public SettingsConfiguration(StreamInput in) throws IOException {
        this.defaultValue = in.readGenericValue();
        this.description = in.readOptionalString();
        this.label = in.readString();
        this.required = in.readBoolean();
        this.sensitive = in.readBoolean();
        this.updatable = in.readBoolean();
        this.type = in.readEnum(SettingsConfigurationFieldType.class);
        this.supportedTaskTypes = in.readEnumSet(TaskType.class);
    }

    static final ParseField DEFAULT_VALUE_FIELD = new ParseField("default_value");
    static final ParseField DESCRIPTION_FIELD = new ParseField("description");
    static final ParseField LABEL_FIELD = new ParseField("label");
    static final ParseField REQUIRED_FIELD = new ParseField("required");
    static final ParseField SENSITIVE_FIELD = new ParseField("sensitive");
    static final ParseField UPDATABLE_FIELD = new ParseField("updatable");
    static final ParseField TYPE_FIELD = new ParseField("type");
    static final ParseField SUPPORTED_TASK_TYPES = new ParseField("supported_task_types");

    @SuppressWarnings("unchecked")
    private static final ConstructingObjectParser<SettingsConfiguration, Void> PARSER = new ConstructingObjectParser<>(
        "service_configuration",
        true,
        args -> {
            int i = 0;

            EnumSet<TaskType> supportedTaskTypes = EnumSet.noneOf(TaskType.class);
            var supportedTaskTypesListOfStrings = (List<String>) args[i++];

            for (var supportedTaskTypeString : supportedTaskTypesListOfStrings) {
                supportedTaskTypes.add(TaskType.fromString(supportedTaskTypeString));
            }

            return new SettingsConfiguration.Builder(supportedTaskTypes).setDefaultValue(args[i++])
                .setDescription((String) args[i++])
                .setLabel((String) args[i++])
                .setRequired((Boolean) args[i++])
                .setSensitive((Boolean) args[i++])
                .setUpdatable((Boolean) args[i++])
                .setType((SettingsConfigurationFieldType) args[i++])
                .build();
        }
    );

    static {
        PARSER.declareStringArray(constructorArg(), SUPPORTED_TASK_TYPES);
        PARSER.declareField(optionalConstructorArg(), (p, c) -> {
            if (p.currentToken() == XContentParser.Token.VALUE_STRING) {
                return p.text();
            } else if (p.currentToken() == XContentParser.Token.VALUE_NUMBER) {
                return p.numberValue();
            } else if (p.currentToken() == XContentParser.Token.VALUE_BOOLEAN) {
                return p.booleanValue();
            } else if (p.currentToken() == XContentParser.Token.VALUE_NULL) {
                return null;
            }
            throw new XContentParseException("Unsupported token [" + p.currentToken() + "]");
        }, DEFAULT_VALUE_FIELD, ObjectParser.ValueType.VALUE);
        PARSER.declareStringOrNull(optionalConstructorArg(), DESCRIPTION_FIELD);
        PARSER.declareString(constructorArg(), LABEL_FIELD);
        PARSER.declareBoolean(optionalConstructorArg(), REQUIRED_FIELD);
        PARSER.declareBoolean(optionalConstructorArg(), SENSITIVE_FIELD);
        PARSER.declareBoolean(optionalConstructorArg(), UPDATABLE_FIELD);
        PARSER.declareField(
            optionalConstructorArg(),
            (p, c) -> p.currentToken() == XContentParser.Token.VALUE_NULL ? null : SettingsConfigurationFieldType.fieldType(p.text()),
            TYPE_FIELD,
            ObjectParser.ValueType.STRING_OR_NULL
        );
    }

    public Object getDefaultValue() {
        return defaultValue;
    }

    public String getDescription() {
        return description;
    }

    public String getLabel() {
        return label;
    }

    public boolean isRequired() {
        return required;
    }

    public boolean isSensitive() {
        return sensitive;
    }

    public boolean isUpdatable() {
        return updatable;
    }

    public SettingsConfigurationFieldType getType() {
        return type;
    }

    public Set<TaskType> getSupportedTaskTypes() {
        return supportedTaskTypes;
    }

    @Override
    public XContentBuilder toXContent(XContentBuilder builder, Params params) throws IOException {
        builder.startObject();
        {
            if (defaultValue != null) {
                builder.field(DEFAULT_VALUE_FIELD.getPreferredName(), defaultValue);
            }
            if (description != null) {
                builder.field(DESCRIPTION_FIELD.getPreferredName(), description);
            }
            builder.field(LABEL_FIELD.getPreferredName(), label);
            builder.field(REQUIRED_FIELD.getPreferredName(), required);
            builder.field(SENSITIVE_FIELD.getPreferredName(), sensitive);
            builder.field(UPDATABLE_FIELD.getPreferredName(), updatable);

            if (type != null) {
                builder.field(TYPE_FIELD.getPreferredName(), type.toString());
            }
            builder.field(SUPPORTED_TASK_TYPES.getPreferredName(), supportedTaskTypes);
        }
        builder.endObject();
        return builder;
    }

    public static SettingsConfiguration fromXContent(XContentParser parser) throws IOException {
        return PARSER.parse(parser, null);
    }

    public static SettingsConfiguration fromXContentBytes(BytesReference source, XContentType xContentType) {
        try (XContentParser parser = XContentHelper.createParser(XContentParserConfiguration.EMPTY, source, xContentType)) {
            return SettingsConfiguration.fromXContent(parser);
        } catch (IOException e) {
            throw new ElasticsearchParseException("Failed to parse service configuration.", e);
        }
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        out.writeGenericValue(defaultValue);
        out.writeOptionalString(description);
        out.writeString(label);
        out.writeBoolean(required);
        out.writeBoolean(sensitive);
        out.writeBoolean(updatable);
        out.writeEnum(type);
        out.writeEnumSet(supportedTaskTypes);
    }

    public Map<String, Object> toMap() {
        Map<String, Object> map = new HashMap<>();

        map.put(DEFAULT_VALUE_FIELD.getPreferredName(), defaultValue);
        Optional.ofNullable(description).ifPresent(t -> map.put(DESCRIPTION_FIELD.getPreferredName(), t));

        map.put(LABEL_FIELD.getPreferredName(), label);

        map.put(REQUIRED_FIELD.getPreferredName(), required);
        map.put(SENSITIVE_FIELD.getPreferredName(), sensitive);
        map.put(UPDATABLE_FIELD.getPreferredName(), updatable);

        Optional.ofNullable(type).ifPresent(t -> map.put(TYPE_FIELD.getPreferredName(), t.toString()));

        map.put(SUPPORTED_TASK_TYPES.getPreferredName(), supportedTaskTypes);
        return map;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        SettingsConfiguration that = (SettingsConfiguration) o;
        return required == that.required
            && sensitive == that.sensitive
            && updatable == that.updatable
            && Objects.equals(defaultValue, that.defaultValue)
            && Objects.equals(description, that.description)
            && Objects.equals(label, that.label)
            && type == that.type
            && Objects.equals(supportedTaskTypes, that.supportedTaskTypes);
    }

    @Override
    public int hashCode() {
        return Objects.hash(defaultValue, description, label, required, sensitive, updatable, type, supportedTaskTypes);
    }

    public static class Builder {

        private Object defaultValue;
        private String description;
        private String label;
        private boolean required;
        private boolean sensitive;
        private boolean updatable;
        private SettingsConfigurationFieldType type;
        private final EnumSet<TaskType> supportedTaskTypes;

        public Builder(EnumSet<TaskType> supportedTaskTypes) {
            this.supportedTaskTypes = TaskType.copyOf(Objects.requireNonNull(supportedTaskTypes));
        }

        public Builder setDefaultValue(Object defaultValue) {
            this.defaultValue = defaultValue;
            return this;
        }

        public Builder setDescription(String description) {
            this.description = description;
            return this;
        }

        public Builder setLabel(String label) {
            this.label = label;
            return this;
        }

        public Builder setRequired(Boolean required) {
            this.required = Objects.requireNonNullElse(required, false);
            return this;
        }

        public Builder setSensitive(Boolean sensitive) {
            this.sensitive = Objects.requireNonNullElse(sensitive, false);
            return this;
        }

        public Builder setUpdatable(Boolean updatable) {
            this.updatable = Objects.requireNonNullElse(updatable, false);
            return this;
        }

        public Builder setType(SettingsConfigurationFieldType type) {
            this.type = type;
            return this;
        }

        public SettingsConfiguration build() {
            return new SettingsConfiguration(defaultValue, description, label, required, sensitive, updatable, type, supportedTaskTypes);
        }
    }
}
