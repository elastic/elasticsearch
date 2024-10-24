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
import org.elasticsearch.inference.configuration.ServiceConfigurationDependency;
import org.elasticsearch.inference.configuration.ServiceConfigurationDisplayType;
import org.elasticsearch.inference.configuration.ServiceConfigurationFieldType;
import org.elasticsearch.inference.configuration.ServiceConfigurationSelectOption;
import org.elasticsearch.inference.configuration.ServiceConfigurationValidation;
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
import java.util.ArrayList;
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
public class ServiceConfiguration implements Writeable, ToXContentObject {

    @Nullable
    private final String category;
    @Nullable
    private final Object defaultValue;
    @Nullable
    private final List<ServiceConfigurationDependency> dependsOn;
    @Nullable
    private final ServiceConfigurationDisplayType display;
    private final String label;
    @Nullable
    private final List<ServiceConfigurationSelectOption> options;
    @Nullable
    private final Integer order;
    @Nullable
    private final String placeholder;
    private final boolean required;
    private final boolean sensitive;
    @Nullable
    private final String tooltip;
    @Nullable
    private final ServiceConfigurationFieldType type;
    @Nullable
    private final List<String> uiRestrictions;
    @Nullable
    private final List<ServiceConfigurationValidation> validations;
    @Nullable
    private final Object value;

    /**
     * Constructs a new {@link ServiceConfiguration} instance with specified properties.
     *
     * @param category       The category of the configuration field.
     * @param defaultValue   The default value for the configuration.
     * @param dependsOn      A list of {@link ServiceConfigurationDependency} indicating dependencies on other configurations.
     * @param display        The display type, defined by {@link ServiceConfigurationDisplayType}.
     * @param label          The display label associated with the config field.
     * @param options        A list of {@link ServiceConfigurationSelectOption} for selectable options.
     * @param order          The order in which this configuration appears.
     * @param placeholder    A placeholder text for the configuration field.
     * @param required       A boolean indicating whether the configuration is required.
     * @param sensitive      A boolean indicating whether the configuration contains sensitive information.
     * @param tooltip        A tooltip text providing additional information about the configuration.
     * @param type           The type of the configuration field, defined by {@link ServiceConfigurationFieldType}.
     * @param uiRestrictions A list of UI restrictions in string format.
     * @param validations    A list of {@link ServiceConfigurationValidation} for validating the configuration.
     * @param value          The current value of the configuration.
     */
    private ServiceConfiguration(
        String category,
        Object defaultValue,
        List<ServiceConfigurationDependency> dependsOn,
        ServiceConfigurationDisplayType display,
        String label,
        List<ServiceConfigurationSelectOption> options,
        Integer order,
        String placeholder,
        boolean required,
        boolean sensitive,
        String tooltip,
        ServiceConfigurationFieldType type,
        List<String> uiRestrictions,
        List<ServiceConfigurationValidation> validations,
        Object value
    ) {
        this.category = category;
        this.defaultValue = defaultValue;
        this.dependsOn = dependsOn;
        this.display = display;
        this.label = label;
        this.options = options;
        this.order = order;
        this.placeholder = placeholder;
        this.required = required;
        this.sensitive = sensitive;
        this.tooltip = tooltip;
        this.type = type;
        this.uiRestrictions = uiRestrictions;
        this.validations = validations;
        this.value = value;
    }

    public ServiceConfiguration(StreamInput in) throws IOException {
        this.category = in.readString();
        this.defaultValue = in.readGenericValue();
        this.dependsOn = in.readOptionalCollectionAsList(ServiceConfigurationDependency::new);
        this.display = in.readEnum(ServiceConfigurationDisplayType.class);
        this.label = in.readString();
        this.options = in.readOptionalCollectionAsList(ServiceConfigurationSelectOption::new);
        this.order = in.readOptionalInt();
        this.placeholder = in.readOptionalString();
        this.required = in.readBoolean();
        this.sensitive = in.readBoolean();
        this.tooltip = in.readOptionalString();
        this.type = in.readEnum(ServiceConfigurationFieldType.class);
        this.uiRestrictions = in.readOptionalStringCollectionAsList();
        this.validations = in.readOptionalCollectionAsList(ServiceConfigurationValidation::new);
        this.value = in.readGenericValue();
    }

    static final ParseField CATEGORY_FIELD = new ParseField("category");
    static final ParseField DEFAULT_VALUE_FIELD = new ParseField("default_value");
    static final ParseField DEPENDS_ON_FIELD = new ParseField("depends_on");
    static final ParseField DISPLAY_FIELD = new ParseField("display");
    static final ParseField LABEL_FIELD = new ParseField("label");
    static final ParseField OPTIONS_FIELD = new ParseField("options");
    static final ParseField ORDER_FIELD = new ParseField("order");
    static final ParseField PLACEHOLDER_FIELD = new ParseField("placeholder");
    static final ParseField REQUIRED_FIELD = new ParseField("required");
    static final ParseField SENSITIVE_FIELD = new ParseField("sensitive");
    static final ParseField TOOLTIP_FIELD = new ParseField("tooltip");
    static final ParseField TYPE_FIELD = new ParseField("type");
    static final ParseField UI_RESTRICTIONS_FIELD = new ParseField("ui_restrictions");
    static final ParseField VALIDATIONS_FIELD = new ParseField("validations");
    static final ParseField VALUE_FIELD = new ParseField("value");

    @SuppressWarnings("unchecked")
    private static final ConstructingObjectParser<ServiceConfiguration, Void> PARSER = new ConstructingObjectParser<>(
        "service_configuration",
        true,
        args -> {
            int i = 0;
            return new ServiceConfiguration.Builder().setCategory((String) args[i++])
                .setDefaultValue(args[i++])
                .setDependsOn((List<ServiceConfigurationDependency>) args[i++])
                .setDisplay((ServiceConfigurationDisplayType) args[i++])
                .setLabel((String) args[i++])
                .setOptions((List<ServiceConfigurationSelectOption>) args[i++])
                .setOrder((Integer) args[i++])
                .setPlaceholder((String) args[i++])
                .setRequired((Boolean) args[i++])
                .setSensitive((Boolean) args[i++])
                .setTooltip((String) args[i++])
                .setType((ServiceConfigurationFieldType) args[i++])
                .setUiRestrictions((List<String>) args[i++])
                .setValidations((List<ServiceConfigurationValidation>) args[i++])
                .setValue(args[i])
                .build();
        }
    );

    static {
        PARSER.declareString(optionalConstructorArg(), CATEGORY_FIELD);
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
        PARSER.declareObjectArray(optionalConstructorArg(), (p, c) -> ServiceConfigurationDependency.fromXContent(p), DEPENDS_ON_FIELD);
        PARSER.declareField(
            optionalConstructorArg(),
            (p, c) -> ServiceConfigurationDisplayType.displayType(p.text()),
            DISPLAY_FIELD,
            ObjectParser.ValueType.STRING_OR_NULL
        );
        PARSER.declareString(constructorArg(), LABEL_FIELD);
        PARSER.declareObjectArray(optionalConstructorArg(), (p, c) -> ServiceConfigurationSelectOption.fromXContent(p), OPTIONS_FIELD);
        PARSER.declareInt(optionalConstructorArg(), ORDER_FIELD);
        PARSER.declareStringOrNull(optionalConstructorArg(), PLACEHOLDER_FIELD);
        PARSER.declareBoolean(optionalConstructorArg(), REQUIRED_FIELD);
        PARSER.declareBoolean(optionalConstructorArg(), SENSITIVE_FIELD);
        PARSER.declareStringOrNull(optionalConstructorArg(), TOOLTIP_FIELD);
        PARSER.declareField(
            optionalConstructorArg(),
            (p, c) -> p.currentToken() == XContentParser.Token.VALUE_NULL ? null : ServiceConfigurationFieldType.fieldType(p.text()),
            TYPE_FIELD,
            ObjectParser.ValueType.STRING_OR_NULL
        );
        PARSER.declareStringArray(optionalConstructorArg(), UI_RESTRICTIONS_FIELD);
        PARSER.declareObjectArray(optionalConstructorArg(), (p, c) -> ServiceConfigurationValidation.fromXContent(p), VALIDATIONS_FIELD);
        PARSER.declareField(
            optionalConstructorArg(),
            (p, c) -> parseConfigurationValue(p),
            VALUE_FIELD,
            ObjectParser.ValueType.VALUE_OBJECT_ARRAY
        );
    }

    public String getCategory() {
        return category;
    }

    public Object getDefaultValue() {
        return defaultValue;
    }

    public List<ServiceConfigurationDependency> getDependsOn() {
        return dependsOn;
    }

    public ServiceConfigurationDisplayType getDisplay() {
        return display;
    }

    public String getLabel() {
        return label;
    }

    public List<ServiceConfigurationSelectOption> getOptions() {
        return options;
    }

    public Integer getOrder() {
        return order;
    }

    public String getPlaceholder() {
        return placeholder;
    }

    public boolean isRequired() {
        return required;
    }

    public boolean isSensitive() {
        return sensitive;
    }

    public String getTooltip() {
        return tooltip;
    }

    public ServiceConfigurationFieldType getType() {
        return type;
    }

    public List<String> getUiRestrictions() {
        return uiRestrictions;
    }

    public List<ServiceConfigurationValidation> getValidations() {
        return validations;
    }

    public Object getValue() {
        return value;
    }

    /**
     * Parses a configuration value from a parser context.
     * This method can parse strings, numbers, booleans, objects, and null values, matching the types commonly
     * supported in {@link ServiceConfiguration}.
     *
     * @param p the {@link org.elasticsearch.xcontent.XContentParser} instance from which to parse the configuration value.
     */
    public static Object parseConfigurationValue(XContentParser p) throws IOException {

        if (p.currentToken() == XContentParser.Token.VALUE_STRING) {
            return p.text();
        } else if (p.currentToken() == XContentParser.Token.VALUE_NUMBER) {
            return p.numberValue();
        } else if (p.currentToken() == XContentParser.Token.VALUE_BOOLEAN) {
            return p.booleanValue();
        } else if (p.currentToken() == XContentParser.Token.START_OBJECT) {
            // Crawler expects the value to be an object
            return p.map();
        } else if (p.currentToken() == XContentParser.Token.VALUE_NULL) {
            return null;
        }
        throw new XContentParseException("Unsupported token [" + p.currentToken() + "]");
    }

    @Override
    public XContentBuilder toXContent(XContentBuilder builder, Params params) throws IOException {
        builder.startObject();
        {
            if (category != null) {
                builder.field(CATEGORY_FIELD.getPreferredName(), category);
            }
            builder.field(DEFAULT_VALUE_FIELD.getPreferredName(), defaultValue);
            if (dependsOn != null) {
                builder.xContentList(DEPENDS_ON_FIELD.getPreferredName(), dependsOn);
            } else {
                builder.xContentList(DEPENDS_ON_FIELD.getPreferredName(), new ArrayList<>());
            }
            if (display != null) {
                builder.field(DISPLAY_FIELD.getPreferredName(), display.toString());
            }
            builder.field(LABEL_FIELD.getPreferredName(), label);
            if (options != null) {
                builder.xContentList(OPTIONS_FIELD.getPreferredName(), options);
            }
            if (order != null) {
                builder.field(ORDER_FIELD.getPreferredName(), order);
            }
            if (placeholder != null) {
                builder.field(PLACEHOLDER_FIELD.getPreferredName(), placeholder);
            }
            builder.field(REQUIRED_FIELD.getPreferredName(), required);
            builder.field(SENSITIVE_FIELD.getPreferredName(), sensitive);
            if (tooltip != null) {
                builder.field(TOOLTIP_FIELD.getPreferredName(), tooltip);
            }
            if (type != null) {
                builder.field(TYPE_FIELD.getPreferredName(), type.toString());
            }
            if (uiRestrictions != null) {
                builder.stringListField(UI_RESTRICTIONS_FIELD.getPreferredName(), uiRestrictions);
            } else {
                builder.stringListField(UI_RESTRICTIONS_FIELD.getPreferredName(), new ArrayList<>());
            }
            if (validations != null) {
                builder.xContentList(VALIDATIONS_FIELD.getPreferredName(), validations);
            } else {
                builder.xContentList(VALIDATIONS_FIELD.getPreferredName(), new ArrayList<>());
            }
            builder.field(VALUE_FIELD.getPreferredName(), value);
        }
        builder.endObject();
        return builder;
    }

    public static ServiceConfiguration fromXContent(XContentParser parser) throws IOException {
        return PARSER.parse(parser, null);
    }

    public static ServiceConfiguration fromXContentBytes(BytesReference source, XContentType xContentType) {
        try (XContentParser parser = XContentHelper.createParser(XContentParserConfiguration.EMPTY, source, xContentType)) {
            return ServiceConfiguration.fromXContent(parser);
        } catch (IOException e) {
            throw new ElasticsearchParseException("Failed to parse service configuration.", e);
        }
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        out.writeString(category);
        out.writeGenericValue(defaultValue);
        out.writeOptionalCollection(dependsOn);
        out.writeEnum(display);
        out.writeString(label);
        out.writeOptionalCollection(options);
        out.writeOptionalInt(order);
        out.writeOptionalString(placeholder);
        out.writeBoolean(required);
        out.writeBoolean(sensitive);
        out.writeOptionalString(tooltip);
        out.writeEnum(type);
        out.writeOptionalStringCollection(uiRestrictions);
        out.writeOptionalCollection(validations);
        out.writeGenericValue(value);
    }

    public Map<String, Object> toMap() {
        Map<String, Object> map = new HashMap<>();

        Optional.ofNullable(category).ifPresent(c -> map.put(CATEGORY_FIELD.getPreferredName(), c));
        map.put(DEFAULT_VALUE_FIELD.getPreferredName(), defaultValue);

        Optional.ofNullable(dependsOn)
            .ifPresent(d -> map.put(DEPENDS_ON_FIELD.getPreferredName(), d.stream().map(ServiceConfigurationDependency::toMap).toList()));

        Optional.ofNullable(display).ifPresent(d -> map.put(DISPLAY_FIELD.getPreferredName(), d.toString()));

        map.put(LABEL_FIELD.getPreferredName(), label);

        Optional.ofNullable(options)
            .ifPresent(o -> map.put(OPTIONS_FIELD.getPreferredName(), o.stream().map(ServiceConfigurationSelectOption::toMap).toList()));

        Optional.ofNullable(order).ifPresent(o -> map.put(ORDER_FIELD.getPreferredName(), o));

        Optional.ofNullable(placeholder).ifPresent(p -> map.put(PLACEHOLDER_FIELD.getPreferredName(), p));

        map.put(REQUIRED_FIELD.getPreferredName(), required);
        map.put(SENSITIVE_FIELD.getPreferredName(), sensitive);

        Optional.ofNullable(tooltip).ifPresent(t -> map.put(TOOLTIP_FIELD.getPreferredName(), t));

        Optional.ofNullable(type).ifPresent(t -> map.put(TYPE_FIELD.getPreferredName(), t.toString()));

        Optional.ofNullable(uiRestrictions).ifPresent(u -> map.put(UI_RESTRICTIONS_FIELD.getPreferredName(), u));

        Optional.ofNullable(validations)
            .ifPresent(v -> map.put(VALIDATIONS_FIELD.getPreferredName(), v.stream().map(ServiceConfigurationValidation::toMap).toList()));

        map.put(VALUE_FIELD.getPreferredName(), value);

        return map;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        ServiceConfiguration that = (ServiceConfiguration) o;
        return required == that.required
            && sensitive == that.sensitive
            && Objects.equals(category, that.category)
            && Objects.equals(defaultValue, that.defaultValue)
            && Objects.equals(dependsOn, that.dependsOn)
            && display == that.display
            && Objects.equals(label, that.label)
            && Objects.equals(options, that.options)
            && Objects.equals(order, that.order)
            && Objects.equals(placeholder, that.placeholder)
            && Objects.equals(tooltip, that.tooltip)
            && type == that.type
            && Objects.equals(uiRestrictions, that.uiRestrictions)
            && Objects.equals(validations, that.validations)
            && Objects.equals(value, that.value);
    }

    @Override
    public int hashCode() {
        return Objects.hash(
            category,
            defaultValue,
            dependsOn,
            display,
            label,
            options,
            order,
            placeholder,
            required,
            sensitive,
            tooltip,
            type,
            uiRestrictions,
            validations,
            value
        );
    }

    public static class Builder {

        private String category;
        private Object defaultValue;
        private List<ServiceConfigurationDependency> dependsOn;
        private ServiceConfigurationDisplayType display;
        private String label;
        private List<ServiceConfigurationSelectOption> options;
        private Integer order;
        private String placeholder;
        private boolean required;
        private boolean sensitive;
        private String tooltip;
        private ServiceConfigurationFieldType type;
        private List<String> uiRestrictions;
        private List<ServiceConfigurationValidation> validations;
        private Object value;

        public Builder setCategory(String category) {
            this.category = category;
            return this;
        }

        public Builder setDefaultValue(Object defaultValue) {
            this.defaultValue = defaultValue;
            return this;
        }

        public Builder setDependsOn(List<ServiceConfigurationDependency> dependsOn) {
            this.dependsOn = dependsOn;
            return this;
        }

        public Builder setDisplay(ServiceConfigurationDisplayType display) {
            this.display = display;
            return this;
        }

        public Builder setLabel(String label) {
            this.label = label;
            return this;
        }

        public Builder setOptions(List<ServiceConfigurationSelectOption> options) {
            this.options = options;
            return this;
        }

        public Builder setOrder(Integer order) {
            this.order = order;
            return this;
        }

        public Builder setPlaceholder(String placeholder) {
            this.placeholder = placeholder;
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

        public Builder setTooltip(String tooltip) {
            this.tooltip = tooltip;
            return this;
        }

        public Builder setType(ServiceConfigurationFieldType type) {
            this.type = type;
            return this;
        }

        public Builder setUiRestrictions(List<String> uiRestrictions) {
            this.uiRestrictions = uiRestrictions;
            return this;
        }

        public Builder setValidations(List<ServiceConfigurationValidation> validations) {
            this.validations = validations;
            return this;
        }

        public Builder setValue(Object value) {
            this.value = value;
            return this;
        }

        public ServiceConfiguration build() {
            return new ServiceConfiguration(
                category,
                defaultValue,
                dependsOn,
                display,
                label,
                options,
                order,
                placeholder,
                required,
                sensitive,
                tooltip,
                type,
                uiRestrictions,
                validations,
                value
            );
        }
    }
}
