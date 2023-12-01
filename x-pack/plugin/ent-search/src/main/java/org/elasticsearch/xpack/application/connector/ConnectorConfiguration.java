/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.application.connector;

import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.common.io.stream.Writeable;
import org.elasticsearch.core.Nullable;
import org.elasticsearch.xcontent.ParseField;
import org.elasticsearch.xcontent.ToXContentObject;
import org.elasticsearch.xcontent.XContentBuilder;
import org.elasticsearch.xpack.application.connector.configuration.ConfigurationDependency;
import org.elasticsearch.xpack.application.connector.configuration.ConfigurationDisplayType;
import org.elasticsearch.xpack.application.connector.configuration.ConfigurationFieldType;
import org.elasticsearch.xpack.application.connector.configuration.ConfigurationSelectOption;
import org.elasticsearch.xpack.application.connector.configuration.ConfigurationValidation;

import java.io.IOException;
import java.util.List;

/**
 * Represents the configuration field settings for a connector.
 */
public class ConnectorConfiguration implements Writeable, ToXContentObject {

    @Nullable
    private final String category;
    private final Object defaultValue;
    private final List<ConfigurationDependency> dependsOn;
    private final ConfigurationDisplayType display;
    private final List<ConfigurationSelectOption> options;
    @Nullable
    private final Integer order;
    @Nullable
    private final String placeholder;
    private final boolean required;
    private final boolean sensitive;
    private final String tooltip;
    private final ConfigurationFieldType type;
    private final List<String> uiRestrictions;
    private final List<ConfigurationValidation> validations;
    private final Object value;

    /**
     * Constructs a new {@link ConnectorConfiguration} instance with specified properties.
     *
     * @param category       The category of the configuration.
     * @param defaultValue   The default value for the configuration.
     * @param dependsOn      A list of {@link ConfigurationDependency} indicating dependencies on other configurations.
     * @param display        The display type, defined by {@link ConfigurationDisplayType}.
     * @param options        A list of {@link ConfigurationSelectOption} for selectable options.
     * @param order          The order in which this configuration appears.
     * @param placeholder    A placeholder text for the configuration field.
     * @param required       A boolean indicating whether the configuration is required.
     * @param sensitive      A boolean indicating whether the configuration contains sensitive information.
     * @param tooltip        A tooltip text providing additional information about the configuration.
     * @param type           The type of the configuration field, defined by {@link ConfigurationFieldType}.
     * @param uiRestrictions A list of UI restrictions in string format.
     * @param validations    A list of {@link ConfigurationValidation} for validating the configuration.
     * @param value          The current value of the configuration.
     */
    private ConnectorConfiguration(String category, Object defaultValue, List<ConfigurationDependency> dependsOn, ConfigurationDisplayType display, List<ConfigurationSelectOption> options, Integer order, String placeholder, boolean required, boolean sensitive, String tooltip, ConfigurationFieldType type, List<String> uiRestrictions, List<ConfigurationValidation> validations, Object value) {
        this.category = category;
        this.defaultValue = defaultValue;
        this.dependsOn = dependsOn;
        this.display = display;
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

    public ConnectorConfiguration(StreamInput in) throws IOException {
        this.category = in.readString();
        this.defaultValue = in.readGenericValue();
        this.dependsOn = in.readOptionalCollectionAsList(ConfigurationDependency::new);
        this.display = in.readEnum(ConfigurationDisplayType.class);
        this.options = in.readOptionalCollectionAsList(ConfigurationSelectOption::new);
        this.order = in.readOptionalInt();
        this.placeholder = in.readOptionalString();
        this.required = in.readBoolean();
        this.sensitive = in.readBoolean();
        this.tooltip = in.readOptionalString();
        this.type = in.readEnum(ConfigurationFieldType.class);
        this.uiRestrictions = in.readOptionalStringCollectionAsList();
        this.validations = in.readOptionalCollectionAsList(ConfigurationValidation::new);
        this.value = in.readGenericValue();
    }

    static final ParseField CATEGORY_FIELD = new ParseField("category");
    static final ParseField DEFAULT_VALUE_FIELD = new ParseField("default_value");
    static final ParseField DEPENDS_ON_FIELD = new ParseField("depends_on");
    static final ParseField DISPLAY_FIELD = new ParseField("display");
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




    @Override
    public XContentBuilder toXContent(XContentBuilder builder, Params params) throws IOException {
        builder.startObject();
        {
            builder.field("category", category);
            builder.field("defaultValue", defaultValue);
            builder.startArray("dependsOn");
            for (ConfigurationDependency dep : dependsOn) {
                dep.toXContent(builder, params);
            }
            builder.endArray();
            builder.field("display", display.toString());
            builder.startArray("options");
            for (ConfigurationSelectOption option : options) {
                option.toXContent(builder, params);
            }
            builder.endArray();
            if (order != null) {
                builder.field("order", order);
            }
            if (placeholder != null) {
                builder.field("placeholder", placeholder);
            }
            builder.field("required", required);
            builder.field("sensitive", sensitive);
            if (tooltip != null) {
                builder.field("tooltip", tooltip);
            }
            builder.field("type", type.toString());
            builder.array("uiRestrictions", uiRestrictions.toArray(new String[0]));
            builder.startArray("validations");
            for (ConfigurationValidation validation : validations) {
                validation.toXContent(builder, params);
            }
            builder.endArray();
            builder.field("value", value); // Assumes that XContentBuilder can handle the serialization
        }
        builder.endObject();
        return builder;
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        out.writeString(category);
        out.writeGenericValue(defaultValue);
        out.writeOptionalCollection(dependsOn);
        out.writeEnum(display);
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

    public static class Builder {

        private String category;
        private Object defaultValue;
        private List<ConfigurationDependency> dependsOn;
        private ConfigurationDisplayType display;
        private List<ConfigurationSelectOption> options;
        private Integer order;
        private String placeholder;
        private boolean required;
        private boolean sensitive;
        private String tooltip;
        private ConfigurationFieldType type;
        private List<String> uiRestrictions;
        private List<ConfigurationValidation> validations;
        private Object value;

        public Builder setCategory(String category) {
            this.category = category;
            return this;
        }

        public Builder setDefaultValue(Object defaultValue) {
            this.defaultValue = defaultValue;
            return this;
        }

        public Builder setDependsOn(List<ConfigurationDependency> dependsOn) {
            this.dependsOn = dependsOn;
            return this;
        }

        public Builder setDisplay(ConfigurationDisplayType display) {
            this.display = display;
            return this;
        }

        public Builder setOptions(List<ConfigurationSelectOption> options) {
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

        public Builder setRequired(boolean required) {
            this.required = required;
            return this;
        }

        public Builder setSensitive(boolean sensitive) {
            this.sensitive = sensitive;
            return this;
        }

        public Builder setTooltip(String tooltip) {
            this.tooltip = tooltip;
            return this;
        }

        public Builder setType(ConfigurationFieldType type) {
            this.type = type;
            return this;
        }

        public Builder setUiRestrictions(List<String> uiRestrictions) {
            this.uiRestrictions = uiRestrictions;
            return this;
        }

        public Builder setValidations(List<ConfigurationValidation> validations) {
            this.validations = validations;
            return this;
        }

        public Builder setValue(Object value) {
            this.value = value;
            return this;
        }

        public ConnectorConfiguration build() {
            return new ConnectorConfiguration(category, defaultValue, dependsOn, display, options, order, placeholder, required, sensitive, tooltip, type, uiRestrictions, validations, value);
        }
    }
}
