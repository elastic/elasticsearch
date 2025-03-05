/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.application.connector.configuration;

import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.common.io.stream.Writeable;
import org.elasticsearch.xcontent.ConstructingObjectParser;
import org.elasticsearch.xcontent.ObjectParser;
import org.elasticsearch.xcontent.ParseField;
import org.elasticsearch.xcontent.ToXContentObject;
import org.elasticsearch.xcontent.XContentBuilder;
import org.elasticsearch.xcontent.XContentParseException;
import org.elasticsearch.xcontent.XContentParser;

import java.io.IOException;
import java.util.HashMap;
import java.util.Map;
import java.util.Objects;

import static org.elasticsearch.xcontent.ConstructingObjectParser.constructorArg;

public class ConfigurationSelectOption implements Writeable, ToXContentObject {
    private final String label;
    private final Object value;

    private ConfigurationSelectOption(String label, Object value) {
        this.label = label;
        this.value = value;
    }

    public ConfigurationSelectOption(StreamInput in) throws IOException {
        this.label = in.readString();
        this.value = in.readGenericValue();
    }

    private static final ParseField LABEL_FIELD = new ParseField("label");
    private static final ParseField VALUE_FIELD = new ParseField("value");

    private static final ConstructingObjectParser<ConfigurationSelectOption, Void> PARSER = new ConstructingObjectParser<>(
        "connector_configuration_select_option",
        true,
        args -> new ConfigurationSelectOption.Builder().setLabel((String) args[0]).setValue(args[1]).build()
    );

    static {
        PARSER.declareString(constructorArg(), LABEL_FIELD);
        PARSER.declareField(constructorArg(), (p, c) -> {
            if (p.currentToken() == XContentParser.Token.VALUE_STRING) {
                return p.text();
            } else if (p.currentToken() == XContentParser.Token.VALUE_NUMBER) {
                return p.numberValue();
            }
            throw new XContentParseException("Unsupported token [" + p.currentToken() + "]");
        }, VALUE_FIELD, ObjectParser.ValueType.VALUE);
    }

    @Override
    public XContentBuilder toXContent(XContentBuilder builder, Params params) throws IOException {
        builder.startObject();
        {
            builder.field(LABEL_FIELD.getPreferredName(), label);
            builder.field(VALUE_FIELD.getPreferredName(), value);
        }
        builder.endObject();
        return builder;
    }

    public Map<String, Object> toMap() {
        Map<String, Object> map = new HashMap<>();
        map.put(LABEL_FIELD.getPreferredName(), label);
        map.put(VALUE_FIELD.getPreferredName(), value);
        return map;
    }

    public static ConfigurationSelectOption fromXContent(XContentParser parser) throws IOException {
        return PARSER.parse(parser, null);
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        out.writeString(label);
        out.writeGenericValue(value);
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        ConfigurationSelectOption that = (ConfigurationSelectOption) o;
        return Objects.equals(label, that.label) && Objects.equals(value, that.value);
    }

    @Override
    public int hashCode() {
        return Objects.hash(label, value);
    }

    public static class Builder {

        private String label;
        private Object value;

        public Builder setLabel(String label) {
            this.label = label;
            return this;
        }

        public Builder setValue(Object value) {
            this.value = value;
            return this;
        }

        public ConfigurationSelectOption build() {
            return new ConfigurationSelectOption(label, value);
        }
    }

}
