/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */
package org.elasticsearch.xpack.idp.saml.support;

import org.elasticsearch.action.ActionRequestValidationException;
import org.elasticsearch.action.ValidateActions;
import org.elasticsearch.common.Strings;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.common.io.stream.Writeable;
import org.elasticsearch.xcontent.ObjectParser;
import org.elasticsearch.xcontent.ParseField;
import org.elasticsearch.xcontent.ToXContentObject;
import org.elasticsearch.xcontent.XContentBuilder;
import org.elasticsearch.xcontent.XContentParser;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Objects;
import java.util.Set;

/**
 * Represents a collection of SAML attributes to be included in the SAML response.
 * Each attribute has a key and a list of values.
 */
public class SamlInitiateSingleSignOnAttributes implements Writeable, ToXContentObject {
    private List<Attribute> attributes;

    public SamlInitiateSingleSignOnAttributes() {
        this.attributes = new ArrayList<>();
    }

    public SamlInitiateSingleSignOnAttributes(StreamInput in) throws IOException {
        int size = in.readVInt();
        attributes = new ArrayList<>(size);
        for (int i = 0; i < size; i++) {
            attributes.add(new Attribute(in));
        }
    }

    /**
     * Validates this SAML attributes object to ensure all attribute keys are valid and unique.
     * @return ActionRequestValidationException containing validation errors, or null if valid
     */
    public ActionRequestValidationException validate() {
        ActionRequestValidationException validationException = null;

        // Check for null/empty attribute keys and duplicate keys
        if (attributes.isEmpty() == false) {
            Set<String> keys = new HashSet<>();
            for (Attribute attribute : attributes) {
                // Check for null or empty key
                if (Strings.isNullOrEmpty(attribute.getKey())) {
                    validationException = ValidateActions.addValidationError("attribute key cannot be null or empty", validationException);
                } else if (keys.add(attribute.getKey()) == false) {
                    // Check for duplicate key
                    validationException = ValidateActions.addValidationError(
                        "duplicate attribute key [" + attribute.getKey() + "] found",
                        validationException
                    );
                }
            }
        }

        return validationException;
    }

    public List<Attribute> getAttributes() {
        return Collections.unmodifiableList(attributes);
    }

    public void setAttributes(List<Attribute> attributes) {
        this.attributes = new ArrayList<>(attributes);
    }

    public static final ObjectParser<SamlInitiateSingleSignOnAttributes, Void> PARSER = new ObjectParser<>(
        "saml_attributes",
        true,
        SamlInitiateSingleSignOnAttributes::new
    );

    static {
        PARSER.declareObjectArray(SamlInitiateSingleSignOnAttributes::setAttributes, Attribute.PARSER, Fields.ATTRIBUTES);
    }

    @Override
    public XContentBuilder toXContent(XContentBuilder builder, Params params) throws IOException {
        builder.startObject();
        builder.startArray(Fields.ATTRIBUTES.getPreferredName());
        for (Attribute attribute : attributes) {
            attribute.toXContent(builder, params);
        }
        builder.endArray();
        return builder.endObject();
    }

    public static SamlInitiateSingleSignOnAttributes fromXContent(XContentParser parser) throws IOException {
        SamlInitiateSingleSignOnAttributes attributes = new SamlInitiateSingleSignOnAttributes();
        return PARSER.parse(parser, attributes, null);
    }

    public interface Fields {
        ParseField ATTRIBUTES = new ParseField("attributes");
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        out.writeVInt(attributes.size());
        for (Attribute attribute : attributes) {
            attribute.writeTo(out);
        }
    }

    @Override
    public String toString() {
        return getClass().getSimpleName() + "{attributes=" + attributes + "}";
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        SamlInitiateSingleSignOnAttributes that = (SamlInitiateSingleSignOnAttributes) o;
        return Objects.equals(attributes, that.attributes);
    }

    @Override
    public int hashCode() {
        return Objects.hash(attributes);
    }

    /**
     * Individual attribute with a key and a list of values.
     */
    public static class Attribute implements Writeable, ToXContentObject {
        private String key;
        private List<String> values;

        public Attribute() {
            this.values = new ArrayList<>();
        }

        public Attribute(String key, List<String> values) {
            this.key = key;
            this.values = new ArrayList<>(values);
        }

        public Attribute(StreamInput in) throws IOException {
            this.key = in.readString();
            int size = in.readVInt();
            this.values = new ArrayList<>(size);
            for (int i = 0; i < size; i++) {
                values.add(in.readString());
            }
        }

        public String getKey() {
            return key;
        }

        public void setKey(String key) {
            this.key = key;
        }

        public List<String> getValues() {
            return Collections.unmodifiableList(values);
        }

        public void setValues(List<String> values) {
            this.values = new ArrayList<>(values);
        }

        public static final ObjectParser<Attribute, Void> PARSER = new ObjectParser<>("attribute", true, Attribute::new);

        static {
            PARSER.declareString(Attribute::setKey, AttributeFields.KEY);
            PARSER.declareStringArray(Attribute::setValues, AttributeFields.VALUES);
        }

        @Override
        public XContentBuilder toXContent(XContentBuilder builder, Params params) throws IOException {
            builder.startObject();
            builder.field(AttributeFields.KEY.getPreferredName(), key);
            builder.startArray(AttributeFields.VALUES.getPreferredName());
            for (String value : values) {
                builder.value(value);
            }
            builder.endArray();
            return builder.endObject();
        }

        @Override
        public void writeTo(StreamOutput out) throws IOException {
            out.writeString(key);
            out.writeVInt(values.size());
            for (String value : values) {
                out.writeString(value);
            }
        }

        @Override
        public boolean equals(Object o) {
            if (this == o) return true;
            if (o == null || getClass() != o.getClass()) return false;
            Attribute attribute = (Attribute) o;
            return Objects.equals(key, attribute.key) && Objects.equals(values, attribute.values);
        }

        @Override
        public int hashCode() {
            return Objects.hash(key, values);
        }

        @Override
        public String toString() {
            return getClass().getSimpleName() + Strings.toString(this);
        }

        public interface AttributeFields {
            ParseField KEY = new ParseField("key");
            ParseField VALUES = new ParseField("values");
        }
    }
}
