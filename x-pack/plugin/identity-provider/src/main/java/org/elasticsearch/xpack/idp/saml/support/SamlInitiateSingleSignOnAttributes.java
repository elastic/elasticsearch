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
import org.elasticsearch.xcontent.ToXContentObject;
import org.elasticsearch.xcontent.XContentBuilder;
import org.elasticsearch.xcontent.XContentParser;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;

/**
 * Represents a collection of SAML attributes to be included in the SAML response.
 * Each attribute has a key and a list of values.
 */
public class SamlInitiateSingleSignOnAttributes implements Writeable, ToXContentObject {
    private final Map<String, List<String>> attributes;

    public SamlInitiateSingleSignOnAttributes(Map<String, List<String>> attributes) {
        this.attributes = attributes;
    }

    /**
     * @return A map of SAML attribute key to list of values
     */
    public Map<String, List<String>> getAttributes() {
        return Collections.unmodifiableMap(attributes);
    }

    /**
     * Creates a SamlInitiateSingleSignOnAttributes object by parsing the provided JSON content.
     * Expects a JSON structure like: { "attr1": ["val1", "val2"], "attr2": ["val3"] }
     *
     * @param parser The XContentParser positioned at the start of the object
     * @return A new SamlInitiateSingleSignOnAttributes instance
     */
    public static SamlInitiateSingleSignOnAttributes fromXContent(XContentParser parser) throws IOException {
        Map<String, List<String>> attributes = new HashMap<>();
        while (parser.nextToken() != XContentParser.Token.END_OBJECT) {
            String key = parser.currentName();
            if (parser.nextToken() == XContentParser.Token.START_ARRAY) {
                List<String> values = new ArrayList<>();
                while (parser.nextToken() != XContentParser.Token.END_ARRAY) {
                    values.add(parser.text());
                }
                attributes.put(key, values);
            }
        }
        return new SamlInitiateSingleSignOnAttributes(attributes);
    }

    @Override
    public XContentBuilder toXContent(XContentBuilder builder, Params params) throws IOException {
        for (Map.Entry<String, List<String>> entry : attributes.entrySet()) {
            builder.startArray(entry.getKey());
            for (String value : entry.getValue()) {
                builder.value(value);
            }
            builder.endArray();
        }
        return builder;
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        out.writeVInt(attributes.size());
        for (Map.Entry<String, List<String>> entry : attributes.entrySet()) {
            out.writeString(entry.getKey());
            List<String> values = entry.getValue();
            out.writeVInt(values.size());
            for (String value : values) {
                out.writeString(value);
            }
        }
    }

    public SamlInitiateSingleSignOnAttributes(StreamInput in) throws IOException {
        int size = in.readVInt();
        attributes = new HashMap<>(size);
        for (int i = 0; i < size; i++) {
            String key = in.readString();
            int valuesSize = in.readVInt();
            List<String> values = new ArrayList<>(valuesSize);
            for (int j = 0; j < valuesSize; j++) {
                values.add(in.readString());
            }
            attributes.put(key, values);
        }
    }

    /**
     * Validates the attributes for correctness.
     * An attribute with an empty key is considered invalid.
     */
    public ActionRequestValidationException validate() {
        ActionRequestValidationException validationException = null;
        if (attributes.isEmpty() == false) {
            for (String key : attributes.keySet()) {
                if (Strings.isNullOrEmpty(key)) {
                    validationException = ValidateActions.addValidationError("attribute key cannot be null or empty", validationException);
                }
            }
        }
        return validationException;
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
}
