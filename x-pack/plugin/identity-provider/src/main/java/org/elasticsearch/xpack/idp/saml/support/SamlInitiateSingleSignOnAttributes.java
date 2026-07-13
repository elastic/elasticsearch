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
import org.elasticsearch.common.xcontent.XContentParserUtils;
import org.elasticsearch.xcontent.ToXContentObject;
import org.elasticsearch.xcontent.XContentBuilder;
import org.elasticsearch.xcontent.XContentParser;

import java.io.IOException;
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
        final Map<String, List<String>> attributes = parser.map(HashMap::new, p -> XContentParserUtils.parseList(p, XContentParser::text));
        return new SamlInitiateSingleSignOnAttributes(attributes);
    }

    @Override
    public XContentBuilder toXContent(XContentBuilder builder, Params params) throws IOException {
        return builder.map(attributes);
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        out.writeMap(attributes, StreamOutput::writeStringCollection);
    }

    public SamlInitiateSingleSignOnAttributes(StreamInput in) throws IOException {
        this.attributes = in.readImmutableMap(StreamInput::readStringCollectionAsImmutableList);
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
