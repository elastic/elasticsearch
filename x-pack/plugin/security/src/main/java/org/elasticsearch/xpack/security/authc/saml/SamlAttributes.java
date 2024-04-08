/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */
package org.elasticsearch.xpack.security.authc.saml;

import org.elasticsearch.common.Strings;
import org.elasticsearch.core.Nullable;
import org.opensaml.saml.saml2.core.Attribute;
import org.opensaml.saml.saml2.core.NameIDType;

import java.util.List;
import java.util.Objects;

/**
 * An lightweight collection of SAML attributes
 */
public class SamlAttributes {

    public static final String NAMEID_SYNTHENTIC_ATTRIBUTE = "nameid";
    public static final String PERSISTENT_NAMEID_SYNTHENTIC_ATTRIBUTE = "nameid:persistent";

    private final SamlNameId name;
    private final String session;
    private final List<SamlAttribute> attributes;

    SamlAttributes(SamlNameId name, String session, List<SamlAttribute> attributes) {
        this.name = name;
        this.session = session;
        this.attributes = attributes;
    }

    /**
     * Finds all values for the specified attribute
     *
     * @param attributeId The name of the attribute - either its {@code name} or @{code friendlyName}
     * @return A list of all matching attribute values (may be empty).
     */
    List<String> getAttributeValues(String attributeId) {
        if (Strings.isNullOrEmpty(attributeId)) {
            return List.of();
        }
        if (attributeId.equals(NAMEID_SYNTHENTIC_ATTRIBUTE)) {
            return name == null ? List.of() : List.of(name.value);
        }
        if (attributeId.equals(PERSISTENT_NAMEID_SYNTHENTIC_ATTRIBUTE) && name != null && NameIDType.PERSISTENT.equals(name.format)) {
            return List.of(name.value);
        }
        return attributes.stream()
            .filter(attr -> attributeId.equals(attr.name) || attributeId.equals(attr.friendlyName))
            .flatMap(attr -> attr.values.stream())
            .toList();
    }

    List<SamlAttribute> attributes() {
        return attributes;
    }

    SamlNameId name() {
        return name;
    }

    String session() {
        return session;
    }

    @Override
    public String toString() {
        return getClass().getSimpleName() + "(" + name + ")[" + session + "]{" + attributes + "}";
    }

    static class SamlAttribute {
        final String name;
        final String friendlyName;
        final List<String> values;

        SamlAttribute(Attribute attribute) {
            this(
                attribute.getName(),
                attribute.getFriendlyName(),
                attribute.getAttributeValues().stream().map(x -> x.getDOM().getTextContent()).filter(Objects::nonNull).toList()
            );
        }

        SamlAttribute(String name, @Nullable String friendlyName, List<String> values) {
            this.name = Objects.requireNonNull(name, "Attribute name cannot be null");
            this.friendlyName = friendlyName;
            this.values = values;
        }

        @Override
        public String toString() {
            if (Strings.isNullOrEmpty(friendlyName)) {
                return name + '=' + values;
            } else {
                return friendlyName + '(' + name + ")=" + values;
            }
        }
    }

}
