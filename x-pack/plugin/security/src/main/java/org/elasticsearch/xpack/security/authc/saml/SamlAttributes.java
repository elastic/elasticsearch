/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */
package org.elasticsearch.xpack.security.authc.saml;

import org.elasticsearch.common.Strings;
import org.elasticsearch.common.settings.SecureString;
import org.elasticsearch.core.IOUtils;
import org.elasticsearch.core.Nullable;
import org.elasticsearch.core.Releasable;
import org.opensaml.saml.saml2.core.Attribute;
import org.opensaml.saml.saml2.core.NameIDType;

import java.util.List;
import java.util.Objects;

/**
 * An lightweight collection of SAML attributes
 */
public class SamlAttributes implements Releasable {

    public static final String NAMEID_SYNTHENTIC_ATTRIBUTE = "nameid";
    public static final String PERSISTENT_NAMEID_SYNTHENTIC_ATTRIBUTE = "nameid:persistent";

    private final SamlNameId name;
    private final String session;
    private final List<SamlAttribute> attributes;
    private final List<SamlPrivateAttribute> privateAttributes;

    SamlAttributes(SamlNameId name, String session, List<SamlAttribute> attributes, List<SamlPrivateAttribute> privateAttributes) {
        this.name = name;
        this.session = session;
        this.attributes = attributes;
        this.privateAttributes = privateAttributes;
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

    List<SecureString> getPrivateAttributeValues(String attributeId) {
        if (Strings.isNullOrEmpty(attributeId)) {
            return List.of();
        }
        return privateAttributes.stream()
            .filter(attr -> attributeId.equals(attr.name) || attributeId.equals(attr.friendlyName))
            .flatMap(attr -> attr.values.stream())
            .toList();
    }

    List<SamlAttribute> attributes() {
        return attributes;
    }

    List<SamlPrivateAttribute> privateAttributes() {
        return privateAttributes;
    }

    boolean isEmpty() {
        return attributes.isEmpty() && privateAttributes.isEmpty();
    }

    SamlNameId name() {
        return name;
    }

    String session() {
        return session;
    }

    @Override
    public String toString() {
        return getClass().getSimpleName() + "(" + name + ")[" + session + "]{" + attributes + "}{" + privateAttributes + "}";
    }

    @Override
    public void close() {
        IOUtils.closeWhileHandlingException(privateAttributes);
    }

    abstract static class AbstractSamlAttribute<T> {

        final String name;
        final String friendlyName;
        final List<T> values;

        protected AbstractSamlAttribute(String name, @Nullable String friendlyName, List<T> values) {
            this.name = Objects.requireNonNull(name, "Attribute name cannot be null");
            this.friendlyName = friendlyName;
            this.values = values;
        }

        String name() {
            return name;
        }

        String friendlyName() {
            return friendlyName;
        }

        List<T> values() {
            return values;
        }
    }

    static class SamlAttribute extends AbstractSamlAttribute<String> {

        SamlAttribute(Attribute attribute) {
            this(
                attribute.getName(),
                attribute.getFriendlyName(),
                attribute.getAttributeValues().stream().map(x -> x.getDOM().getTextContent()).filter(Objects::nonNull).toList()
            );
        }

        SamlAttribute(String name, @Nullable String friendlyName, List<String> values) {
            super(name, friendlyName, values);
        }

        @Override
        public String toString() {
            StringBuilder str = new StringBuilder();
            if (Strings.isNullOrEmpty(friendlyName)) {
                str.append(name);
            } else {
                str.append(friendlyName).append('(').append(name).append(')');
            }
            str.append("=").append(values).append("(len=").append(values.size()).append(')');
            return str.toString();
        }
    }

    static class SamlPrivateAttribute extends AbstractSamlAttribute<SecureString> implements Releasable {

        SamlPrivateAttribute(Attribute attribute) {
            super(
                attribute.getName(),
                attribute.getFriendlyName(),
                attribute.getAttributeValues()
                    .stream()
                    .map(x -> x.getDOM().getTextContent())
                    .filter(Objects::nonNull)
                    .map(String::toCharArray)
                    .map(SecureString::new)
                    .toList()
            );
        }

        SamlPrivateAttribute(String name, @Nullable String friendlyName, List<SecureString> values) {
            super(name, friendlyName, values);
        }

        @Override
        public String toString() {
            StringBuilder str = new StringBuilder();
            if (Strings.isNullOrEmpty(friendlyName)) {
                str.append(name);
            } else {
                str.append(friendlyName).append('(').append(name).append(')');
            }
            str.append("=[").append(values.size()).append(" value(s)]");
            return str.toString();
        }

        @Override
        public void close() {
            IOUtils.closeWhileHandlingException(values);
        }
    }
}
