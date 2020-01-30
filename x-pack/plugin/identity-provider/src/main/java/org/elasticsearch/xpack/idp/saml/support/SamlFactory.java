/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */

package org.elasticsearch.xpack.idp.saml.support;

import org.elasticsearch.common.hash.MessageDigests;
import org.opensaml.core.xml.XMLObject;
import org.opensaml.core.xml.XMLObjectBuilderFactory;
import org.opensaml.core.xml.config.XMLObjectProviderRegistrySupport;

import javax.xml.namespace.QName;
import java.security.SecureRandom;

/**
 * Utility object for constructing new objects and values in a SAML 2.0 / OpenSAML context
 */
public class SamlFactory {

    private final XMLObjectBuilderFactory builderFactory;
    private final SecureRandom random;

    public SamlFactory() {
        SamlInit.initialize();
        builderFactory = XMLObjectProviderRegistrySupport.getBuilderFactory();
        random = new SecureRandom();
    }

    public <T extends XMLObject> T object(Class<T> type, QName elementName) {
        final XMLObject obj = builderFactory.getBuilder(elementName).buildObject(elementName);
        return cast(type, elementName, obj);
    }

    public <T extends XMLObject> T object(Class<T> type, QName elementName, QName schemaType) {
        final XMLObject obj = builderFactory.getBuilder(schemaType).buildObject(elementName, schemaType);
        return cast(type, elementName, obj);
    }

    private <T extends XMLObject> T cast(Class<T> type, QName elementName, XMLObject obj) {
        if (type.isInstance(obj)) {
            return type.cast(obj);
        } else {
            throw new IllegalArgumentException("Object for element " + elementName.getLocalPart() + " is of type " + obj.getClass()
                + " not " + type);
        }
    }

    public String secureIdentifier() {
        return randomNCName(20);
    }

    private String randomNCName(int numberBytes) {
        final byte[] randomBytes = new byte[numberBytes];
        random.nextBytes(randomBytes);
        // NCNames (https://www.w3.org/TR/xmlschema-2/#NCName) can't start with a number, so start them all with "_" to be safe
        return "_".concat(MessageDigests.toHexString(randomBytes));
    }

}
