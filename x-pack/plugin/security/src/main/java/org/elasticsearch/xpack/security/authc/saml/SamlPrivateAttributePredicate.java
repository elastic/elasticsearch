/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.security.authc.saml;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.elasticsearch.xpack.core.security.authc.RealmConfig;
import org.elasticsearch.xpack.core.security.authc.saml.SamlRealmSettings;
import org.elasticsearch.xpack.security.authc.saml.SamlAttributes.SamlPrivateAttribute;
import org.opensaml.saml.saml2.core.Attribute;

import java.util.List;
import java.util.Set;
import java.util.function.Predicate;
import java.util.stream.Collectors;

import static org.elasticsearch.xpack.core.security.authc.saml.SamlRealmSettings.PRIVATE_ATTRIBUTES;

/**
 * The predicate which is constructed based on the values of {@link SamlRealmSettings#PRIVATE_ATTRIBUTES} setting.
 * When the setting is configured, the attributes whose {@code Attribute#getName} or {@code Attribute#getFriendlyName} match,
 * will be treated as private ({@link SamlPrivateAttribute}).
 */
class SamlPrivateAttributePredicate implements Predicate<Attribute> {

    private static final Logger logger = LogManager.getLogger(SamlPrivateAttributePredicate.class);

    private static final Predicate<Attribute> MATCH_NONE = new Predicate<Attribute>() {
        @Override
        public boolean test(Attribute attribute) {
            return false;
        }

        @Override
        public String toString() {
            return "<matching no SAML private attributes>";
        }
    };

    private final Predicate<Attribute> predicate;

    SamlPrivateAttributePredicate(RealmConfig config) {
        this.predicate = buildPrivateAttributesPredicate(config);
    }

    private static Predicate<Attribute> buildPrivateAttributesPredicate(RealmConfig config) {

        if (false == config.hasSetting(PRIVATE_ATTRIBUTES)) {
            logger.trace("No SAML private attributes setting configured.");
            return MATCH_NONE;
        }

        final List<String> attributesList = config.getSetting(PRIVATE_ATTRIBUTES);
        if (attributesList == null || attributesList.isEmpty()) {
            logger.trace("No SAML private attributes configured for setting [{}].", PRIVATE_ATTRIBUTES);
            return MATCH_NONE;
        }

        final Set<String> attributesSet = attributesList.stream()
            .filter(name -> name != null && false == name.isBlank())
            .collect(Collectors.toUnmodifiableSet());

        if (attributesSet.isEmpty()) {
            return MATCH_NONE;
        }

        logger.trace("SAML private attributes configured: {}", attributesSet);
        return new Predicate<>() {

            @Override
            public boolean test(Attribute attribute) {
                if (attribute == null) {
                    return false;
                }
                if (attribute.getName() != null && attributesSet.contains(attribute.getName())) {
                    return true;
                }
                return attribute.getFriendlyName() != null && attributesSet.contains(attribute.getFriendlyName());
            }

            @Override
            public String toString() {
                return "<matching " + attributesSet + " SAML private attributes>";
            }
        };
    }

    @Override
    public boolean test(Attribute attribute) {
        return predicate.test(attribute);
    }

    @Override
    public String toString() {
        return this.getClass().getSimpleName() + " {predicate=" + predicate + "}";
    }

}
