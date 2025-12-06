/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.security.authc.saml;

import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.xpack.core.security.authc.RealmConfig;
import org.mockito.Mockito;
import org.opensaml.saml.saml2.core.Attribute;

import java.util.List;

import static org.elasticsearch.xpack.core.security.authc.saml.SamlRealmSettings.PRIVATE_ATTRIBUTES;
import static org.hamcrest.Matchers.is;
import static org.mockito.Mockito.doReturn;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

public class SamlSecureAttributePredicateTests extends ESTestCase {

    public void testPredicateWithSettingConfigured() {

        final List<String> privateAttributes = List.of("private", "http://elastic.co/confidential");
        final RealmConfig config = realmConfig(privateAttributes);
        final SamlPrivateAttributePredicate predicate = new SamlPrivateAttributePredicate(config);

        final String privateAttribute = randomFrom(privateAttributes);
        final String nonPrivateAttribute = randomFrom(new String[] { null, " ", randomAlphaOfLengthBetween(0, 3) });

        assertThat(predicate.test(attribute("private", "http://elastic.co/confidential")), is(true));
        assertThat(predicate.test(attribute(privateAttribute, nonPrivateAttribute)), is(true));
        assertThat(predicate.test(attribute(nonPrivateAttribute, privateAttribute)), is(true));

        assertThat(predicate.test(attribute(privateAttribute, null)), is(true));
        assertThat(predicate.test(attribute(null, privateAttribute)), is(true));

        assertThat(predicate.test(attribute(nonPrivateAttribute, null)), is(false));
        assertThat(predicate.test(attribute(null, nonPrivateAttribute)), is(false));
        assertThat(predicate.test(attribute(null, null)), is(false));

        assertThat(predicate.test(attribute("something", "else")), is(false));
        assertThat(predicate.test(attribute("", "")), is(false));

    }

    public void testPredicateWhenSettingIsNotConfigured() {

        List<String> privateAttributes = randomBoolean() ? List.of() : null;
        RealmConfig config = realmConfig(privateAttributes);
        SamlPrivateAttributePredicate predicate = new SamlPrivateAttributePredicate(config);

        String name = randomFrom(randomAlphaOfLengthBetween(0, 5), null);
        String friendlyName = randomFrom(randomAlphaOfLengthBetween(0, 5), null);

        assertThat(predicate.test(attribute(name, friendlyName)), is(false));

    }

    private static Attribute attribute(String name, String friendlyName) {
        Attribute attribute = mock(Attribute.class);
        when(attribute.getName()).thenReturn(name);
        when(attribute.getFriendlyName()).thenReturn(friendlyName);
        return attribute;
    }

    private static RealmConfig realmConfig(List<String> privateAttributeNames) {
        RealmConfig config = Mockito.mock(RealmConfig.class);
        when(config.hasSetting(PRIVATE_ATTRIBUTES)).thenReturn(privateAttributeNames != null);
        doReturn(privateAttributeNames).when(config).getSetting(PRIVATE_ATTRIBUTES);
        return config;
    }

}
