/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */

package org.elasticsearch.xpack.security.authc.saml;

import org.elasticsearch.test.ESTestCase;
import org.opensaml.core.xml.XMLObject;
import org.w3c.dom.Element;

import static org.hamcrest.Matchers.equalTo;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

public class SamlObjectHandlerTests extends ESTestCase {

    public void testXmlObjectToTextWhenExceedsLength() {
        final int prefixLength = randomIntBetween(10, 30);
        final int suffixLength = randomIntBetween(5, 20);
        final String prefix = randomAlphaOfLength(prefixLength);
        final String suffix = randomAlphaOfLength(suffixLength);
        final String text = prefix + randomAlphaOfLengthBetween(1, 50) + suffix;

        final XMLObject xml = mock(XMLObject.class);
        final Element element = mock(Element.class);
        when(xml.getDOM()).thenReturn(element);
        when(element.getTextContent()).thenReturn(text);

        assertThat(SamlObjectHandler.text(xml, prefixLength, suffixLength), equalTo(prefix + "..." + suffix));
    }

    public void testXmlObjectToTextPrefixOnly() {
        final int length = randomIntBetween(10, 30);
        final String prefix = randomAlphaOfLength(length);
        final String text = prefix + randomAlphaOfLengthBetween(1, 50);

        final XMLObject xml = mock(XMLObject.class);
        final Element element = mock(Element.class);
        when(xml.getDOM()).thenReturn(element);
        when(element.getTextContent()).thenReturn(text);

        assertThat(SamlObjectHandler.text(xml, length, 0), equalTo(prefix + "..."));
    }

    public void testXmlObjectToTextWhenShortedThanRequiredLength() {
        final int prefixLength = randomIntBetween(10, 30);
        final int suffixLength = randomIntBetween(10, 25);
        final String text = randomAlphaOfLengthBetween(prefixLength + 1, prefixLength + suffixLength);

        final XMLObject xml = mock(XMLObject.class);
        final Element element = mock(Element.class);
        when(xml.getDOM()).thenReturn(element);
        when(element.getTextContent()).thenReturn(text);

        assertThat(SamlObjectHandler.text(xml, prefixLength, suffixLength), equalTo(text));
    }

}
