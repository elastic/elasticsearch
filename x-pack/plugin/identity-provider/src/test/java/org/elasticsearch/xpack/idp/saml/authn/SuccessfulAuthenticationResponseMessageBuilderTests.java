/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.idp.saml.authn;

import org.elasticsearch.core.Nullable;
import org.elasticsearch.xpack.idp.saml.idp.SamlIdentityProvider;
import org.elasticsearch.xpack.idp.saml.sp.SamlServiceProvider;
import org.elasticsearch.xpack.idp.saml.sp.ServiceProviderDefaults;
import org.elasticsearch.xpack.idp.saml.support.SamlFactory;
import org.elasticsearch.xpack.idp.saml.support.SamlInit;
import org.elasticsearch.xpack.idp.saml.support.SamlInitiateSingleSignOnAttributes;
import org.elasticsearch.xpack.idp.saml.support.XmlValidator;
import org.elasticsearch.xpack.idp.saml.test.IdpSamlTestCase;
import org.junit.Before;
import org.opensaml.saml.saml2.core.Attribute;
import org.opensaml.saml.saml2.core.AttributeStatement;
import org.opensaml.saml.saml2.core.Response;

import java.net.URI;
import java.time.Clock;
import java.time.Duration;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;

import static org.hamcrest.Matchers.containsString;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;
import static org.opensaml.saml.saml2.core.NameIDType.TRANSIENT;

public class SuccessfulAuthenticationResponseMessageBuilderTests extends IdpSamlTestCase {

    private SamlIdentityProvider idp;
    private XmlValidator validator;
    private SamlFactory samlFactory;

    @Before
    public void setupSaml() throws Exception {
        SamlInit.initialize();
        samlFactory = new SamlFactory();
        validator = new XmlValidator("saml-schema-protocol-2.0.xsd");

        idp = mock(SamlIdentityProvider.class);
        when(idp.getEntityId()).thenReturn("https://cloud.elastic.co/saml/idp");
        when(idp.getSigningCredential()).thenReturn(readCredentials("RSA", 2048));
        when(idp.getServiceProviderDefaults()).thenReturn(new ServiceProviderDefaults("elastic-cloud", TRANSIENT, Duration.ofMinutes(5)));
    }

    public void testSignedResponseIsValidAgainstXmlSchema() throws Exception {
        final Response response = buildResponse(null);
        final String xml = super.toString(response);
        assertThat(xml, containsString("SignedInfo>"));
        validator.validate(xml);
    }

    public void testSignedResponseWithCustomAttributes() throws Exception {
        // Create custom attributes
        Map<String, List<String>> attributeMap = new HashMap<>();
        attributeMap.put("customAttr1", Collections.singletonList("value1"));

        List<String> multipleValues = new ArrayList<>();
        multipleValues.add("value2A");
        multipleValues.add("value2B");
        attributeMap.put("customAttr2", multipleValues);
        SamlInitiateSingleSignOnAttributes attributes = new SamlInitiateSingleSignOnAttributes(attributeMap);

        // Build response with custom attributes
        final Response response = buildResponse(attributes);
        final String xml = super.toString(response);

        // Validate that response is correctly signed
        assertThat(xml, containsString("SignedInfo>"));
        validator.validate(xml);

        // Verify custom attributes are included
        boolean foundCustomAttr1 = false;
        boolean foundCustomAttr2 = false;

        for (AttributeStatement statement : response.getAssertions().get(0).getAttributeStatements()) {
            for (Attribute attribute : statement.getAttributes()) {
                String name = attribute.getName();
                if (name.equals("customAttr1")) {
                    foundCustomAttr1 = true;
                    assertEquals(1, attribute.getAttributeValues().size());
                    assertThat(attribute.getAttributeValues().get(0).getDOM().getTextContent(), containsString("value1"));
                } else if (name.equals("customAttr2")) {
                    foundCustomAttr2 = true;
                    assertEquals(2, attribute.getAttributeValues().size());
                    assertThat(attribute.getAttributeValues().get(0).getDOM().getTextContent(), containsString("value2A"));
                    assertThat(attribute.getAttributeValues().get(1).getDOM().getTextContent(), containsString("value2B"));
                }
            }
        }

        assertTrue("Custom attribute 'customAttr1' not found in SAML response", foundCustomAttr1);
        assertTrue("Custom attribute 'customAttr2' not found in SAML response", foundCustomAttr2);
    }

    public void testRejectInvalidCustomAttributes() throws Exception {
        final var customAttributes = new SamlInitiateSingleSignOnAttributes(
            Map.of("https://idp.example.org/attribute/department", Collections.singletonList("engineering"))
        );

        // Build response with custom attributes
        final IllegalArgumentException ex = expectThrows(
            IllegalArgumentException.class,
            () -> buildResponse(
                new SamlServiceProvider.AttributeNames(
                    "https://idp.example.org/attribute/principal",
                    null,
                    null,
                    null,
                    Set.of("https://idp.example.org/attribute/avatar")
                ),
                customAttributes
            )
        );
        assertThat(ex.getMessage(), containsString("custom attribute [https://idp.example.org/attribute/department]"));
        assertThat(ex.getMessage(), containsString("allowed attribute names are [https://idp.example.org/attribute/avatar]"));
    }

    private Response buildResponse(@Nullable SamlInitiateSingleSignOnAttributes customAttributes) throws Exception {
        return buildResponse(
            new SamlServiceProvider.AttributeNames(
                "principal",
                null,
                null,
                null,
                customAttributes == null ? Set.of() : customAttributes.getAttributes().keySet()
            ),
            customAttributes
        );
    }

    private Response buildResponse(
        final SamlServiceProvider.AttributeNames attributes,
        @Nullable SamlInitiateSingleSignOnAttributes customAttributes
    ) throws Exception {
        final Clock clock = Clock.systemUTC();

        final SamlServiceProvider sp = mock(SamlServiceProvider.class);
        final String baseServiceUrl = "https://" + randomAlphaOfLength(32) + ".us-east-1.aws.found.io/";
        final String acs = baseServiceUrl + "api/security/saml/callback";
        when(sp.getEntityId()).thenReturn(baseServiceUrl);
        when(sp.getAssertionConsumerService()).thenReturn(URI.create(acs).toURL());
        when(sp.getAuthnExpiry()).thenReturn(Duration.ofMinutes(10));
        when(sp.getAttributeNames()).thenReturn(attributes);

        final UserServiceAuthentication user = mock(UserServiceAuthentication.class);
        when(user.getPrincipal()).thenReturn(randomAlphaOfLengthBetween(4, 12));
        when(user.getRoles()).thenReturn(Set.of(randomArray(1, 5, String[]::new, () -> randomAlphaOfLengthBetween(4, 12))));
        when(user.getEmail()).thenReturn(randomAlphaOfLength(8) + "@elastic.co");
        when(user.getName()).thenReturn(randomAlphaOfLength(6) + " " + randomAlphaOfLength(8));
        when(user.getServiceProvider()).thenReturn(sp);

        final SuccessfulAuthenticationResponseMessageBuilder builder = new SuccessfulAuthenticationResponseMessageBuilder(
            samlFactory,
            clock,
            idp
        );
        return builder.build(user, null, customAttributes);
    }

}
