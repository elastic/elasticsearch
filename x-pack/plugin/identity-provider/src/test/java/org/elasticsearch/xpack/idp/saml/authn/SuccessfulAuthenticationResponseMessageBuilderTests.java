/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.idp.saml.authn;

import org.elasticsearch.xpack.idp.saml.idp.SamlIdentityProvider;
import org.elasticsearch.xpack.idp.saml.sp.SamlServiceProvider;
import org.elasticsearch.xpack.idp.saml.sp.ServiceProviderDefaults;
import org.elasticsearch.xpack.idp.saml.support.SamlFactory;
import org.elasticsearch.xpack.idp.saml.support.SamlInit;
import org.elasticsearch.xpack.idp.saml.support.XmlValidator;
import org.elasticsearch.xpack.idp.saml.test.IdpSamlTestCase;
import org.junit.Before;
import org.opensaml.saml.saml2.core.Response;

import java.net.URL;
import java.time.Clock;
import java.time.Duration;
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
        when(idp.getServiceProviderDefaults())
            .thenReturn(new ServiceProviderDefaults("elastic-cloud", TRANSIENT, Duration.ofMinutes(5)));
    }

    public void testSignedResponseIsValidAgainstXmlSchema() throws Exception {
        final Response response = buildResponse();
        final String xml = super.toString(response);
        assertThat(xml, containsString("SignedInfo>"));
        validator.validate(xml);
    }

    private Response buildResponse() throws Exception{
        final Clock clock = Clock.systemUTC();

        final SamlServiceProvider sp = mock(SamlServiceProvider.class);
        final String baseServiceUrl = "https://" + randomAlphaOfLength(32) + ".us-east-1.aws.found.io/";
        final String acs = baseServiceUrl + "api/security/saml/callback";
        when(sp.getEntityId()).thenReturn(baseServiceUrl);
        when(sp.getAssertionConsumerService()).thenReturn(new URL(acs));
        when(sp.getAuthnExpiry()).thenReturn(Duration.ofMinutes(10));
        when(sp.getAttributeNames()).thenReturn(new SamlServiceProvider.AttributeNames("principal", null, null, null));

        final UserServiceAuthentication user = mock(UserServiceAuthentication.class);
        when(user.getPrincipal()).thenReturn(randomAlphaOfLengthBetween(4, 12));
        when(user.getRoles()).thenReturn(Set.of(randomArray(1, 5, String[]::new, () -> randomAlphaOfLengthBetween(4, 12))));
        when(user.getEmail()).thenReturn(randomAlphaOfLength(8) + "@elastic.co");
        when(user.getName()).thenReturn(randomAlphaOfLength(6) + " " + randomAlphaOfLength(8));
        when(user.getServiceProvider()).thenReturn(sp);

        final SuccessfulAuthenticationResponseMessageBuilder builder =
            new SuccessfulAuthenticationResponseMessageBuilder(samlFactory, clock, idp);
        return builder.build(user, null);
    }


}
