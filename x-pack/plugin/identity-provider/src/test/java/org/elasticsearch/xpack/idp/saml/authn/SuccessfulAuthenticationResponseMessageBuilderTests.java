/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */

package org.elasticsearch.xpack.idp.saml.authn;

import org.elasticsearch.xpack.idp.saml.idp.SamlIdentityProvider;
import org.elasticsearch.xpack.idp.saml.sp.SamlServiceProvider;
import org.elasticsearch.xpack.idp.saml.support.SamlFactory;
import org.elasticsearch.xpack.idp.saml.support.SamlObjectSigner;
import org.elasticsearch.xpack.idp.saml.support.XmlValidator;
import org.elasticsearch.xpack.idp.saml.test.IdpSamlTestCase;
import org.joda.time.Duration;
import org.junit.Before;
import org.mockito.Mockito;
import org.opensaml.saml.saml2.core.Response;
import org.w3c.dom.Element;

import java.net.URI;
import java.net.URISyntaxException;
import java.time.Clock;
import java.util.Set;

import static org.hamcrest.Matchers.containsString;
import static org.hamcrest.Matchers.not;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

public class SuccessfulAuthenticationResponseMessageBuilderTests extends IdpSamlTestCase {

    private SamlIdentityProvider idp;
    private SamlFactory samlFactory;
    private XmlValidator validator;

    @Before
    public void setupSaml() throws Exception {
        samlFactory = new SamlFactory();
        validator = new XmlValidator("saml-schema-protocol-2.0.xsd");

        idp = mock(SamlIdentityProvider.class);
        when(idp.getEntityId()).thenReturn("https://cloud.elastic.co/saml/idp");
        when(idp.getSigningCredential()).thenReturn(readCredentials("RSA", 2048));
    }

    public void testUnsignedResponseIsValidAgainstXmlSchema() throws Exception {
        final Response response = buildResponse();

        final String xml = super.toString(response);
        assertThat(xml, not(containsString("SignedInfo>")));
        validator.validate(xml);
    }

    public void testSignedResponseIsValidAgainstXmlSchema() throws Exception {
        final SamlObjectSigner signer = new SamlObjectSigner(samlFactory, idp);
        final Response response = buildResponse();

        final Element signed = signer.sign(response);
        final String xml = super.toString(signed);
        assertThat(xml, containsString("SignedInfo>"));
        validator.validate(xml);
    }

    private Response buildResponse() throws URISyntaxException {
        final Clock clock = Clock.systemUTC();

        final SamlServiceProvider sp = mock(SamlServiceProvider.class);
        final String baseServiceUrl = "https://" + randomAlphaOfLength(32) + ".us-east-1.aws.found.io/";
        final URI acs = new URI(baseServiceUrl + "api/security/saml/callback");
        when(sp.getEntityId()).thenReturn(baseServiceUrl);
        when(sp.getAssertionConsumerService()).thenReturn(acs);
        when(sp.getAuthnExpiry()).thenReturn(Duration.standardMinutes(10));
        when(sp.getAttributeNames()).thenReturn(new SamlServiceProvider.AttributeNames());

        final UserServiceAuthentication user = mock(UserServiceAuthentication.class);
        when(user.getPrincipal()).thenReturn(randomAlphaOfLengthBetween(4, 12));
        when(user.getGroups()).thenReturn(Set.of(randomArray(1, 5, String[]::new, () -> randomAlphaOfLengthBetween(4, 12))));
        when(user.getServiceProvider()).thenReturn(sp);

        final SuccessfulAuthenticationResponseMessageBuilder builder = new SuccessfulAuthenticationResponseMessageBuilder(samlFactory,
            clock, idp);
        return builder.build(user, null);
    }


}
