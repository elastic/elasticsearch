/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */

package org.elasticsearch.xpack.idp.saml.authn;

import org.elasticsearch.xpack.idp.saml.idp.SamlIdentityProvider;
import org.elasticsearch.xpack.idp.saml.sp.SamlServiceProvider;
import org.elasticsearch.xpack.idp.saml.support.XmlValidator;
import org.elasticsearch.xpack.idp.saml.test.IdpSamlTestCase;
import org.joda.time.Duration;
import org.mockito.Mockito;
import org.opensaml.saml.saml2.core.Response;

import java.io.ByteArrayInputStream;
import java.nio.charset.StandardCharsets;
import java.time.Clock;
import java.util.Set;

public class SuccessfulAuthenticationResponseMessageBuilderTests extends IdpSamlTestCase {

    public void testResponseIsValidAgainstXmlSchema() throws Exception {
        final Clock clock = Clock.systemUTC();

        final SamlIdentityProvider idp = Mockito.mock(SamlIdentityProvider.class);
        Mockito.when(idp.getEntityId()).thenReturn("https://cloud.elastic.co/saml/idp");

        final SamlServiceProvider sp = Mockito.mock(SamlServiceProvider.class);
        final String baseServiceUrl = "https://" + randomAlphaOfLength(32) + ".us-east-1.aws.found.io/";
        final String acs = baseServiceUrl + "api/security/saml/callback";
        Mockito.when(sp.getEntityId()).thenReturn(baseServiceUrl);
        Mockito.when(sp.getAssertionConsumerService()).thenReturn(acs);
        Mockito.when(sp.getAuthnExpiry()).thenReturn(Duration.standardMinutes(10));
        Mockito.when(sp.getAttributeNames()).thenReturn(new SamlServiceProvider.AttributeNames());

        final UserServiceAuthentication user = Mockito.mock(UserServiceAuthentication.class);
        Mockito.when(user.getPrincipal()).thenReturn(randomAlphaOfLengthBetween(4, 12));
        Mockito.when(user.getGroups()).thenReturn(Set.of(randomArray(1, 5, String[]::new, () -> randomAlphaOfLengthBetween(4, 12))));
        Mockito.when(user.getServiceProvider()).thenReturn(sp);

        SuccessfulAuthenticationResponseMessageBuilder builder = new SuccessfulAuthenticationResponseMessageBuilder(clock, idp);
        final Response response = builder.build(user, null);

        final String xml = super.toString(response);
        XmlValidator validator = new XmlValidator("saml-schema-protocol-2.0.xsd");
        validator.validate(new ByteArrayInputStream(xml.getBytes(StandardCharsets.UTF_8)));
    }

}
