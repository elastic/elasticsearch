/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.idp.saml.authn;

import org.elasticsearch.xpack.idp.saml.idp.SamlIdentityProvider;
import org.elasticsearch.xpack.idp.saml.support.SamlFactory;
import org.elasticsearch.xpack.idp.saml.support.SamlInit;
import org.elasticsearch.xpack.idp.saml.support.XmlValidator;
import org.elasticsearch.xpack.idp.saml.test.IdpSamlTestCase;
import org.junit.Before;
import org.opensaml.saml.saml2.core.Response;
import org.opensaml.saml.saml2.core.StatusCode;

import java.time.Clock;

import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

public class FailedAuthenticationResponseBuilderTests extends IdpSamlTestCase {

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
    }

    public void testSimpleErrorResponseIsValid() throws Exception {
        final Clock clock = Clock.systemUTC();
        final FailedAuthenticationResponseMessageBuilder builder = new FailedAuthenticationResponseMessageBuilder(samlFactory, clock, idp);
        final Response response = builder.setAcsUrl(
            "https://" + randomAlphaOfLengthBetween(4, 8) + "." + randomAlphaOfLengthBetween(4, 8) + "/saml/acs"
        ).setPrimaryStatusCode(StatusCode.REQUESTER).setInResponseTo(randomAlphaOfLength(12)).build();
        final String xml = super.toString(response);
        validator.validate(xml);
    }

    public void testErrorResponseWithCodeIsValid() throws Exception {
        final Clock clock = Clock.systemUTC();
        final FailedAuthenticationResponseMessageBuilder builder = new FailedAuthenticationResponseMessageBuilder(samlFactory, clock, idp);
        final Response response = builder.setAcsUrl(
            "https://" + randomAlphaOfLengthBetween(4, 8) + "." + randomAlphaOfLengthBetween(4, 8) + "/saml/acs"
        )
            .setPrimaryStatusCode(StatusCode.REQUESTER)
            .setInResponseTo(randomAlphaOfLength(12))
            .setSecondaryStatusCode(StatusCode.INVALID_NAMEID_POLICY)
            .build();
        final String xml = super.toString(response);
        validator.validate(xml);
    }
}
