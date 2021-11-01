/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */
package org.elasticsearch.xpack.security.authc.saml;

import org.hamcrest.Matchers;
import org.junit.Before;
import org.opensaml.saml.common.xml.SAMLConstants;
import org.opensaml.saml.saml2.core.LogoutRequest;
import org.opensaml.saml.saml2.core.NameID;
import org.opensaml.saml.saml2.metadata.EntityDescriptor;
import org.opensaml.saml.saml2.metadata.IDPSSODescriptor;
import org.opensaml.saml.saml2.metadata.SingleLogoutService;

import java.time.Clock;
import java.time.Instant;
import java.time.ZoneOffset;
import java.util.Collections;

import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.greaterThan;
import static org.hamcrest.Matchers.iterableWithSize;
import static org.hamcrest.Matchers.notNullValue;
import static org.hamcrest.Matchers.nullValue;

public class SamlLogoutRequestMessageBuilderTests extends SamlTestCase {

    private static final String SP_ENTITY_ID = "http://sp.saml/";
    private static final String IDP_ENTITY_ID = "http://sp.saml/";

    private SpConfiguration sp;
    private EntityDescriptor idp;
    private IDPSSODescriptor idpRole;
    private NameID nameId;
    private String session;

    @Before
    public void init() throws Exception {
        SamlUtils.initialize(logger);
        sp = new SpConfiguration(SP_ENTITY_ID, "http://sp.example.com/saml/acs", null, null, null, Collections.emptyList());
        idpRole = SamlUtils.buildObject(IDPSSODescriptor.class, IDPSSODescriptor.DEFAULT_ELEMENT_NAME);
        idp = SamlUtils.buildObject(EntityDescriptor.class, EntityDescriptor.DEFAULT_ELEMENT_NAME);
        idp.setEntityID(IDP_ENTITY_ID);
        idp.getRoleDescriptors().add(idpRole);
        nameId = new SamlNameId(NameID.PERSISTENT, randomAlphaOfLengthBetween(12, 24), IDP_ENTITY_ID, SP_ENTITY_ID, null).asXml();
        session = randomAlphaOfLengthBetween(8, 16);
    }

    public void testBuildNullRequestWhenLogoutNotSupportedByIdp() throws Exception {
        idpRole.getSingleLogoutServices().clear();
        final SamlLogoutRequestMessageBuilder builder = new SamlLogoutRequestMessageBuilder(Clock.systemUTC(), sp, idp, nameId, session);
        assertThat(builder.build(), Matchers.nullValue());
    }

    public void testBuildValidRequest() throws Exception {
        final SingleLogoutService sloPost = logoutService(SAMLConstants.SAML2_POST_BINDING_URI, "http://idp.example.com/saml/logout/post");
        idpRole.getSingleLogoutServices().add(sloPost);

        final SingleLogoutService sloRedirect = logoutService(
            SAMLConstants.SAML2_REDIRECT_BINDING_URI,
            "http://idp.example.com/saml/logout/redirect"
        );
        idpRole.getSingleLogoutServices().add(sloRedirect);

        final SingleLogoutService sloArtifact = logoutService(
            SAMLConstants.SAML2_ARTIFACT_BINDING_URI,
            "http://idp.example.com/saml/logout/artifact"
        );
        idpRole.getSingleLogoutServices().add(sloArtifact);

        Clock fixedClock = Clock.fixed(Instant.now(), ZoneOffset.UTC);
        final SamlLogoutRequestMessageBuilder builder = new SamlLogoutRequestMessageBuilder(fixedClock, sp, idp, nameId, session);
        final LogoutRequest logoutRequest = builder.build();
        assertThat(logoutRequest, notNullValue());
        assertThat(logoutRequest.getReason(), nullValue());
        assertThat(logoutRequest.getBaseID(), nullValue());
        assertThat(logoutRequest.getEncryptedID(), nullValue());
        assertThat(logoutRequest.getNameID(), notNullValue());
        assertThat(logoutRequest.getNameID().getFormat(), equalTo(NameID.PERSISTENT));
        assertThat(logoutRequest.getNameID().getValue(), equalTo(nameId.getValue()));
        assertThat(logoutRequest.getNameID().getNameQualifier(), equalTo(IDP_ENTITY_ID));
        assertThat(logoutRequest.getNameID().getSPNameQualifier(), equalTo(SP_ENTITY_ID));
        assertThat(logoutRequest.getConsent(), nullValue());
        assertThat(logoutRequest.getNotOnOrAfter(), nullValue());
        assertThat(logoutRequest.getIssueInstant(), notNullValue());
        assertThat(logoutRequest.getIssueInstant(), equalTo(fixedClock.instant()));
        assertThat(logoutRequest.getSessionIndexes(), iterableWithSize(1));
        assertThat(logoutRequest.getSessionIndexes().get(0).getValue(), equalTo(session));
        assertThat(logoutRequest.getDestination(), equalTo("http://idp.example.com/saml/logout/redirect"));
        assertThat(logoutRequest.getID(), notNullValue());
        assertThat(logoutRequest.getID().length(), greaterThan(20));
        assertThat(logoutRequest.getIssuer(), notNullValue());
        assertThat(logoutRequest.getIssuer().getValue(), equalTo(sp.getEntityId()));
    }

    private SingleLogoutService logoutService(String binding, String location) {
        final SingleLogoutService sls = SamlUtils.buildObject(SingleLogoutService.class, SingleLogoutService.DEFAULT_ELEMENT_NAME);
        sls.setBinding(binding);
        sls.setLocation(location);
        return sls;
    }

}
