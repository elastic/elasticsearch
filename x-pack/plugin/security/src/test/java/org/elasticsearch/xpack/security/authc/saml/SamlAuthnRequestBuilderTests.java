/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.xpack.security.authc.saml;

import java.time.Clock;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;

import org.joda.time.Instant;
import org.junit.Before;
import org.opensaml.saml.common.xml.SAMLConstants;
import org.opensaml.saml.saml2.core.AuthnRequest;
import org.opensaml.saml.saml2.core.NameID;
import org.opensaml.saml.saml2.metadata.EntityDescriptor;
import org.opensaml.saml.saml2.metadata.IDPSSODescriptor;
import org.opensaml.saml.saml2.metadata.SingleSignOnService;

import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.greaterThan;
import static org.hamcrest.Matchers.notNullValue;
import static org.hamcrest.Matchers.nullValue;
import static org.opensaml.saml.saml2.core.AuthnContext.KERBEROS_AUTHN_CTX;
import static org.opensaml.saml.saml2.core.AuthnContext.SMARTCARD_AUTHN_CTX;

public class SamlAuthnRequestBuilderTests extends SamlTestCase {

    private static final String SP_ENTITY_ID = "https://sp.example.com/";
    private static final String IDP_ENTITY_ID = "https://idp.example.net/";

    private static final String ACS_URL = "https://sp.example.com/saml/acs";
    private static final String IDP_URL = "https://idp.example.net/saml/sso/redirect";
    private EntityDescriptor idpDescriptor;

    @Before
    public void init() throws Exception {
        SamlUtils.initialize(logger);

        final SingleSignOnService sso = SamlUtils.buildObject(SingleSignOnService.class, SingleSignOnService.DEFAULT_ELEMENT_NAME);
        sso.setLocation(IDP_URL);
        sso.setBinding(SAMLConstants.SAML2_REDIRECT_BINDING_URI);

        final IDPSSODescriptor idpRole = SamlUtils.buildObject(IDPSSODescriptor.class, IDPSSODescriptor.DEFAULT_ELEMENT_NAME);
        idpRole.getSingleSignOnServices().add(sso);

        idpDescriptor = SamlUtils.buildObject(EntityDescriptor.class, EntityDescriptor.DEFAULT_ELEMENT_NAME);
        idpDescriptor.setEntityID(IDP_ENTITY_ID);
        idpDescriptor.getRoleDescriptors().add(idpRole);
    }

    public void testBuildRequestWithDefaultSettingsHasNoNameIdPolicy() {
        SpConfiguration sp = new SpConfiguration(SP_ENTITY_ID, ACS_URL, null, null, null, Collections.emptyList());
        final SamlAuthnRequestBuilder builder = new SamlAuthnRequestBuilder(
            sp, SAMLConstants.SAML2_POST_BINDING_URI,
            idpDescriptor, SAMLConstants.SAML2_REDIRECT_BINDING_URI,
            Clock.systemUTC());

        final AuthnRequest request = buildAndValidateAuthnRequest(builder);

        assertThat(request.getIssuer().getValue(), equalTo(SP_ENTITY_ID));
        assertThat(request.getProtocolBinding(), equalTo(SAMLConstants.SAML2_POST_BINDING_URI));

        assertThat(request.getAssertionConsumerServiceURL(), equalTo(ACS_URL));

        assertThat(request.getNameIDPolicy(), notNullValue());
        assertThat(request.getNameIDPolicy().getFormat(), nullValue());
        assertThat(request.getNameIDPolicy().getSPNameQualifier(), nullValue());
        assertThat(request.getNameIDPolicy().getAllowCreate(), equalTo(Boolean.FALSE));
    }

    public void testBuildRequestWithPersistentNameAndNoForceAuth() throws Exception {
        SpConfiguration sp = new SpConfiguration(SP_ENTITY_ID, ACS_URL, null, null, null, Collections.emptyList());
        final SamlAuthnRequestBuilder builder = new SamlAuthnRequestBuilder(
                sp, SAMLConstants.SAML2_POST_BINDING_URI,
                idpDescriptor, SAMLConstants.SAML2_REDIRECT_BINDING_URI,
                Clock.systemUTC());
        builder.nameIDPolicy(new SamlAuthnRequestBuilder.NameIDPolicySettings(NameID.PERSISTENT, false, SP_ENTITY_ID));
        builder.forceAuthn(null);

        final AuthnRequest request = buildAndValidateAuthnRequest(builder);

        assertThat(request.getIssuer().getValue(), equalTo(SP_ENTITY_ID));
        assertThat(request.getProtocolBinding(), equalTo(SAMLConstants.SAML2_POST_BINDING_URI));

        assertThat(request.getAssertionConsumerServiceURL(), equalTo(ACS_URL));

        assertThat(request.getNameIDPolicy(), notNullValue());
        assertThat(request.getNameIDPolicy().getFormat(), equalTo(NameID.PERSISTENT));
        assertThat(request.getNameIDPolicy().getSPNameQualifier(), equalTo(SP_ENTITY_ID));
        assertThat(request.getNameIDPolicy().getAllowCreate(), equalTo(Boolean.FALSE));

        assertThat(request.isForceAuthn(), equalTo(Boolean.FALSE));
        assertThat(request.getRequestedAuthnContext(), equalTo(null));
    }

    public void testBuildRequestWithTransientNameAndForceAuthTrue() throws Exception {
        SpConfiguration sp = new SpConfiguration(SP_ENTITY_ID, ACS_URL, null, null, null, Collections.emptyList());
        final SamlAuthnRequestBuilder builder = new SamlAuthnRequestBuilder(
            sp, SAMLConstants.SAML2_POST_BINDING_URI,
            idpDescriptor, SAMLConstants.SAML2_REDIRECT_BINDING_URI,
            Clock.systemUTC());

        final String noSpNameQualifier = randomBoolean() ? "" : null;
        builder.nameIDPolicy(new SamlAuthnRequestBuilder.NameIDPolicySettings(NameID.TRANSIENT, true, noSpNameQualifier));
        builder.forceAuthn(Boolean.TRUE);

        final AuthnRequest request = buildAndValidateAuthnRequest(builder);

        assertThat(request.getIssuer().getValue(), equalTo(SP_ENTITY_ID));
        assertThat(request.getProtocolBinding(), equalTo(SAMLConstants.SAML2_POST_BINDING_URI));

        assertThat(request.getAssertionConsumerServiceURL(), equalTo(ACS_URL));

        assertThat(request.getNameIDPolicy(), notNullValue());
        assertThat(request.getNameIDPolicy().getFormat(), equalTo(NameID.TRANSIENT));
        assertThat(request.getNameIDPolicy().getSPNameQualifier(), nullValue());
        assertThat(request.getNameIDPolicy().getAllowCreate(), equalTo(Boolean.TRUE));

        assertThat(request.isForceAuthn(), equalTo(Boolean.TRUE));
        assertThat(request.getRequestedAuthnContext(), equalTo(null));
    }

    public void testBuildRequestWithRequestedAuthnContext() throws Exception {
        SpConfiguration sp = new SpConfiguration(SP_ENTITY_ID, ACS_URL, null, null, null,
            Collections.singletonList(KERBEROS_AUTHN_CTX));
        final SamlAuthnRequestBuilder builder = new SamlAuthnRequestBuilder(
            sp, SAMLConstants.SAML2_POST_BINDING_URI,
            idpDescriptor, SAMLConstants.SAML2_REDIRECT_BINDING_URI,
            Clock.systemUTC());
        builder.nameIDPolicy(new SamlAuthnRequestBuilder.NameIDPolicySettings(NameID.PERSISTENT, false, SP_ENTITY_ID));
        builder.forceAuthn(null);

        final AuthnRequest request = buildAndValidateAuthnRequest(builder);

        assertThat(request.getIssuer().getValue(), equalTo(SP_ENTITY_ID));
        assertThat(request.getProtocolBinding(), equalTo(SAMLConstants.SAML2_POST_BINDING_URI));

        assertThat(request.getAssertionConsumerServiceURL(), equalTo(ACS_URL));

        assertThat(request.getNameIDPolicy(), notNullValue());
        assertThat(request.getNameIDPolicy().getFormat(), equalTo(NameID.PERSISTENT));
        assertThat(request.getNameIDPolicy().getSPNameQualifier(), equalTo(SP_ENTITY_ID));
        assertThat(request.getNameIDPolicy().getAllowCreate(), equalTo(Boolean.FALSE));

        assertThat(request.isForceAuthn(), equalTo(Boolean.FALSE));
        assertThat(request.getRequestedAuthnContext().getAuthnContextClassRefs().size(), equalTo(1));
        assertThat(request.getRequestedAuthnContext().getAuthnContextClassRefs().get(0).getAuthnContextClassRef(),
            equalTo(KERBEROS_AUTHN_CTX));
    }

    public void testBuildRequestWithRequestedAuthnContexts() throws Exception {
        List<String> reqAuthnCtxClassRef = Arrays.asList(KERBEROS_AUTHN_CTX,
            SMARTCARD_AUTHN_CTX,
            "http://an.arbitrary/mfa-profile");
        SpConfiguration sp = new SpConfiguration(SP_ENTITY_ID, ACS_URL, null, null, null, reqAuthnCtxClassRef);
        final SamlAuthnRequestBuilder builder = new SamlAuthnRequestBuilder(
            sp, SAMLConstants.SAML2_POST_BINDING_URI,
            idpDescriptor, SAMLConstants.SAML2_REDIRECT_BINDING_URI,
            Clock.systemUTC());
        builder.nameIDPolicy(new SamlAuthnRequestBuilder.NameIDPolicySettings(NameID.PERSISTENT, false, SP_ENTITY_ID));
        builder.forceAuthn(null);
        final AuthnRequest request = buildAndValidateAuthnRequest(builder);

        assertThat(request.getIssuer().getValue(), equalTo(SP_ENTITY_ID));
        assertThat(request.getProtocolBinding(), equalTo(SAMLConstants.SAML2_POST_BINDING_URI));

        assertThat(request.getAssertionConsumerServiceURL(), equalTo(ACS_URL));

        assertThat(request.getNameIDPolicy(), notNullValue());
        assertThat(request.getNameIDPolicy().getFormat(), equalTo(NameID.PERSISTENT));
        assertThat(request.getNameIDPolicy().getSPNameQualifier(), equalTo(SP_ENTITY_ID));
        assertThat(request.getNameIDPolicy().getAllowCreate(), equalTo(Boolean.FALSE));

        assertThat(request.isForceAuthn(), equalTo(Boolean.FALSE));
        assertThat(request.getRequestedAuthnContext().getAuthnContextClassRefs().size(), equalTo(3));
        assertThat(request.getRequestedAuthnContext().getAuthnContextClassRefs().get(0).getAuthnContextClassRef(),
            equalTo(KERBEROS_AUTHN_CTX));
        assertThat(request.getRequestedAuthnContext().getAuthnContextClassRefs().get(1).getAuthnContextClassRef(),
            equalTo(SMARTCARD_AUTHN_CTX));
        assertThat(request.getRequestedAuthnContext().getAuthnContextClassRefs().get(2).getAuthnContextClassRef(),
            equalTo("http://an.arbitrary/mfa-profile"));
    }

    private AuthnRequest buildAndValidateAuthnRequest(SamlAuthnRequestBuilder builder) {
        Instant before = Instant.now();
        final AuthnRequest request = builder.build();
        Instant after = Instant.now();
        assertThat(request, notNullValue());

        assertThat(request.getID(), notNullValue());
        assertThat(request.getID().length(), greaterThan(20));

        assertThat(request.getIssuer(), notNullValue());

        assertThat(request.getIssueInstant(), notNullValue());
        assertThat(request.getIssueInstant().isBefore(before), equalTo(false));
        assertThat(request.getIssueInstant().isAfter(after), equalTo(false));
        assertThat(request.getDestination(), equalTo(IDP_URL));
        return request;
    }

}
