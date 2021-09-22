/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */
package org.elasticsearch.xpack.idp.saml.authn;

import org.elasticsearch.ElasticsearchException;
import org.elasticsearch.ElasticsearchSecurityException;
import org.elasticsearch.action.support.PlainActionFuture;
import org.elasticsearch.core.Nullable;
import org.elasticsearch.xpack.idp.action.SamlValidateAuthnRequestResponse;
import org.elasticsearch.xpack.idp.saml.idp.SamlIdentityProvider;
import org.elasticsearch.xpack.idp.saml.sp.SamlServiceProvider;
import org.elasticsearch.xpack.idp.saml.support.SamlFactory;
import org.elasticsearch.xpack.idp.saml.support.SamlInit;
import org.elasticsearch.xpack.idp.saml.test.IdpSamlTestCase;
import org.junit.Before;
import org.mockito.Mockito;
import org.opensaml.core.xml.util.XMLObjectSupport;
import org.opensaml.saml.common.SAMLObject;
import org.opensaml.saml.saml2.core.AuthnRequest;
import org.opensaml.saml.saml2.core.Issuer;
import org.opensaml.saml.saml2.core.NameIDPolicy;
import org.opensaml.security.SecurityException;
import org.opensaml.security.x509.X509Credential;
import org.opensaml.xmlsec.crypto.XMLSigningUtil;
import org.opensaml.xmlsec.signature.support.SignatureConstants;

import java.io.ByteArrayOutputStream;
import java.io.UnsupportedEncodingException;
import java.net.URL;
import java.net.URLEncoder;
import java.nio.charset.StandardCharsets;
import java.time.Instant;
import java.util.Base64;
import java.util.Set;
import java.util.zip.Deflater;
import java.util.zip.DeflaterOutputStream;

import static org.hamcrest.CoreMatchers.containsString;
import static org.hamcrest.CoreMatchers.equalTo;
import static org.mockito.Mockito.when;
import static org.opensaml.saml.common.xml.SAMLConstants.SAML2_REDIRECT_BINDING_URI;
import static org.opensaml.saml.saml2.core.NameIDType.PERSISTENT;
import static org.opensaml.saml.saml2.core.NameIDType.TRANSIENT;

public class SamlAuthnRequestValidatorTests extends IdpSamlTestCase {

    private SamlAuthnRequestValidator validator;
    private SamlIdentityProvider idp;
    private SamlFactory samlFactory = new SamlFactory();

    @Before
    public void setupValidator() throws Exception {
        SamlInit.initialize();
        idp = Mockito.mock(SamlIdentityProvider.class);
        when(idp.getEntityId()).thenReturn("https://cloud.elastic.co/saml/idp");
        when(idp.getSingleSignOnEndpoint(SAML2_REDIRECT_BINDING_URI)).thenReturn(new URL("https://cloud.elastic.co/saml/init"));
        final SamlServiceProvider sp1 = Mockito.mock(SamlServiceProvider.class);
        when(sp1.getEntityId()).thenReturn("https://sp1.kibana.org");
        when(sp1.getAssertionConsumerService()).thenReturn(new URL("https://sp1.kibana.org/saml/acs"));
        when(sp1.getAllowedNameIdFormat()).thenReturn(TRANSIENT);
        when(sp1.shouldSignAuthnRequests()).thenReturn(false);
        final SamlServiceProvider sp2 = Mockito.mock(SamlServiceProvider.class);
        when(sp2.getEntityId()).thenReturn("https://sp2.kibana.org");
        when(sp2.getAssertionConsumerService()).thenReturn(new URL("https://sp2.kibana.org/saml/acs"));
        when(sp2.getAllowedNameIdFormat()).thenReturn(PERSISTENT);
        when(sp2.getSpSigningCredentials()).thenReturn(Set.of(readCredentials("RSA", 4096)));
        when(sp2.shouldSignAuthnRequests()).thenReturn(true);
        mockRegisteredServiceProvider(idp, "https://sp1.kibana.org", sp1);
        mockRegisteredServiceProvider(idp, "https://sp2.kibana.org", sp2);
        validator = new SamlAuthnRequestValidator(samlFactory, idp);
    }

    public void testValidAuthnRequest() throws Exception {
        final String relayState = randomAlphaOfLength(6);
        final AuthnRequest authnRequest = buildAuthnRequest("https://sp1.kibana.org", new URL("https://sp1.kibana.org/saml/acs"),
            idp.getSingleSignOnEndpoint(SAML2_REDIRECT_BINDING_URI), TRANSIENT);
        PlainActionFuture<SamlValidateAuthnRequestResponse> future = new PlainActionFuture<>();
        validator.processQueryString(getQueryString(authnRequest, relayState), future);
        SamlValidateAuthnRequestResponse response = future.actionGet();
        assertThat(response.isForceAuthn(), equalTo(false));
        assertThat(response.getSpEntityId(), equalTo("https://sp1.kibana.org"));
        assertThat(response.getAuthnState().size(), equalTo(2));
        assertThat(response.getAuthnState().get("authn_request_id"), equalTo(authnRequest.getID()));
        assertThat(response.getAuthnState().get("nameid_format"), equalTo(TRANSIENT));
    }

    public void testValidSignedAuthnRequest() throws Exception {
        final String relayState = randomAlphaOfLength(6);
        final AuthnRequest authnRequest = buildAuthnRequest("https://sp2.kibana.org", new URL("https://sp2.kibana.org/saml/acs"),
            idp.getSingleSignOnEndpoint(SAML2_REDIRECT_BINDING_URI), PERSISTENT);
        PlainActionFuture<SamlValidateAuthnRequestResponse> future = new PlainActionFuture<>();
        validator.processQueryString(getQueryString(authnRequest, relayState, true,
            readCredentials("RSA", 4096)), future);
        SamlValidateAuthnRequestResponse response = future.actionGet();
        assertThat(response.isForceAuthn(), equalTo(false));
        assertThat(response.getSpEntityId(), equalTo("https://sp2.kibana.org"));
        assertThat(response.getAuthnState().size(), equalTo(2));
        assertThat(response.getAuthnState().get("authn_request_id"), equalTo(authnRequest.getID()));
        assertThat(response.getAuthnState().get("nameid_format"), equalTo(PERSISTENT));
    }

    public void testValidSignedAuthnRequestWithoutRelayState() throws Exception {
        final AuthnRequest authnRequest = buildAuthnRequest("https://sp2.kibana.org", new URL("https://sp2.kibana.org/saml/acs"),
            idp.getSingleSignOnEndpoint(SAML2_REDIRECT_BINDING_URI), PERSISTENT);
        PlainActionFuture<SamlValidateAuthnRequestResponse> future = new PlainActionFuture<>();
        validator.processQueryString(getQueryString(authnRequest, null, true,
            readCredentials("RSA", 4096)), future);
        SamlValidateAuthnRequestResponse response = future.actionGet();
        assertThat(response.isForceAuthn(), equalTo(false));
        assertThat(response.getSpEntityId(), equalTo("https://sp2.kibana.org"));
        assertThat(response.getAuthnState().size(), equalTo(2));
        assertThat(response.getAuthnState().get("authn_request_id"), equalTo(authnRequest.getID()));
        assertThat(response.getAuthnState().get("nameid_format"), equalTo(PERSISTENT));
    }

    public void testValidSignedAuthnRequestWhenServiceProviderShouldNotSign() throws Exception {
        final String relayState = randomAlphaOfLength(6);
        final AuthnRequest authnRequest = buildAuthnRequest("https://sp1.kibana.org", new URL("https://sp1.kibana.org/saml/acs"),
            idp.getSingleSignOnEndpoint(SAML2_REDIRECT_BINDING_URI), TRANSIENT);
        PlainActionFuture<SamlValidateAuthnRequestResponse> future = new PlainActionFuture<>();
        validator.processQueryString(getQueryString(authnRequest, relayState, true, readCredentials("RSA", 4096)), future);
        SamlValidateAuthnRequestResponse response = future.actionGet();
        assertThat(response.isForceAuthn(), equalTo(false));
        assertThat(response.getSpEntityId(), equalTo("https://sp1.kibana.org"));
        assertThat(response.getAuthnState().size(), equalTo(2));
        assertThat(response.getAuthnState().get("authn_request_id"), equalTo(authnRequest.getID()));
        assertThat(response.getAuthnState().get("nameid_format"), equalTo(TRANSIENT));
    }

    public void testValidUnSignedAuthnRequestWhenServiceProviderShouldSign() throws Exception {
        final String relayState = randomAlphaOfLength(6);
        final AuthnRequest authnRequest = buildAuthnRequest("https://sp2.kibana.org", new URL("https://sp2.kibana.org/saml/acs"),
            idp.getSingleSignOnEndpoint(SAML2_REDIRECT_BINDING_URI), TRANSIENT);
        PlainActionFuture<SamlValidateAuthnRequestResponse> future = new PlainActionFuture<>();
        validator.processQueryString(getQueryString(authnRequest, relayState), future);
        ElasticsearchSecurityException e = expectThrows(ElasticsearchSecurityException.class,
            future::actionGet);
        assertThat(e.getMessage(), containsString("must sign authentication requests but no signature was found"));
    }

    public void testSignedAuthnRequestWithWrongKey() throws Exception {
        final String relayState = randomAlphaOfLength(6);
        final AuthnRequest authnRequest = buildAuthnRequest("https://sp2.kibana.org", new URL("https://sp2.kibana.org/saml/acs"),
            idp.getSingleSignOnEndpoint(SAML2_REDIRECT_BINDING_URI), TRANSIENT);
        PlainActionFuture<SamlValidateAuthnRequestResponse> future = new PlainActionFuture<>();
        validator.processQueryString(getQueryString(authnRequest, relayState, true, readCredentials("RSA2", 4096)), future);
        ElasticsearchSecurityException e = expectThrows(ElasticsearchSecurityException.class,
            future::actionGet);
        assertThat(e.getMessage(), containsString("Unable to validate signature of authentication request"));
    }

    public void testSignedAuthnRequestWithWrongSizeKey() throws Exception {
        final String relayState = randomAlphaOfLength(6);
        final AuthnRequest authnRequest = buildAuthnRequest("https://sp2.kibana.org", new URL("https://sp2.kibana.org/saml/acs"),
            idp.getSingleSignOnEndpoint(SAML2_REDIRECT_BINDING_URI), TRANSIENT);
        PlainActionFuture<SamlValidateAuthnRequestResponse> future = new PlainActionFuture<>();
        validator.processQueryString(getQueryString(authnRequest, relayState, true, readCredentials("RSA", 2048)), future);
        ElasticsearchSecurityException e = expectThrows(ElasticsearchSecurityException.class,
            future::actionGet);
        assertThat(e.getMessage(), containsString("Unable to validate signature of authentication request"));
    }

    public void testWrongDestination() throws Exception {
        final String relayState = randomAlphaOfLength(6);
        final AuthnRequest authnRequest = buildAuthnRequest("https://sp1.kibana.org", new URL("https://sp1.kibana.org/saml/acs"),
            new URL("https://wrong.destination.org"), TRANSIENT);
        PlainActionFuture<SamlValidateAuthnRequestResponse> future = new PlainActionFuture<>();
        validator.processQueryString(getQueryString(authnRequest, relayState), future);
        ElasticsearchSecurityException e = expectThrows(ElasticsearchSecurityException.class,
            future::actionGet);
        assertThat(e.getMessage(), containsString("but the SSO endpoint of this Identity Provider is"));
        assertThat(e.getMessage(), containsString("wrong.destination.org"));
    }

    public void testUnregisteredAcsForSp() throws Exception {
        final String relayState = randomAlphaOfLength(6);
        final AuthnRequest authnRequest = buildAuthnRequest("https://sp1.kibana.org", new URL("https://malicious.kibana.org/saml/acs"),
            idp.getSingleSignOnEndpoint(SAML2_REDIRECT_BINDING_URI), TRANSIENT);
        PlainActionFuture<SamlValidateAuthnRequestResponse> future = new PlainActionFuture<>();
        validator.processQueryString(getQueryString(authnRequest, relayState), future);
        ElasticsearchSecurityException e = expectThrows(ElasticsearchSecurityException.class,
            future::actionGet);
        assertThat(e.getMessage(), containsString("The registered ACS URL for this Service Provider is"));
        assertThat(e.getMessage(), containsString("https://malicious.kibana.org/saml/acs"));
    }

    public void testUnregisteredSp()throws Exception {
        final String relayState = randomAlphaOfLength(6);
        final AuthnRequest authnRequest = buildAuthnRequest("https://unknown.kibana.org", new URL("https://unknown.kibana.org/saml/acs"),
            idp.getSingleSignOnEndpoint(SAML2_REDIRECT_BINDING_URI), TRANSIENT);
        mockRegisteredServiceProvider(idp, "https://unknown.kibana.org", null);
        PlainActionFuture<SamlValidateAuthnRequestResponse> future = new PlainActionFuture<>();
        validator.processQueryString(getQueryString(authnRequest, relayState), future);
        ElasticsearchSecurityException e = expectThrows(ElasticsearchSecurityException.class, future::actionGet);
        assertThat(e.getMessage(), containsString("is not known to this Identity Provider"));
        assertThat(e.getMessage(), containsString("https://unknown.kibana.org"));
    }

    public void testAuthnRequestWithoutAcsUrl() throws Exception{
        final String relayState = randomAlphaOfLength(6);
        final AuthnRequest authnRequest = buildAuthnRequest("https://sp1.kibana.org", new URL("https://sp1.kibana.org/saml/acs"),
            idp.getSingleSignOnEndpoint(SAML2_REDIRECT_BINDING_URI), TRANSIENT);
        // remove ACS
        authnRequest.setAssertionConsumerServiceURL(null);
        final boolean containsIndex = randomBoolean();
        if (containsIndex) {
            authnRequest.setAssertionConsumerServiceIndex(randomInt(10));
        }
        PlainActionFuture<SamlValidateAuthnRequestResponse> future = new PlainActionFuture<>();
        validator.processQueryString(getQueryString(authnRequest, relayState), future);
        ElasticsearchSecurityException e = expectThrows(ElasticsearchSecurityException.class,
            future::actionGet);
        assertThat(e.getMessage(), containsString("SAML authentication does not contain an AssertionConsumerService URL"));
        if (containsIndex) {
            assertThat(e.getMessage(), containsString("It contains an Assertion Consumer Service Index "));
        }
    }

    public void testAuthnRequestWithoutIssuer() throws Exception {
        final String relayState = randomAlphaOfLength(6);
        final AuthnRequest authnRequest = buildAuthnRequest("https://sp1.kibana.org", new URL("https://sp1.kibana.org/saml/acs"),
            idp.getSingleSignOnEndpoint(SAML2_REDIRECT_BINDING_URI), TRANSIENT);
        // remove issuer
        authnRequest.setIssuer(null);
        PlainActionFuture<SamlValidateAuthnRequestResponse> future = new PlainActionFuture<>();
        validator.processQueryString(getQueryString(authnRequest, relayState), future);
        ElasticsearchSecurityException e = expectThrows(ElasticsearchSecurityException.class,
            future::actionGet);
        assertThat(e.getMessage(), containsString("SAML authentication request has no issuer"));
    }

    public void testInvalidNameIDPolicy() throws Exception {
        final String relayState = randomAlphaOfLength(6);
        final AuthnRequest authnRequest = buildAuthnRequest("https://sp1.kibana.org", new URL("https://sp1.kibana.org/saml/acs"),
            idp.getSingleSignOnEndpoint(SAML2_REDIRECT_BINDING_URI), PERSISTENT);
        PlainActionFuture<SamlValidateAuthnRequestResponse> future = new PlainActionFuture<>();
        validator.processQueryString(getQueryString(authnRequest, relayState), future);
        ElasticsearchSecurityException e = expectThrows(ElasticsearchSecurityException.class,
            future::actionGet);
        assertThat(e.getMessage(), containsString("doesn't match the allowed NameID format"));
    }

    private AuthnRequest buildAuthnRequest(String entityId, URL acs, URL destination, String nameIdFormat) {
        final Issuer issuer = samlFactory.buildObject(Issuer.class, Issuer.DEFAULT_ELEMENT_NAME);
        issuer.setValue(entityId);
        final NameIDPolicy nameIDPolicy = samlFactory.buildObject(NameIDPolicy.class, NameIDPolicy.DEFAULT_ELEMENT_NAME);
        nameIDPolicy.setFormat(nameIdFormat);
        final AuthnRequest authnRequest = samlFactory.buildObject(AuthnRequest.class, AuthnRequest.DEFAULT_ELEMENT_NAME);
        authnRequest.setID(samlFactory.secureIdentifier());
        authnRequest.setIssuer(issuer);
        authnRequest.setIssueInstant(Instant.now());
        authnRequest.setAssertionConsumerServiceURL(acs.toString());
        authnRequest.setDestination(destination.toString());
        authnRequest.setNameIDPolicy(nameIDPolicy);
        return authnRequest;
    }

    private String getQueryString(AuthnRequest authnRequest, String relayState) {
        return getQueryString(authnRequest, relayState, false, null);
    }

    private String getQueryString(AuthnRequest authnRequest, String relayState, boolean sign, @Nullable X509Credential credential) {
        try {
            final String request = deflateAndBase64Encode(authnRequest);
            String queryParam = "SAMLRequest=" + urlEncode(request);
            if (relayState != null) {
                queryParam += "&RelayState=" + urlEncode(relayState);
            }
            if (sign) {
                final String algo = SignatureConstants.ALGO_ID_SIGNATURE_RSA_SHA256;
                queryParam += "&SigAlg=" + urlEncode(algo);
                final byte[] sig = sign(queryParam, algo, credential);
                queryParam += "&Signature=" + urlEncode(base64Encode(sig));
            }
            return queryParam;
        } catch (Exception e) {
            throw new ElasticsearchException("Cannot construct SAML redirect", e);
        }
    }

    private String base64Encode(byte[] bytes) {
        return Base64.getEncoder().encodeToString(bytes);
    }

    private String urlEncode(String param) throws UnsupportedEncodingException {
        return URLEncoder.encode(param, StandardCharsets.UTF_8.name());
    }

    private String deflateAndBase64Encode(SAMLObject message)
        throws Exception {
        Deflater deflater = new Deflater(Deflater.DEFLATED, true);
        try (ByteArrayOutputStream bytesOut = new ByteArrayOutputStream();
             DeflaterOutputStream deflaterStream = new DeflaterOutputStream(bytesOut, deflater)) {
            String messageStr = samlFactory.toString(XMLObjectSupport.marshall(message), false);
            deflaterStream.write(messageStr.getBytes(StandardCharsets.UTF_8));
            deflaterStream.finish();
            return base64Encode(bytesOut.toByteArray());
        }
    }

    private byte[] sign(String text, String algo, X509Credential credential) throws SecurityException {
        return sign(text.getBytes(StandardCharsets.UTF_8), algo, credential);
    }

    private byte[] sign(byte[] content, String algo, X509Credential credential) throws SecurityException {
        return XMLSigningUtil.signWithURI(credential, algo, content);
    }

}
