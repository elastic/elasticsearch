/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.xpack.idp.saml.authn;

import org.elasticsearch.ElasticsearchException;
import org.elasticsearch.ElasticsearchSecurityException;
import org.elasticsearch.common.Nullable;
import org.elasticsearch.xpack.idp.action.SamlValidateAuthnRequestResponse;
import org.elasticsearch.xpack.idp.saml.idp.SamlIdentityProvider;
import org.elasticsearch.xpack.idp.saml.sp.SamlServiceProvider;
import org.elasticsearch.xpack.idp.saml.support.SamlUtils;
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
import java.net.URLEncoder;
import java.nio.charset.StandardCharsets;
import java.time.Clock;
import java.util.Base64;
import java.util.HashMap;
import java.util.Map;
import java.util.zip.Deflater;
import java.util.zip.DeflaterOutputStream;

import static org.hamcrest.CoreMatchers.containsString;
import static org.hamcrest.CoreMatchers.equalTo;
import static org.joda.time.DateTime.now;
import static org.mockito.Mockito.when;
import static org.opensaml.saml.saml2.core.NameIDType.PERSISTENT;
import static org.opensaml.saml.saml2.core.NameIDType.TRANSIENT;

public class SamlAuthnRequestValidatorTests extends IdpSamlTestCase {

    private SamlAuthnRequestValidator validator;
    private SamlIdentityProvider idp;
    private final Clock clock = Clock.systemUTC();

    @Before
    public void setupValidator() throws Exception {
        SamlUtils.initialize();
        idp = Mockito.mock(SamlIdentityProvider.class);
        when(idp.getEntityId()).thenReturn("https://cloud.elastic.co/saml/idp");
        when(idp.getSingleSignOnEndpoint("redirect")).thenReturn("https://cloud.elastic.co/saml/init");
        Map<String, SamlServiceProvider> providers = new HashMap<>();
        final SamlServiceProvider sp1 = Mockito.mock(SamlServiceProvider.class);
        when(sp1.getEntityId()).thenReturn("https://sp1.kibana.org");
        when(sp1.getAssertionConsumerService()).thenReturn("https://sp1.kibana.org/saml/acs");
        when(sp1.getNameIDPolicyFormat()).thenReturn(TRANSIENT);
        when(sp1.getSigningCredential()).thenReturn(null);
        final SamlServiceProvider sp2 = Mockito.mock(SamlServiceProvider.class);
        when(sp2.getEntityId()).thenReturn("https://sp2.kibana.org");
        when(sp2.getAssertionConsumerService()).thenReturn("https://sp2.kibana.org/saml/acs");
        when(sp2.getNameIDPolicyFormat()).thenReturn(PERSISTENT);
        when(sp2.getSigningCredential()).thenReturn(readCredentials("RSA", 4096));
        providers.put("https://sp1.kibana.org", sp1);
        providers.put("https://sp2.kibana.org", sp2);
        when(idp.getRegisteredServiceProviders()).thenReturn(providers);
        validator = new SamlAuthnRequestValidator(idp);
    }

    public void testValidAuthnRequest() {
        final String relayState = randomAlphaOfLength(6);
        final AuthnRequest authnRequest = buildAuthnRequest("https://sp1.kibana.org", "https://sp1.kibana.org/saml/acs",
            idp.getSingleSignOnEndpoint("redirect"), TRANSIENT);
        SamlValidateAuthnRequestResponse response = validator.processQueryString(getQueryString(authnRequest, relayState));
        assertThat(response.isForceAuthn(), equalTo(false));
        assertThat(response.getSpEntityId(), equalTo("https://sp1.kibana.org"));
        assertThat(response.getAdditionalData().size(), equalTo(1));
        assertThat(response.getAdditionalData().get("nameid_format"), equalTo(TRANSIENT));
    }

    public void testValidSignedAuthnRequest() throws Exception {
        final String relayState = randomAlphaOfLength(6);
        final AuthnRequest authnRequest = buildAuthnRequest("https://sp2.kibana.org", "https://sp2.kibana.org/saml/acs",
            idp.getSingleSignOnEndpoint("redirect"), PERSISTENT);
        SamlValidateAuthnRequestResponse response = validator.processQueryString(getQueryString(authnRequest, relayState, true,
            readCredentials("RSA", 4096)));
        assertThat(response.isForceAuthn(), equalTo(false));
        assertThat(response.getSpEntityId(), equalTo("https://sp2.kibana.org"));
        assertThat(response.getAdditionalData().size(), equalTo(1));
        assertThat(response.getAdditionalData().get("nameid_format"), equalTo(PERSISTENT));
    }

    public void testValidSignedAuthnRequestWithoutSigningCredential() {
        final String relayState = randomAlphaOfLength(6);
        final AuthnRequest authnRequest = buildAuthnRequest("https://sp1.kibana.org", "https://sp1.kibana.org/saml/acs",
            idp.getSingleSignOnEndpoint("redirect"), TRANSIENT);
        ElasticsearchSecurityException e = expectThrows(ElasticsearchSecurityException.class,
            () -> validator.processQueryString(getQueryString(authnRequest, relayState, true, readCredentials("RSA", 4096))));
        assertThat(e.getMessage(), containsString("Service Provider hasn't registered signing credentials"));
    }

    public void testWrongDestination() {
        final String relayState = randomAlphaOfLength(6);
        final AuthnRequest authnRequest = buildAuthnRequest("https://sp1.kibana.org", "https://sp1.kibana.org/saml/acs",
            "wrong_destination", TRANSIENT);
        ElasticsearchSecurityException e = expectThrows(ElasticsearchSecurityException.class,
            () -> validator.processQueryString(getQueryString(authnRequest, relayState)));
        assertThat(e.getMessage(), containsString("but the SSO endpoint of this Identity Provider is"));
        assertThat(e.getMessage(), containsString("wrong_destination"));
    }

    public void testUnregisteredAcsForSp() {
        final String relayState = randomAlphaOfLength(6);
        final AuthnRequest authnRequest = buildAuthnRequest("https://sp1.kibana.org", "https://malicious.kibana.org/saml/acs",
            idp.getSingleSignOnEndpoint("redirect"), TRANSIENT);
        ElasticsearchSecurityException e = expectThrows(ElasticsearchSecurityException.class,
            () -> validator.processQueryString(getQueryString(authnRequest, relayState)));
        assertThat(e.getMessage(), containsString("The registered ACS URL for this Service Provider is"));
        assertThat(e.getMessage(), containsString("https://malicious.kibana.org/saml/acs"));
    }

    public void testUnregisteredSp() {
        final String relayState = randomAlphaOfLength(6);
        final AuthnRequest authnRequest = buildAuthnRequest("https://unknown.kibana.org", "https://unknown.kibana.org/saml/acs",
            idp.getSingleSignOnEndpoint("redirect"), TRANSIENT);
        ElasticsearchSecurityException e = expectThrows(ElasticsearchSecurityException.class,
            () -> validator.processQueryString(getQueryString(authnRequest, relayState)));
        assertThat(e.getMessage(), containsString("is not registered with this Identity Provider"));
        assertThat(e.getMessage(), containsString("https://unknown.kibana.org"));
    }

    public void testAuthnRequestWithoutAcsUrl() {
        final String relayState = randomAlphaOfLength(6);
        final AuthnRequest authnRequest = buildAuthnRequest("https://sp1.kibana.org", "https://sp1.kibana.org/saml/acs",
            idp.getSingleSignOnEndpoint("redirect"), TRANSIENT);
        // remove ACS
        authnRequest.setAssertionConsumerServiceURL(null);
        final boolean containsIndex = randomBoolean();
        if (containsIndex) {
            authnRequest.setAssertionConsumerServiceIndex(randomInt(10));
        }
        ElasticsearchSecurityException e = expectThrows(ElasticsearchSecurityException.class,
            () -> validator.processQueryString(getQueryString(authnRequest, relayState)));
        assertThat(e.getMessage(), containsString("SAML authentication does not contain an AssertionConsumerService URL"));
        if (containsIndex) {
            assertThat(e.getMessage(), containsString("It contains an Assertion Consumer Service Index "));
        }
    }

    public void testAuthnRequestWithoutIssuer() {
        final String relayState = randomAlphaOfLength(6);
        final AuthnRequest authnRequest = buildAuthnRequest("https://sp1.kibana.org", "https://sp1.kibana.org/saml/acs",
            idp.getSingleSignOnEndpoint("redirect"), TRANSIENT);
        // remove issuer
        authnRequest.setIssuer(null);
        ElasticsearchSecurityException e = expectThrows(ElasticsearchSecurityException.class,
            () -> validator.processQueryString(getQueryString(authnRequest, relayState)));
        assertThat(e.getMessage(), containsString("SAML authentication request has no issuer"));
    }

    public void testInvalidNameIDPolicy() {
        final String relayState = randomAlphaOfLength(6);
        final AuthnRequest authnRequest = buildAuthnRequest("https://sp1.kibana.org", "https://sp1.kibana.org/saml/acs",
            idp.getSingleSignOnEndpoint("redirect"), PERSISTENT);
        SamlValidateAuthnRequestResponse response = validator.processQueryString(getQueryString(authnRequest, relayState));
        assertThat(response.isForceAuthn(), equalTo(false));
        assertThat(response.getSpEntityId(), equalTo("https://sp1.kibana.org"));
        assertThat(response.getAdditionalData().size(), equalTo(2));
        assertThat(response.getAdditionalData().get("nameid_format"), equalTo(PERSISTENT));
        assertThat(response.getAdditionalData().get("error"), equalTo("invalid_nameid_policy"));
    }

    private AuthnRequest buildAuthnRequest(String entityId, String acs, String destination, String nameIdFormat) {
        final Issuer issuer = SamlUtils.buildObject(Issuer.class, Issuer.DEFAULT_ELEMENT_NAME);
        issuer.setValue(entityId);
        final NameIDPolicy nameIDPolicy = SamlUtils.buildObject(NameIDPolicy.class, NameIDPolicy.DEFAULT_ELEMENT_NAME);
        nameIDPolicy.setFormat(nameIdFormat);
        final AuthnRequest authnRequest = SamlUtils.buildObject(AuthnRequest.class, AuthnRequest.DEFAULT_ELEMENT_NAME);
        authnRequest.setID(SamlUtils.secureIdentifier());
        authnRequest.setIssuer(issuer);
        authnRequest.setIssueInstant(now());
        authnRequest.setAssertionConsumerServiceURL(acs);
        authnRequest.setDestination(destination);
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
            String messageStr = SamlUtils.toString(XMLObjectSupport.marshall(message), false);
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
