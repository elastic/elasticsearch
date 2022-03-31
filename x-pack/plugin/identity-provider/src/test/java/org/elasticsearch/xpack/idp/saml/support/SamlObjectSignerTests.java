/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.idp.saml.support;

import org.elasticsearch.xpack.core.security.support.RestorableContextClassLoader;
import org.elasticsearch.xpack.idp.saml.idp.SamlIdentityProvider;
import org.elasticsearch.xpack.idp.saml.test.IdpSamlTestCase;
import org.hamcrest.Matchers;
import org.junit.Before;
import org.opensaml.saml.common.SignableSAMLObject;
import org.opensaml.saml.saml2.core.Issuer;
import org.opensaml.saml.saml2.core.LogoutRequest;
import org.opensaml.saml.saml2.core.NameID;
import org.opensaml.saml.saml2.core.NameIDType;
import org.opensaml.saml.saml2.core.Response;
import org.opensaml.saml.saml2.core.Status;
import org.opensaml.saml.saml2.core.StatusCode;
import org.opensaml.security.x509.X509Credential;
import org.opensaml.xmlsec.signature.support.SignatureException;
import org.opensaml.xmlsec.signature.support.SignatureValidator;
import org.w3c.dom.Element;
import org.w3c.dom.Node;
import org.w3c.dom.NodeList;

import java.time.Duration;
import java.time.Instant;
import java.util.ArrayList;
import java.util.List;

import static org.hamcrest.Matchers.greaterThanOrEqualTo;
import static org.hamcrest.Matchers.iterableWithSize;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

public class SamlObjectSignerTests extends IdpSamlTestCase {

    private SamlFactory samlFactory;

    @Before
    public void setupState() {
        SamlInit.initialize();
        samlFactory = new SamlFactory();
    }

    public void testSignLogoutRequest() throws Exception {
        final List<X509Credential> credentials = readCredentials();
        assertThat(credentials.size(), greaterThanOrEqualTo(2));
        final X509Credential credential = credentials.get(0);
        final X509Credential alternateCredential = credentials.get(1);

        final String entityId = "https://" + randomAlphaOfLengthBetween(3, 8) + ".example.com/" + randomAlphaOfLengthBetween(2, 12);
        final LogoutRequest request = createLogoutRequest(entityId);
        final Element signedElement = sign(request, entityId, credential);
        verifySignatureExists(signedElement);

        final LogoutRequest signedRequest = domElementToXmlObject(signedElement, LogoutRequest.class);
        try (RestorableContextClassLoader ignore = new RestorableContextClassLoader(SignatureValidator.class)) {
            // verify with correct credential
            SignatureValidator.validate(signedRequest.getSignature(), credential);
            // fail with incorrect credential
            expectThrows(SignatureException.class, () -> SignatureValidator.validate(signedRequest.getSignature(), alternateCredential));
        }
    }

    public void testSignAuthResponse() throws Exception {
        final List<X509Credential> credentials = readCredentials();
        assertThat(credentials.size(), greaterThanOrEqualTo(2));
        final X509Credential credential = credentials.get(0);
        final X509Credential alternateCredential = credentials.get(1);

        final String entityId = "uri://" + randomAlphaOfLengthBetween(3, 8) + ".example.com/" + randomAlphaOfLengthBetween(2, 12);
        final Response request = createAuthnResponse(entityId);
        final Element signedElement = sign(request, entityId, credential);
        verifySignatureExists(signedElement);

        final Response signedResponse = domElementToXmlObject(signedElement, Response.class);
        try (RestorableContextClassLoader ignore = new RestorableContextClassLoader(SignatureValidator.class)) {
            // verify with correct credential
            SignatureValidator.validate(signedResponse.getSignature(), credential);
            // fail with incorrect credential
            expectThrows(SignatureException.class, () -> SignatureValidator.validate(signedResponse.getSignature(), alternateCredential));
        }
    }

    private void verifySignatureExists(Element signedElement) {
        final NodeList signatures = signedElement.getElementsByTagNameNS("http://www.w3.org/2000/09/xmldsig#", "Signature");
        assertThat(signatures.getLength(), Matchers.equalTo(1));

        final Node sigNode = signatures.item(0);
        assertThat(sigNode, Matchers.instanceOf(Element.class));
        assertThat(sigNode.getLocalName(), Matchers.equalTo("Signature"));

        final List<Element> children = getChildren(sigNode, Element.class);
        assertThat(children, iterableWithSize(2));
        assertThat(children.get(0).getLocalName(), Matchers.equalTo("SignedInfo"));
        assertThat(children.get(1).getLocalName(), Matchers.equalTo("SignatureValue"));
    }

    private Element sign(SignableSAMLObject request, String entityId, X509Credential credential) {
        SamlIdentityProvider idp = buildIdP(entityId, credential);

        SamlObjectSigner signer = new SamlObjectSigner(samlFactory, idp);
        return signer.sign(request);
    }

    private SamlIdentityProvider buildIdP(String entityId, X509Credential credential) {
        SamlIdentityProvider idp = mock(SamlIdentityProvider.class);
        when(idp.getEntityId()).thenReturn(entityId);
        when(idp.getSigningCredential()).thenReturn(credential);
        when(idp.getEntityId()).thenReturn(entityId);
        return idp;
    }

    private LogoutRequest createLogoutRequest(String entityId) {
        final LogoutRequest request = samlFactory.buildObject(LogoutRequest.class, LogoutRequest.DEFAULT_ELEMENT_NAME);
        request.setNotOnOrAfter(Instant.now().plus(Duration.ofMinutes(15)));

        final NameID nameID = samlFactory.buildObject(NameID.class, NameID.DEFAULT_ELEMENT_NAME);
        nameID.setFormat(NameIDType.TRANSIENT);
        nameID.setValue(randomAlphaOfLengthBetween(24, 64));
        request.setNameID(nameID);

        final Issuer issuer = samlFactory.buildObject(Issuer.class, Issuer.DEFAULT_ELEMENT_NAME);
        issuer.setValue(entityId);
        request.setIssuer(issuer);
        return request;
    }

    private Response createAuthnResponse(String entityId) {
        final Response response = samlFactory.buildObject(Response.class, Response.DEFAULT_ELEMENT_NAME);
        final Instant now = Instant.now();
        response.setIssueInstant(now);
        response.setID(randomAlphaOfLength(24));

        final StatusCode code = samlFactory.buildObject(StatusCode.class, StatusCode.DEFAULT_ELEMENT_NAME);
        code.setValue(StatusCode.AUTHN_FAILED);

        Status status = samlFactory.buildObject(Status.class, Status.DEFAULT_ELEMENT_NAME);
        status.setStatusCode(code);
        response.setStatus(status);

        final Issuer issuer = samlFactory.buildObject(Issuer.class, Issuer.DEFAULT_ELEMENT_NAME);
        issuer.setValue(entityId);
        response.setIssuer(issuer);
        return response;
    }

    private <T extends Node> List<T> getChildren(Node node, Class<T> type) {
        final NodeList childNodes = node.getChildNodes();
        final List<T> result = new ArrayList<>(childNodes.getLength());
        for (int i = 0; i < childNodes.getLength(); i++) {
            final Node child = childNodes.item(i);
            if (type.isInstance(child)) {
                result.add(type.cast(child));
            }
        }
        return result;
    }

}
