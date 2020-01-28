/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */

package org.elasticsearch.xpack.idp.saml.support;

import org.elasticsearch.xpack.core.security.support.RestorableContextClassLoader;
import org.elasticsearch.xpack.idp.saml.idp.SamlIdentityProvider;
import org.elasticsearch.xpack.idp.saml.test.IdpSamlTestCase;
import org.hamcrest.Matchers;
import org.joda.time.DateTime;
import org.mockito.Mockito;
import org.opensaml.saml.saml2.core.Issuer;
import org.opensaml.saml.saml2.core.LogoutRequest;
import org.opensaml.saml.saml2.core.NameID;
import org.opensaml.saml.saml2.core.NameIDType;
import org.opensaml.security.x509.X509Credential;
import org.opensaml.xmlsec.signature.support.SignatureException;
import org.opensaml.xmlsec.signature.support.SignatureValidator;
import org.w3c.dom.Element;
import org.w3c.dom.Node;
import org.w3c.dom.NodeList;

import java.util.ArrayList;
import java.util.List;

import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

public class SamlObjectSignerTests extends IdpSamlTestCase {

    public void testSignLogoutRequest() throws Exception {
        final SamlFactory factory = new SamlFactory();
        final X509Credential credential;
        final X509Credential alternateCredential;
        if (randomBoolean()) {
            credential = readCredentials("RSA", 1024);
            alternateCredential = readCredentials("RSA", 2048);
        } else {
            credential = readCredentials("RSA", 2048);
            alternateCredential = readCredentials("RSA", 1024);
        }

        final Element signedElement = createSignedElement(factory, credential);
        verifySignatureExists(signedElement);

        final LogoutRequest signedRequest = domElementToXmlObject(signedElement, LogoutRequest.class);
        try (RestorableContextClassLoader ignore = new RestorableContextClassLoader(SignatureValidator.class)) {
            // verify with correct credential
            SignatureValidator.validate(signedRequest.getSignature(), credential);
            // fail with incorrect credential
            expectThrows(SignatureException.class,
                () -> SignatureValidator.validate(signedRequest.getSignature(), alternateCredential)
            );
        }
    }

    private void verifySignatureExists(Element signedElement) {
        final NodeList signatures = signedElement.getElementsByTagNameNS("http://www.w3.org/2000/09/xmldsig#", "Signature");
        assertThat(signatures.getLength(), Matchers.equalTo(1));

        final Node sigNode = signatures.item(0);
        assertThat(sigNode, Matchers.instanceOf(Element.class));
        assertThat(sigNode.getLocalName(), Matchers.equalTo("Signature"));

        final List<Element> children = getChildren(sigNode, Element.class);
        assertThat(children, Matchers.iterableWithSize(2));
        assertThat(children.get(0).getLocalName(), Matchers.equalTo("SignedInfo"));
        assertThat(children.get(1).getLocalName(), Matchers.equalTo("SignatureValue"));
    }

    private Element createSignedElement(SamlFactory factory, X509Credential credential) {
        final String entityId = "https://" + randomAlphaOfLengthBetween(3, 8) + ".example.com/" + randomAlphaOfLengthBetween(2, 12);

        final LogoutRequest request = factory.object(LogoutRequest.class, LogoutRequest.DEFAULT_ELEMENT_NAME);
        request.setNotOnOrAfter(DateTime.now().plusMinutes(15));

        final NameID nameID = factory.object(NameID.class, NameID.DEFAULT_ELEMENT_NAME);
        nameID.setFormat(NameIDType.TRANSIENT);
        nameID.setValue(randomAlphaOfLengthBetween(24, 64));
        request.setNameID(nameID);

        final Issuer issuer = factory.object(Issuer.class, Issuer.DEFAULT_ELEMENT_NAME);
        issuer.setValue(entityId);
        request.setIssuer(issuer);

        SamlIdentityProvider idp = mock(SamlIdentityProvider.class);
        when(idp.getEntityId()).thenReturn(entityId);
        Mockito.when(idp.getSigningCredential()).thenReturn(credential);
        when(idp.getEntityId()).thenReturn(entityId);

        SamlObjectSigner signer = new SamlObjectSigner(factory, idp);
        return signer.sign(request);
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
