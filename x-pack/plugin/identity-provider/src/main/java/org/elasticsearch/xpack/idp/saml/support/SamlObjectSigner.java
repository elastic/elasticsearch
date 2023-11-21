/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.idp.saml.support;

import org.elasticsearch.xpack.core.security.support.RestorableContextClassLoader;
import org.elasticsearch.xpack.idp.saml.idp.SamlIdentityProvider;
import org.opensaml.xmlsec.signature.SignableXMLObject;
import org.opensaml.xmlsec.signature.Signature;
import org.opensaml.xmlsec.signature.support.SignatureConstants;
import org.opensaml.xmlsec.signature.support.SignatureException;
import org.opensaml.xmlsec.signature.support.Signer;
import org.opensaml.xmlsec.signature.support.SignerProvider;
import org.w3c.dom.Element;

import java.security.AccessController;
import java.security.PrivilegedActionException;
import java.security.PrivilegedExceptionAction;

/**
 * Signs OpenSAML {@link SignableXMLObject} instances using {@link SamlIdentityProvider#getSigningCredential()}.
 */
public class SamlObjectSigner {

    private final SamlIdentityProvider idp;
    private final SamlFactory samlFactory;

    public SamlObjectSigner(SamlFactory samlFactory, SamlIdentityProvider idp) {
        SamlInit.initialize();
        this.idp = idp;
        this.samlFactory = samlFactory;
    }

    public Element sign(SignableXMLObject object) {
        final Signature signature = samlFactory.buildObject(Signature.class, Signature.DEFAULT_ELEMENT_NAME);
        signature.setSigningCredential(idp.getSigningCredential());
        signature.setSignatureAlgorithm(SignatureConstants.ALGO_ID_SIGNATURE_RSA_SHA256);
        signature.setCanonicalizationAlgorithm(SignatureConstants.ALGO_ID_C14N_EXCL_OMIT_COMMENTS);
        object.setSignature(signature);
        Element element = SamlFactory.toDomElement(object);
        try {
            AccessController.doPrivileged((PrivilegedExceptionAction<Void>) () -> {
                try (RestorableContextClassLoader ignore = new RestorableContextClassLoader(SignerProvider.class)) {
                    Signer.signObject(signature);
                } catch (SignatureException e) {
                    throw new SecurityException("failed to sign SAML object " + object, e);
                }
                return null;
            });
        } catch (PrivilegedActionException e) {
            throw new SecurityException("failed to sign SAML object " + object, e);
        }
        return element;
    }
}
