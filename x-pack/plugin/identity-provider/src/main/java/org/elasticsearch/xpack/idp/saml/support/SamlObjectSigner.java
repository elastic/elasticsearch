/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */

package org.elasticsearch.xpack.idp.saml.support;

import org.elasticsearch.ElasticsearchException;
import org.elasticsearch.xpack.idp.saml.sp.SamlServiceProvider;
import org.opensaml.xmlsec.signature.SignableXMLObject;
import org.opensaml.xmlsec.signature.Signature;
import org.opensaml.xmlsec.signature.support.SignatureConstants;
import org.opensaml.xmlsec.signature.support.SignatureException;
import org.opensaml.xmlsec.signature.support.Signer;
import org.w3c.dom.Element;

/**
 * Signs OpenSAML {@link SignableXMLObject} instances using {@link SamlServiceProvider#getIdpSigningCredential()}.
 */
public class SamlObjectSigner {

    private final SamlServiceProvider sp;
    private final SamlFactory samlFactory;

    public SamlObjectSigner(SamlFactory samlFactory, SamlServiceProvider sp) {
        this.samlFactory = samlFactory;
        this.sp = sp;
        SamlInit.initialize();
    }

    public Element sign(SignableXMLObject object) {
        final Signature signature = samlFactory.buildObject(Signature.class, Signature.DEFAULT_ELEMENT_NAME);
        signature.setSigningCredential(sp.getIdpSigningCredential());
        signature.setSignatureAlgorithm(SignatureConstants.ALGO_ID_SIGNATURE_RSA_SHA256);
        signature.setCanonicalizationAlgorithm(SignatureConstants.ALGO_ID_C14N_EXCL_OMIT_COMMENTS);
        object.setSignature(signature);
        Element element = samlFactory.toDomElement(object);
        try {
            Signer.signObject(signature);
        } catch (SignatureException e) {
            throw new ElasticsearchException("failed to sign SAML object " + object, e);
        }
        return element;
    }
}
