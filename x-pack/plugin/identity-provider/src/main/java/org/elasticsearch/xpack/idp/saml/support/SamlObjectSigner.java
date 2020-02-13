/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */

package org.elasticsearch.xpack.idp.saml.support;

import org.elasticsearch.ElasticsearchException;
import org.elasticsearch.xpack.idp.saml.idp.SamlIdentityProvider;
import org.opensaml.xmlsec.signature.SignableXMLObject;
import org.opensaml.xmlsec.signature.Signature;
import org.opensaml.xmlsec.signature.support.SignatureConstants;
import org.opensaml.xmlsec.signature.support.SignatureException;
import org.opensaml.xmlsec.signature.support.Signer;
import org.w3c.dom.Element;

/**
 * Signs OpenSAML {@link SignableXMLObject} instances using {@link SamlIdentityProvider#getSigningCredential()}.
 */
public class SamlObjectSigner {

    private final SamlIdentityProvider idp;

    public SamlObjectSigner(SamlIdentityProvider idp) {
        this.idp = idp;
        SamlUtils.initialize();
    }

    public Element sign(SignableXMLObject object) {
        final Signature signature = SamlUtils.buildObject(Signature.class, Signature.DEFAULT_ELEMENT_NAME);
        signature.setSigningCredential(idp.getSigningCredential());
        signature.setSignatureAlgorithm(SignatureConstants.ALGO_ID_SIGNATURE_RSA_SHA256);
        signature.setCanonicalizationAlgorithm(SignatureConstants.ALGO_ID_C14N_EXCL_OMIT_COMMENTS);
        object.setSignature(signature);
        Element element = SamlUtils.toDomElement(object);
        try {
            Signer.signObject(signature);
        } catch (SignatureException e) {
            throw new ElasticsearchException("failed to sign SAML object " + object, e);
        }
        return element;
    }
}
