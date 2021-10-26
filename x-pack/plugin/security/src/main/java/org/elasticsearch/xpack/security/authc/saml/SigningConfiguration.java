/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */
package org.elasticsearch.xpack.security.authc.saml;

import java.nio.charset.StandardCharsets;
import java.util.Set;

import org.opensaml.saml.common.SAMLObject;
import org.opensaml.security.SecurityException;
import org.opensaml.security.x509.X509Credential;
import org.opensaml.xmlsec.crypto.XMLSigningUtil;

/**
 * Encapsulates the rules and credentials for how and when Elasticsearch should sign outgoing SAML messages.
 */
public class SigningConfiguration {

    private final Set<String> messageTypes;
    private final X509Credential credential;

    SigningConfiguration(Set<String> messageTypes, X509Credential credential) {
        this.messageTypes = messageTypes;
        this.credential = credential;
    }

    boolean shouldSign(SAMLObject object) {
        return shouldSign(object.getElementQName().getLocalPart());
    }

    public boolean shouldSign(String elementName) {
        if (credential == null) {
            return false;
        }
        return messageTypes.contains(elementName) || messageTypes.contains("*");
    }

    byte[] sign(String text, String algo) throws SecurityException {
        return sign(text.getBytes(StandardCharsets.UTF_8), algo);
    }

    byte[] sign(byte[] content, String algo) throws SecurityException {
        return XMLSigningUtil.signWithURI(this.credential, algo, content);
    }

    public X509Credential getCredential() {
        return credential;
    }
}
