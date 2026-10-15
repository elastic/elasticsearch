/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */
package org.elasticsearch.xpack.security.cli.bc;

import org.bouncycastle.asn1.x509.KeyPurposeId;

/**
 * The X.509 <em>Extended Key Usage</em> purposes that security-cli can place in a certificate, expressed without Bouncy Castle types.
 */
public enum KeyPurpose {
    /** TLS WWW server authentication (RFC 5280 {@code id-kp-serverAuth}). */
    SERVER_AUTH(KeyPurposeId.id_kp_serverAuth),
    /** Any extended key usage (RFC 5280 {@code anyExtendedKeyUsage}). */
    ANY(KeyPurposeId.anyExtendedKeyUsage);

    private final KeyPurposeId keyPurposeId;

    KeyPurpose(KeyPurposeId keyPurposeId) {
        this.keyPurposeId = keyPurposeId;
    }

    KeyPurposeId keyPurposeId() {
        return keyPurposeId;
    }

    /**
     * @return the dotted-decimal OID of this purpose, as reported by {@link java.security.cert.X509Certificate#getExtendedKeyUsage()}
     */
    public String oid() {
        return keyPurposeId.getId();
    }
}
