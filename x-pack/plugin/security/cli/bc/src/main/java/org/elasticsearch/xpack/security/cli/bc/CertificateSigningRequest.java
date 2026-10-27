/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */
package org.elasticsearch.xpack.security.cli.bc;

import org.bouncycastle.pkcs.PKCS10CertificationRequest;

import java.io.IOException;
import java.util.Objects;

/**
 * An opaque PKCS#10 certificate signing request. Callers obtain one from {@link CertGenUtils#generateCSR} and hand it to
 * {@link PemWriter#writeCertificateSigningRequest}; the underlying Bouncy Castle object never leaves this package.
 */
public final class CertificateSigningRequest {

    private final PKCS10CertificationRequest request;

    CertificateSigningRequest(PKCS10CertificationRequest request) {
        this.request = Objects.requireNonNull(request);
    }

    PKCS10CertificationRequest pkcs10() {
        return request;
    }

    /**
     * @return the DER encoding of the request
     */
    public byte[] getEncoded() throws IOException {
        return request.getEncoded();
    }
}
