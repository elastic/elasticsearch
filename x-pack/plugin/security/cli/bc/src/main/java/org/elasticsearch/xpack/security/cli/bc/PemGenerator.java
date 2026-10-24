/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.security.cli.bc;

import org.bouncycastle.openssl.jcajce.JcaMiscPEMGenerator;
import org.bouncycastle.util.io.pem.PemObjectGenerator;

import java.io.IOException;
import java.security.PrivateKey;
import java.security.cert.Certificate;

/**
 * A certificate, private key or certificate signing request that is to be written in PEM format by
 * {@link PemWriter#writeObject(PemGenerator)}. The PEM encoding is generated when the object is written.
 * <p>
 * This is an opaque handle for a Bouncy Castle {@code JcaMiscPEMGenerator} so that callers outside this module do not depend on
 * Bouncy Castle types, which are shaded into this module's jar.
 * </p>
 */
public final class PemGenerator {

    private final PemObjectGenerator generator;

    public PemGenerator(Certificate certificate) throws IOException {
        this.generator = new JcaMiscPEMGenerator(certificate);
    }

    public PemGenerator(PrivateKey privateKey) throws IOException {
        this.generator = new JcaMiscPEMGenerator(privateKey);
    }

    public PemGenerator(PrivateKey privateKey, PemEncryptor encryptor) throws IOException {
        this.generator = new JcaMiscPEMGenerator(privateKey, encryptor.encryptor());
    }

    public PemGenerator(CertificateSigningRequest csr) throws IOException {
        this.generator = new JcaMiscPEMGenerator(csr.request());
    }

    PemObjectGenerator generator() {
        return generator;
    }
}
