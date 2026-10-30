/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.security.cli.bc;

import org.bouncycastle.openssl.jcajce.JcaPEMWriter;

import java.io.Closeable;
import java.io.IOException;
import java.io.Writer;
import java.security.PrivateKey;
import java.security.cert.Certificate;

/**
 * Writes certificates, private keys and certificate signing requests in PEM format.
 * <p>
 * This wraps a Bouncy Castle {@code JcaPEMWriter} so that callers outside this module do not depend on Bouncy Castle types,
 * which are shaded into this module's jar.
 * </p>
 */
public final class PemWriter implements Closeable {

    private final JcaPEMWriter pemWriter;

    public PemWriter(Writer out) {
        this.pemWriter = new JcaPEMWriter(out);
    }

    public void writeObject(Certificate certificate) throws IOException {
        pemWriter.writeObject(certificate);
    }

    public void writeObject(PrivateKey privateKey) throws IOException {
        pemWriter.writeObject(privateKey);
    }

    /**
     * Writes the private key encrypted with the given encryptor.
     */
    public void writeObject(PrivateKey privateKey, PemEncryptor encryptor) throws IOException {
        pemWriter.writeObject(privateKey, encryptor.encryptor());
    }

    public void writeObject(CertificateSigningRequest csr) throws IOException {
        pemWriter.writeObject(csr.request());
    }

    public void flush() throws IOException {
        pemWriter.flush();
    }

    @Override
    public void close() throws IOException {
        pemWriter.close();
    }
}
