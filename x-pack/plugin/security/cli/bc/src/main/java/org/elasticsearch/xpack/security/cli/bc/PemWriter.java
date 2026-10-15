/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */
package org.elasticsearch.xpack.security.cli.bc;

import org.bouncycastle.jce.provider.BouncyCastleProvider;
import org.bouncycastle.openssl.PEMEncryptor;
import org.bouncycastle.openssl.jcajce.JcaPEMWriter;
import org.bouncycastle.openssl.jcajce.JcePEMEncryptorBuilder;

import java.io.Closeable;
import java.io.Flushable;
import java.io.IOException;
import java.io.Writer;
import java.security.PrivateKey;
import java.security.cert.X509Certificate;
import java.util.Objects;

/**
 * Writes certificates, private keys and certificate signing requests in PEM format. This is the only way security-cli produces PEM
 * output, and it keeps the Bouncy Castle PEM API (and the non-FIPS provider used for legacy OpenSSL key encryption) inside this package.
 */
public final class PemWriter implements Closeable, Flushable {

    private static final BouncyCastleProvider BC_PROV = new BouncyCastleProvider();

    private final JcaPEMWriter pemWriter;

    public PemWriter(Writer writer) {
        this.pemWriter = new JcaPEMWriter(writer);
    }

    public void writeCertificate(X509Certificate certificate) throws IOException {
        pemWriter.writeObject(certificate);
    }

    /**
     * Writes an unencrypted private key.
     */
    public void writePrivateKey(PrivateKey privateKey) throws IOException {
        pemWriter.writeObject(privateKey);
    }

    /**
     * Writes a private key encrypted with the given password, using OpenSSL's legacy (RFC 1421 {@code DEK-Info}) PEM encryption.
     * The password is used as given, even if it is empty; callers decide whether an absent password means "unencrypted".
     */
    public void writeEncryptedPrivateKey(PrivateKey privateKey, char[] password, PemEncryption encryption) throws IOException {
        Objects.requireNonNull(password, "password must not be null");
        final PEMEncryptor encryptor = new JcePEMEncryptorBuilder(encryption.algorithm()).setProvider(BC_PROV).build(password);
        pemWriter.writeObject(privateKey, encryptor);
    }

    public void writeCertificateSigningRequest(CertificateSigningRequest csr) throws IOException {
        pemWriter.writeObject(csr.pkcs10());
    }

    @Override
    public void flush() throws IOException {
        pemWriter.flush();
    }

    @Override
    public void close() throws IOException {
        pemWriter.close();
    }
}
