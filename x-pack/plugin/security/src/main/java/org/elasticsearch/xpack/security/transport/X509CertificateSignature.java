/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.security.transport;

import org.elasticsearch.TransportVersion;
import org.elasticsearch.common.bytes.BytesReference;
import org.elasticsearch.common.io.stream.BytesStreamOutput;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.common.io.stream.Writeable;
import org.elasticsearch.common.ssl.SslUtil;
import org.elasticsearch.core.CheckedConsumer;
import org.elasticsearch.core.CheckedFunction;
import org.elasticsearch.logging.LogManager;
import org.elasticsearch.logging.Logger;

import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.security.cert.Certificate;
import java.security.cert.CertificateEncodingException;
import java.security.cert.CertificateException;
import java.security.cert.CertificateFactory;
import java.security.cert.X509Certificate;
import java.util.Base64;
import java.util.Objects;

public final class X509CertificateSignature implements Writeable {

    private static final Logger logger = LogManager.getLogger(X509CertificateSignature.class);

    private final X509Certificate certificate;
    private final String algorithm;
    private final BytesReference signature;

    public X509CertificateSignature(X509Certificate certificate, String algorithm, BytesReference signature) {
        this.certificate = Objects.requireNonNull(certificate);
        this.algorithm = Objects.requireNonNull(algorithm);
        this.signature = Objects.requireNonNull(signature);
    }

    public X509CertificateSignature(StreamInput in) throws IOException {
        final byte[] certBytes = in.readByteArray();
        if (certBytes == null || certBytes.length == 0) {
            throw new IOException("Certificate bytes cannot be empty");
        }
        try (var bais = new ByteArrayInputStream(certBytes)) {
            CertificateFactory certFactory = CertificateFactory.getInstance("X.509");
            final Certificate cert = certFactory.generateCertificate(bais);
            if (cert instanceof X509Certificate x509) {
                this.certificate = x509;
            } else {
                throw new IOException("Input bytes are not an X509 certificate [" + cert.getClass() + "] [" + cert + "]");
            }
        } catch (CertificateException e) {
            throw new IOException("Cannot read certificate", e);
        }
        this.algorithm = in.readString();
        this.signature = in.readBytesReference();
    }

    public X509Certificate certificate() {
        return certificate;
    }

    public String algorithm() {
        return algorithm;
    }

    public BytesReference signature() {
        return signature;
    }

    @Override
    public boolean equals(Object obj) {
        if (obj == this) return true;
        if (obj == null || obj.getClass() != this.getClass()) return false;
        var that = (X509CertificateSignature) obj;
        return Objects.equals(this.certificate, that.certificate)
            && Objects.equals(this.algorithm, that.algorithm)
            && Objects.equals(this.signature, that.signature);
    }

    @Override
    public int hashCode() {
        return Objects.hash(certificate, algorithm, signature);
    }

    @Override
    public String toString() {
        return "X509CertificateSignature["
            + "certificate=("
            + certificate.getSubjectX500Principal()
            + ";"
            + certificate.getType()
            + ";"
            + fingerprint()
            + "), "
            + "algorithm="
            + algorithm
            + ", "
            + "signature="
            + signature
            + ']';
    }

    private String fingerprint() {
        try {
            return "SHA1:" + SslUtil.calculateFingerprint(this.certificate, "SHA-1");
        } catch (CertificateEncodingException e) {
            return "<bad-encoding:" + e.getMessage() + ">";
        }
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        try {
            final byte[] encoded = certificate.getEncoded();
            out.writeByteArray(encoded);
        } catch (CertificateEncodingException e) {
            throw new IOException("Cannot convert certificate for " + certificate.getSubjectX500Principal() + " to bytes", e);
        }
        out.writeString(algorithm);
        out.writeBytesReference(signature);
    }

    public String encodeToString() throws IOException {
        final String encoded = encode(this);
        logger.trace("Encoding {} as [{}]", this, encoded);
        return encoded;
    }

    public static X509CertificateSignature decode(String encoded) throws IOException {
        logger.trace("Decoding [{}]", encoded);
        try {
            return decode(encoded, X509CertificateSignature::new);
        } catch (IOException e) {
            logger.debug("Failed to decode signature", e);
            throw e;
        }
    }

    public static String encode(Writeable writeable) throws IOException {
        return encode(TransportVersion.current(), writeable::writeTo);
    }

    public static String encode(TransportVersion transportVersion, CheckedConsumer<StreamOutput, IOException> body) throws IOException {
        try (BytesStreamOutput out = new BytesStreamOutput()) {
            out.setTransportVersion(transportVersion);
            TransportVersion.writeVersion(transportVersion, out);
            body.accept(out);
            out.flush();
            return Base64.getEncoder().encodeToString(BytesReference.toBytes(out.bytes()));
        }
    }

    public static <T> T decode(String encoded, CheckedFunction<StreamInput, T, IOException> body) throws IOException {
        Objects.requireNonNull(encoded);
        final byte[] bytes = Base64.getDecoder().decode(encoded);
        final StreamInput in = StreamInput.wrap(bytes);
        final TransportVersion transportVersion = TransportVersion.readVersion(in);
        in.setTransportVersion(transportVersion);
        return body.apply(in);
    }
}
