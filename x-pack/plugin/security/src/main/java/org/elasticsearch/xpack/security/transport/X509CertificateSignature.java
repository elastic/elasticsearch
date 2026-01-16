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
import org.elasticsearch.common.util.concurrent.ThreadContext;
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
import java.util.Arrays;
import java.util.Base64;
import java.util.Objects;
import java.util.stream.Collectors;

public final class X509CertificateSignature implements Writeable {

    private static final Logger logger = LogManager.getLogger(X509CertificateSignature.class);
    public static final String CROSS_CLUSTER_ACCESS_SIGNATURE_HEADER_KEY = "_cross_cluster_access_signature";

    private final X509Certificate[] certificateChain;
    private final String algorithm;
    private final BytesReference signature;

    public X509CertificateSignature(X509Certificate[] certificateChain, String algorithm, BytesReference signature) {
        this.certificateChain = Objects.requireNonNull(certificateChain);
        this.algorithm = Objects.requireNonNull(algorithm);
        this.signature = Objects.requireNonNull(signature);
    }

    public X509CertificateSignature(StreamInput in) throws IOException {
        this.certificateChain = in.readArray((arrayIn) -> {
            final byte[] certBytes = arrayIn.readByteArray();
            if (certBytes == null || certBytes.length == 0) {
                throw new IOException("Certificate bytes cannot be empty");
            }
            try (var bais = new ByteArrayInputStream(certBytes)) {
                CertificateFactory certFactory = CertificateFactory.getInstance("X.509");
                final Certificate cert = certFactory.generateCertificate(bais);
                if (cert instanceof X509Certificate x509) {
                    return x509;
                } else {
                    throw new IOException("Input bytes are not an X509 certificate [" + cert.getClass() + "] [" + cert + "]");
                }
            } catch (CertificateException e) {
                throw new IOException("Cannot read certificate", e);
            }
        }, X509Certificate[]::new);

        this.algorithm = in.readString();
        this.signature = in.readBytesReference();
    }

    public X509Certificate[] certificates() {
        return certificateChain;
    }

    public X509Certificate leafCertificate() {
        assert certificateChain.length > 0;
        return certificateChain[0];
    }

    public X509Certificate topCertificate() {
        return certificateChain[certificateChain.length - 1];
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
        return Arrays.equals(this.certificateChain, that.certificateChain)
            && Objects.equals(this.algorithm, that.algorithm)
            && Objects.equals(this.signature, that.signature);
    }

    @Override
    public int hashCode() {
        return Objects.hash(Arrays.hashCode(certificateChain), algorithm, signature);
    }

    @Override
    public String toString() {
        return "X509CertificateSignature["
            + "certificates="
            + Arrays.stream(certificateChain).map(X509CertificateSignature::certificateToString).collect(Collectors.joining(","))
            + ", "
            + "algorithm="
            + algorithm
            + ", "
            + "signature="
            + signature.toBytesRef()
            + ']';
    }

    public static String certificateToString(X509Certificate certificate) {
        return "(" + certificate.getSubjectX500Principal() + ";" + certificate.getType() + ";" + fingerprint(certificate) + ")";
    }

    private static String fingerprint(X509Certificate certificate) {
        try {
            return "SHA1:" + SslUtil.calculateFingerprint(certificate, "SHA-1");
        } catch (CertificateEncodingException e) {
            return "<bad-encoding:" + e.getMessage() + ">";
        }
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        out.writeArray((arrayOut, certificate) -> {
            try {
                final byte[] encoded = certificate.getEncoded();
                arrayOut.writeByteArray(encoded);
            } catch (CertificateEncodingException e) {
                throw new IOException("Cannot convert certificate for " + certificate.getSubjectX500Principal() + " to bytes", e);
            }
        }, certificateChain);

        out.writeString(algorithm);
        out.writeBytesReference(signature);
    }

    public String encodeToString() throws IOException {
        final String encoded = encode(this);
        logger.trace("Encoding {} as [{}]", this, encoded);
        return encoded;
    }

    public static void writeToContext(ThreadContext ctx, X509CertificateSignature signature) throws IOException {
        ctx.putHeader(CROSS_CLUSTER_ACCESS_SIGNATURE_HEADER_KEY, signature.encodeToString());
    }

    public static X509CertificateSignature readFromContext(ThreadContext ctx) throws IOException {
        var encodedSignature = ctx.getHeader(CROSS_CLUSTER_ACCESS_SIGNATURE_HEADER_KEY);
        return encodedSignature != null ? X509CertificateSignature.decode(encodedSignature) : null;
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
