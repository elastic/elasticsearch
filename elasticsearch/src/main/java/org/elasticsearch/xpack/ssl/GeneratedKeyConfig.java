/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.xpack.ssl;

import org.bouncycastle.asn1.x509.GeneralNames;
import org.bouncycastle.operator.OperatorCreationException;
import org.elasticsearch.common.Nullable;
import org.elasticsearch.common.hash.MessageDigests;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.env.Environment;
import org.elasticsearch.node.Node;

import javax.net.ssl.X509ExtendedKeyManager;
import javax.net.ssl.X509ExtendedTrustManager;
import javax.security.auth.DestroyFailedException;
import javax.security.auth.x500.X500Principal;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.io.Reader;
import java.io.UncheckedIOException;
import java.net.InetAddress;
import java.net.NetworkInterface;
import java.net.SocketException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Path;
import java.security.KeyPair;
import java.security.KeyStoreException;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.security.PrivateKey;
import java.security.UnrecoverableKeyException;
import java.security.cert.Certificate;
import java.security.cert.CertificateException;
import java.security.cert.CertificateFactory;
import java.security.cert.X509Certificate;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.Enumeration;
import java.util.HashSet;
import java.util.List;
import java.util.Objects;
import java.util.Set;

/**
 * Represents a {@link KeyConfig} that is automatically generated on node startup if necessary. This helps with the default user experience
 * so that the user does not need to have any knowledge about SSL setup to start a node
 */
final class GeneratedKeyConfig extends KeyConfig {

    // these values have been generated using openssl
    // For private key: openssl pkcs8 -in private.pem -inform PEM -nocrypt -topk8 -outform DER | openssl dgst -sha256 -hex
    // For certificate: openssl x509 -in ca.pem -noout -fingerprint -sha256
    private static final String PRIVATE_KEY_SHA256 = "eec5bdb422c17c75d3850ffc64a724e52a99ec64366677da2fe4e782d7426e9f";
    private static final String CA_CERT_FINGERPRINT_SHA256 = "A147166C71EB8B61DADFC5B19ECAC8443BE2DB32A56FC1A73BC1623250738598";

    private final X509ExtendedKeyManager keyManager;
    private final X509ExtendedTrustManager trustManager;

    GeneratedKeyConfig(Settings settings) throws NoSuchAlgorithmException, IOException, CertificateException, OperatorCreationException,
            UnrecoverableKeyException, KeyStoreException {
        final KeyPair keyPair = CertUtils.generateKeyPair(2048);
        final X500Principal principal = new X500Principal("CN=" + Node.NODE_NAME_SETTING.get(settings));
        final Certificate caCert = readCACert();
        final PrivateKey privateKey = readPrivateKey();
        final GeneralNames generalNames = CertUtils.getSubjectAlternativeNames(false, getLocalAddresses());
        X509Certificate certificate =
                CertUtils.generateSignedCertificate(principal, generalNames, keyPair, (X509Certificate) caCert, privateKey, 365);
        try {
            privateKey.destroy();
        } catch (DestroyFailedException e) {
            // best effort attempt. This is known to fail for RSA keys on the oracle JDK but maybe they'll fix it in ten years or so...
        }
        keyManager = CertUtils.keyManager(new Certificate[] { certificate, caCert }, keyPair.getPrivate(), new char[0]);
        trustManager = CertUtils.trustManager(new Certificate[] { caCert });
    }

    @Override
    X509ExtendedTrustManager createTrustManager(@Nullable Environment environment) {
        return trustManager;
    }

    @Override
    List<Path> filesToMonitor(@Nullable Environment environment) {
        // no files to watch
        return Collections.emptyList();
    }

    @Override
    public String toString() {
        return "Generated Key Config. DO NOT USE IN PRODUCTION";
    }

    @Override
    public boolean equals(Object o) {
        return this == o;
    }

    @Override
    public int hashCode() {
        return Objects.hash(keyManager, trustManager);
    }

    @Override
    X509ExtendedKeyManager createKeyManager(@Nullable Environment environment) {
        return keyManager;
    }

    @Override
    List<PrivateKey> privateKeys(@Nullable Environment environment) {
        try {
            return Collections.singletonList(readPrivateKey());
        } catch (IOException e) {
            throw new UncheckedIOException("failed to read key", e);
        }
    }

    /**
     * Enumerates all of the loopback and link local addresses so these can be used as SubjectAlternativeNames inside the certificate for
     * a good out of the box experience with TLS
     */
    private Set<InetAddress> getLocalAddresses() throws SocketException {
        Enumeration<NetworkInterface> networkInterfaces = NetworkInterface.getNetworkInterfaces();
        Set<InetAddress> inetAddresses = new HashSet<>();
        while (networkInterfaces.hasMoreElements()) {
            NetworkInterface intf = networkInterfaces.nextElement();
            if (intf.isUp()) {
                if (intf.isLoopback()) {
                    inetAddresses.addAll(Collections.list(intf.getInetAddresses()));
                } else {
                    Enumeration<InetAddress> inetAddressEnumeration = intf.getInetAddresses();
                    while (inetAddressEnumeration.hasMoreElements()) {
                        InetAddress inetAddress = inetAddressEnumeration.nextElement();
                        if (inetAddress.isLoopbackAddress() || inetAddress.isLinkLocalAddress()) {
                            inetAddresses.add(inetAddress);
                        }
                    }
                }
            }
        }
        return inetAddresses;
    }

    /**
     * Reads the bundled CA private key. This key is used for signing a automatically generated certificate that allows development nodes
     * to talk to each other on the same machine.
     *
     * This private key is the same for every distribution and is only here for a nice out of the box experience. Once in production mode
     * this key should not be used!
     */
    static PrivateKey readPrivateKey() throws IOException {
        try (InputStream inputStream = GeneratedKeyConfig.class.getResourceAsStream("private.pem");
            Reader reader = new InputStreamReader(inputStream, StandardCharsets.UTF_8)) {
            PrivateKey privateKey = CertUtils.readPrivateKey(reader, () -> null);
            MessageDigest md = MessageDigests.sha256();
            final byte[] privateKeyBytes = privateKey.getEncoded();
            try {
                final byte[] digest = md.digest(privateKeyBytes);
                final byte[] expected = hexStringToByteArray(PRIVATE_KEY_SHA256);
                if (Arrays.equals(digest, expected) == false) {
                    throw new IllegalStateException("private key hash does not match the expected value!");
                }
            } finally {
                Arrays.fill(privateKeyBytes, (byte) 0);
            }
            return privateKey;
        }
    }

    /**
     * Reads the bundled CA certificate
     */
    static Certificate readCACert() throws IOException, CertificateException {
        try (InputStream inputStream = GeneratedKeyConfig.class.getResourceAsStream("ca.pem");
             Reader reader = new InputStreamReader(inputStream, StandardCharsets.UTF_8)) {
            CertificateFactory certificateFactory = CertificateFactory.getInstance("X.509");
            List<Certificate> certificateList = new ArrayList<>(1);
            CertUtils.readCertificates(reader, certificateList, certificateFactory);
            if (certificateList.size() != 1) {
                throw new IllegalStateException("expected [1] default CA certificate but found [" + certificateList.size() + "]");
            }
            Certificate certificate = certificateList.get(0);
            final byte[] encoded = MessageDigests.sha256().digest(certificate.getEncoded());
            final byte[] expected = hexStringToByteArray(CA_CERT_FINGERPRINT_SHA256);
            if (Arrays.equals(encoded, expected) == false) {
                throw new IllegalStateException("CA certificate fingerprint does not match!");
            }
            return certificateList.get(0);
        }
    }

    private static byte[] hexStringToByteArray(String hexString) {
        if (hexString.length() % 2 != 0) {
            throw new IllegalArgumentException("String must be an even length");
        }
        final int numBytes = hexString.length() / 2;
        final byte[] data = new byte[numBytes];

        for(int i = 0; i < numBytes; i++) {
            final int index = i * 2;
            final int index2 = index + 1;
            data[i] = (byte) ((Character.digit(hexString.charAt(index), 16) << 4) + Character.digit(hexString.charAt(index2), 16));
        }

        return data;
    }
}
