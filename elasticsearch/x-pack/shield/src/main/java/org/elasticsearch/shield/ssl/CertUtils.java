/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.shield.ssl;

import org.bouncycastle.asn1.pkcs.PrivateKeyInfo;
import org.bouncycastle.asn1.x500.X500Name;
import org.bouncycastle.asn1.x509.Extension;
import org.bouncycastle.asn1.x509.GeneralName;
import org.bouncycastle.asn1.x509.GeneralNames;
import org.bouncycastle.asn1.x509.Time;
import org.bouncycastle.cert.X509CertificateHolder;
import org.bouncycastle.cert.jcajce.JcaX509CertificateConverter;
import org.bouncycastle.cert.jcajce.JcaX509ExtensionUtils;
import org.bouncycastle.cert.jcajce.JcaX509v3CertificateBuilder;
import org.bouncycastle.jce.provider.BouncyCastleProvider;
import org.bouncycastle.openssl.PEMEncryptedKeyPair;
import org.bouncycastle.openssl.PEMKeyPair;
import org.bouncycastle.openssl.PEMParser;
import org.bouncycastle.openssl.X509TrustedCertificateBlock;
import org.bouncycastle.openssl.jcajce.JcaPEMKeyConverter;
import org.bouncycastle.openssl.jcajce.JcePEMDecryptorProviderBuilder;
import org.bouncycastle.operator.ContentSigner;
import org.bouncycastle.operator.jcajce.JcaContentSignerBuilder;
import org.elasticsearch.common.Nullable;
import org.elasticsearch.common.Strings;
import org.elasticsearch.common.SuppressForbidden;
import org.elasticsearch.common.io.PathUtils;
import org.elasticsearch.common.network.InetAddressHelper;
import org.elasticsearch.common.network.NetworkAddress;
import org.elasticsearch.env.Environment;
import org.joda.time.DateTime;
import org.joda.time.DateTimeZone;

import javax.net.ssl.KeyManager;
import javax.net.ssl.KeyManagerFactory;
import javax.net.ssl.TrustManager;
import javax.net.ssl.TrustManagerFactory;
import javax.net.ssl.X509ExtendedKeyManager;
import javax.net.ssl.X509ExtendedTrustManager;
import java.io.ByteArrayInputStream;
import java.io.Reader;
import java.math.BigInteger;
import java.net.InetAddress;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.security.KeyPair;
import java.security.KeyPairGenerator;
import java.security.KeyStore;
import java.security.PrivateKey;
import java.security.SecureRandom;
import java.security.cert.Certificate;
import java.security.cert.CertificateFactory;
import java.security.cert.X509Certificate;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Locale;
import java.util.Set;

class CertUtils {

    private static final int SERIAL_BIT_LENGTH = 20 * 8;
    static final BouncyCastleProvider BC_PROV = new BouncyCastleProvider();

    private CertUtils() {}

    @SuppressForbidden(reason = "we don't have the environment to resolve files from when running in a transport client")
    static Path resolvePath(String path, @Nullable Environment environment) {
        if (environment != null) {
            return environment.configFile().resolve(path);
        }
        return PathUtils.get(Strings.cleanPath(path));
    }

    static X509ExtendedKeyManager[] keyManagers(Certificate[] certificateChain, PrivateKey privateKey, char[] password) throws Exception {
        KeyStore keyStore = KeyStore.getInstance("jks");
        keyStore.load(null, null);
        // password must be non-null for keystore...
        keyStore.setKeyEntry("key", privateKey, password, certificateChain);
        return keyManagers(keyStore, password, KeyManagerFactory.getDefaultAlgorithm());
    }

    static X509ExtendedKeyManager[] keyManagers(KeyStore keyStore, char[] password, String algorithm) throws Exception {
        KeyManagerFactory kmf = KeyManagerFactory.getInstance(algorithm);
        kmf.init(keyStore, password);
        KeyManager[] keyManagers = kmf.getKeyManagers();
        for (KeyManager keyManager : keyManagers) {
            if (keyManager instanceof X509ExtendedKeyManager) {
                return new X509ExtendedKeyManager[] { (X509ExtendedKeyManager) keyManager };
            }
        }
        throw new IllegalStateException("failed to find a X509ExtendedKeyManager");
    }

    static X509ExtendedTrustManager[] trustManagers(Certificate[] certificates) throws Exception {
        KeyStore store = KeyStore.getInstance("jks");
        store.load(null, null);
        int counter = 0;
        for (Certificate certificate : certificates) {
            store.setCertificateEntry("cert" + counter, certificate);
            counter++;
        }
        return trustManagers(store, TrustManagerFactory.getDefaultAlgorithm());
    }

    static X509ExtendedTrustManager[] trustManagers(KeyStore keyStore, String algorithm) throws Exception {
        TrustManagerFactory tmf = TrustManagerFactory.getInstance(algorithm);
        tmf.init(keyStore);
        TrustManager[] trustManagers = tmf.getTrustManagers();
        for (TrustManager trustManager : trustManagers) {
            if (trustManager instanceof X509ExtendedTrustManager) {
                return new X509ExtendedTrustManager[] { (X509ExtendedTrustManager) trustManager };
            }
        }
        throw new IllegalStateException("failed to find a X509ExtendedTrustManager");
    }

    static Certificate[] readCertificates(List<String> certPaths, Environment environment) throws Exception {
        List<Certificate> certificates = new ArrayList<>(certPaths.size());
        CertificateFactory certFactory = CertificateFactory.getInstance("X.509");
        for (String path : certPaths) {
            try (Reader reader = Files.newBufferedReader(resolvePath(path, environment), StandardCharsets.UTF_8)) {
                readCertificates(reader, certificates, certFactory);
            }
        }
        return certificates.toArray(new Certificate[certificates.size()]);
    }

    static void readCertificates(Reader reader, List<Certificate> certificates, CertificateFactory certFactory) throws Exception {
        try (PEMParser pemParser = new PEMParser(reader)) {

            Object parsed = pemParser.readObject();
            if (parsed == null) {
                throw new IllegalArgumentException("could not parse pem certificate");
            }

            while (parsed != null) {
                X509CertificateHolder holder;
                if (parsed instanceof X509CertificateHolder) {
                    holder = (X509CertificateHolder) parsed;
                } else if (parsed instanceof X509TrustedCertificateBlock) {
                    X509TrustedCertificateBlock certificateBlock = (X509TrustedCertificateBlock) parsed;
                    holder = certificateBlock.getCertificateHolder();
                } else {
                    throw new IllegalArgumentException("parsed an unsupported object [" +
                            parsed.getClass().getSimpleName() + "]");
                }
                certificates.add(certFactory.generateCertificate(new ByteArrayInputStream(holder.getEncoded())));
                parsed = pemParser.readObject();
            }
        }
    }

    static PrivateKey readPrivateKey(Reader reader, char[] keyPassword) throws Exception {
        try (PEMParser parser = new PEMParser(reader)) {
            Object parsed;
            List<Object> list = new ArrayList<>(1);
            do {
                parsed = parser.readObject();
                if (parsed != null) {
                    list.add(parsed);
                }
            } while (parsed != null);

            if (list.size() != 1) {
                throw new IllegalStateException("key file contained [" + list.size() + "] entries, expected one");
            }

            PrivateKeyInfo privateKeyInfo;
            Object parsedObject = list.get(0);
            if (parsedObject instanceof PEMEncryptedKeyPair) {
                if (keyPassword == null) {
                    throw new IllegalArgumentException("cannot read encrypted key without a password");
                }
                // we have an encrypted key pair so we need to decrypt it
                PEMEncryptedKeyPair encryptedKeyPair = (PEMEncryptedKeyPair) parsedObject;
                privateKeyInfo = encryptedKeyPair
                        .decryptKeyPair(new JcePEMDecryptorProviderBuilder().setProvider(BC_PROV).build(keyPassword))
                        .getPrivateKeyInfo();
            } else if (parsedObject instanceof PEMKeyPair) {
                privateKeyInfo = ((PEMKeyPair) parsedObject).getPrivateKeyInfo();
            } else if (parsedObject instanceof PrivateKeyInfo) {
                privateKeyInfo = (PrivateKeyInfo) parsedObject;
            } else {
                throw new IllegalArgumentException("parsed an unsupported object [" +
                        parsedObject.getClass().getSimpleName() + "]");
            }

            JcaPEMKeyConverter converter = new JcaPEMKeyConverter();
            return converter.getPrivateKey(privateKeyInfo);
        }
    }

    static X509Certificate generateSignedCertificate(boolean resolveHostname, String nodeName, Set<InetAddress> addresses, KeyPair keyPair,
                                                     Certificate caCert, PrivateKey caPrivKey) throws Exception {
        final DateTime notBefore = new DateTime(DateTimeZone.UTC);
        final DateTime notAfter = notBefore.plusYears(1);
        final BigInteger serial = getSerial();

        X509Certificate x509CACert = (X509Certificate) caCert;
        X500Name subject = new X500Name("CN=" + nodeName);
        JcaX509v3CertificateBuilder builder =
                new JcaX509v3CertificateBuilder(X500Name.getInstance(x509CACert.getIssuerX500Principal().getEncoded()), serial,
                        new Time(notBefore.toDate(), Locale.ROOT), new Time(notAfter.toDate(), Locale.ROOT), subject, keyPair.getPublic());

        JcaX509ExtensionUtils extUtils = new JcaX509ExtensionUtils();
        builder.addExtension(Extension.subjectKeyIdentifier, false, extUtils.createSubjectKeyIdentifier(keyPair.getPublic()));
        builder.addExtension(Extension.authorityKeyIdentifier, false, extUtils.createAuthorityKeyIdentifier(x509CACert));
        if (addresses.isEmpty() == false) {
            builder.addExtension(Extension.subjectAlternativeName, false, getSubjectAlternativeNames(resolveHostname, addresses));
        }

        ContentSigner signer = new JcaContentSignerBuilder("SHA256withRSA").build(caPrivKey);
        X509CertificateHolder certificateHolder = builder.build(signer);
        return new JcaX509CertificateConverter().getCertificate(certificateHolder);
    }

    static BigInteger getSerial() {
        SecureRandom random = new SecureRandom();
        BigInteger serial = new BigInteger(SERIAL_BIT_LENGTH, random);
        assert serial.compareTo(BigInteger.valueOf(0L)) >= 0;
        return serial;
    }

    static KeyPair generateKeyPair() throws Exception {
        // generate a private key
        KeyPairGenerator keyPairGenerator = KeyPairGenerator.getInstance("RSA");
        keyPairGenerator.initialize(2048);
        return keyPairGenerator.generateKeyPair();
    }

    static GeneralNames getSubjectAlternativeNames(boolean resolveName, Set<InetAddress> addresses) throws Exception {
        Set<GeneralName> generalNameList = new HashSet<>();
        for (InetAddress address : addresses) {
            if (address.isAnyLocalAddress()) {
                // it is a wildcard address
                for (InetAddress inetAddress : InetAddressHelper.getAllAddresses()) {
                    addSubjectAlternativeNames(resolveName, inetAddress, generalNameList);
                }
            } else {
                addSubjectAlternativeNames(resolveName, address, generalNameList);
            }
        }
        return new GeneralNames(generalNameList.toArray(new GeneralName[generalNameList.size()]));
    }

    @SuppressForbidden(reason = "need to use getHostName to resolve DNS name and getHostAddress to ensure we resolved the name")
    static void addSubjectAlternativeNames(boolean resolveName, InetAddress inetAddress, Set<GeneralName> list) {
        String hostaddress = inetAddress.getHostAddress();
        String ip = NetworkAddress.format(inetAddress);
        list.add(new GeneralName(GeneralName.iPAddress, ip));
        if (resolveName && (inetAddress.isLinkLocalAddress() == false)) {
            String possibleHostName = inetAddress.getHostName();
            if (possibleHostName.equals(hostaddress) == false) {
                list.add(new GeneralName(GeneralName.dNSName, possibleHostName));
            }
        }
    }
}
