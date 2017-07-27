/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.xpack.ssl;

import javax.net.ssl.KeyManager;
import javax.net.ssl.KeyManagerFactory;
import javax.net.ssl.TrustManager;
import javax.net.ssl.TrustManagerFactory;
import javax.net.ssl.X509ExtendedKeyManager;
import javax.net.ssl.X509ExtendedTrustManager;
import javax.security.auth.x500.X500Principal;
import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.Reader;
import java.math.BigInteger;
import java.net.InetAddress;
import java.net.SocketException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.security.KeyPair;
import java.security.KeyPairGenerator;
import java.security.KeyStore;
import java.security.KeyStoreException;
import java.security.NoSuchAlgorithmException;
import java.security.PrivateKey;
import java.security.SecureRandom;
import java.security.UnrecoverableKeyException;
import java.security.cert.Certificate;
import java.security.cert.CertificateException;
import java.security.cert.CertificateFactory;
import java.security.cert.X509Certificate;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Locale;
import java.util.Set;
import java.util.function.Supplier;

import org.bouncycastle.asn1.ASN1Encodable;
import org.bouncycastle.asn1.ASN1ObjectIdentifier;
import org.bouncycastle.asn1.DERIA5String;
import org.bouncycastle.asn1.DERSequence;
import org.bouncycastle.asn1.pkcs.PKCSObjectIdentifiers;
import org.bouncycastle.asn1.pkcs.PrivateKeyInfo;
import org.bouncycastle.asn1.x500.X500Name;
import org.bouncycastle.asn1.x509.AuthorityKeyIdentifier;
import org.bouncycastle.asn1.x509.BasicConstraints;
import org.bouncycastle.asn1.x509.Extension;
import org.bouncycastle.asn1.x509.ExtensionsGenerator;
import org.bouncycastle.asn1.x509.GeneralName;
import org.bouncycastle.asn1.x509.GeneralNames;
import org.bouncycastle.asn1.x509.Time;
import org.bouncycastle.cert.CertIOException;
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
import org.bouncycastle.operator.OperatorCreationException;
import org.bouncycastle.operator.jcajce.JcaContentSignerBuilder;
import org.bouncycastle.pkcs.PKCS10CertificationRequest;
import org.bouncycastle.pkcs.jcajce.JcaPKCS10CertificationRequestBuilder;
import org.elasticsearch.common.Nullable;
import org.elasticsearch.common.SuppressForbidden;
import org.elasticsearch.common.io.PathUtils;
import org.elasticsearch.common.network.InetAddressHelper;
import org.elasticsearch.common.network.NetworkAddress;
import org.elasticsearch.env.Environment;
import org.joda.time.DateTime;
import org.joda.time.DateTimeZone;

/**
 * Utility methods that deal with {@link Certificate}, {@link KeyStore}, {@link X509ExtendedTrustManager}, {@link X509ExtendedKeyManager}
 * and other certificate related objects.
 */
public class CertUtils {

    static final String CN_OID = "2.5.4.3";

    private static final int SERIAL_BIT_LENGTH = 20 * 8;
    static final BouncyCastleProvider BC_PROV = new BouncyCastleProvider();

    private CertUtils() {}

    /**
     * Resolves a path with or without an {@link Environment} as we may be running in a transport client where we do not have access to
     * the environment
     */
    @SuppressForbidden(reason = "we don't have the environment to resolve files from when running in a transport client")
    static Path resolvePath(String path, @Nullable Environment environment) {
        if (environment != null) {
            return environment.configFile().resolve(path);
        }
        return PathUtils.get(path).normalize();
    }

    /**
     * Returns a {@link X509ExtendedKeyManager} that is built from the provided private key and certificate chain
     */
    public static X509ExtendedKeyManager keyManager(Certificate[] certificateChain, PrivateKey privateKey, char[] password)
            throws NoSuchAlgorithmException, UnrecoverableKeyException, KeyStoreException, IOException, CertificateException {
        KeyStore keyStore = KeyStore.getInstance("jks");
        keyStore.load(null, null);
        // password must be non-null for keystore...
        keyStore.setKeyEntry("key", privateKey, password, certificateChain);
        return keyManager(keyStore, password, KeyManagerFactory.getDefaultAlgorithm());
    }

    /**
     * Returns a {@link X509ExtendedKeyManager} that is built from the provided keystore
     */
    static X509ExtendedKeyManager keyManager(KeyStore keyStore, char[] password, String algorithm)
            throws NoSuchAlgorithmException, UnrecoverableKeyException, KeyStoreException {
        KeyManagerFactory kmf = KeyManagerFactory.getInstance(algorithm);
        kmf.init(keyStore, password);
        KeyManager[] keyManagers = kmf.getKeyManagers();
        for (KeyManager keyManager : keyManagers) {
            if (keyManager instanceof X509ExtendedKeyManager) {
                return (X509ExtendedKeyManager) keyManager;
            }
        }
        throw new IllegalStateException("failed to find a X509ExtendedKeyManager");
    }

    /**
     * Creates a {@link X509ExtendedTrustManager} based on the provided certificates
     * @param certificates the certificates to trust
     * @return a trust manager that trusts the provided certificates
     */
    public static X509ExtendedTrustManager trustManager(Certificate[] certificates)
            throws NoSuchAlgorithmException, UnrecoverableKeyException, KeyStoreException, IOException, CertificateException {
        assert certificates != null : "Cannot create trust manager with null certificates";
        KeyStore store = KeyStore.getInstance("jks");
        store.load(null, null);
        int counter = 0;
        for (Certificate certificate : certificates) {
            store.setCertificateEntry("cert" + counter, certificate);
            counter++;
        }
        return trustManager(store, TrustManagerFactory.getDefaultAlgorithm());
    }

    /**
     * Loads the truststore and creates a {@link X509ExtendedTrustManager}
     * @param trustStorePath the path to the truststore
     * @param trustStorePassword the password to the truststore
     * @param trustStoreAlgorithm the algorithm to use for the truststore
     * @param env the environment to use for file resolution. May be {@code null}
     * @return a trust manager with the trust material from the store
     */
    public static X509ExtendedTrustManager trustManager(String trustStorePath, String trustStoreType, char[] trustStorePassword,
                                                        String trustStoreAlgorithm, @Nullable Environment env)
            throws NoSuchAlgorithmException, UnrecoverableKeyException, KeyStoreException, IOException, CertificateException {
        try (InputStream in = Files.newInputStream(resolvePath(trustStorePath, env))) {
            KeyStore trustStore = KeyStore.getInstance(trustStoreType);
            assert trustStorePassword != null;
            trustStore.load(in, trustStorePassword);
            return trustManager(trustStore, trustStoreAlgorithm);
        }
    }

    /**
     * Creates a {@link X509ExtendedTrustManager} based on the trust material in the provided {@link KeyStore}
     */
    static X509ExtendedTrustManager trustManager(KeyStore keyStore, String algorithm)
            throws NoSuchAlgorithmException, UnrecoverableKeyException, KeyStoreException, IOException, CertificateException {
        TrustManagerFactory tmf = TrustManagerFactory.getInstance(algorithm);
        tmf.init(keyStore);
        TrustManager[] trustManagers = tmf.getTrustManagers();
        for (TrustManager trustManager : trustManagers) {
            if (trustManager instanceof X509ExtendedTrustManager) {
                return (X509ExtendedTrustManager) trustManager ;
            }
        }
        throw new IllegalStateException("failed to find a X509ExtendedTrustManager");
    }

    /**
     * Reads the provided paths and parses them into {@link Certificate} objects
     * @param certPaths the paths to the PEM encoded certificates
     * @param environment the environment to resolve files against. May be {@code null}
     * @return an array of {@link Certificate} objects
     */
    public static Certificate[] readCertificates(List<String> certPaths, @Nullable Environment environment)
            throws CertificateException, IOException {
        List<Certificate> certificates = new ArrayList<>(certPaths.size());
        CertificateFactory certFactory = CertificateFactory.getInstance("X.509");
        for (String path : certPaths) {
            try (Reader reader = Files.newBufferedReader(resolvePath(path, environment), StandardCharsets.UTF_8)) {
                readCertificates(reader, certificates, certFactory);
            }
        }
        return certificates.toArray(new Certificate[certificates.size()]);
    }

    /**
     * Reads the certificates from the provided reader
     */
    static void readCertificates(Reader reader, List<Certificate> certificates, CertificateFactory certFactory)
            throws IOException, CertificateException {
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

    /**
     * Reads the private key from the reader and optionally uses the password supplier to retrieve a password if the key is encrypted
     */
    public static PrivateKey readPrivateKey(Reader reader, Supplier<char[]> passwordSupplier) throws IOException {
        try (PEMParser parser = new PEMParser(reader)) {
            PrivateKeyInfo privateKeyInfo = innerReadPrivateKey(parser, passwordSupplier);
            if (parser.readObject() != null) {
                throw new IllegalStateException("key file contained more that one entry");
            }
            JcaPEMKeyConverter converter = new JcaPEMKeyConverter();
            converter.setProvider(BC_PROV);
            return converter.getPrivateKey(privateKeyInfo);
        }
    }

    private static PrivateKeyInfo innerReadPrivateKey(PEMParser parser, Supplier<char[]> passwordSupplier) throws IOException {
        final Object parsed = parser.readObject();
        if (parsed == null) {
            throw new IllegalStateException("key file did not contain a supported key");
        }

        PrivateKeyInfo privateKeyInfo;
        if (parsed instanceof PEMEncryptedKeyPair) {
            char[] keyPassword = passwordSupplier.get();
            if (keyPassword == null) {
                throw new IllegalArgumentException("cannot read encrypted key without a password");
            }
            // we have an encrypted key pair so we need to decrypt it
            PEMEncryptedKeyPair encryptedKeyPair = (PEMEncryptedKeyPair) parsed;
            privateKeyInfo = encryptedKeyPair
                    .decryptKeyPair(new JcePEMDecryptorProviderBuilder().setProvider(BC_PROV).build(keyPassword))
                    .getPrivateKeyInfo();
        } else if (parsed instanceof PEMKeyPair) {
            privateKeyInfo = ((PEMKeyPair) parsed).getPrivateKeyInfo();
        } else if (parsed instanceof PrivateKeyInfo) {
            privateKeyInfo = (PrivateKeyInfo) parsed;
        } else if (parsed instanceof ASN1ObjectIdentifier) {
            // skip this object and recurse into this method again to read the next object
            return innerReadPrivateKey(parser, passwordSupplier);
        } else {
            throw new IllegalArgumentException("parsed an unsupported object [" + parsed.getClass().getSimpleName() + "]");
        }

        return privateKeyInfo;
    }
    /**
     * Generates a CA certificate
     */
    static X509Certificate generateCACertificate(X500Principal x500Principal, KeyPair keyPair, int days)
            throws OperatorCreationException, CertificateException, CertIOException, NoSuchAlgorithmException {
        return generateSignedCertificate(x500Principal, null, keyPair, null, null, true, days);
    }

    /**
     * Generates a signed certificate using the provided CA private key and information from the CA certificate
     */
    public static X509Certificate generateSignedCertificate(X500Principal principal, GeneralNames subjectAltNames, KeyPair keyPair,
                                                     X509Certificate caCert, PrivateKey caPrivKey, int days)
            throws OperatorCreationException, CertificateException, CertIOException, NoSuchAlgorithmException {
        return generateSignedCertificate(principal, subjectAltNames, keyPair, caCert, caPrivKey, false, days);
    }

    /**
     * Generates a signed certificate
     * @param principal the principal of the certificate; commonly referred to as the distinguished name (DN)
     * @param subjectAltNames the subject alternative names that should be added to the certificate as an X509v3 extension. May be
     *                        {@code null}
     * @param keyPair the key pair that will be associated with the certificate
     * @param caCert the CA certificate. If {@code null}, this results in a self signed certificate
     * @param caPrivKey the CA private key. If {@code null}, this results in a self signed certificate
     * @param isCa whether or not the generated certificate is a CA
     * @return a signed {@link X509Certificate}
     */
    private static X509Certificate generateSignedCertificate(X500Principal principal, GeneralNames subjectAltNames, KeyPair keyPair,
                                                     X509Certificate caCert, PrivateKey caPrivKey, boolean isCa, int days)
            throws NoSuchAlgorithmException, CertificateException, CertIOException, OperatorCreationException {
        final DateTime notBefore = new DateTime(DateTimeZone.UTC);
        if (days < 1) {
            throw new IllegalArgumentException("the certificate must be valid for at least one day");
        }
        final DateTime notAfter = notBefore.plusDays(days);
        final BigInteger serial = CertUtils.getSerial();
        JcaX509ExtensionUtils extUtils = new JcaX509ExtensionUtils();

        X500Name subject = X500Name.getInstance(principal.getEncoded());
        final X500Name issuer;
        final AuthorityKeyIdentifier authorityKeyIdentifier;
        if (caCert != null) {
            if (caCert.getBasicConstraints() < 0) {
                throw new IllegalArgumentException("ca certificate is not a CA!");
            }
            issuer = X500Name.getInstance(caCert.getIssuerX500Principal().getEncoded());
            authorityKeyIdentifier = extUtils.createAuthorityKeyIdentifier(caCert);
        } else {
            issuer = subject;
            authorityKeyIdentifier =
                    extUtils.createAuthorityKeyIdentifier(keyPair.getPublic(), new X500Principal(issuer.toString()), serial);
        }

        JcaX509v3CertificateBuilder builder =
                new JcaX509v3CertificateBuilder(issuer, serial,
                        new Time(notBefore.toDate(), Locale.ROOT), new Time(notAfter.toDate(), Locale.ROOT), subject, keyPair.getPublic());

        builder.addExtension(Extension.subjectKeyIdentifier, false, extUtils.createSubjectKeyIdentifier(keyPair.getPublic()));
        builder.addExtension(Extension.authorityKeyIdentifier, false, authorityKeyIdentifier);
        if (subjectAltNames != null) {
            builder.addExtension(Extension.subjectAlternativeName, false, subjectAltNames);
        }
        builder.addExtension(Extension.basicConstraints, isCa, new BasicConstraints(isCa));

        PrivateKey signingKey = caPrivKey != null ? caPrivKey : keyPair.getPrivate();
        ContentSigner signer = new JcaContentSignerBuilder("SHA256withRSA").build(signingKey);
        X509CertificateHolder certificateHolder = builder.build(signer);
        return new JcaX509CertificateConverter().getCertificate(certificateHolder);
    }

    /**
     * Generates a certificate signing request
     * @param keyPair the key pair that will be associated by the certificate generated from the certificate signing request
     * @param principal the principal of the certificate; commonly referred to as the distinguished name (DN)
     * @param sanList the subject alternative names that should be added to the certificate as an X509v3 extension. May be
*                     {@code null}
     * @return a certificate signing request
     */
    static PKCS10CertificationRequest generateCSR(KeyPair keyPair, X500Principal principal, GeneralNames sanList)
            throws IOException, OperatorCreationException {
        JcaPKCS10CertificationRequestBuilder builder = new JcaPKCS10CertificationRequestBuilder(principal, keyPair.getPublic());
        if (sanList != null) {
            ExtensionsGenerator extGen = new ExtensionsGenerator();
            extGen.addExtension(Extension.subjectAlternativeName, false, sanList);
            builder.addAttribute(PKCSObjectIdentifiers.pkcs_9_at_extensionRequest, extGen.generate());
        }

        return builder.build(new JcaContentSignerBuilder("SHA256withRSA").setProvider(CertUtils.BC_PROV).build(keyPair.getPrivate()));
    }

    /**
     * Gets a random serial for a certificate that is generated from a {@link SecureRandom}
     */
    static BigInteger getSerial() {
        SecureRandom random = new SecureRandom();
        BigInteger serial = new BigInteger(SERIAL_BIT_LENGTH, random);
        assert serial.compareTo(BigInteger.valueOf(0L)) >= 0;
        return serial;
    }

    /**
     * Generates a RSA key pair with the provided key size (in bits)
     */
    static KeyPair generateKeyPair(int keysize) throws NoSuchAlgorithmException {
        // generate a private key
        KeyPairGenerator keyPairGenerator = KeyPairGenerator.getInstance("RSA");
        keyPairGenerator.initialize(keysize);
        return keyPairGenerator.generateKeyPair();
    }

    /**
     * Converts the {@link InetAddress} objects into a {@link GeneralNames} object that is used to represent subject alternative names.
     */
    static GeneralNames getSubjectAlternativeNames(boolean resolveName, Set<InetAddress> addresses) throws SocketException {
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
    private static void addSubjectAlternativeNames(boolean resolveName, InetAddress inetAddress, Set<GeneralName> list) {
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

    /**
     * Creates an X.509 {@link GeneralName} for use as a <em>Common Name</em> in the certificate's <em>Subject Alternative Names</em>
     * extension. A <em>common name</em> is a name with a tag of {@link GeneralName#otherName OTHER}, with an object-id that references
     * the {@link #CN_OID cn} attribute, and a DER encoded IA5 (ASCII) string for the name.
     * This usage of using the {@code cn} OID as a <em>Subject Alternative Name</em> is <strong>non-standard</strong> and will not be
     * recognised by other X.509/TLS implementations.
     */
    static GeneralName createCommonName(String cn) {
        final ASN1Encodable[] sequence = { new ASN1ObjectIdentifier(CN_OID), new DERIA5String(cn) };
        return new GeneralName(GeneralName.otherName, new DERSequence(sequence));
    }
}
