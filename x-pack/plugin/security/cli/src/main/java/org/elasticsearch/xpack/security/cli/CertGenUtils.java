/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */
package org.elasticsearch.xpack.security.cli;

import org.bouncycastle.asn1.ASN1Encodable;
import org.bouncycastle.asn1.ASN1ObjectIdentifier;
import org.bouncycastle.asn1.DERSequence;
import org.bouncycastle.asn1.DERTaggedObject;
import org.bouncycastle.asn1.DERUTF8String;
import org.bouncycastle.asn1.pkcs.PKCSObjectIdentifiers;
import org.bouncycastle.asn1.x500.X500Name;
import org.bouncycastle.asn1.x509.AuthorityKeyIdentifier;
import org.bouncycastle.asn1.x509.BasicConstraints;
import org.bouncycastle.asn1.x509.ExtendedKeyUsage;
import org.bouncycastle.asn1.x509.Extension;
import org.bouncycastle.asn1.x509.ExtensionsGenerator;
import org.bouncycastle.asn1.x509.GeneralName;
import org.bouncycastle.asn1.x509.GeneralNames;
import org.bouncycastle.asn1.x509.KeyUsage;
import org.bouncycastle.asn1.x509.Time;
import org.bouncycastle.cert.CertIOException;
import org.bouncycastle.cert.X509CertificateHolder;
import org.bouncycastle.cert.jcajce.JcaX509CertificateConverter;
import org.bouncycastle.cert.jcajce.JcaX509ExtensionUtils;
import org.bouncycastle.cert.jcajce.JcaX509v3CertificateBuilder;
import org.bouncycastle.jce.provider.BouncyCastleProvider;
import org.bouncycastle.operator.ContentSigner;
import org.bouncycastle.operator.OperatorCreationException;
import org.bouncycastle.operator.jcajce.JcaContentSignerBuilder;
import org.bouncycastle.pkcs.PKCS10CertificationRequest;
import org.bouncycastle.pkcs.jcajce.JcaPKCS10CertificationRequestBuilder;
import org.elasticsearch.common.Randomness;
import org.elasticsearch.common.Strings;
import org.elasticsearch.common.network.NetworkAddress;
import org.elasticsearch.common.network.NetworkUtils;
import org.elasticsearch.core.SuppressForbidden;

import java.io.IOException;
import java.math.BigInteger;
import java.net.InetAddress;
import java.security.KeyPair;
import java.security.KeyPairGenerator;
import java.security.KeyStore;
import java.security.NoSuchAlgorithmException;
import java.security.PrivateKey;
import java.security.SecureRandom;
import java.security.cert.Certificate;
import java.security.cert.CertificateException;
import java.security.cert.X509Certificate;
import java.sql.Date;
import java.time.ZoneOffset;
import java.time.ZonedDateTime;
import java.util.Collection;
import java.util.Collections;
import java.util.HashSet;
import java.util.Locale;
import java.util.Map;
import java.util.Objects;
import java.util.Set;
import java.util.TreeMap;

import javax.net.ssl.X509ExtendedKeyManager;
import javax.net.ssl.X509ExtendedTrustManager;
import javax.security.auth.x500.X500Principal;

/**
 * Utility methods that deal with {@link Certificate}, {@link KeyStore}, {@link X509ExtendedTrustManager}, {@link X509ExtendedKeyManager}
 * and other certificate related objects.
 */
public class CertGenUtils {

    private static final String CN_OID = "2.5.4.3";

    private static final int SERIAL_BIT_LENGTH = 20 * 8;
    private static final BouncyCastleProvider BC_PROV = new BouncyCastleProvider();

    /**
     * The mapping of key usage names to their corresponding integer values as defined in {@code KeyUsage} class.
     */
    public static final Map<String, Integer> KEY_USAGE_MAPPINGS = Collections.unmodifiableMap(
        new TreeMap<>(
            Map.ofEntries(
                Map.entry("digitalSignature", KeyUsage.digitalSignature),
                Map.entry("nonRepudiation", KeyUsage.nonRepudiation),
                Map.entry("keyEncipherment", KeyUsage.keyEncipherment),
                Map.entry("dataEncipherment", KeyUsage.dataEncipherment),
                Map.entry("keyAgreement", KeyUsage.keyAgreement),
                Map.entry("keyCertSign", KeyUsage.keyCertSign),
                Map.entry("cRLSign", KeyUsage.cRLSign),
                Map.entry("encipherOnly", KeyUsage.encipherOnly),
                Map.entry("decipherOnly", KeyUsage.decipherOnly)
            )
        )
    );

    private CertGenUtils() {}

    /**
     * Generates a CA certificate
     */
    public static X509Certificate generateCACertificate(X500Principal x500Principal, KeyPair keyPair, int days, KeyUsage keyUsage)
        throws OperatorCreationException, CertificateException, CertIOException, NoSuchAlgorithmException {
        return generateSignedCertificate(x500Principal, null, keyPair, null, null, true, days, null, keyUsage, Set.of());
    }

    /**
     * Generates a signed certificate using the provided CA private key and
     * information from the CA certificate
     *
     * @param principal       the principal of the certificate; commonly referred to as the
     *                        distinguished name (DN)
     * @param subjectAltNames the subject alternative names that should be added to the
     *                        certificate as an X509v3 extension. May be {@code null}
     * @param keyPair         the key pair that will be associated with the certificate
     * @param caCert          the CA certificate. If {@code null}, this results in a self signed
     *                        certificate
     * @param caPrivKey       the CA private key. If {@code null}, this results in a self signed
     *                        certificate
     * @param days            no of days certificate will be valid from now
     * @return a signed {@link X509Certificate}
     */
    public static X509Certificate generateSignedCertificate(
        X500Principal principal,
        GeneralNames subjectAltNames,
        KeyPair keyPair,
        X509Certificate caCert,
        PrivateKey caPrivKey,
        int days
    ) throws OperatorCreationException, CertificateException, CertIOException, NoSuchAlgorithmException {
        return generateSignedCertificate(principal, subjectAltNames, keyPair, caCert, caPrivKey, false, days, null, null, Set.of());
    }

    /**
     * Generates a signed certificate using the provided CA private key and
     * information from the CA certificate
     *
     * @param principal          the principal of the certificate; commonly referred to as the
     *                           distinguished name (DN)
     * @param subjectAltNames    the subject alternative names that should be added to the
     *                           certificate as an X509v3 extension. May be {@code null}
     * @param keyPair            the key pair that will be associated with the certificate
     * @param caCert             the CA certificate. If {@code null}, this results in a self signed
     *                           certificate
     * @param caPrivKey          the CA private key. If {@code null}, this results in a self signed
     *                           certificate
     * @param isCa               whether or not the generated certificate is a CA
     * @param days               no of days certificate will be valid from now
     * @param signatureAlgorithm algorithm used for signing certificate. If {@code null} or
     *                           empty, then use default algorithm {@link CertGenUtils#getDefaultSignatureAlgorithm(PrivateKey)}
     * @param keyUsage          the key usage that should be added to the certificate as a X509v3 extension (can be {@code null})
     * @param extendedKeyUsages the extended key usages that should be added to the certificate as a X509v3 extension (can be empty)
     * @return a signed {@link X509Certificate}
     */
    public static X509Certificate generateSignedCertificate(
        X500Principal principal,
        GeneralNames subjectAltNames,
        KeyPair keyPair,
        X509Certificate caCert,
        PrivateKey caPrivKey,
        boolean isCa,
        int days,
        String signatureAlgorithm,
        KeyUsage keyUsage,
        Set<ExtendedKeyUsage> extendedKeyUsages
    ) throws NoSuchAlgorithmException, CertificateException, CertIOException, OperatorCreationException {
        Objects.requireNonNull(keyPair, "Key-Pair must not be null");
        final ZonedDateTime notBefore = ZonedDateTime.now(ZoneOffset.UTC);
        if (days < 1) {
            throw new IllegalArgumentException("the certificate must be valid for at least one day");
        }
        final ZonedDateTime notAfter = notBefore.plusDays(days);
        return generateSignedCertificate(
            principal,
            subjectAltNames,
            keyPair,
            caCert,
            caPrivKey,
            isCa,
            notBefore,
            notAfter,
            signatureAlgorithm,
            keyUsage,
            extendedKeyUsages
        );
    }

    public static X509Certificate generateSignedCertificate(
        X500Principal principal,
        GeneralNames subjectAltNames,
        KeyPair keyPair,
        X509Certificate caCert,
        PrivateKey caPrivKey,
        boolean isCa,
        ZonedDateTime notBefore,
        ZonedDateTime notAfter,
        String signatureAlgorithm
    ) throws NoSuchAlgorithmException, CertIOException, OperatorCreationException, CertificateException {
        return generateSignedCertificate(
            principal,
            subjectAltNames,
            keyPair,
            caCert,
            caPrivKey,
            isCa,
            notBefore,
            notAfter,
            signatureAlgorithm,
            null,
            Set.of()
        );
    }

    public static X509Certificate generateSignedCertificate(
        X500Principal principal,
        GeneralNames subjectAltNames,
        KeyPair keyPair,
        X509Certificate caCert,
        PrivateKey caPrivKey,
        boolean isCa,
        ZonedDateTime notBefore,
        ZonedDateTime notAfter,
        String signatureAlgorithm,
        KeyUsage keyUsage,
        Set<ExtendedKeyUsage> extendedKeyUsages
    ) throws NoSuchAlgorithmException, CertIOException, OperatorCreationException, CertificateException {
        final BigInteger serial = CertGenUtils.getSerial();
        JcaX509ExtensionUtils extUtils = new JcaX509ExtensionUtils();

        X500Name subject = X500Name.getInstance(principal.getEncoded());
        final X500Name issuer;
        final AuthorityKeyIdentifier authorityKeyIdentifier;
        if (caCert != null) {
            if (caCert.getBasicConstraints() < 0) {
                throw new IllegalArgumentException("ca certificate is not a CA!");
            }
            issuer = X500Name.getInstance(caCert.getSubjectX500Principal().getEncoded());
            authorityKeyIdentifier = extUtils.createAuthorityKeyIdentifier(caCert.getPublicKey());
        } else {
            issuer = subject;
            authorityKeyIdentifier = extUtils.createAuthorityKeyIdentifier(keyPair.getPublic());
        }

        JcaX509v3CertificateBuilder builder = new JcaX509v3CertificateBuilder(
            issuer,
            serial,
            new Time(Date.from(notBefore.toInstant()), Locale.ROOT),
            new Time(Date.from(notAfter.toInstant()), Locale.ROOT),
            subject,
            keyPair.getPublic()
        );

        builder.addExtension(Extension.subjectKeyIdentifier, false, extUtils.createSubjectKeyIdentifier(keyPair.getPublic()));
        builder.addExtension(Extension.authorityKeyIdentifier, false, authorityKeyIdentifier);
        if (subjectAltNames != null) {
            builder.addExtension(Extension.subjectAlternativeName, false, subjectAltNames);
        }
        builder.addExtension(Extension.basicConstraints, isCa, new BasicConstraints(isCa));

        if (keyUsage != null) {
            // as per RFC 5280 (section 4.2.1.3), if the key usage is present, then it SHOULD be marked as critical.
            final boolean isCritical = true;
            builder.addExtension(Extension.keyUsage, isCritical, keyUsage);
        }
        if (extendedKeyUsages != null) {
            for (ExtendedKeyUsage extendedKeyUsage : extendedKeyUsages) {
                builder.addExtension(Extension.extendedKeyUsage, false, extendedKeyUsage);
            }
        }

        PrivateKey signingKey = caPrivKey != null ? caPrivKey : keyPair.getPrivate();
        ContentSigner signer = new JcaContentSignerBuilder(
            (Strings.isNullOrEmpty(signatureAlgorithm)) ? getDefaultSignatureAlgorithm(signingKey) : signatureAlgorithm
        ).setProvider(CertGenUtils.BC_PROV).build(signingKey);
        X509CertificateHolder certificateHolder = builder.build(signer);
        return new JcaX509CertificateConverter().getCertificate(certificateHolder);
    }

    /**
     * Based on the private key algorithm {@link PrivateKey#getAlgorithm()}
     * determines default signing algorithm used by CertGenUtils
     *
     * @param key {@link PrivateKey}
     * @return algorithm
     */
    private static String getDefaultSignatureAlgorithm(PrivateKey key) {
        String signatureAlgorithm = switch (key.getAlgorithm()) {
            case "RSA" -> "SHA256withRSA";
            case "DSA" -> "SHA256withDSA";
            case "EC" -> "SHA256withECDSA";
            default -> throw new IllegalArgumentException(
                "Unsupported algorithm : "
                    + key.getAlgorithm()
                    + " for signature, allowed values for private key algorithm are [RSA, DSA, EC]"
            );
        };
        return signatureAlgorithm;
    }

    /**
     * Generates a certificate signing request
     *
     * @param keyPair   the key pair that will be associated by the certificate generated from the certificate signing request
     * @param principal the principal of the certificate; commonly referred to as the distinguished name (DN)
     * @param sanList   the subject alternative names that should be added to the certificate as an X509v3 extension. May be
     *                  {@code null}
     * @return a certificate signing request
     */
    static PKCS10CertificationRequest generateCSR(KeyPair keyPair, X500Principal principal, GeneralNames sanList) throws IOException,
        OperatorCreationException {
        return generateCSR(keyPair, principal, sanList, null, Set.of());
    }

    /**
     * Generates a certificate signing request
     *
     * @param keyPair   the key pair that will be associated by the certificate generated from the certificate signing request
     * @param principal the principal of the certificate; commonly referred to as the distinguished name (DN)
     * @param sanList   the subject alternative names that should be added to the certificate as an X509v3 extension. May be
     *                  {@code null}
     * @param extendedKeyUsages the extended key usages that should be added to the certificate as an X509v3 extension. May be empty.
     * @return a certificate signing request
     */
    static PKCS10CertificationRequest generateCSR(
        KeyPair keyPair,
        X500Principal principal,
        GeneralNames sanList,
        KeyUsage keyUsage,
        Set<ExtendedKeyUsage> extendedKeyUsages
    ) throws IOException, OperatorCreationException {
        Objects.requireNonNull(keyPair, "Key-Pair must not be null");
        Objects.requireNonNull(keyPair.getPublic(), "Public-Key must not be null");
        Objects.requireNonNull(principal, "Principal must not be null");
        Objects.requireNonNull(extendedKeyUsages, "extendedKeyUsages must not be null");
        JcaPKCS10CertificationRequestBuilder builder = new JcaPKCS10CertificationRequestBuilder(principal, keyPair.getPublic());

        ExtensionsGenerator extGen = new ExtensionsGenerator();
        if (sanList != null) {
            extGen.addExtension(Extension.subjectAlternativeName, false, sanList);
        }
        if (keyUsage != null) {
            extGen.addExtension(Extension.keyUsage, true, keyUsage);
        }
        for (ExtendedKeyUsage extendedKeyUsage : extendedKeyUsages) {
            extGen.addExtension(Extension.extendedKeyUsage, false, extendedKeyUsage);
        }

        if (extGen.isEmpty() == false) {
            builder.addAttribute(PKCSObjectIdentifiers.pkcs_9_at_extensionRequest, extGen.generate());
        }

        return builder.build(new JcaContentSignerBuilder("SHA256withRSA").setProvider(CertGenUtils.BC_PROV).build(keyPair.getPrivate()));
    }

    /**
     * Gets a random serial for a certificate that is generated from a {@link SecureRandom}
     */
    public static BigInteger getSerial() {
        SecureRandom random = Randomness.createSecure();
        BigInteger serial = new BigInteger(SERIAL_BIT_LENGTH, random);
        assert serial.compareTo(BigInteger.valueOf(0L)) >= 0;
        return serial;
    }

    /**
     * Generates a RSA key pair with the provided key size (in bits)
     */
    public static KeyPair generateKeyPair(int keysize) throws NoSuchAlgorithmException {
        // generate a private key
        KeyPairGenerator keyPairGenerator = KeyPairGenerator.getInstance("RSA");
        keyPairGenerator.initialize(keysize, Randomness.createSecure());
        return keyPairGenerator.generateKeyPair();
    }

    /**
     * Converts the {@link InetAddress} objects into a {@link GeneralNames} object that is used to represent subject alternative names.
     */
    public static GeneralNames getSubjectAlternativeNames(boolean resolveName, Set<InetAddress> addresses) throws IOException {
        Set<GeneralName> generalNameList = new HashSet<>();
        for (InetAddress address : addresses) {
            if (address.isAnyLocalAddress()) {
                // it is a wildcard address
                for (InetAddress inetAddress : NetworkUtils.getAllAddresses()) {
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
     * the {@link #CN_OID cn} attribute, an explicit tag of '0', and a DER encoded UTF8 string for the name.
     * This usage of using the {@code cn} OID as a <em>Subject Alternative Name</em> is <strong>non-standard</strong> and will not be
     * recognised by other X.509/TLS implementations.
     */
    public static GeneralName createCommonName(String cn) {
        final ASN1Encodable[] sequence = { new ASN1ObjectIdentifier(CN_OID), new DERTaggedObject(true, 0, new DERUTF8String(cn)) };
        return new GeneralName(GeneralName.otherName, new DERSequence(sequence));
    }

    /**
     * See RFC 2247 Using Domains in LDAP/X.500 Distinguished Names
     * @param domain active directory domain name
     * @return LDAP DN, distinguished name, of the root of the domain
     */
    public static String buildDnFromDomain(String domain) {
        return "DC=" + domain.replace(".", ",DC=");
    }

    public static KeyUsage buildKeyUsage(Collection<String> keyUsages) {
        if (keyUsages == null || keyUsages.isEmpty()) {
            return null;
        }

        int usageBits = 0;
        for (String keyUsageName : keyUsages) {
            Integer keyUsageValue = findKeyUsageByName(keyUsageName);
            if (keyUsageValue == null) {
                throw new IllegalArgumentException("Unknown keyUsage: " + keyUsageName);
            }
            usageBits |= keyUsageValue;
        }
        return new KeyUsage(usageBits);
    }

    public static boolean isValidKeyUsage(String keyUsage) {
        return findKeyUsageByName(keyUsage) != null;
    }

    private static Integer findKeyUsageByName(String keyUsageName) {
        if (keyUsageName == null) {
            return null;
        }
        return KEY_USAGE_MAPPINGS.get(keyUsageName.trim());
    }
}
