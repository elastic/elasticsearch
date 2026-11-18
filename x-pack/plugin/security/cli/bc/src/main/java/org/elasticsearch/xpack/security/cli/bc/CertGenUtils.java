/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */
package org.elasticsearch.xpack.security.cli.bc;

import org.bouncycastle.asn1.ASN1Encodable;
import org.bouncycastle.asn1.ASN1ObjectIdentifier;
import org.bouncycastle.asn1.DERIA5String;
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
import org.bouncycastle.asn1.x509.Time;
import org.bouncycastle.cert.X509CertificateHolder;
import org.bouncycastle.cert.jcajce.JcaX509CertificateConverter;
import org.bouncycastle.cert.jcajce.JcaX509ExtensionUtils;
import org.bouncycastle.cert.jcajce.JcaX509v3CertificateBuilder;
import org.bouncycastle.operator.ContentSigner;
import org.bouncycastle.operator.OperatorCreationException;
import org.bouncycastle.operator.jcajce.JcaContentSignerBuilder;
import org.bouncycastle.pkcs.jcajce.JcaPKCS10CertificationRequestBuilder;
import org.elasticsearch.common.Randomness;
import org.elasticsearch.common.Strings;
import org.elasticsearch.common.network.NetworkAddress;
import org.elasticsearch.common.network.NetworkUtils;
import org.elasticsearch.core.SuppressForbidden;

import java.io.IOException;
import java.math.BigInteger;
import java.net.InetAddress;
import java.security.GeneralSecurityException;
import java.security.KeyPair;
import java.security.KeyPairGenerator;
import java.security.KeyStore;
import java.security.NoSuchAlgorithmException;
import java.security.PrivateKey;
import java.security.SecureRandom;
import java.security.cert.Certificate;
import java.security.cert.X509Certificate;
import java.sql.Date;
import java.time.ZoneOffset;
import java.time.ZonedDateTime;
import java.util.Collection;
import java.util.Collections;
import java.util.EnumSet;
import java.util.HashSet;
import java.util.List;
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
 * <p>
 * This class is the entry point of the {@code bc} module, which shades Bouncy Castle. Neither its public API nor that of the other
 * public classes in this package exposes Bouncy Castle types.
 * </p>
 * <p>
 * Bouncy Castle is only used for ASN.1, X.509, PKCS#10 and PEM encoding. All cryptographic primitives (key generation, signing,
 * digests, ciphers, randomness) are resolved through the JVM's configured JCA/JCE providers and are never bound to the Bouncy Castle
 * provider that is shaded into this module's jar. Which algorithms are available or rejected is therefore determined by that provider
 * configuration, for example by the FIPS provider in approved-only mode, and not by this module.
 * </p>
 */
public class CertGenUtils {

    private static final String CN_OID = "2.5.4.3";

    private static final int SERIAL_BIT_LENGTH = 20 * 8;

    /**
     * The mapping of key usage names to their corresponding integer values as defined in {@link KeyUsage}.
     */
    public static final Map<String, Integer> KEY_USAGE_MAPPINGS;

    static {
        Map<String, Integer> mappings = new TreeMap<>();
        for (KeyUsage keyUsage : KeyUsage.values()) {
            mappings.put(keyUsage.usageName(), keyUsage.bits());
        }
        KEY_USAGE_MAPPINGS = Collections.unmodifiableMap(mappings);
    }

    private CertGenUtils() {}

    /**
     * Generates a CA certificate
     */
    public static X509Certificate generateCACertificate(X500Principal x500Principal, KeyPair keyPair, int days, Set<KeyUsage> keyUsage)
        throws IOException, GeneralSecurityException {
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
        SubjectAlternativeNames subjectAltNames,
        KeyPair keyPair,
        X509Certificate caCert,
        PrivateKey caPrivKey,
        int days
    ) throws IOException, GeneralSecurityException {
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
     * @param keyUsage          the key usages (see {@link #buildKeyUsage(Collection)}) that should be added to the certificate as an
     *                           X509v3 extension. {@code null} or empty omits the extension
     * @param extendedKeyUsages the extended key usages that should be added to the certificate as a X509v3 extension (can be empty)
     * @return a signed {@link X509Certificate}
     */
    public static X509Certificate generateSignedCertificate(
        X500Principal principal,
        SubjectAlternativeNames subjectAltNames,
        KeyPair keyPair,
        X509Certificate caCert,
        PrivateKey caPrivKey,
        boolean isCa,
        int days,
        String signatureAlgorithm,
        Set<KeyUsage> keyUsage,
        Set<KeyPurpose> extendedKeyUsages
    ) throws IOException, GeneralSecurityException {
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
        SubjectAlternativeNames subjectAltNames,
        KeyPair keyPair,
        X509Certificate caCert,
        PrivateKey caPrivKey,
        boolean isCa,
        ZonedDateTime notBefore,
        ZonedDateTime notAfter,
        String signatureAlgorithm
    ) throws IOException, GeneralSecurityException {
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
        SubjectAlternativeNames subjectAltNames,
        KeyPair keyPair,
        X509Certificate caCert,
        PrivateKey caPrivKey,
        boolean isCa,
        ZonedDateTime notBefore,
        ZonedDateTime notAfter,
        String signatureAlgorithm,
        Set<KeyUsage> keyUsage,
        Set<KeyPurpose> extendedKeyUsages
    ) throws IOException, GeneralSecurityException {
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
            builder.addExtension(Extension.subjectAlternativeName, false, subjectAltNames.generalNames());
        }
        builder.addExtension(Extension.basicConstraints, isCa, new BasicConstraints(isCa));

        final org.bouncycastle.asn1.x509.KeyUsage keyUsageExtension = buildKeyUsage(keyUsage);
        if (keyUsageExtension != null) {
            // as per RFC 5280 (section 4.2.1.3), if the key usage is present, then it SHOULD be marked as critical.
            final boolean isCritical = true;
            builder.addExtension(Extension.keyUsage, isCritical, keyUsageExtension);
        }
        if (extendedKeyUsages != null) {
            for (KeyPurpose extendedKeyUsage : extendedKeyUsages) {
                builder.addExtension(Extension.extendedKeyUsage, false, new ExtendedKeyUsage(extendedKeyUsage.keyPurposeId()));
            }
        }

        PrivateKey signingKey = caPrivKey != null ? caPrivKey : keyPair.getPrivate();
        ContentSigner signer = buildContentSigner(
            (Strings.isNullOrEmpty(signatureAlgorithm)) ? getDefaultSignatureAlgorithm(signingKey) : signatureAlgorithm,
            signingKey
        );
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
     * Builds the signer for the given algorithm and key. The {@link java.security.Signature} is resolved through the JVM's configured
     * JCA providers, see the class documentation. The Bouncy Castle {@link OperatorCreationException} is translated into a
     * {@link GeneralSecurityException} so that it does not surface in the API of this module.
     */
    private static ContentSigner buildContentSigner(String signatureAlgorithm, PrivateKey signingKey) throws GeneralSecurityException {
        try {
            return new JcaContentSignerBuilder(signatureAlgorithm).build(signingKey);
        } catch (OperatorCreationException e) {
            throw new GeneralSecurityException(e.getMessage(), e);
        }
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
    public static CertificateSigningRequest generateCSR(KeyPair keyPair, X500Principal principal, SubjectAlternativeNames sanList)
        throws IOException, GeneralSecurityException {
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
    public static CertificateSigningRequest generateCSR(
        KeyPair keyPair,
        X500Principal principal,
        SubjectAlternativeNames sanList,
        Set<KeyUsage> keyUsage,
        Set<KeyPurpose> extendedKeyUsages
    ) throws IOException, GeneralSecurityException {
        Objects.requireNonNull(keyPair, "Key-Pair must not be null");
        Objects.requireNonNull(keyPair.getPublic(), "Public-Key must not be null");
        Objects.requireNonNull(principal, "Principal must not be null");
        Objects.requireNonNull(extendedKeyUsages, "extendedKeyUsages must not be null");
        JcaPKCS10CertificationRequestBuilder builder = new JcaPKCS10CertificationRequestBuilder(principal, keyPair.getPublic());

        ExtensionsGenerator extGen = new ExtensionsGenerator();
        if (sanList != null) {
            extGen.addExtension(Extension.subjectAlternativeName, false, sanList.generalNames());
        }
        final org.bouncycastle.asn1.x509.KeyUsage keyUsageExtension = buildKeyUsage(keyUsage);
        if (keyUsageExtension != null) {
            extGen.addExtension(Extension.keyUsage, true, keyUsageExtension);
        }
        for (KeyPurpose extendedKeyUsage : extendedKeyUsages) {
            extGen.addExtension(Extension.extendedKeyUsage, false, new ExtendedKeyUsage(extendedKeyUsage.keyPurposeId()));
        }

        if (extGen.isEmpty() == false) {
            builder.addAttribute(PKCSObjectIdentifiers.pkcs_9_at_extensionRequest, extGen.generate());
        }

        return new CertificateSigningRequest(builder.build(buildContentSigner("SHA256withRSA", keyPair.getPrivate())));
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
     * Converts the {@link InetAddress} objects into a {@link SubjectAlternativeNames} object that is used to represent subject alternative
     * names.
     */
    public static SubjectAlternativeNames getSubjectAlternativeNames(boolean resolveName, Set<InetAddress> addresses) throws IOException {
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
        return new SubjectAlternativeNames(new GeneralNames(generalNameList.toArray(new GeneralName[generalNameList.size()])));
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
     * Converts the IP addresses, DNS names and (non-standard) common names into a {@link SubjectAlternativeNames} object that is used to
     * represent subject alternative names.
     *
     * @return the subject alternative names, or {@code null} if there are none
     */
    public static SubjectAlternativeNames getSubjectAlternativeNamesValue(
        List<String> ipAddresses,
        List<String> dnsNames,
        List<String> commonNames
    ) {
        Set<GeneralName> generalNameList = new HashSet<>();
        for (String ip : ipAddresses) {
            generalNameList.add(new GeneralName(GeneralName.iPAddress, ip));
        }

        for (String dns : dnsNames) {
            generalNameList.add(new GeneralName(GeneralName.dNSName, dns));
        }

        for (String cn : commonNames) {
            generalNameList.add(CertGenUtils.createCommonName(cn));
        }

        if (generalNameList.isEmpty()) {
            return null;
        }
        return new SubjectAlternativeNames(new GeneralNames(generalNameList.toArray(new GeneralName[0])));
    }

    /**
     * Creates an X.509 {@link GeneralName} for use as a <em>Common Name</em> in the certificate's <em>Subject Alternative Names</em>
     * extension. A <em>common name</em> is a name with a tag of {@link GeneralName#otherName OTHER}, with an object-id that references
     * the {@link #CN_OID cn} attribute, an explicit tag of '0', and a DER encoded UTF8 string for the name.
     * This usage of using the {@code cn} OID as a <em>Subject Alternative Name</em> is <strong>non-standard</strong> and will not be
     * recognised by other X.509/TLS implementations.
     */
    private static GeneralName createCommonName(String cn) {
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

    /**
     * Whether the given string only contains characters that are valid in an ASN.1 {@code IA5String}, as is required of the DNS names
     * in subject alternative names.
     */
    public static boolean isIA5String(String str) {
        return DERIA5String.isIA5String(str);
    }

    /**
     * Resolves CLI key usage names (see {@link #KEY_USAGE_MAPPINGS}) to key usage values.
     *
     * @return the key usages, or an empty set if no names are given
     * @throws IllegalArgumentException if any of the names is not a known key usage
     */
    public static Set<KeyUsage> buildKeyUsage(Collection<String> keyUsages) {
        if (keyUsages == null || keyUsages.isEmpty()) {
            return EnumSet.noneOf(KeyUsage.class);
        }

        EnumSet<KeyUsage> resolved = EnumSet.noneOf(KeyUsage.class);
        for (String keyUsageName : keyUsages) {
            KeyUsage keyUsage = KeyUsage.fromName(keyUsageName);
            if (keyUsage == null) {
                throw new IllegalArgumentException("Unknown keyUsage: " + keyUsageName);
            }
            resolved.add(keyUsage);
        }
        return resolved;
    }

    /**
     * Combines key usages into the value of the key usage extension.
     *
     * @return the extension value, or {@code null} if no key usages are given
     */
    private static org.bouncycastle.asn1.x509.KeyUsage buildKeyUsage(Set<KeyUsage> keyUsages) {
        if (keyUsages == null || keyUsages.isEmpty()) {
            return null;
        }

        int usageBits = 0;
        for (KeyUsage keyUsage : keyUsages) {
            usageBits |= keyUsage.bits();
        }
        return new org.bouncycastle.asn1.x509.KeyUsage(usageBits);
    }

    public static boolean isValidKeyUsage(String keyUsage) {
        return KeyUsage.fromName(keyUsage) != null;
    }
}
