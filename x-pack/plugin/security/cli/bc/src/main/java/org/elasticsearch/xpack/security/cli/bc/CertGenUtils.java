/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */
package org.elasticsearch.xpack.security.cli.bc;

import org.bouncycastle.asn1.DERIA5String;
import org.bouncycastle.asn1.pkcs.PKCSObjectIdentifiers;
import org.bouncycastle.asn1.x500.X500Name;
import org.bouncycastle.asn1.x509.AuthorityKeyIdentifier;
import org.bouncycastle.asn1.x509.BasicConstraints;
import org.bouncycastle.asn1.x509.ExtendedKeyUsage;
import org.bouncycastle.asn1.x509.Extension;
import org.bouncycastle.asn1.x509.ExtensionsGenerator;
import org.bouncycastle.asn1.x509.GeneralNames;
import org.bouncycastle.asn1.x509.KeyUsage;
import org.bouncycastle.asn1.x509.Time;
import org.bouncycastle.cert.X509CertificateHolder;
import org.bouncycastle.cert.jcajce.JcaX509CertificateConverter;
import org.bouncycastle.cert.jcajce.JcaX509ExtensionUtils;
import org.bouncycastle.cert.jcajce.JcaX509v3CertificateBuilder;
import org.bouncycastle.jce.provider.BouncyCastleProvider;
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
 * This class is the boundary between security-cli and Bouncy Castle: its public API only uses JDK types and the small set of
 * value types in this package ({@link SubjectAlternativeNames}, {@link KeyPurpose}, {@link CertificateSigningRequest}), so that the
 * (relocated) Bouncy Castle classes never appear in callers' signatures or imports.
 */
public class CertGenUtils {

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
     *
     * @param keyUsages the key usage names (see {@link #KEY_USAGE_MAPPINGS}) to add as a critical X509v3 extension; may be {@code null}
     *                  or empty in which case no key usage extension is added
     */
    public static X509Certificate generateCACertificate(
        X500Principal x500Principal,
        KeyPair keyPair,
        int days,
        Collection<String> keyUsages
    ) throws GeneralSecurityException, IOException {
        return generateSignedCertificate(x500Principal, null, keyPair, null, null, true, days, null, keyUsages, Set.of());
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
    ) throws GeneralSecurityException, IOException {
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
     * @param keyUsages          the key usage names that should be added to the certificate as a X509v3 extension (can be {@code null})
     * @param extendedKeyUsages  the extended key usages that should be added to the certificate as a X509v3 extension (can be empty)
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
        Collection<String> keyUsages,
        Set<KeyPurpose> extendedKeyUsages
    ) throws GeneralSecurityException, IOException {
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
            keyUsages,
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
    ) throws GeneralSecurityException, IOException {
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
        Collection<String> keyUsages,
        Set<KeyPurpose> extendedKeyUsages
    ) throws GeneralSecurityException, IOException {
        final KeyUsage keyUsage = buildKeyUsage(keyUsages);
        final GeneralNames generalNames = subjectAltNames == null ? null : subjectAltNames.toGeneralNames();
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
        if (generalNames != null) {
            builder.addExtension(Extension.subjectAlternativeName, false, generalNames);
        }
        builder.addExtension(Extension.basicConstraints, isCa, new BasicConstraints(isCa));

        if (keyUsage != null) {
            // as per RFC 5280 (section 4.2.1.3), if the key usage is present, then it SHOULD be marked as critical.
            final boolean isCritical = true;
            builder.addExtension(Extension.keyUsage, isCritical, keyUsage);
        }
        if (extendedKeyUsages != null) {
            for (KeyPurpose keyPurpose : extendedKeyUsages) {
                builder.addExtension(Extension.extendedKeyUsage, false, new ExtendedKeyUsage(keyPurpose.keyPurposeId()));
            }
        }

        PrivateKey signingKey = caPrivKey != null ? caPrivKey : keyPair.getPrivate();
        final ContentSigner signer;
        try {
            signer = new JcaContentSignerBuilder(
                (Strings.isNullOrEmpty(signatureAlgorithm)) ? getDefaultSignatureAlgorithm(signingKey) : signatureAlgorithm
            ).setProvider(CertGenUtils.BC_PROV).build(signingKey);
        } catch (OperatorCreationException e) {
            throw new GeneralSecurityException("failed to create content signer", e);
        }
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
     * @param keyUsages the key usage names that should be added to the request as a X509v3 extension (can be {@code null})
     * @param extendedKeyUsages the extended key usages that should be added to the certificate as an X509v3 extension. May be empty.
     * @return a certificate signing request
     */
    public static CertificateSigningRequest generateCSR(
        KeyPair keyPair,
        X500Principal principal,
        SubjectAlternativeNames sanList,
        Collection<String> keyUsages,
        Set<KeyPurpose> extendedKeyUsages
    ) throws IOException, GeneralSecurityException {
        Objects.requireNonNull(keyPair, "Key-Pair must not be null");
        Objects.requireNonNull(keyPair.getPublic(), "Public-Key must not be null");
        Objects.requireNonNull(principal, "Principal must not be null");
        Objects.requireNonNull(extendedKeyUsages, "extendedKeyUsages must not be null");
        final KeyUsage keyUsage = buildKeyUsage(keyUsages);
        final GeneralNames generalNames = sanList == null ? null : sanList.toGeneralNames();
        JcaPKCS10CertificationRequestBuilder builder = new JcaPKCS10CertificationRequestBuilder(principal, keyPair.getPublic());

        ExtensionsGenerator extGen = new ExtensionsGenerator();
        if (generalNames != null) {
            extGen.addExtension(Extension.subjectAlternativeName, false, generalNames);
        }
        if (keyUsage != null) {
            extGen.addExtension(Extension.keyUsage, true, keyUsage);
        }
        for (KeyPurpose keyPurpose : extendedKeyUsages) {
            extGen.addExtension(Extension.extendedKeyUsage, false, new ExtendedKeyUsage(keyPurpose.keyPurposeId()));
        }

        if (extGen.isEmpty() == false) {
            builder.addAttribute(PKCSObjectIdentifiers.pkcs_9_at_extensionRequest, extGen.generate());
        }

        try {
            return new CertificateSigningRequest(
                builder.build(new JcaContentSignerBuilder("SHA256withRSA").setProvider(CertGenUtils.BC_PROV).build(keyPair.getPrivate()))
            );
        } catch (OperatorCreationException e) {
            throw new GeneralSecurityException("failed to create content signer", e);
        }
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
     * Converts the {@link InetAddress} objects into a {@link SubjectAlternativeNames} object that is used to represent subject
     * alternative names.
     */
    public static SubjectAlternativeNames getSubjectAlternativeNames(boolean resolveName, Set<InetAddress> addresses) throws IOException {
        final SubjectAlternativeNames.Builder builder = SubjectAlternativeNames.builder();
        for (InetAddress address : addresses) {
            if (address.isAnyLocalAddress()) {
                // it is a wildcard address
                for (InetAddress inetAddress : NetworkUtils.getAllAddresses()) {
                    addSubjectAlternativeNames(resolveName, inetAddress, builder);
                }
            } else {
                addSubjectAlternativeNames(resolveName, address, builder);
            }
        }
        return builder.build();
    }

    @SuppressForbidden(reason = "need to use getHostName to resolve DNS name and getHostAddress to ensure we resolved the name")
    private static void addSubjectAlternativeNames(boolean resolveName, InetAddress inetAddress, SubjectAlternativeNames.Builder builder) {
        String hostaddress = inetAddress.getHostAddress();
        String ip = NetworkAddress.format(inetAddress);
        builder.addIpAddress(ip);
        if (resolveName && (inetAddress.isLinkLocalAddress() == false)) {
            String possibleHostName = inetAddress.getHostName();
            if (possibleHostName.equals(hostaddress) == false) {
                builder.addDnsName(possibleHostName);
            }
        }
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
     * @return whether the string only contains characters permitted in an ASN.1 {@code IA5String}, which is the encoding used for
     * DNS names in X.509 subject alternative names
     */
    public static boolean isIA5String(String value) {
        return DERIA5String.isIA5String(value);
    }

    /**
     * Converts a collection of key usage names (see {@link #KEY_USAGE_MAPPINGS}) into a {@link KeyUsage} extension value.
     *
     * @return {@code null} if the collection is {@code null} or empty
     * @throws IllegalArgumentException if any name is not a known key usage
     */
    private static KeyUsage buildKeyUsage(Collection<String> keyUsages) {
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
