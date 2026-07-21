/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.test.fixtures.tls;

import org.bouncycastle.asn1.x509.BasicConstraints;
import org.bouncycastle.asn1.x509.ExtendedKeyUsage;
import org.bouncycastle.asn1.x509.Extension;
import org.bouncycastle.asn1.x509.GeneralName;
import org.bouncycastle.asn1.x509.GeneralNames;
import org.bouncycastle.asn1.x509.KeyPurposeId;
import org.bouncycastle.asn1.x509.KeyUsage;
import org.bouncycastle.cert.jcajce.JcaX509CertificateConverter;
import org.bouncycastle.cert.jcajce.JcaX509ExtensionUtils;
import org.bouncycastle.cert.jcajce.JcaX509v3CertificateBuilder;
import org.bouncycastle.operator.jcajce.JcaContentSignerBuilder;
import org.elasticsearch.common.ssl.PemUtils;

import java.io.ByteArrayInputStream;
import java.io.InputStream;
import java.math.BigInteger;
import java.nio.charset.StandardCharsets;
import java.security.KeyFactory;
import java.security.PrivateKey;
import java.security.Provider;
import java.security.SecureRandom;
import java.security.Security;
import java.security.cert.CertificateEncodingException;
import java.security.cert.X509Certificate;
import java.security.interfaces.RSAPrivateCrtKey;
import java.security.spec.RSAPublicKeySpec;
import java.time.Instant;
import java.time.temporal.ChronoUnit;
import java.util.Arrays;
import java.util.Base64;
import java.util.Date;

import javax.security.auth.x500.X500Principal;

import static org.elasticsearch.test.ESTestCase.asInstanceOf;
import static org.elasticsearch.test.ESTestCase.inFipsJvm;

/**
 * A self-signed TLS certificate and private key for HTTPS test fixtures.
 */
public record TestTlsCertificate(X509Certificate certificate, PrivateKey privateKey) {

    private static final String FIPS_PROVIDER_NAME = "BCFIPS";

    private static Provider bcProvider() {
        if (inFipsJvm()) {
            final Provider provider = Security.getProvider(FIPS_PROVIDER_NAME);
            if (provider == null) {
                throw new AssertionError(FIPS_PROVIDER_NAME + " security provider is not available");
            }
            return provider;
        }
        try {
            final Class<?> providerClass = Class.forName("org.bouncycastle.jce.provider.BouncyCastleProvider");
            final Provider provider = (Provider) providerClass.getConstructor().newInstance();
            if (Security.getProvider(provider.getName()) == null) {
                Security.addProvider(provider);
            }
            return Security.getProvider(provider.getName());
        } catch (ReflectiveOperationException e) {
            throw new AssertionError("failed to initialize Bouncy Castle provider", e);
        }
    }

    /**
     * Base64-encoded PKCS#8 representation of a test-only 2048-bit RSA private key. Must not be used outside of tests.
     * <p>
     * RSA private keys contain only the raw key material, no extra metadata, so there's no issue with hard-coding this: all the interesting
     * stuff relates to the metadata attached to the certificate which is generated programmatically below. Moreover, generating a fresh key
     * each time can be a little costly so there is an advantage to fixing this key here.
     */
    private static final String PKCS8_PRIVATE_KEY = """
        MIIEvAIBADANBgkqhkiG9w0BAQEFAASCBKYwggSiAgEAAoIBAQCqaV+24/U47HEVkcB6Rn4hpfOOESs59KqqWF4itkg78E8MyNWmDgGC47C/29x6ysVKlEd0BYhdwpjeHLd\
        2cfRyFV2asa8Zt14TuYnoY9cjYErndCSguLaEJU4t4EN8LWJDr0RJZaVXR5hzwNvaMNHf7p2zvAM+RTWcqPC7GwUDsXQk3+uIFJRfBMwLrTiWElzJ5KmIlmaxAui7X2Gq2X\
        JQhwcW43xWvtBEx5a9HbqZrbVrT0VF7fOacgf5fG55VgGL65fkPtcbPJGYqPsqFTe87xcD11wTxtnWgJIighheYZv4HmkrUCTY4xzhDfInkKnF/QDKF40NszWySLotYD97A\
        gMBAAECggEANm/t9wcwNWx2nXzPf2AYd9hDCv2aEOwDPuJ2w+D2B0u3fO6FLYQo4G6q7kcmUgWHa9EdWEdSLh11ZvLGeqxhebYwjjO3q1/jyipJjzahqbffhbuY3czT5Tfj\
        lw0ekcMPsm6BQm8Zl92TqlqQAM36pW3c6+ciJBjIzRA8wpCZk/ltFPSAu8xL144tDusVTYj0lDQndNwHmwFNpvjAzNqhW2O/EOnA28M8b7ILKFBqVsR3oYg8c/wvmhoEezi\
        3KgRTnrelEINw4toxNuYVFZUGovv+UJHlUhe/hg8H9VleX/EKEJQ4EwuTv718/L1Ku+cshW4CjgbQEC6YAUtx13xIFQKBgQDVzdXs7nKAJOuaUOPJul8wo8+v9MJCp2uWCc\
        cTvEfR6LNUpCp+tXT4Ga5oeArhQsb1aNuUQAzXowuQYSMXGR4niU7tp9ErwfmHVD3hpUAXCxXR7Kf39ErUFVeQjRFq2WUXZSjcDou9omp8dBntH/V2SXueNSB84XQLE1/tA\
        jYEtQKBgQDMCzGReTXKvP/R16St0aOhFWAmoaij1QxhS/gKXVZcX+oFFt+7YNuYEeDXhQ/hvnaEKmPq4iEEm3sNUxAC3XUyrmZ/WbPGHLz+3p9Zq9avIgI+C+PUTZh1FLay\
        xPj+QeRpBEj+Uogx4efDtPE888YEvMBJSaqeV5ZLiKrNu2GBbwKBgAZQCIPxWmIcNPSedMtM/GiEPaqVUHMFXHDWoxEbGwfAliLmofaRxv1YTMT63l9eSF+QlAMhjP+E6d9\
        +brnM3Q4PyMvNi6h+Fq7/NsCFz4meoytKkH8KsHbolmhHMf90ob56FyXALDISLJC2INnWerneW48FRItLYNC+5rMpwIyNAoGAUl/qgQpoNxMaTEM8zpel8bBJgw8coBewyc\
        77smOALAbk2W0koec2gCwnk5q6kK5t9mmOsRLdtZh3kyeHfUCewfk6lAtI0qBjhJmnx3HiWA3ozdfLALja5dmY8I8o3q0HY4ZBWbtEFK9Y+9+ezLa1qM/y6SN+aDKAELp6C\
        litwq0CgYAK2O3hjmdpJ7O7XPgf+dz+MMsUA79Z6HcIpC36kOcridmkFI/qlW3aC9BL9Al8lt7SmtFbJFIKEjctkYvO7VHT1oq8OOdZhXFjc+QG7ffPb++3itdXMuyjYHts\
        Bp6tNyzTSBmdLfl06Irf4MK3zW6690Rc7JVHSrPNWDQvpNgYGQ==""";

    public static TestTlsCertificate generate(String... dnsNames) {
        if (dnsNames == null || dnsNames.length == 0) {
            throw new AssertionError("at least one DNS name is required");
        }
        if (Arrays.stream(dnsNames).anyMatch(name -> name == null || name.isEmpty())) {
            throw new AssertionError("DNS names must not be null or empty");
        }
        try {
            final var provider = bcProvider();
            final var privateKey = asInstanceOf(RSAPrivateCrtKey.class, PemUtils.parsePKCS8PemString(PKCS8_PRIVATE_KEY));
            final var publicKey = KeyFactory.getInstance("RSA", provider)
                .generatePublic(new RSAPublicKeySpec(privateKey.getModulus(), privateKey.getPublicExponent()));

            final X500Principal principal = new X500Principal("CN=" + dnsNames[0]);
            final var builder = new JcaX509v3CertificateBuilder(
                principal,
                new BigInteger(160, new SecureRandom()),
                Date.from(Instant.now()),
                Date.from(Instant.now().plus(1, ChronoUnit.DAYS)),
                principal,
                publicKey
            );
            final var extensionUtils = new JcaX509ExtensionUtils();
            builder.addExtension(Extension.subjectKeyIdentifier, false, extensionUtils.createSubjectKeyIdentifier(publicKey));
            builder.addExtension(Extension.authorityKeyIdentifier, false, extensionUtils.createAuthorityKeyIdentifier(publicKey));
            builder.addExtension(Extension.subjectAlternativeName, false, dnsSubjectAlternativeNames(dnsNames));
            builder.addExtension(Extension.basicConstraints, true, new BasicConstraints(false));
            builder.addExtension(Extension.keyUsage, true, new KeyUsage(KeyUsage.digitalSignature | KeyUsage.keyEncipherment));
            builder.addExtension(Extension.extendedKeyUsage, false, new ExtendedKeyUsage(KeyPurposeId.id_kp_serverAuth));
            return new TestTlsCertificate(
                new JcaX509CertificateConverter().setProvider(provider)
                    .getCertificate(builder.build(new JcaContentSignerBuilder("SHA256withRSA").setProvider(provider).build(privateKey))),
                privateKey
            );
        } catch (Exception e) {
            throw new AssertionError("failed to generate test TLS certificate", e);
        }
    }

    public InputStream getPemCertificateStream() {
        try {
            return new ByteArrayInputStream(
                ("-----BEGIN CERTIFICATE-----\n"
                    + Base64.getMimeEncoder(64, new byte[] { '\n' }).encodeToString(certificate.getEncoded())
                    + "\n-----END CERTIFICATE-----\n").getBytes(StandardCharsets.US_ASCII)
            );
        } catch (CertificateEncodingException e) {
            throw new AssertionError("failed to encode certificate as PEM", e);
        }
    }

    /** Returns the private key as an unencrypted PKCS#8 PEM stream. */
    public InputStream getPemPrivateKeyStream() {
        return new ByteArrayInputStream(
            ("-----BEGIN PRIVATE KEY-----\n"
                + Base64.getMimeEncoder(64, new byte[] { '\n' }).encodeToString(privateKey.getEncoded())
                + "\n-----END PRIVATE KEY-----\n").getBytes(StandardCharsets.US_ASCII)
        );
    }

    private static GeneralNames dnsSubjectAlternativeNames(String[] dnsNames) {
        final GeneralName[] names = new GeneralName[dnsNames.length];
        for (int i = 0; i < dnsNames.length; i++) {
            names[i] = new GeneralName(GeneralName.dNSName, dnsNames[i]);
        }
        return new GeneralNames(names);
    }
}
