/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.security.cli;

import com.unboundid.util.ssl.cert.KeyUsageExtension;

import org.bouncycastle.asn1.x509.GeneralName;
import org.bouncycastle.asn1.x509.KeyPurposeId;
import org.elasticsearch.common.network.InetAddresses;
import org.elasticsearch.common.network.NetworkAddress;
import org.elasticsearch.core.SuppressForbidden;
import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.xpack.security.cli.bc.CertGenUtils;
import org.elasticsearch.xpack.security.cli.bc.KeyPurpose;
import org.elasticsearch.xpack.security.cli.bc.SubjectAlternativeNames;
import org.junit.BeforeClass;

import java.math.BigInteger;
import java.net.InetAddress;
import java.security.KeyPair;
import java.security.KeyPairGenerator;
import java.security.KeyStore;
import java.security.cert.X509Certificate;
import java.security.spec.ECGenParameterSpec;
import java.time.ZoneOffset;
import java.time.ZonedDateTime;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;

import javax.net.ssl.TrustManager;
import javax.net.ssl.TrustManagerFactory;
import javax.net.ssl.X509ExtendedTrustManager;
import javax.security.auth.x500.X500Principal;

import static org.elasticsearch.xpack.security.cli.bc.CertGenUtils.KEY_USAGE_MAPPINGS;
import static org.elasticsearch.xpack.security.cli.bc.CertGenUtils.isValidKeyUsage;
import static org.hamcrest.Matchers.contains;
import static org.hamcrest.Matchers.containsInAnyOrder;
import static org.hamcrest.Matchers.containsString;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.is;
import static org.hamcrest.Matchers.notNullValue;
import static org.hamcrest.Matchers.nullValue;

/**
 * Unit tests for cert utils
 */
public class CertGenUtilsTests extends ESTestCase {

    /**
     * The mapping of key usage names to their corresponding bit index as defined in {@code KeyUsage} class:
     *
     * <ul>
     * <li>digitalSignature        (0)</li>
     * <li>nonRepudiation          (1)</li>
     * <li>keyEncipherment         (2)</li>
     * <li>dataEncipherment        (3)</li>
     * <li>keyAgreement            (4)</li>
     * <li>keyCertSign             (5)</li>
     * <li>cRLSign                 (6)</li>
     * <li>encipherOnly            (7)</li>
     * <li>decipherOnly            (8)</li>
     * </ul>
     */
    private static final Map<String, Integer> KEY_USAGE_BITS = Map.ofEntries(
        Map.entry("digitalSignature", 0),
        Map.entry("nonRepudiation", 1),
        Map.entry("keyEncipherment", 2),
        Map.entry("dataEncipherment", 3),
        Map.entry("keyAgreement", 4),
        Map.entry("keyCertSign", 5),
        Map.entry("cRLSign", 6),
        Map.entry("encipherOnly", 7),
        Map.entry("decipherOnly", 8)
    );

    @BeforeClass
    public static void muteInFips() {
        assumeFalse("Can't run in a FIPS JVM", inFipsJvm());
    }

    public void testSerialNotRepeated() {
        int iterations = scaledRandomIntBetween(10, 100);
        List<BigInteger> list = new ArrayList<>(iterations);
        for (int i = 0; i < iterations; i++) {
            BigInteger serial = CertGenUtils.getSerial();
            assertThat(list.contains(serial), is(false));
            list.add(serial);
        }
    }

    public void testGenerateKeyPair() throws Exception {
        KeyPair keyPair = CertGenUtils.generateKeyPair(randomFrom(1024, 2048));
        assertThat(keyPair.getPrivate().getAlgorithm(), is("RSA"));
        assertThat(keyPair.getPublic().getAlgorithm(), is("RSA"));
    }

    public void testSubjectAlternativeNames() throws Exception {
        final boolean resolveName = randomBoolean();
        InetAddress address = InetAddresses.forString("127.0.0.1");

        SubjectAlternativeNames generalNames = CertGenUtils.getSubjectAlternativeNames(resolveName, Collections.singleton(address));
        assertThat(generalNames, notNullValue());
        // the subject alternative names are opaque, so inspect them through a certificate that carries them
        final KeyPair keyPair = CertGenUtils.generateKeyPair(2048);
        final X509Certificate certificate = CertGenUtils.generateSignedCertificate(
            new X500Principal("CN=test"),
            generalNames,
            keyPair,
            null,
            null,
            30
        );
        // each subject alternative name is a list of the GeneralName tag number followed by the name itself
        final List<Integer> generalNameTags = certificate.getSubjectAlternativeNames().stream().map(name -> (Integer) name.get(0)).toList();

        logger.info("resolve name [{}], address [{}], subject alt names [{}]", resolveName, NetworkAddress.format(address), generalNames);
        if (resolveName && isResolvable(address)) {
            assertThat(generalNameTags, containsInAnyOrder(GeneralName.iPAddress, GeneralName.dNSName));
        } else {
            assertThat(generalNameTags, contains(GeneralName.iPAddress));
        }
    }

    @SuppressForbidden(reason = "need to use getHostName to resolve DNS name and getHostAddress to ensure we resolved the name")
    private boolean isResolvable(InetAddress inetAddress) {
        String hostname = inetAddress.getHostName();
        return hostname.equals(inetAddress.getHostAddress()) == false;
    }

    public void testIssuerCertSubjectDN() throws Exception {
        final ZonedDateTime notBefore = ZonedDateTime.now(ZoneOffset.UTC);
        final ZonedDateTime notAfter = ZonedDateTime.parse("2099-12-31T23:23:59.999999+00:00");

        // root CA
        final X500Principal rootCaPrincipal = new X500Principal("DC=example.com");
        final KeyPair rootCaKeyPair = CertGenUtils.generateKeyPair(2048);
        final List<String> rootCaKeyUsages = List.of("keyCertSign", "cRLSign");
        final X509Certificate rootCaCert = CertGenUtils.generateSignedCertificate(
            rootCaPrincipal,
            null,
            rootCaKeyPair,
            null,
            rootCaKeyPair.getPrivate(),
            true,
            notBefore,
            notAfter,
            null,
            CertGenUtils.buildKeyUsage(rootCaKeyUsages),
            Set.of()
        );

        // sub CA
        final X500Principal subCaPrincipal = new X500Principal("DC=Sub CA,DC=example.com");
        final KeyPair subCaKeyPair = CertGenUtils.generateKeyPair(2048);
        final List<String> subCaKeyUsage = List.of("digitalSignature", "keyCertSign", "cRLSign");
        final X509Certificate subCaCert = CertGenUtils.generateSignedCertificate(
            subCaPrincipal,
            null,
            subCaKeyPair,
            rootCaCert,
            rootCaKeyPair.getPrivate(),
            true,
            notBefore,
            notAfter,
            null,
            CertGenUtils.buildKeyUsage(subCaKeyUsage),
            Set.of()
        );

        // end entity
        final X500Principal endEntityPrincipal = new X500Principal("CN=TLS Client\\+Server,DC=Sub CA,DC=example.com");
        final KeyPair endEntityKeyPair = CertGenUtils.generateKeyPair(2048);
        final List<String> endEntityKeyUsage = randomBoolean() ? null : List.of("digitalSignature", "keyEncipherment");
        final X509Certificate endEntityCert = CertGenUtils.generateSignedCertificate(
            endEntityPrincipal,
            null,
            endEntityKeyPair,
            subCaCert,
            subCaKeyPair.getPrivate(),
            true,
            notBefore,
            notAfter,
            null,
            CertGenUtils.buildKeyUsage(endEntityKeyUsage),
            Set.of(KeyPurpose.ANY)
        );

        final X509Certificate[] certChain = new X509Certificate[] { endEntityCert, subCaCert, rootCaCert };

        // verify generateSignedCertificate performed DN chaining correctly
        assertThat(endEntityCert.getIssuerX500Principal(), equalTo(subCaCert.getSubjectX500Principal()));
        assertThat(subCaCert.getIssuerX500Principal(), equalTo(rootCaCert.getSubjectX500Principal()));
        assertThat(rootCaCert.getIssuerX500Principal(), equalTo(rootCaCert.getSubjectX500Principal()));

        // verify custom extended key usage
        assertThat(endEntityCert.getExtendedKeyUsage(), equalTo(List.of(KeyPurposeId.anyExtendedKeyUsage.toASN1Primitive().toString())));

        // verify cert chaining based on PKIX rules (ex: SubjectDNs/IssuerDNs, SKIs/AKIs, BC, KU, EKU, etc)
        final KeyStore trustStore = KeyStore.getInstance("PKCS12", "SunJSSE"); // EX: SunJSSE, BC, BC-FIPS
        trustStore.load(null, null);
        trustStore.setCertificateEntry("trustAnchor", rootCaCert); // anchor: any part of the chain, or issuer of last entry in chain

        validateEndEntityTlsChain(trustStore, certChain, true, true);

        // verify custom key usages
        assertExpectedKeyUsage(rootCaCert, rootCaKeyUsages);
        assertExpectedKeyUsage(subCaCert, subCaKeyUsage);
        // when key usage is not specified, the key usage bits should be null
        if (endEntityKeyUsage == null) {
            assertThat(endEntityCert.getKeyUsage(), is(nullValue()));
            assertThat(endEntityCert.getCriticalExtensionOIDs().contains(KeyUsageExtension.KEY_USAGE_OID.toString()), is(false));
        } else {
            assertExpectedKeyUsage(endEntityCert, endEntityKeyUsage);
        }

    }

    /**
     * The signature algorithm is chosen from the CA key type, and the signer comes from the JVM's configured providers rather than from
     * the Bouncy Castle provider shaded into the bc module, so each supported key type is checked against the JDK providers.
     */
    public void testSignedCertificateForEachCaKeyType() throws Exception {
        final KeyPairGenerator rsa = KeyPairGenerator.getInstance("RSA");
        rsa.initialize(2048);
        final KeyPairGenerator ec = KeyPairGenerator.getInstance("EC");
        ec.initialize(new ECGenParameterSpec("secp256r1"));
        final KeyPairGenerator dsa = KeyPairGenerator.getInstance("DSA");
        dsa.initialize(2048);
        final Map<String, KeyPair> caKeyPairs = Map.of(
            "SHA256withRSA",
            rsa.generateKeyPair(),
            "SHA256withECDSA",
            ec.generateKeyPair(),
            "SHA256withDSA",
            dsa.generateKeyPair()
        );
        final KeyPair keyPair = CertGenUtils.generateKeyPair(2048);
        for (Map.Entry<String, KeyPair> entry : caKeyPairs.entrySet()) {
            final String expectedSignatureAlgorithm = entry.getKey();
            final KeyPair caKeyPair = entry.getValue();
            final X509Certificate caCert = CertGenUtils.generateSignedCertificate(
                new X500Principal("CN=" + caKeyPair.getPrivate().getAlgorithm() + " CA"),
                null,
                caKeyPair,
                null,
                null,
                true,
                30,
                null,
                CertGenUtils.buildKeyUsage(List.of("keyCertSign")),
                Set.of()
            );
            final X509Certificate cert = CertGenUtils.generateSignedCertificate(
                new X500Principal("CN=node"),
                null,
                keyPair,
                caCert,
                caKeyPair.getPrivate(),
                30
            );
            assertThat(caCert.getSigAlgName(), equalTo(expectedSignatureAlgorithm));
            assertThat(cert.getSigAlgName(), equalTo(expectedSignatureAlgorithm));
            caCert.verify(caCert.getPublicKey());
            cert.verify(caCert.getPublicKey());
            assertThat(cert.getIssuerX500Principal(), equalTo(caCert.getSubjectX500Principal()));
        }
    }

    public void testBuildKeyUsage() throws Exception {
        // sanity check that lookup maps are containing the same keyUsage entries
        assertThat(KEY_USAGE_BITS.keySet(), containsInAnyOrder(KEY_USAGE_MAPPINGS.keySet().toArray()));

        // the key usage extension is built from the names that are passed to generateSignedCertificate, so inspect it through a certificate
        final KeyPair keyPair = CertGenUtils.generateKeyPair(2048);

        // passing null or empty list of keyUsage names should not add a key usage extension
        assertThat(generateCertificateWithKeyUsage(keyPair, null).getKeyUsage(), is(nullValue()));
        assertThat(generateCertificateWithKeyUsage(keyPair, List.of()).getKeyUsage(), is(nullValue()));

        // invalid names should throw IAE
        var e = expectThrows(
            IllegalArgumentException.class,
            () -> generateCertificateWithKeyUsage(keyPair, List.of(randomAlphanumericOfLength(5)))
        );
        assertThat(e.getMessage(), containsString("Unknown keyUsage"));

        {
            final List<String> keyUsages = randomNonEmptySubsetOf(KEY_USAGE_MAPPINGS.keySet());
            // asserts that the bits of the given key usages are set, and that the bits of all the other key usages are not set
            assertExpectedKeyUsage(generateCertificateWithKeyUsage(keyPair, keyUsages), keyUsages);
        }

        {
            // test that duplicates and whitespaces are ignored
            final X509Certificate certificate = generateCertificateWithKeyUsage(
                keyPair,
                List.of("digitalSignature   ", "    nonRepudiation", "\tkeyEncipherment", "keyEncipherment\n")
            );
            assertExpectedKeyUsage(certificate, List.of("digitalSignature", "nonRepudiation", "keyEncipherment"));
        }
    }

    private static X509Certificate generateCertificateWithKeyUsage(KeyPair keyPair, List<String> keyUsage) throws Exception {
        return CertGenUtils.generateSignedCertificate(
            new X500Principal("CN=test"),
            null,
            keyPair,
            null,
            null,
            true,
            30,
            null,
            CertGenUtils.buildKeyUsage(keyUsage),
            Set.of()
        );
    }

    public void testIsValidKeyUsage() {
        assertThat(isValidKeyUsage(randomFrom(KEY_USAGE_MAPPINGS.keySet())), is(true));
        assertThat(isValidKeyUsage(randomAlphanumericOfLength(5)), is(false));

        // keyUsage names are case-sensitive
        assertThat(isValidKeyUsage("DigitalSignature"), is(false));

        // white-spaces are ignored
        assertThat(isValidKeyUsage("keyAgreement "), is(true));
        assertThat(isValidKeyUsage("keyCertSign\n"), is(true));
        assertThat(isValidKeyUsage("\tcRLSign  "), is(true));
    }

    public static void assertExpectedKeyUsage(X509Certificate certificate, List<String> expectedKeyUsage) {
        final boolean[] keyUsage = certificate.getKeyUsage();
        assertThat("Expected " + KEY_USAGE_BITS.size() + " bits for key usage", keyUsage.length, equalTo(KEY_USAGE_BITS.size()));
        final Set<Integer> expectedBitsToBeSet = expectedKeyUsage.stream().map(KEY_USAGE_BITS::get).collect(Collectors.toSet());

        for (int i = 0; i < keyUsage.length; i++) {
            if (expectedBitsToBeSet.contains(i)) {
                assertThat("keyUsage bit [" + i + "] expected to be set: " + expectedKeyUsage, keyUsage[i], equalTo(true));
            } else {
                assertThat("keyUsage bit [" + i + "] not expected to be set: " + expectedKeyUsage, keyUsage[i], equalTo(false));
            }
        }
        // key usage must be marked as critical
        assertThat(
            "keyUsage extension should be marked as critical",
            certificate.getCriticalExtensionOIDs().contains(KeyUsageExtension.KEY_USAGE_OID.toString()),
            is(true)
        );
    }

    /**
     * Perform PKIX TLS certificate chain validation. This validates trust and chain correctness, not server hostname verification.
     * Wrap TrustStore with TrustManager[], and select first element that implements the X509ExtendedTrustManager interface.
     * Use it to call checkClientTrusted() and/or checkServerTrusted().
     *
     * @param trustStore       TrustStore must contain at least one trust anchor (ex: root/sub/cross/link CA certs, self-signed end entity)
     * @param certChain        Cert chain to be validated using trust anchor(s) in TrustStore. Partial chains are supported.
     * @param doTlsClientCheck Flag to enable TLS client end entity validation checking.
     * @param doTlsServerCheck Flag to enable TLS server end entity validation checking.
     * @throws Exception X509ExtendedTrustManager initialization failed, or cert chain validation failed.
     */
    public static void validateEndEntityTlsChain(
        final KeyStore trustStore,
        final X509Certificate[] certChain,
        final boolean doTlsClientCheck,
        final boolean doTlsServerCheck
    ) throws Exception {
        final TrustManagerFactory trustManagerFactory = TrustManagerFactory.getInstance("PKIX", "SunJSSE");
        trustManagerFactory.init(trustStore);
        final TrustManager[] trustManagers = trustManagerFactory.getTrustManagers(); // Usually only 1 is returned

        X509ExtendedTrustManager x509ExtendedTrustManager = null;
        for (final TrustManager trustManager : trustManagers) {
            if (trustManager instanceof X509ExtendedTrustManager) {
                x509ExtendedTrustManager = (X509ExtendedTrustManager) trustManagers[0];
                break; // use the first TrustManager that implements the javax.net.ssl.X509ExtendedTrustManager interface
            }
        }
        if (null == x509ExtendedTrustManager) {
            throw new UnsupportedOperationException("Expected at least one javax.net.ssl.X509ExtendedTrustManager");
        }

        // TLS authType is a substring of cipher suite. It controls what OpenJDK EndEntityChecker.java checks in KU/EKU/etc extensions.

        // EKU=null|clientAuth, KU=digitalSignature, authType=DHE_DSS/DHE_RSA/ECDHE_ECDSA/ECDHE_RSA/RSA_EXPORT/UNKNOWN
        if (doTlsClientCheck) {
            x509ExtendedTrustManager.checkClientTrusted(certChain, "ECDHE_RSA");
        }

        // EKU=null|serverAuth, KU=digitalSignature, authType=DHE_DSS/DHE_RSA/ECDHE_ECDSA/ECDHE_RSA/RSA_EXPORT/UNKNOWN
        // EKU=null|serverAuth, KU=keyEncipherment, authType=RSA
        // EKU=null|serverAuth, KU=keyAgreement, authType=DH_DSS/DH_RSA/ECDH_ECDSA/ECDH_RSA
        if (doTlsServerCheck) {
            x509ExtendedTrustManager.checkServerTrusted(certChain, "ECDHE_RSA");
        }
    }
}
