/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.xpack.security.authc.saml;

import org.apache.logging.log4j.Logger;
import org.elasticsearch.ElasticsearchSecurityException;
import org.elasticsearch.ExceptionsHelper;
import org.elasticsearch.common.collect.Tuple;
import org.elasticsearch.common.logging.Loggers;
import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.xpack.core.ssl.CertUtils;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.opensaml.security.credential.Credential;
import org.opensaml.security.x509.X509Credential;
import org.opensaml.security.x509.impl.X509KeyManagerX509CredentialAdapter;

import javax.security.auth.x500.X500Principal;

import java.io.IOException;
import java.security.KeyPair;
import java.security.KeyPairGenerator;
import java.security.KeyStoreException;
import java.security.NoSuchAlgorithmException;
import java.security.PrivateKey;
import java.security.UnrecoverableKeyException;
import java.security.cert.Certificate;
import java.security.cert.CertificateException;
import java.security.cert.X509Certificate;
import java.util.Arrays;
import java.util.List;
import java.util.Locale;
import java.util.stream.Collectors;

import static org.hamcrest.Matchers.is;

public abstract class SamlTestCase extends ESTestCase {

    private static Locale restoreLocale;

    @BeforeClass
    public static void setupSaml() throws Exception {
        Logger logger = Loggers.getLogger(SamlTestCase.class);
        if (isTurkishLocale()) {
            // See: https://github.com/elastic/x-pack-elasticsearch/issues/2815
            logger.warn("Attempting to run SAML test on turkish-like locale, but that breaks OpenSAML. Switching to English.");
            restoreLocale = Locale.getDefault();
            Locale.setDefault(Locale.ENGLISH);
        }
        SamlUtils.initialize(logger);
    }

    private static boolean isTurkishLocale() {
        return Locale.getDefault().getLanguage().equals(new Locale("tr").getLanguage())
                || Locale.getDefault().getLanguage().equals(new Locale("az").getLanguage());
    }

    @AfterClass
    public static void restoreLocale() throws Exception {
        if (restoreLocale != null) {
            Locale.setDefault(restoreLocale);
            restoreLocale = null;
        }
    }

    /**
     * Generates signed certificate and associates with generated key pair.
     * @see #createKeyPair(String)
     * @return X509Certificate a signed certificate, it's PrivateKey {@link Tuple}
     * @throws Exception
     */
    protected static Tuple<X509Certificate, PrivateKey> createKeyPair() throws Exception {
        return createKeyPair("RSA");
    }

    /**
     * Generates key pair for given algorithm and then associates with a certificate.
     * For testing, for "EC" algorithm 256 key size is used, others use 2048 as default.
     * @param algorithm
     * @return X509Certificate a signed certificate, it's PrivateKey {@link Tuple}
     * @throws Exception
     */
    protected static Tuple<X509Certificate, PrivateKey> createKeyPair(String algorithm) throws Exception {
        KeyPairGenerator keyPairGenerator = KeyPairGenerator.getInstance(algorithm);
        final boolean useBigKeySizes = rarely();
        switch (algorithm) {
            case "EC":
                keyPairGenerator.initialize(randomFrom(256, 384));
                break;
            case "RSA":
                keyPairGenerator.initialize(randomFrom(Arrays.stream(new int[] { 1024, 2048, 4096 }).boxed()
                        .filter((ksize) -> (ksize <= 2048 || useBigKeySizes)).collect(Collectors.toList())));
                break;
            case "DSA":
                keyPairGenerator.initialize(randomFrom(Arrays.stream(new int[] { 1024, 2048, 3072 }).boxed()
                        .filter((ksize) -> (ksize <= 2048 || useBigKeySizes)).collect(Collectors.toList())));
                break;
            default:
                keyPairGenerator.initialize(randomFrom(1024, 2048));
        }
        final KeyPair pair = keyPairGenerator.generateKeyPair();
        final String name = randomAlphaOfLength(8);
        final X509Certificate cert = CertUtils.generateSignedCertificate(new X500Principal("CN=test-" + name), null, pair, null, null, 30);
        return new Tuple<>(cert, pair.getPrivate());
    }

    protected static List<Credential> buildOpenSamlCredential(final Tuple<X509Certificate, PrivateKey> keyPair) {
        try {
            return Arrays.asList(new X509KeyManagerX509CredentialAdapter(
                    CertUtils.keyManager(new Certificate[] { keyPair.v1() }, keyPair.v2(), new char[0]), "key"));
        } catch (Exception e) {
            throw ExceptionsHelper.convertToRuntime(e);
        }
    }

    protected static List<Credential> buildOpenSamlCredential(final List<Tuple<X509Certificate, PrivateKey>> keyPairs) {
        final List<Credential> credentials = keyPairs.stream().map((keyPair) -> {
            try {
                return new X509KeyManagerX509CredentialAdapter(
                        CertUtils.keyManager(new Certificate[] { keyPair.v1() }, keyPair.v2(), new char[0]), "key");
            } catch (Exception e) {
                throw ExceptionsHelper.convertToRuntime(e);
            }
        }).collect(Collectors.toList());
        return credentials;
    }

    protected ElasticsearchSecurityException expectSamlException(ThrowingRunnable runnable) {
        final ElasticsearchSecurityException exception = expectThrows(ElasticsearchSecurityException.class, runnable);
        assertThat("Exception " + exception + " should be a SAML exception", SamlUtils.isSamlException(exception), is(true));
        return exception;
    }
}
