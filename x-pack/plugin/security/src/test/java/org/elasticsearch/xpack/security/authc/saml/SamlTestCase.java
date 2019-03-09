/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.xpack.security.authc.saml;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.elasticsearch.ElasticsearchSecurityException;
import org.elasticsearch.ExceptionsHelper;
import org.elasticsearch.common.collect.Tuple;
import org.elasticsearch.common.io.PathUtils;
import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.xpack.core.ssl.CertParsingUtils;
import org.elasticsearch.xpack.core.ssl.PemUtils;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.opensaml.security.credential.Credential;
import org.opensaml.security.x509.impl.X509KeyManagerX509CredentialAdapter;

import java.nio.file.Path;
import java.security.PrivateKey;
import java.security.cert.Certificate;
import java.security.cert.X509Certificate;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.Locale;
import java.util.stream.Collectors;

import static org.hamcrest.Matchers.is;

public abstract class SamlTestCase extends ESTestCase {

    private static Locale restoreLocale;

    @BeforeClass
    public static void setupSaml() throws Exception {
        Logger logger = LogManager.getLogger(SamlTestCase.class);
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
    public static void restoreLocale() {
        if (restoreLocale != null) {
            Locale.setDefault(restoreLocale);
            restoreLocale = null;
        }
    }

    /**
     * Generates signed certificate and associates with generated key pair.
     * @see #readRandomKeyPair(String)
     * @return X509Certificate a signed certificate, it's PrivateKey {@link Tuple}
     */
    protected static Tuple<X509Certificate, PrivateKey> readRandomKeyPair() throws Exception {
        return readRandomKeyPair("RSA");
    }

    /**
     * Reads a key pair and associated certificate for given algorithm and key length
     * For testing, for "EC" algorithm 256 key size is used, others use 2048 as default.
     * @return X509Certificate a signed certificate, it's PrivateKey
     */
    protected static Tuple<X509Certificate, PrivateKey> readRandomKeyPair(String algorithm) throws Exception {
        int keySize;
        switch (algorithm) {
            case "EC":
                keySize = randomFrom(256, 384);
                break;
            case "RSA":
                keySize = randomFrom(1024, 2048, 4096);
                break;
            case "DSA":
                keySize = randomFrom(1024, 2048, 3072);
                break;
            default:
                keySize = randomFrom(1024, 2048);
        }
        Path keyPath = PathUtils.get(SamlTestCase.class.getResource
                ("/org/elasticsearch/xpack/security/authc/saml/saml_" + algorithm + "_" + keySize + ".key").toURI());
        Path certPath = PathUtils.get(SamlTestCase.class.getResource
                ("/org/elasticsearch/xpack/security/authc/saml/saml_" + algorithm + "_" + keySize + ".crt").toURI());
        X509Certificate certificate = CertParsingUtils.readX509Certificates(Collections.singletonList(certPath))[0];
        PrivateKey privateKey = PemUtils.readPrivateKey(keyPath, ""::toCharArray);
        return new Tuple<>(certificate, privateKey);
    }

    protected static Tuple<X509Certificate, PrivateKey> readKeyPair(String keyName) throws Exception {
        Path keyPath = PathUtils.get(SamlTestCase.class.getResource
            ("/org/elasticsearch/xpack/security/authc/saml/saml_" + keyName + ".key").toURI());
        Path certPath = PathUtils.get(SamlTestCase.class.getResource
            ("/org/elasticsearch/xpack/security/authc/saml/saml_" + keyName+ ".crt").toURI());
        X509Certificate certificate = CertParsingUtils.readX509Certificates(Collections.singletonList(certPath))[0];
        PrivateKey privateKey = PemUtils.readPrivateKey(keyPath, ""::toCharArray);
        return new Tuple<>(certificate, privateKey);
    }

    protected static List<Credential> buildOpenSamlCredential(final Tuple<X509Certificate, PrivateKey> keyPair) {
        try {
            return Arrays.asList(new X509KeyManagerX509CredentialAdapter(
                    CertParsingUtils.keyManager(new Certificate[]{keyPair.v1()}, keyPair.v2(), new char[0]), "key"));

        } catch (Exception e) {
            throw ExceptionsHelper.convertToRuntime(e);
        }
    }

    protected static List<Credential> buildOpenSamlCredential(final List<Tuple<X509Certificate, PrivateKey>> keyPairs) {
        final List<Credential> credentials = keyPairs.stream().map((keyPair) -> {
            try {
                return new X509KeyManagerX509CredentialAdapter(
                        CertParsingUtils.keyManager(new Certificate[]{keyPair.v1()}, keyPair.v2(), new char[0]), "key");
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
