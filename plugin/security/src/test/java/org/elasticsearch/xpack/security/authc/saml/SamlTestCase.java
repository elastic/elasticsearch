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
import org.opensaml.security.x509.X509Credential;
import org.opensaml.security.x509.impl.X509KeyManagerX509CredentialAdapter;

import javax.security.auth.x500.X500Principal;

import java.security.KeyPair;
import java.security.KeyPairGenerator;
import java.security.PrivateKey;
import java.security.cert.Certificate;
import java.security.cert.X509Certificate;
import java.util.Locale;

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

    protected static Tuple<X509Certificate, PrivateKey> createKeyPair() throws Exception {
        KeyPairGenerator keyPairGenerator = KeyPairGenerator.getInstance("RSA");
        keyPairGenerator.initialize(2048);
        final KeyPair pair = keyPairGenerator.generateKeyPair();
        final String name = randomAlphaOfLength(8);
        final X509Certificate cert = CertUtils.generateSignedCertificate(new X500Principal("CN=test-" + name), null, pair, null, null, 30);
        return new Tuple<>(cert, pair.getPrivate());
    }

    protected static X509Credential buildOpenSamlCredential(Tuple<X509Certificate, PrivateKey> keyPair) {
        try {
            final Certificate[] certificateChain = { keyPair.v1() };
            final PrivateKey privateKey = keyPair.v2();
            return new X509KeyManagerX509CredentialAdapter(CertUtils.keyManager(certificateChain, privateKey, new char[0]), "key");
        } catch (Exception e) {
            throw ExceptionsHelper.convertToRuntime(e);
        }
    }

    protected ElasticsearchSecurityException expectSamlException(ThrowingRunnable runnable) {
        final ElasticsearchSecurityException exception = expectThrows(ElasticsearchSecurityException.class, runnable);
        assertThat("Exception " + exception + " should be a SAML exception", SamlUtils.isSamlException(exception), is(true));
        return exception;
    }
}
