/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.xpack.core.ssl;

import org.elasticsearch.ElasticsearchException;
import org.elasticsearch.common.settings.SecureString;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.env.TestEnvironment;
import org.elasticsearch.test.ESTestCase;

import javax.net.ssl.KeyManagerFactory;
import javax.net.ssl.TrustManagerFactory;
import javax.net.ssl.X509ExtendedKeyManager;

import java.security.PrivateKey;

import static org.elasticsearch.test.TestMatchers.throwableWithMessage;
import static org.hamcrest.Matchers.containsString;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.notNullValue;

public class StoreKeyConfigTests extends ESTestCase {

    public void testCreateKeyManagerUsingJKS() throws Exception {
        assumeFalse("Can't run in a FIPS JVM", inFipsJvm());
        tryReadPrivateKeyFromKeyStore("jks", ".jks");
    }

    public void testCreateKeyManagerUsingPKCS12() throws Exception {
        assumeFalse("Can't run in a FIPS JVM", inFipsJvm());
        tryReadPrivateKeyFromKeyStore("PKCS12", ".p12");
    }

    public void testKeyStorePathCanBeEmptyForPkcs11() throws Exception {
        assumeFalse("Can't run in a FIPS JVM", inFipsJvm());
        final Settings settings = Settings.builder().put("path.home", createTempDir()).build();
        final SecureString keyStorePassword = new SecureString("password".toCharArray());
        final StoreKeyConfig keyConfig = new StoreKeyConfig(null, "PKCS12", keyStorePassword, keyStorePassword,
            KeyManagerFactory.getDefaultAlgorithm(), TrustManagerFactory.getDefaultAlgorithm());
        Exception e = expectThrows(IllegalArgumentException.class, () ->
            keyConfig.createKeyManager(TestEnvironment.newEnvironment(settings)));
        assertThat(e.getMessage(), equalTo("keystore.path or truststore.path can only be empty when using a PKCS#11 token"));
        final StoreKeyConfig keyConfigPkcs11 = new StoreKeyConfig(null, "PKCS11", keyStorePassword, keyStorePassword,
            KeyManagerFactory.getDefaultAlgorithm(), TrustManagerFactory.getDefaultAlgorithm());
        ElasticsearchException ee = expectThrows(ElasticsearchException.class, () ->
            keyConfigPkcs11.createKeyManager(TestEnvironment.newEnvironment(settings)));
        assertThat(ee, throwableWithMessage(containsString("failed to initialize SSL KeyManager")));
        assertThat(ee.getCause().getMessage(), containsString("PKCS11 not found"));
    }

    private void tryReadPrivateKeyFromKeyStore(String type, String extension) {
        final Settings settings = Settings.builder().put("path.home", createTempDir()).build();
        final String path = getDataPath("/org/elasticsearch/xpack/security/transport/ssl/certs/simple/testnode" + extension).toString();
        final SecureString keyStorePassword = new SecureString("testnode".toCharArray());
        final StoreKeyConfig keyConfig = new StoreKeyConfig(path, type, keyStorePassword, keyStorePassword,
                KeyManagerFactory.getDefaultAlgorithm(), TrustManagerFactory.getDefaultAlgorithm());
        final X509ExtendedKeyManager keyManager = keyConfig.createKeyManager(TestEnvironment.newEnvironment(settings));
        final PrivateKey key = keyManager.getPrivateKey("testnode_rsa");
        assertThat(key, notNullValue());
        assertThat(key.getAlgorithm(), equalTo("RSA"));
        assertThat(key.getFormat(), equalTo("PKCS#8"));
    }
}
