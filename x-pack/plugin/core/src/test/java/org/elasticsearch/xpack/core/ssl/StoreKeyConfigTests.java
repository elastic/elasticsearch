/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.xpack.core.ssl;

import org.elasticsearch.common.settings.SecureString;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.env.TestEnvironment;
import org.elasticsearch.test.ESTestCase;

import javax.net.ssl.KeyManagerFactory;
import javax.net.ssl.TrustManagerFactory;
import javax.net.ssl.X509ExtendedKeyManager;

import java.security.PrivateKey;

import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.notNullValue;

public class StoreKeyConfigTests extends ESTestCase {

    public void testCreateKeyManagerUsingJKS() throws Exception {
        tryReadPrivateKeyFromKeyStore("jks", ".jks");
    }

    public void testCreateKeyManagerUsingPKCS12() throws Exception {
        tryReadPrivateKeyFromKeyStore("PKCS12", ".p12");
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
