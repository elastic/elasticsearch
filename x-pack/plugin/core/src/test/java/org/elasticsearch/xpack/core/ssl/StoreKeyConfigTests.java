/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */
package org.elasticsearch.xpack.core.ssl;

import org.elasticsearch.common.settings.SecureString;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.env.TestEnvironment;
import org.elasticsearch.test.ESTestCase;

import java.io.InputStream;
import java.nio.file.Files;
import java.nio.file.Path;
import java.security.KeyStore;
import java.security.PrivateKey;
import java.security.cert.X509Certificate;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import javax.net.ssl.KeyManagerFactory;
import javax.net.ssl.TrustManagerFactory;
import javax.net.ssl.X509ExtendedKeyManager;

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

    public void testCreateKeyManagerFromPKCS12ContainingCA() throws Exception {
        assumeFalse("Can't run in a FIPS JVM", inFipsJvm());
        final Settings settings = Settings.builder().put("path.home", createTempDir()).build();
        final Path path = getDataPath("/org/elasticsearch/xpack/security/transport/ssl/certs/simple/httpCa.p12");
        final SecureString keyStorePassword = new SecureString("password".toCharArray());
        final StoreKeyConfig keyConfig = new StoreKeyConfig(path.toString(), "PKCS12", keyStorePassword, keyStorePassword,
            KeyManagerFactory.getDefaultAlgorithm(), TrustManagerFactory.getDefaultAlgorithm());
        KeyStore keyStore = KeyStore.getInstance("PKCS12");
        try (InputStream in = Files.newInputStream(path)) {
            keyStore.load(in, keyStorePassword.getChars());
        }
        List<String> aliases = new ArrayList<>();
        for (String s : Collections.list(keyStore.aliases())) {
            if (keyStore.isKeyEntry(s)) {
                aliases.add(s);
            }
        }
        assertThat(aliases.size(), equalTo(2));
        final X509ExtendedKeyManager keyManager = keyConfig.createKeyManager(TestEnvironment.newEnvironment(settings));
        for (String alias : aliases) {
            PrivateKey key = keyManager.getPrivateKey(alias);
            assertTrue(key == null || alias.equals("http"));
        }
        final String[] new_aliases = keyManager.getServerAliases("RSA", null);
        final X509Certificate[] certificates = keyManager.getCertificateChain("http");
        assertThat(new_aliases.length, equalTo(1));
        assertThat(certificates.length, equalTo(2));
    }

    public void testCreateKeyManagerFromPKCS12ContainingCAOnly() throws Exception {
        assumeFalse("Can't run in a FIPS JVM", inFipsJvm());
        final Settings settings = Settings.builder().put("path.home", createTempDir()).build();
        final String path = getDataPath("/org/elasticsearch/xpack/security/transport/ssl/certs/simple/ca.p12").toString();
        final SecureString keyStorePassword = new SecureString("password".toCharArray());
        final StoreKeyConfig keyConfig = new StoreKeyConfig(path, "PKCS12", keyStorePassword, keyStorePassword,
            KeyManagerFactory.getDefaultAlgorithm(), TrustManagerFactory.getDefaultAlgorithm());
        final X509ExtendedKeyManager keyManager = keyConfig.createKeyManager(TestEnvironment.newEnvironment(settings));
        final PrivateKey ca_key = keyManager.getPrivateKey("ca");
        final String[] aliases = keyManager.getServerAliases("RSA", null);
        final X509Certificate[] certificates = keyManager.getCertificateChain("ca");
        assertThat(ca_key, notNullValue());
        assertThat(aliases.length, equalTo(1));
        assertThat(aliases[0], equalTo("ca"));
        assertThat(certificates.length, equalTo(1));
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
