/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.core.ssl;

import org.elasticsearch.common.settings.SecureString;
import org.elasticsearch.env.Environment;
import org.elasticsearch.test.ESTestCase;
import org.hamcrest.Matchers;

import javax.net.ssl.X509ExtendedKeyManager;

import static org.hamcrest.Matchers.notNullValue;

public class PEMKeyConfigTests extends ESTestCase {

    public static final SecureString NO_PASSWORD = new SecureString("".toCharArray());
    public static final SecureString TESTNODE_PASSWORD = new SecureString("testnode".toCharArray());

    public void testEncryptedPkcs8RsaKey() throws Exception {
        assumeFalse("Can't run in a FIPS JVM, PBE KeySpec is not available", inFipsJvm());
        verifyKeyConfig("testnode.crt", "key_pkcs8_encrypted.pem", TESTNODE_PASSWORD);
    }

    public void testUnencryptedPkcs8RsaKey() throws Exception {
        verifyKeyConfig("testnode.crt", "rsa_key_pkcs8_plain.pem", NO_PASSWORD);
    }

    public void testUnencryptedPkcs8DsaKey() throws Exception {
        verifyKeyConfig("testnode_dsa.crt", "dsa_key_pkcs8_plain.pem", NO_PASSWORD);
    }

    public void testUnencryptedPkcs8EcKey() throws Exception {
        verifyKeyConfig("testnode_ec.crt", "ec_key_pkcs8_plain.pem", NO_PASSWORD);
    }

    public void testEncryptedPkcs1RsaKey() throws Exception {
        verifyKeyConfig("testnode.crt", "testnode-aes" + randomFrom(128, 192, 256) + ".pem", TESTNODE_PASSWORD);
    }

    public void testUnencryptedPkcs1RsaKey() throws Exception {
        verifyKeyConfig("testnode.crt", "testnode-unprotected.pem", NO_PASSWORD);
    }

    private void verifyKeyConfig(String certName, String keyName, SecureString keyPassword) throws Exception {
        final Environment env = newEnvironment();
        PEMKeyConfig config = new PEMKeyConfig(getPath(keyName), keyPassword, getPath(certName));
        assertThat(config.certificates(env), Matchers.iterableWithSize(1));
        X509ExtendedKeyManager keyManager = config.createKeyManager(env);
        assertThat(keyManager, notNullValue());
        assertThat(keyManager.getPrivateKey("key"), notNullValue());
    }

    private String getPath(String fileName) {
        return getDataPath("/org/elasticsearch/xpack/security/transport/ssl/certs/simple/" + fileName)
            .toAbsolutePath().toString();
    }

}
