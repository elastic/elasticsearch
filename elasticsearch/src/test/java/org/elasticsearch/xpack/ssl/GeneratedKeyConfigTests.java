/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.xpack.ssl;

import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.node.Node;
import org.elasticsearch.test.ESTestCase;

import javax.net.ssl.X509ExtendedKeyManager;

import java.security.PrivateKey;
import java.security.cert.X509Certificate;
import java.security.interfaces.RSAPrivateKey;

import static org.hamcrest.Matchers.empty;
import static org.hamcrest.Matchers.instanceOf;
import static org.hamcrest.Matchers.is;

public class GeneratedKeyConfigTests extends ESTestCase {

    public void testGenerating() throws Exception {
        Settings settings = Settings.builder().put(Node.NODE_NAME_SETTING.getKey(), randomAsciiOfLengthBetween(1, 8)).build();
        GeneratedKeyConfig keyConfig = new GeneratedKeyConfig(settings);
        assertThat(keyConfig.filesToMonitor(null), is(empty()));
        X509ExtendedKeyManager keyManager = keyConfig.createKeyManager(null);
        assertNotNull(keyManager);
        assertNotNull(keyConfig.createTrustManager(null));

        String[] aliases = keyManager.getServerAliases("RSA", null);
        assertEquals(1, aliases.length);
        PrivateKey privateKey = keyManager.getPrivateKey(aliases[0]);
        assertNotNull(privateKey);
        assertThat(privateKey, instanceOf(RSAPrivateKey.class));
        X509Certificate[] certificates = keyManager.getCertificateChain(aliases[0]);
        assertEquals(2, certificates.length);
        assertEquals(GeneratedKeyConfig.readCACert(), certificates[1]);

        X509Certificate generatedCertificate = certificates[0];
        assertEquals("CN=" + Node.NODE_NAME_SETTING.get(settings), generatedCertificate.getSubjectX500Principal().getName());
        assertEquals(certificates[1].getSubjectX500Principal(), generatedCertificate.getIssuerX500Principal());
    }
}
