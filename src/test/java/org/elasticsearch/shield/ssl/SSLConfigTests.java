/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.shield.ssl;

import org.elasticsearch.common.settings.ImmutableSettings;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.test.ElasticsearchTestCase;
import org.junit.Before;
import org.junit.Test;

import javax.net.ssl.SSLEngine;
import java.io.File;
import java.security.NoSuchAlgorithmException;

import static org.elasticsearch.common.settings.ImmutableSettings.settingsBuilder;
import static org.hamcrest.Matchers.instanceOf;
import static org.hamcrest.Matchers.is;

public class SSLConfigTests extends ElasticsearchTestCase {

    File testnodeStore;

    @Before
    public void setup() throws Exception {
        testnodeStore = new File(getClass().getResource("/certs/simple/testnode.jks").toURI());
    }

    @Test
    public void testThatInvalidContextAlgoThrowsException() throws Exception {
        try {
            new SSLConfig(ImmutableSettings.EMPTY,
                    settingsBuilder()
                            .put("shield.ssl.context_algorithm", "non-existing")
                            .put("shield.ssl.keystore", testnodeStore.getPath())
                            .put("shield.ssl.keystore_password", "testnode")
                            .put("shield.ssl.truststore", testnodeStore.getPath())
                            .put("shield.ssl.truststore_password", "testnode")
                        .build());
        } catch (ElasticsearchSSLException e) {
            assertThat(e.getRootCause(), instanceOf(NoSuchAlgorithmException.class));
        }
    }

    @Test
    public void testThatExactConfigOverwritesDefaultConfig() throws Exception {
        Settings concreteSettings = settingsBuilder()
                .put("ciphers", "TLS_RSA_WITH_AES_128_CBC_SHA")
                .build();

        Settings genericSettings = settingsBuilder()
                .putArray("shield.ssl.ciphers", "TLS_RSA_WITH_AES_128_CBC_SHA256", "TLS_RSA_WITH_AES_128_CBC_SHA")
                .put("shield.ssl.keystore", testnodeStore.getPath())
                .put("shield.ssl.keystore_password", "testnode")
                .put("shield.ssl.truststore", testnodeStore.getPath())
                .put("shield.ssl.truststore_password", "testnode")
                .build();

        SSLConfig sslConfig = new SSLConfig(concreteSettings, genericSettings);
        SSLEngine sslEngine = sslConfig.createSSLEngine();
        assertThat(sslEngine.getEnabledCipherSuites().length, is(1));
    }



}
