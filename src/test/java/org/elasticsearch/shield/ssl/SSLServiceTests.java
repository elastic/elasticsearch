/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.shield.ssl;

import org.elasticsearch.common.settings.ImmutableSettings;
import org.elasticsearch.test.ElasticsearchTestCase;
import org.junit.Before;
import org.junit.Test;

import javax.net.ssl.SSLContext;
import javax.net.ssl.SSLEngine;
import java.nio.file.Path;
import java.nio.file.Paths;

import static org.elasticsearch.common.settings.ImmutableSettings.settingsBuilder;
import static org.hamcrest.Matchers.*;

public class SSLServiceTests extends ElasticsearchTestCase {

    Path testnodeStore;

    @Before
    public void setup() throws Exception {
        testnodeStore = Paths.get(getClass().getResource("/org/elasticsearch/shield/transport/ssl/certs/simple/testnode.jks").toURI());
    }

    @Test(expected = ElasticsearchSSLException.class)
    public void testThatInvalidProtocolThrowsException() throws Exception {
        new SSLService(settingsBuilder()
                            .put("shield.ssl.protocol", "non-existing")
                            .put("shield.ssl.keystore.path", testnodeStore)
                            .put("shield.ssl.keystore.password", "testnode")
                            .put("shield.ssl.truststore.path", testnodeStore)
                            .put("shield.ssl.truststore.password", "testnode")
                        .build()).createSSLEngine();
    }

    @Test
    public void testThatCustomTruststoreCanBeSpecified() throws Exception {
        Path testClientStore = Paths.get(getClass().getResource("/org/elasticsearch/shield/transport/ssl/certs/simple/testclient.jks").toURI());

        SSLService sslService = new SSLService(settingsBuilder()
                .put("shield.ssl.keystore.path", testnodeStore)
                .put("shield.ssl.keystore.password", "testnode")
                .build());

        ImmutableSettings.Builder settingsBuilder = settingsBuilder()
                .put("truststore.path", testClientStore)
                .put("truststore.password", "testclient");

        SSLEngine sslEngineWithTruststore = sslService.createSSLEngine(settingsBuilder.build());
        assertThat(sslEngineWithTruststore, is(not(nullValue())));

        SSLEngine sslEngine = sslService.createSSLEngine();
        assertThat(sslEngineWithTruststore, is(not(sameInstance(sslEngine))));
    }

    @Test
    public void testThatSslContextCachingWorks() throws Exception {
        SSLService sslService = new SSLService(settingsBuilder()
            .put("shield.ssl.keystore.path", testnodeStore)
            .put("shield.ssl.keystore.password", "testnode")
            .build());

        SSLContext sslContext = sslService.getSslContext();
        SSLContext cachedSslContext = sslService.getSslContext();

        assertThat(sslContext, is(sameInstance(cachedSslContext)));
    }

    @Test
    public void testThatKeyStoreAndKeyCanHaveDifferentPasswords() throws Exception {
        Path differentPasswordsStore = Paths.get(getClass().getResource("/org/elasticsearch/shield/transport/ssl/certs/simple/testnode-different-passwords.jks").toURI());
        new SSLService(settingsBuilder()
                .put("shield.ssl.keystore.path", differentPasswordsStore)
                .put("shield.ssl.keystore.password", "testnode")
                .put("shield.ssl.keystore.key_password", "testnode1")
                .build()).createSSLEngine();
    }

    @Test(expected = ElasticsearchSSLException.class)
    public void testIncorrectKeyPasswordThrowsException() throws Exception {
        Path differentPasswordsStore = Paths.get(getClass().getResource("/org/elasticsearch/shield/transport/ssl/certs/simple/testnode-different-passwords.jks").toURI());
        new SSLService(settingsBuilder()
                .put("shield.ssl.keystore.path", differentPasswordsStore)
                .put("shield.ssl.keystore.password", "testnode")
                .build()).createSSLEngine();
    }
}
