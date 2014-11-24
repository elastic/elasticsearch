/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.shield.ssl;

import org.elasticsearch.common.settings.ImmutableSettings;
import org.elasticsearch.test.ElasticsearchTestCase;
import org.junit.Before;
import org.junit.Ignore;
import org.junit.Test;

import javax.net.ssl.SSLEngine;
import java.io.File;

import static org.elasticsearch.common.settings.ImmutableSettings.settingsBuilder;
import static org.hamcrest.Matchers.*;

public class SSLServiceTests extends ElasticsearchTestCase {

    File testnodeStore;

    @Before
    public void setup() throws Exception {
        testnodeStore = new File(getClass().getResource("/org/elasticsearch/shield/transport/ssl/certs/simple/testnode.jks").toURI());
    }

    @Test(expected = ElasticsearchSSLException.class)
    public void testThatInvalidProtocolThrowsException() throws Exception {
        new SSLService(settingsBuilder()
                            .put("shield.ssl.protocol", "non-existing")
                            .put("shield.ssl.keystore.path", testnodeStore.getPath())
                            .put("shield.ssl.keystore.password", "testnode")
                            .put("shield.ssl.truststore.path", testnodeStore.getPath())
                            .put("shield.ssl.truststore.password", "testnode")
                        .build());
    }

    @Test @Ignore //TODO it appears that setting a specific protocol doesn't make a difference
    public void testSpecificProtocol() {
        SSLService ssl = new SSLService(settingsBuilder()
                .put("shield.ssl.protocol", "TLSv1.2")
                .put("shield.ssl.keystore.path", testnodeStore.getPath())
                .put("shield.ssl.keystore.password", "testnode")
                .put("shield.ssl.truststore.path", testnodeStore.getPath())
                .put("shield.ssl.truststore.password", "testnode")
                .build());
        assertThat(ssl.createSSLEngine().getSSLParameters().getProtocols(), arrayContaining("TLSv1.2"));
    }

    @Test
    public void testThatCustomTruststoreCanBeSpecified() throws Exception {
        File testClientStore = new File(getClass().getResource("/org/elasticsearch/shield/transport/ssl/certs/simple/testclient.jks").toURI());

        SSLService sslService = new SSLService(settingsBuilder()
                .put("shield.ssl.keystore.path", testnodeStore.getPath())
                .put("shield.ssl.keystore.password", "testnode")
                .build());

        ImmutableSettings.Builder settingsBuilder = settingsBuilder()
                .put("truststore.path", testClientStore.getPath())
                .put("truststore.password", "testclient");

        SSLEngine sslEngineWithTruststore = sslService.createSSLEngineWithTruststore(settingsBuilder.build());
        assertThat(sslEngineWithTruststore, is(not(nullValue())));
    }
}
