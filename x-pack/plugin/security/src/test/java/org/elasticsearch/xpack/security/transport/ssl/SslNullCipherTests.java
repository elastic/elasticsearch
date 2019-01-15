/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.xpack.security.transport.ssl;

import org.elasticsearch.action.DocWriteResponse.Result;
import org.elasticsearch.action.index.IndexResponse;
import org.elasticsearch.bootstrap.JavaVersion;
import org.elasticsearch.client.Client;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.test.SecurityIntegTestCase;
import org.junit.BeforeClass;

/**
 * An extremely simple test that shows SSL will work with a cipher that does not perform encryption
 */
public class SslNullCipherTests extends SecurityIntegTestCase {

    @BeforeClass
    public static void muteInFips() {
        assumeFalse("Can't run in a FIPS JVM", inFipsJvm());
    }

    @BeforeClass
    public static void muteInJDK12() {
        assumeFalse("https://github.com/elastic/elasticsearch/issues/37403", JavaVersion.current().equals(JavaVersion.parse("12")));
    }

    @Override
    public boolean transportSSLEnabled() {
        return true;
    }

    @Override
    public Settings nodeSettings(int nodeOrdinal) {
        Settings settings = super.nodeSettings(nodeOrdinal);
        Settings.Builder builder = Settings.builder()
                .put(settings);
        builder.put("xpack.security.transport.ssl.cipher_suites", "TLS_RSA_WITH_NULL_SHA256");
        return builder.build();
    }

    @Override
    public Settings transportClientSettings() {
        Settings settings = super.transportClientSettings();
        Settings.Builder builder = Settings.builder()
                .put(settings);

        builder.put("xpack.security.transport.ssl.cipher_suites", "TLS_RSA_WITH_NULL_SHA256");
        return builder.build();
    }

    public void testClusterIsFormed() {
        ensureGreen();
        Client client = internalCluster().transportClient();
        IndexResponse response = client.prepareIndex("index", "type").setSource("foo", "bar").get();
        assertEquals(Result.CREATED, response.getResult());
    }
}
