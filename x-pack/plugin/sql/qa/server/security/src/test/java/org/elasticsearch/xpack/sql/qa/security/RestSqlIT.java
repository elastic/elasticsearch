/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */
package org.elasticsearch.xpack.sql.qa.security;

import org.elasticsearch.common.settings.SecureString;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.util.concurrent.ThreadContext;
import org.elasticsearch.core.Booleans;
import org.elasticsearch.core.PathUtils;
import org.elasticsearch.test.rest.ESRestTestCase;
import org.elasticsearch.xpack.sql.qa.rest.RestSqlTestCase;

import java.net.URISyntaxException;
import java.nio.file.Files;
import java.nio.file.Path;

/**
 * Integration test for the rest sql action. The one that speaks json directly to a
 * user rather than to the JDBC driver or CLI.
 */
public class RestSqlIT extends RestSqlTestCase {
    static final boolean SSL_ENABLED = Booleans.parseBoolean(System.getProperty("tests.ssl.enabled"), false);

    static Settings securitySettings() {
        String token = basicAuthHeaderValue("test_admin", new SecureString("x-pack-test-password".toCharArray()));
        Settings.Builder builder = Settings.builder().put(ThreadContext.PREFIX + ".Authorization", token);
        if (SSL_ENABLED) {
            Path keyStore;
            try {
                keyStore = PathUtils.get(RestSqlIT.class.getResource("/test-node.jks").toURI());
            } catch (URISyntaxException e) {
                throw new RuntimeException("exception while reading the store", e);
            }
            if (Files.exists(keyStore) == false) {
                throw new IllegalStateException("Keystore file [" + keyStore + "] does not exist.");
            }
            builder.put(ESRestTestCase.TRUSTSTORE_PATH, keyStore).put(ESRestTestCase.TRUSTSTORE_PASSWORD, "keypass");
        }
        return builder.build();
    }

    @Override
    protected Settings restClientSettings() {
        return securitySettings();
    }

    @Override
    protected String getProtocol() {
        return RestSqlIT.SSL_ENABLED ? "https" : "http";
    }
}
