/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */
package org.elasticsearch.xpack.sql.qa.jdbc.security;

import org.elasticsearch.core.Booleans;
import org.elasticsearch.core.PathUtils;
import org.elasticsearch.common.settings.SecureString;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.util.concurrent.ThreadContext;
import org.elasticsearch.test.rest.ESRestTestCase;
import org.elasticsearch.xpack.sql.qa.jdbc.ConnectionTestCase;

import java.net.URISyntaxException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Properties;

public class JdbcConnectionIT extends ConnectionTestCase {

    static final boolean SSL_ENABLED = Booleans.parseBoolean(System.getProperty("tests.ssl.enabled"), false);

    static Settings securitySettings() {
        String token = basicAuthHeaderValue("test_admin", new SecureString("x-pack-test-password".toCharArray()));
        Settings.Builder builder = Settings.builder().put(ThreadContext.PREFIX + ".Authorization", token);
        if (SSL_ENABLED) {
            Path keyStore;
            try {
                keyStore = PathUtils.get(getTestClass().getResource("/test-node.jks").toURI());
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
        return SSL_ENABLED ? "https" : "http";
    }

    @Override
    protected Properties connectionProperties() {
        Properties properties = super.connectionProperties();
        properties.putAll(JdbcSecurityUtils.adminProperties());
        return properties;
    }
}
