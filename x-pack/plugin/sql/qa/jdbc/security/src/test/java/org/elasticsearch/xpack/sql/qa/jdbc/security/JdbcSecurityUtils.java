/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.xpack.sql.qa.jdbc.security;

import org.elasticsearch.common.io.PathUtils;

import java.net.URISyntaxException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Properties;

import static org.apache.lucene.util.LuceneTestCase.getTestClass;

final class JdbcSecurityUtils {

    private JdbcSecurityUtils() {}

    static Properties adminProperties() {
        // tag::admin_properties
        Properties properties = new Properties();
        properties.put("user", "test_admin");
        properties.put("password", "x-pack-test-password");
        // end::admin_properties
        addSslPropertiesIfNeeded(properties);
        return properties;
    }

    private static void addSslPropertiesIfNeeded(Properties properties) {
        if (false == JdbcConnectionIT.SSL_ENABLED) {
            return;
        }
        Path keyStore;
        try {
            keyStore = PathUtils.get(getTestClass().getResource("/test-node.jks").toURI());
        } catch (URISyntaxException e) {
            throw new RuntimeException("exception while reading the store", e);
        }
        if (!Files.exists(keyStore)) {
            throw new IllegalStateException("Keystore file [" + keyStore + "] does not exist.");
        }
        String keyStoreStr = keyStore.toAbsolutePath().toString();

        properties.put("ssl", "true");
        properties.put("ssl.keystore.location", keyStoreStr);
        properties.put("ssl.keystore.pass", "keypass");
        properties.put("ssl.truststore.location", keyStoreStr);
        properties.put("ssl.truststore.pass", "keypass");
    }
}
