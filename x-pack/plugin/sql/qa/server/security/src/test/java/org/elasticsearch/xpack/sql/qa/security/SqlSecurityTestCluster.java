/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */
package org.elasticsearch.xpack.sql.qa.security;

import org.elasticsearch.core.Booleans;
import org.elasticsearch.core.PathUtils;
import org.elasticsearch.test.cluster.ElasticsearchCluster;
import org.elasticsearch.test.cluster.local.LocalClusterSpecBuilder;
import org.elasticsearch.test.cluster.local.distribution.DistributionType;
import org.elasticsearch.test.cluster.util.resource.Resource;

import java.net.URISyntaxException;
import java.net.URL;
import java.nio.file.Path;

/**
 * Unified cluster configuration for SQL security tests.
 * Reads the {@code tests.ssl.enabled} system property to determine whether
 * to configure SSL. When SSL is enabled, the keystore must be available on
 * the classpath (provided by the elasticsearch.test-with-ssl plugin).
 */
public final class SqlSecurityTestCluster {

    public static final boolean SSL_ENABLED = Booleans.parseBoolean(System.getProperty("tests.ssl.enabled"), false);
    public static final String KEYSTORE_PASSWORD = "keypass";

    /**
     * Returns the absolute path to the test-node.jks keystore file.
     * Only valid when {@link #SSL_ENABLED} is true.
     * @throws IllegalStateException if SSL is not enabled or keystore not found
     */
    public static String getKeystorePath() {
        if (SSL_ENABLED == false) {
            throw new IllegalStateException("SSL is not enabled");
        }
        return KEYSTORE_PATH;
    }

    // Lazy-initialized keystore path (only resolved when SSL is enabled)
    private static final String KEYSTORE_PATH = SSL_ENABLED ? resolveKeystorePath() : null;

    private static String resolveKeystorePath() {
        URL keystoreUrl = SqlSecurityTestCluster.class.getResource("/test-node.jks");
        if (keystoreUrl == null) {
            throw new IllegalStateException("SSL is enabled but test-node.jks not found on classpath.");
        }
        try {
            Path keyStore = PathUtils.get(keystoreUrl.toURI());
            return keyStore.toAbsolutePath().toString();
        } catch (URISyntaxException e) {
            throw new RuntimeException("Failed to get keystore path", e);
        }
    }

    private SqlSecurityTestCluster() {}

    public static ElasticsearchCluster getCluster() {
        return CLUSTER;
    }

    private static final ElasticsearchCluster CLUSTER = createCluster();

    private static ElasticsearchCluster createCluster() {
        LocalClusterSpecBuilder<ElasticsearchCluster> builder = ElasticsearchCluster.local()
            .name("javaRestTest")
            .distribution(DistributionType.DEFAULT)
            // Setup auditing so we can use it in some tests
            .setting("xpack.security.audit.enabled", "true")
            .setting("xpack.security.enabled", "true")
            .setting("xpack.license.self_generated.type", "trial")
            // skip automatically creating the "elastic" user (and the associated .security index)
            .setting("xpack.security.autoconfiguration.enabled", "false")
            // Disable unused features
            .setting("xpack.ml.enabled", "false")
            .setting("xpack.watcher.enabled", "false")
            // Setup roles used by tests
            .rolesFile(Resource.fromClasspath("roles.yml"))
            // Setup the one admin user that we run the tests as.
            // Tests use "run as" to get different users.
            .user("test_admin", "x-pack-test-password")
            .user("user1", "x-pack-test-password", "user1", false)
            .user("user2", "x-pack-test-password", "user2", false)
            .user("manage_user", "x-pack-test-password", "manage_user", false);

        if (SSL_ENABLED) {
            configureSsl(builder);
        }

        return builder.build();
    }

    private static void configureSsl(LocalClusterSpecBuilder<ElasticsearchCluster> builder) {
        builder
            // SSL settings - keystore is provided by test-with-ssl plugin on classpath
            .setting("xpack.security.http.ssl.enabled", "true")
            .setting("xpack.security.transport.ssl.enabled", "true")
            .setting("xpack.security.transport.ssl.keystore.path", "test-node.jks")
            .setting("xpack.security.http.ssl.keystore.path", "test-node.jks")
            .keystore("xpack.security.transport.ssl.keystore.secure_password", "keypass")
            .keystore("xpack.security.http.ssl.keystore.secure_password", "keypass")
            // Copy keystore into config directory from classpath
            .configFile("test-node.jks", Resource.fromClasspath("test-node.jks"));
    }
}
