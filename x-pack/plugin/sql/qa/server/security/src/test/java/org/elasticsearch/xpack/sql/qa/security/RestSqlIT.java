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
import org.elasticsearch.test.cluster.ElasticsearchCluster;
import org.elasticsearch.test.rest.ESRestTestCase;
import org.elasticsearch.xpack.sql.qa.rest.RestSqlTestCase;
import org.junit.ClassRule;

/**
 * Integration test for the rest sql action. The one that speaks json directly to a
 * user rather than to the JDBC driver or CLI.
 */
public class RestSqlIT extends RestSqlTestCase {

    @ClassRule
    public static ElasticsearchCluster cluster = SqlSecurityTestCluster.getCluster();

    @Override
    protected String getTestRestCluster() {
        return cluster.getHttpAddresses();
    }

    static final boolean SSL_ENABLED = SqlSecurityTestCluster.SSL_ENABLED;

    static Settings securitySettings() {
        String token = basicAuthHeaderValue("test_admin", new SecureString("x-pack-test-password".toCharArray()));
        Settings.Builder builder = Settings.builder().put(ThreadContext.PREFIX + ".Authorization", token);
        if (SSL_ENABLED) {
            builder.put(ESRestTestCase.TRUSTSTORE_PATH, SqlSecurityTestCluster.getKeystorePath())
                .put(ESRestTestCase.TRUSTSTORE_PASSWORD, SqlSecurityTestCluster.KEYSTORE_PASSWORD);
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
