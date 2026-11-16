/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.sql.qa.jdbc.security.with_ssl;

import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.test.cluster.ElasticsearchCluster;
import org.elasticsearch.xpack.sql.qa.jdbc.JdbcSecurityUtils;
import org.elasticsearch.xpack.sql.qa.jdbc.JdbcWarningsTestCase;
import org.junit.ClassRule;

import java.util.Properties;

public class SecurityWithSslJdbcWarningsIT extends JdbcWarningsTestCase {
    @ClassRule
    public static final ElasticsearchCluster cluster = withSecurityCluster(true);

    @Override
    public ElasticsearchCluster getCluster() {
        return cluster;
    }

    @Override
    protected Settings restClientSettings() {
        return securitySettings(true);
    }

    @Override
    protected String getProtocol() {
        return "https";
    }

    @Override
    protected Properties connectionProperties() {
        Properties properties = super.connectionProperties();
        properties.putAll(JdbcSecurityUtils.adminProperties(true));
        return properties;
    }

}
